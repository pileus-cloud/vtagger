"""
VTagger Mapping Engine.

Generic dimension mapping engine with user-defined dimensions.
Replaces BPVtagger's hardcoded BizMapping chain with dynamic ordered dimensions.
"""

import hashlib
import json
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from app.database import execute_query, execute_write
from app.core.dsl_parser import build_indexes, parse_value_expression, extract_tag_keys
from app.services.agent_logger import log_timing


class Dimension:
    """Single dimension with pre-parsed indexes."""

    def __init__(self, vtag_name: str, index: int, kind: str,
                 default_value: str, statements: List[Dict]):
        self.vtag_name = vtag_name
        self.index = index
        self.kind = kind
        self.default_value = default_value
        self.statements = statements
        self.indexes = build_indexes(statements)

    def match(self, tag_context: Dict[str, str],
              dimension_context: Dict[str, str]) -> str:
        """Evaluate this dimension against contexts.

        Fast-path logic:
        1. TAG exact lookup (hash)
        2. DIMENSION exact lookup (hash)
        3. TAG CONTAINS (substring scan)
        4. DIMENSION CONTAINS (substring scan)
        5. Return default_value if no match
        """
        idx = self.indexes

        # Fast path 1: TAG exact match
        for key, value in tag_context.items():
            if value:
                val_lower = str(value).lower()
                result = idx["tag_exact"].get((key, val_lower))
                if result:
                    return result

        # Fast path 2: DIMENSION exact match
        for key, value in dimension_context.items():
            if value:
                val_lower = str(value).lower()
                result = idx["dim_exact"].get((key, val_lower))
                if result:
                    return result

        # Fast path 3: TAG CONTAINS (substring)
        for key, value in tag_context.items():
            if value:
                val_lower = str(value).lower()
                for ckey, cvalue, result in idx["tag_contains"]:
                    if ckey == key and cvalue in val_lower:
                        return result

        # Fast path 4: DIMENSION CONTAINS (substring)
        for key, value in dimension_context.items():
            if value:
                val_lower = str(value).lower()
                for ckey, cvalue, result in idx["dim_contains"]:
                    if ckey == key and cvalue in val_lower:
                        return result

        return self.default_value


class MappingEngine:
    """Generic dimension mapping engine."""

    def __init__(self):
        self.dimensions: Dict[str, Dimension] = {}
        self._sorted_dimensions: List[Dimension] = []
        self._loaded = False
        self._required_tag_keys: Set[str] = set()

    # Timing tracking
    _map_call_count = 0
    _timing_totals = {"load": 0, "tags": 0, "match": 0}

    @classmethod
    def reset_timing(cls):
        """Reset timing counters."""
        cls._map_call_count = 0
        cls._timing_totals = {"load": 0, "tags": 0, "match": 0}

    def load_dimensions(self):
        """Load all dimensions from database, sorted by index_number."""
        self.dimensions.clear()
        self._sorted_dimensions.clear()
        self._required_tag_keys.clear()

        rows = execute_query(
            "SELECT * FROM dimensions ORDER BY index_number"
        )

        for row in rows:
            try:
                content = json.loads(row["content"]) if row["content"] else {}
                statements = content.get("statements", []) if isinstance(content, dict) else []

                dim = Dimension(
                    vtag_name=row["vtag_name"],
                    index=row["index_number"],
                    kind=row["kind"],
                    default_value=row["default_value"],
                    statements=statements
                )
                self.dimensions[dim.vtag_name] = dim
                self._sorted_dimensions.append(dim)

                # Collect required tag keys
                for key in dim.indexes["tag_keys_used"]:
                    self._required_tag_keys.add(key)

            except Exception as e:
                print(f"Error loading dimension {row.get('vtag_name', '?')}: {e}")

        self._loaded = True
        print(f"[INFO] Loaded {len(self.dimensions)} dimensions, {len(self._required_tag_keys)} tag keys")

    def get_required_tag_keys(self) -> Set[str]:
        """Return set of tag keys needed from Umbrella API."""
        if not self._loaded:
            self.load_dimensions()
        return self._required_tag_keys

    def map_resource(self, resource: Dict) -> Dict:
        """Map a single resource through all dimensions.

        1. Extract tag values from resource
        2. Initialize dimension_context = {}
        3. For each dimension (sorted by index):
            a. result = dimension.match(tag_context, dimension_context)
            b. dimension_context[dimension.vtag_name] = result
        4. Return result dict
        """
        t_start = time.time()
        MappingEngine._map_call_count += 1

        if not self._loaded:
            self.load_dimensions()

        # Pad AWS account IDs
        def pad_aws_account_id(account_id: str) -> str:
            if account_id and account_id.isdigit() and len(account_id) < 12:
                return account_id.zfill(12)
            return account_id

        linked_account = pad_aws_account_id(resource.get("linkedaccid", "") or resource.get("payeraccount", ""))
        payer_account = pad_aws_account_id(resource.get("payeraccount", "") or resource.get("linkedaccid", ""))

        # Extract tags from resource
        tags = {}

        # Method 1: customTags array
        custom_tags = resource.get("customTags", [])
        for tag in custom_tags:
            if isinstance(tag, dict):
                tags[tag.get("key", "")] = tag.get("value", "")

        # Method 2: customTagValue_N columns (dynamic - no hardcoded indices)
        for key, value in resource.items():
            if key.startswith("customTagValue_") and value and value != "no tag":
                # Map back to tag names using column index
                tags[key] = value
            elif key.startswith("Tag: ") and value and value != "no tag":
                tag_name = key[5:]
                tags[tag_name] = value

        # Build tag context
        tag_context = dict(tags)
        dimension_context = {}

        # Track mapping source
        mapping_source = "unallocated"
        dimension_results = {}
        dimension_sources = {}

        # Process each dimension in order
        for dim in self._sorted_dimensions:
            result = dim.match(tag_context, dimension_context)
            dimension_context[dim.vtag_name] = result
            dimension_results[dim.vtag_name] = result

            if result != dim.default_value:
                dimension_sources[dim.vtag_name] = "matched"
                mapping_source = f"dimension:{dim.vtag_name}"
            else:
                dimension_sources[dim.vtag_name] = "default"

        # Check if any dimension matched
        has_match = any(
            dimension_results.get(d.vtag_name) != d.default_value
            for d in self._sorted_dimensions
        )
        if not has_match:
            mapping_source = "unallocated"

        return {
            "resource_id": resource.get("resourceid", ""),
            "account_id": linked_account or resource.get("payeraccount", ""),
            "payer_account": payer_account,
            "mapping_source": mapping_source,
            "dimensions": dimension_results,
            "dimension_sources": dimension_sources,
            "tags_extracted": tags,
        }

    def resolve_tags(self, tags: Dict[str, str]) -> Dict[str, str]:
        """Resolve a set of tags through all dimensions."""
        if not self._loaded:
            self.load_dimensions()

        tag_context = {k: v for k, v in tags.items()}
        dimension_context = {}
        results = {}

        for dim in self._sorted_dimensions:
            result = dim.match(tag_context, dimension_context)
            dimension_context[dim.vtag_name] = result
            results[dim.vtag_name] = result

        return results

    def get_dimensions_metadata(self) -> List[Dict]:
        """Return metadata for all loaded dimensions."""
        return execute_query(
            "SELECT id, vtag_name, index_number, kind, default_value, "
            "statement_count, checksum, created_at, updated_at "
            "FROM dimensions ORDER BY index_number"
        )


# Global instance
mapping_engine = MappingEngine()
