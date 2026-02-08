"""
VTagger Tag Discovery Service.

Discovers and caches physical tag keys from Umbrella assets.
"""

import json
from typing import Dict, List

from app.database import execute_query, execute_write


class TagDiscoveryService:
    """Discovers and caches physical tag keys from Umbrella assets."""

    def discover_tags(self, resources: List[Dict]):
        """Extract unique tag keys and sample values from a batch of resources."""
        tag_data = {}

        for resource in resources:
            # From customTags array
            custom_tags = resource.get("customTags", [])
            for tag in custom_tags:
                if isinstance(tag, dict):
                    key = tag.get("key", "")
                    value = tag.get("value", "")
                    if key and value and value != "no tag":
                        if key not in tag_data:
                            tag_data[key] = set()
                        tag_data[key].add(value)

            # From Tag: prefix columns
            for col_key, col_value in resource.items():
                if col_key.startswith("Tag: ") and col_value and col_value != "no tag":
                    tag_name = col_key[5:]
                    if tag_name not in tag_data:
                        tag_data[tag_name] = set()
                    tag_data[tag_name].add(col_value)

        # Update database
        for tag_key, values in tag_data.items():
            sample_values = list(values)[:10]

            existing = execute_query(
                "SELECT id, sample_values, occurrence_count FROM discovered_tags WHERE tag_key = ?",
                (tag_key,)
            )

            if existing:
                row = existing[0]
                try:
                    existing_samples = json.loads(row["sample_values"] or "[]")
                except (json.JSONDecodeError, TypeError):
                    existing_samples = []

                # Merge samples (keep up to 10)
                merged = list(set(existing_samples + sample_values))[:10]
                new_count = (row["occurrence_count"] or 0) + len(values)

                execute_write(
                    "UPDATE discovered_tags SET sample_values = ?, last_seen_at = CURRENT_TIMESTAMP, "
                    "occurrence_count = ? WHERE tag_key = ?",
                    (json.dumps(merged), new_count, tag_key)
                )
            else:
                execute_write(
                    "INSERT INTO discovered_tags (tag_key, sample_values, occurrence_count) "
                    "VALUES (?, ?, ?)",
                    (tag_key, json.dumps(sample_values), len(values))
                )

    def get_discovered_tags(self) -> List[Dict]:
        """Return all discovered tag keys with sample values."""
        rows = execute_query(
            "SELECT tag_key, sample_values, first_seen_at, last_seen_at, occurrence_count "
            "FROM discovered_tags ORDER BY occurrence_count DESC"
        )
        results = []
        for row in rows:
            try:
                samples = json.loads(row["sample_values"] or "[]")
            except (json.JSONDecodeError, TypeError):
                samples = []
            results.append({
                "tag_key": row["tag_key"],
                "sample_values": samples,
                "first_seen_at": row["first_seen_at"],
                "last_seen_at": row["last_seen_at"],
                "occurrence_count": row["occurrence_count"],
            })
        return results

    def clear_discovered_tags(self):
        """Clear the discovered_tags table."""
        execute_write("DELETE FROM discovered_tags")


# Global instance
tag_discovery_service = TagDiscoveryService()
