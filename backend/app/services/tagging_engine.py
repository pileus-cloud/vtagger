"""
VTagger Tagging Engine.

Core engine that fetches assets from Umbrella, applies dimension mappings,
and produces tagged output. Supports streaming pagination, progress tracking,
JSONL output, reservoir sampling, and cancellation.

Ported from BPVtagger with generic dimension handling replacing BP-specific logic.
"""

import csv
import io
import json
import os
import random
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple

from app.config import settings
from app.services.agent_logger import log_timing


class MappingStats:
    """Tracks mapping statistics during a tagging run."""

    def __init__(self):
        self.total_assets = 0
        self.matched_assets = 0
        self.unmatched_assets = 0
        self.dimension_count = 0
        self.dimension_details: Dict[str, int] = {}
        self.error_count = 0
        self.errors: List[str] = []

    def record_match(self, dimension_name: str):
        """Record a successful dimension match."""
        self.dimension_count += 1
        self.dimension_details[dimension_name] = (
            self.dimension_details.get(dimension_name, 0) + 1
        )

    def record_error(self, error_msg: str):
        """Record an error."""
        self.error_count += 1
        if len(self.errors) < 100:
            self.errors.append(error_msg)

    def to_dict(self) -> dict:
        """Convert stats to dictionary."""
        match_rate = 0.0
        if self.total_assets > 0:
            match_rate = (self.matched_assets / self.total_assets) * 100

        return {
            "total_assets": self.total_assets,
            "matched_assets": self.matched_assets,
            "unmatched_assets": self.unmatched_assets,
            "dimension_matches": self.dimension_count,
            "dimension_details": self.dimension_details,
            "match_rate": round(match_rate, 2),
            "error_count": self.error_count,
            "errors": self.errors[:10],
        }


@dataclass
class TaggingProgress:
    """Tracks progress of a tagging run."""

    status: str = "idle"
    phase: str = ""
    total_pages: int = 0
    current_page: int = 0
    total_assets: int = 0
    processed_assets: int = 0
    matched_assets: int = 0
    unmatched_assets: int = 0
    dimension_matches: int = 0
    start_time: float = 0.0
    elapsed_seconds: float = 0.0
    estimated_remaining: float = 0.0
    error_message: str = ""
    sample_records: List[Dict] = field(default_factory=list)
    cancelled: bool = False

    def to_dict(self) -> dict:
        """Convert progress to dictionary for SSE streaming."""
        pct = 0.0
        if self.total_assets > 0:
            pct = (self.processed_assets / self.total_assets) * 100

        return {
            "status": self.status,
            "phase": self.phase,
            "total_pages": self.total_pages,
            "current_page": self.current_page,
            "total_assets": self.total_assets,
            "processed_assets": self.processed_assets,
            "matched_assets": self.matched_assets,
            "unmatched_assets": self.unmatched_assets,
            "dimension_matches": self.dimension_matches,
            "progress_pct": round(pct, 1),
            "elapsed_seconds": round(self.elapsed_seconds, 1),
            "estimated_remaining": round(self.estimated_remaining, 1),
            "error_message": self.error_message,
            "sample_count": len(self.sample_records),
        }


class TaggingEngine:
    """Core tagging engine that fetches, maps, and outputs tagged assets."""

    # Reservoir sampling size
    SAMPLE_SIZE = 50

    def __init__(self):
        self.progress = TaggingProgress()
        self._cancel_requested = False

    def cancel(self):
        """Request cancellation of the current run."""
        self._cancel_requested = True
        self.progress.cancelled = True
        self.progress.status = "cancelling"
        log_timing("Cancellation requested")

    def is_cancelled(self) -> bool:
        """Check if cancellation has been requested."""
        return self._cancel_requested

    def reset(self):
        """Reset engine state for a new run."""
        self.progress = TaggingProgress()
        self._cancel_requested = False

    def fetch_and_map(
        self,
        umbrella_client,
        mapping_engine,
        account_key: str,
        start_date: str,
        end_date: str,
        output_dir: str,
        progress_callback: Optional[Callable] = None,
        vtag_filter_dimensions: Optional[List[str]] = None,
    ) -> Tuple[str, MappingStats]:
        """
        Fetch assets from Umbrella, apply dimension mappings, and write JSONL output.

        Uses streaming pagination to process large datasets efficiently.

        Args:
            umbrella_client: UmbrellaClient instance.
            mapping_engine: MappingEngine with loaded dimension mappings.
            account_key: Umbrella account key.
            start_date: Start date (YYYY-MM-DD).
            end_date: End date (YYYY-MM-DD).
            output_dir: Directory for output files.
            progress_callback: Optional callback for progress updates.
            vtag_filter_dimensions: Optional list of dimension names to filter.

        Returns:
            Tuple of (output_file_path, mapping_stats).
        """
        self.reset()
        self.progress.status = "running"
        self.progress.phase = "initializing"
        self.progress.start_time = time.time()

        stats = MappingStats()
        sample_reservoir: List[Dict] = []

        # Ensure output directory exists
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        # Build output filename
        date_suffix = f"{start_date}_to_{end_date}".replace("-", "")
        output_file = os.path.join(output_dir, f"tagged_{date_suffix}.jsonl")
        csv_file = os.path.join(output_dir, f"tagged_{date_suffix}.csv")

        log_timing(f"Starting tagging: {start_date} to {end_date}, account={account_key}")

        # Get required tag keys from mapping engine
        tag_keys = mapping_engine.get_required_tag_keys()
        log_timing(f"Required tag keys: {tag_keys}")

        # Determine which dimensions to process
        if vtag_filter_dimensions:
            active_dims = [
                d for d in mapping_engine._sorted_dimensions
                if d.vtag_name in vtag_filter_dimensions
            ]
        else:
            active_dims = list(mapping_engine._sorted_dimensions)

        try:
            self.progress.phase = "fetching"

            # Open JSONL output file
            with open(output_file, "w") as jsonl_out:
                page_num = 0

                # Stream assets from Umbrella with pagination
                for page_assets in umbrella_client.fetch_assets_stream(
                    account_key=account_key,
                    start_date=start_date,
                    end_date=end_date,
                    tag_keys=tag_keys,
                    vtag_filter_dimensions=vtag_filter_dimensions,
                ):
                    if self._cancel_requested:
                        log_timing("Cancelled during fetch")
                        self.progress.status = "cancelled"
                        break

                    page_num += 1
                    self.progress.current_page = page_num
                    self.progress.phase = f"processing page {page_num}"

                    log_timing(
                        f"Processing page {page_num}: "
                        f"{len(page_assets)} assets"
                    )

                    for asset in page_assets:
                        if self._cancel_requested:
                            break

                        stats.total_assets += 1
                        self.progress.processed_assets += 1

                        # Use mapping_engine.map_resource for full mapping
                        mapped = mapping_engine.map_resource(asset)
                        all_dimensions = mapped.get("dimensions", {})

                        # Filter to active dimensions if needed
                        if vtag_filter_dimensions:
                            dimensions = {
                                k: v for k, v in all_dimensions.items()
                                if k in vtag_filter_dimensions
                            }
                        else:
                            dimensions = all_dimensions

                        # Check for matches per dimension
                        any_match = False
                        for dim in active_dims:
                            dim_value = dimensions.get(dim.vtag_name, dim.default_value)
                            if dim_value != dim.default_value:
                                any_match = True
                                stats.record_match(dim.vtag_name)

                        if any_match:
                            stats.matched_assets += 1
                            self.progress.matched_assets += 1
                        else:
                            stats.unmatched_assets += 1
                            self.progress.unmatched_assets += 1

                        self.progress.dimension_matches = stats.dimension_count

                        # Build output record
                        record = {
                            "resourceid": asset.get("resourceid", asset.get("resourceId", "")),
                            "linkedaccid": asset.get(
                                "linkedaccid",
                                asset.get("linkedAccountId", asset.get("linkedaccountid", "")),
                            ),
                            "payeraccount": asset.get(
                                "payeraccount",
                                asset.get("payerAccountId", asset.get("payerAccount", "")),
                            ),
                            "dimensions": dimensions,
                            "tags": _extract_tags(asset),
                        }

                        # Write JSONL line
                        jsonl_out.write(json.dumps(record) + "\n")

                        # Reservoir sampling for preview
                        if len(sample_reservoir) < self.SAMPLE_SIZE:
                            sample_reservoir.append(record)
                        else:
                            idx = random.randint(0, stats.total_assets - 1)
                            if idx < self.SAMPLE_SIZE:
                                sample_reservoir[idx] = record

                    # Update elapsed time
                    self.progress.elapsed_seconds = time.time() - self.progress.start_time

                    # Estimate remaining time
                    if self.progress.processed_assets > 0 and self.progress.total_assets > 0:
                        rate = self.progress.processed_assets / self.progress.elapsed_seconds
                        remaining = self.progress.total_assets - self.progress.processed_assets
                        self.progress.estimated_remaining = remaining / rate if rate > 0 else 0

                    # Update total_assets estimate from processed count
                    self.progress.total_assets = max(
                        self.progress.total_assets, self.progress.processed_assets
                    )

                    # Fire progress callback
                    if progress_callback:
                        progress_callback(self.progress)

            # Generate CSV from JSONL
            if not self._cancel_requested and os.path.exists(output_file):
                self.progress.phase = "generating CSV"
                self._process_and_generate_csv(
                    output_file, csv_file, mapping_engine, vtag_filter_dimensions
                )

            self.progress.sample_records = sample_reservoir
            self.progress.elapsed_seconds = time.time() - self.progress.start_time

            if self._cancel_requested:
                self.progress.status = "cancelled"
                log_timing("Tagging cancelled")
            else:
                self.progress.status = "completed"
                self.progress.phase = "done"
                log_timing(
                    f"Tagging complete: {stats.total_assets} assets, "
                    f"{stats.matched_assets} matched, "
                    f"{stats.dimension_count} dimension matches"
                )

            return output_file, stats

        except Exception as e:
            self.progress.status = "error"
            self.progress.error_message = str(e)
            stats.record_error(str(e))
            log_timing(f"Tagging error: {e}")
            raise

    def _process_and_generate_csv(
        self,
        jsonl_file: str,
        csv_file: str,
        mapping_engine,
        vtag_filter_dimensions: Optional[List[str]] = None,
    ):
        """
        Generate CSV from JSONL output with dynamic dimension columns.

        CSV format: resourceid,linkedaccid,payeraccount,vtags:<dim1>,vtags:<dim2>,...
        Columns derived from mapping_engine._sorted_dimensions (Dimension objects).
        """
        # Get sorted dimension names for column headers
        sorted_dims = [d.vtag_name for d in mapping_engine._sorted_dimensions]
        if vtag_filter_dimensions:
            sorted_dims = [d for d in sorted_dims if d in vtag_filter_dimensions]

        # Build CSV headers
        headers = ["resourceid", "linkedaccid", "payeraccount"]
        for dim_name in sorted_dims:
            headers.append(f"vtags:{dim_name}")

        log_timing(f"Generating CSV with {len(sorted_dims)} dimension columns")

        with open(csv_file, "w", newline="") as csvout:
            writer = csv.writer(csvout)
            writer.writerow(headers)

            with open(jsonl_file, "r") as jsonl_in:
                for line in jsonl_in:
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    row = [
                        record.get("resourceid", ""),
                        record.get("linkedaccid", ""),
                        record.get("payeraccount", ""),
                    ]

                    dims = record.get("dimensions", {})
                    for dim_name in sorted_dims:
                        row.append(dims.get(dim_name, "Unallocated"))

                    writer.writerow(row)

        log_timing(f"CSV generated: {csv_file}")

    def run_sync(
        self,
        umbrella_client,
        mapping_engine,
        account_key: str,
        start_date: str,
        end_date: str,
        progress_callback: Optional[Callable] = None,
        vtag_filter_dimensions: Optional[List[str]] = None,
    ) -> Tuple[str, MappingStats]:
        """
        Run a tagging sync for a date range.

        Args:
            umbrella_client: Client for Umbrella API.
            mapping_engine: Mapping engine with loaded dimensions.
            account_key: Umbrella account key.
            start_date: Start date (YYYY-MM-DD).
            end_date: End date (YYYY-MM-DD).
            progress_callback: Optional progress callback.
            vtag_filter_dimensions: Optional dimension filter.

        Returns:
            Tuple of (output_file_path, mapping_stats).
        """
        output_dir = settings.output_dir
        return self.fetch_and_map(
            umbrella_client=umbrella_client,
            mapping_engine=mapping_engine,
            account_key=account_key,
            start_date=start_date,
            end_date=end_date,
            output_dir=output_dir,
            progress_callback=progress_callback,
            vtag_filter_dimensions=vtag_filter_dimensions,
        )

    def get_progress(self) -> dict:
        """Get current progress as a dictionary."""
        return self.progress.to_dict()


def _extract_tags(asset: Dict) -> Dict[str, str]:
    """Extract tag key-value pairs from an asset record."""
    tags = {}

    # From customTags array
    custom_tags = asset.get("customTags", [])
    if isinstance(custom_tags, list):
        for tag in custom_tags:
            if isinstance(tag, dict):
                key = tag.get("key", "")
                value = tag.get("value", "")
                if key and value and value != "no tag":
                    tags[key] = value

    # From Tag: prefix columns
    for col_key, col_value in asset.items():
        if col_key.startswith("Tag: ") and col_value and col_value != "no tag":
            tag_name = col_key[5:]
            tags[tag_name] = col_value

    return tags


# Global instance
tagging_engine = TaggingEngine()
