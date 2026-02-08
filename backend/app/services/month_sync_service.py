"""
VTagger Month Sync Service.

Dedicated service for month-level sync operations.
Provides higher-level coordination for syncing entire months,
with progress tracking and status management.

Ported from BPVtagger with vtag_filter_dimensions support.
"""

import time
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

from app.config import settings
from app.database import execute_query, execute_write, get_db
from app.services.agent_logger import log_timing
from app.services.sync_service import SyncService, _get_month_ranges, _get_week_ranges


class MonthSyncService:
    """
    Manages month-level sync operations.

    Coordinates weekly syncs within a month, tracks overall progress,
    and manages the month_syncs / month_sync_weeks database records.
    """

    def __init__(self):
        self._sync_service = SyncService()
        self._current_sync_id: Optional[int] = None
        self._cancelled = False

    def cancel(self):
        """Cancel the current month sync."""
        self._cancelled = True
        self._sync_service.cancel()
        log_timing("Month sync cancellation requested")

    def is_cancelled(self) -> bool:
        """Check if cancellation was requested."""
        return self._cancelled

    def get_progress(self) -> dict:
        """Get progress of the current month sync."""
        base_progress = self._sync_service.get_progress()

        if self._current_sync_id:
            # Augment with month-level info
            rows = execute_query(
                """SELECT status, total_weeks, completed_weeks,
                   total_statements, fetched_statements
                   FROM month_syncs WHERE id = ?""",
                (self._current_sync_id,),
            )
            if rows:
                row = rows[0]
                base_progress["month_sync"] = {
                    "sync_id": self._current_sync_id,
                    "status": row["status"],
                    "total_weeks": row["total_weeks"],
                    "completed_weeks": row["completed_weeks"],
                    "total_statements": row["total_statements"],
                    "fetched_statements": row["fetched_statements"],
                }

        return base_progress

    def run_month_sync(
        self,
        umbrella_client,
        mapping_engine,
        account_key: str,
        month: str,
        progress_callback: Optional[Callable] = None,
        vtag_filter_dimensions: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Run a full month sync.

        Delegates to SyncService.run_month_sync with vtag_filter_dimensions.

        Args:
            umbrella_client: Client for Umbrella API.
            mapping_engine: Mapping engine with loaded dimensions.
            account_key: Umbrella account key.
            month: Month string (YYYY-MM).
            progress_callback: Optional progress callback.
            vtag_filter_dimensions: Optional dimension filter list.

        Returns:
            Dict with month sync results.
        """
        self._cancelled = False
        log_timing(f"MonthSyncService: starting month sync for {month}")

        try:
            result = self._sync_service.run_month_sync(
                umbrella_client=umbrella_client,
                mapping_engine=mapping_engine,
                account_key=account_key,
                month=month,
                progress_callback=progress_callback,
                vtag_filter_dimensions=vtag_filter_dimensions,
            )

            self._current_sync_id = result.get("sync_id")
            return result

        except Exception as e:
            log_timing(f"MonthSyncService error: {e}")
            raise

    def run_multi_month_sync(
        self,
        umbrella_client,
        mapping_engine,
        account_key: str,
        months: List[str],
        progress_callback: Optional[Callable] = None,
        vtag_filter_dimensions: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Run sync for multiple months sequentially.

        Args:
            umbrella_client: Client for Umbrella API.
            mapping_engine: Mapping engine with loaded dimensions.
            account_key: Umbrella account key.
            months: List of month strings (YYYY-MM).
            progress_callback: Optional progress callback.
            vtag_filter_dimensions: Optional dimension filter list.

        Returns:
            Dict with aggregated results across all months.
        """
        self._cancelled = False
        log_timing(f"MonthSyncService: starting multi-month sync for {months}")

        total_stats = {
            "total_assets": 0,
            "matched_assets": 0,
            "unmatched_assets": 0,
            "dimension_matches": 0,
        }
        month_results = []
        completed_months = 0

        for month in months:
            if self._cancelled:
                log_timing(f"Multi-month sync cancelled at {month}")
                break

            try:
                result = self.run_month_sync(
                    umbrella_client=umbrella_client,
                    mapping_engine=mapping_engine,
                    account_key=account_key,
                    month=month,
                    progress_callback=progress_callback,
                    vtag_filter_dimensions=vtag_filter_dimensions,
                )

                month_stats = result.get("stats", {})
                total_stats["total_assets"] += month_stats.get("total_assets", 0)
                total_stats["matched_assets"] += month_stats.get("matched_assets", 0)
                total_stats["unmatched_assets"] += month_stats.get("unmatched_assets", 0)
                total_stats["dimension_matches"] += month_stats.get("dimension_matches", 0)

                completed_months += 1
                month_results.append(result)

            except Exception as e:
                log_timing(f"Month {month} sync error: {e}")
                month_results.append({
                    "month": month,
                    "status": "error",
                    "error": str(e),
                })

        final_status = "completed"
        if self._cancelled:
            final_status = "cancelled"
        elif completed_months < len(months):
            final_status = "partial"

        return {
            "status": final_status,
            "total_months": len(months),
            "completed_months": completed_months,
            "stats": total_stats,
            "month_results": month_results,
        }

    def get_month_sync_status(self, sync_id: int) -> Optional[Dict[str, Any]]:
        """
        Get the status of a month sync by ID.

        Returns:
            Dict with sync status and week details, or None if not found.
        """
        rows = execute_query(
            "SELECT * FROM month_syncs WHERE id = ?", (sync_id,)
        )
        if not rows:
            return None

        sync = dict(rows[0])

        # Get week details
        weeks = execute_query(
            """SELECT * FROM month_sync_weeks
            WHERE sync_id = ?
            ORDER BY week_start""",
            (sync_id,),
        )
        sync["weeks"] = [dict(w) for w in weeks]

        return sync

    def list_month_syncs(
        self, limit: int = 20, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        List recent month syncs.

        Returns:
            List of month sync records.
        """
        rows = execute_query(
            """SELECT * FROM month_syncs
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?""",
            (limit, offset),
        )
        return [dict(r) for r in rows]


# Global instance
month_sync_service = MonthSyncService()
