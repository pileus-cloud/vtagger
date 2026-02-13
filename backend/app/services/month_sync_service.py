"""
VTagger Month Sync Service.

Dedicated service for month-level sync operations.
Delegates to SyncService.run_month_sync for the actual work.
"""

from typing import Any, Callable, Dict, List, Optional

from app.database import execute_query
from app.services.agent_logger import log_timing
from app.services.sync_service import SyncService


class MonthSyncService:
    """
    Manages month-level sync operations.

    Delegates to SyncService for the actual fetch-map-upload flow.
    """

    def __init__(self):
        self._sync_service = SyncService()
        self._cancelled = False

    def cancel(self):
        """Cancel the current month sync."""
        self._cancelled = True
        self._sync_service.cancel()
        log_timing("Month sync cancellation requested")

    def get_progress(self) -> dict:
        """Get progress of the current month sync."""
        return self._sync_service.get_progress()

    def run_month_sync(
        self,
        umbrella_client,
        mapping_engine,
        account_key: str = "0",
        month: str = "",
        progress_callback: Optional[Callable] = None,
        vtag_filter_dimensions: Optional[List[str]] = None,
        filter_mode: str = "not_vtagged",
        account_keys: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Run a full month sync.

        Delegates to SyncService.run_month_sync which parses the month
        string into date range and runs fetch-map-upload.
        """
        self._cancelled = False
        log_timing(f"MonthSyncService: starting month sync for {month}")

        try:
            result = self._sync_service.run_month_sync(
                umbrella_client=umbrella_client,
                mapping_engine=mapping_engine,
                account_key=account_key,
                account_keys=account_keys,
                month=month,
                vtag_filter_dimensions=vtag_filter_dimensions,
                filter_mode=filter_mode,
            )
            return result

        except Exception as e:
            log_timing(f"MonthSyncService error: {e}")
            raise

    def get_month_sync_status(self, sync_id: int) -> Optional[Dict[str, Any]]:
        """Get the status of a month sync by ID."""
        rows = execute_query(
            "SELECT * FROM month_syncs WHERE id = ?", (sync_id,)
        )
        if not rows:
            return None

        sync = dict(rows[0])

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
        """List recent month syncs."""
        rows = execute_query(
            """SELECT * FROM month_syncs
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?""",
            (limit, offset),
        )
        return [dict(r) for r in rows]


# Global instance
month_sync_service = MonthSyncService()
