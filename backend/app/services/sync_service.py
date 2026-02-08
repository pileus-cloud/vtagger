"""
VTagger Sync Service.

Manages weekly and range-based sync operations.
Handles breaking date ranges into weekly chunks, running tagging for each,
and aggregating results with daily statistics.

Ported from BPVtagger with generic dimension handling.
"""

import json
import time
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

from app.config import settings
from app.database import execute_query, execute_write, get_db
from app.services.agent_logger import log_timing
from app.services.tagging_engine import TaggingEngine, MappingStats


def _parse_date(date_str: str) -> datetime:
    """Parse a date string (YYYY-MM-DD) to datetime."""
    return datetime.strptime(date_str, "%Y-%m-%d")


def _format_date(dt: datetime) -> str:
    """Format a datetime to YYYY-MM-DD string."""
    return dt.strftime("%Y-%m-%d")


def _get_week_ranges(start_date: str, end_date: str) -> List[Tuple[str, str]]:
    """
    Break a date range into weekly chunks (Mon-Sun).

    Returns list of (week_start, week_end) tuples.
    """
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    ranges = []

    current = start
    while current <= end:
        # End of this week (Sunday) or end_date, whichever is sooner
        week_end = current + timedelta(days=(6 - current.weekday()))
        if week_end > end:
            week_end = end

        ranges.append((_format_date(current), _format_date(week_end)))

        # Move to next Monday
        current = week_end + timedelta(days=1)

    return ranges


def _get_month_ranges(month: str) -> Tuple[str, str]:
    """
    Get the start and end date for a given month (YYYY-MM).

    Returns (first_day, last_day) tuple.
    """
    dt = datetime.strptime(month, "%Y-%m")
    first_day = dt.replace(day=1)

    # Last day of month
    if dt.month == 12:
        last_day = dt.replace(year=dt.year + 1, month=1, day=1) - timedelta(days=1)
    else:
        last_day = dt.replace(month=dt.month + 1, day=1) - timedelta(days=1)

    return _format_date(first_day), _format_date(last_day)


class SyncService:
    """Manages sync operations for tagging runs."""

    def __init__(self):
        self._current_engine: Optional[TaggingEngine] = None
        self._current_job_id: Optional[int] = None

    def cancel(self):
        """Cancel the current sync operation."""
        if self._current_engine:
            self._current_engine.cancel()

    def get_progress(self) -> dict:
        """Get progress of the current sync operation."""
        if self._current_engine:
            return self._current_engine.get_progress()
        return {"status": "idle"}

    def run_week_sync(
        self,
        umbrella_client,
        mapping_engine,
        account_key: str,
        start_date: str,
        end_date: str,
        progress_callback: Optional[Callable] = None,
        vtag_filter_dimensions: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Run a sync for a single week range.

        Args:
            umbrella_client: Client for Umbrella API.
            mapping_engine: Mapping engine with loaded dimensions.
            account_key: Umbrella account key.
            start_date: Week start date (YYYY-MM-DD).
            end_date: Week end date (YYYY-MM-DD).
            progress_callback: Optional progress callback.
            vtag_filter_dimensions: Optional dimension filter list.

        Returns:
            Dict with sync results.
        """
        log_timing(f"Starting week sync: {start_date} to {end_date}")

        # Create job record
        job_id = execute_write(
            """INSERT INTO tagging_jobs
            (job_date, status, started_at, created_at)
            VALUES (?, 'running', ?, CURRENT_TIMESTAMP)""",
            (start_date, datetime.now().isoformat()),
        )
        self._current_job_id = job_id

        engine = TaggingEngine()
        self._current_engine = engine

        try:
            output_file, stats = engine.fetch_and_map(
                umbrella_client=umbrella_client,
                mapping_engine=mapping_engine,
                account_key=account_key,
                start_date=start_date,
                end_date=end_date,
                output_dir=settings.output_dir,
                progress_callback=progress_callback,
                vtag_filter_dimensions=vtag_filter_dimensions,
            )

            stats_dict = stats.to_dict()

            # Update job record
            status = "completed" if not engine.is_cancelled() else "cancelled"
            execute_write(
                """UPDATE tagging_jobs SET
                status = ?,
                total_statements = ?,
                processed_statements = ?,
                matched_statements = ?,
                unmatched_statements = ?,
                dimensions_applied = ?,
                completed_at = ?,
                updated_at = CURRENT_TIMESTAMP
                WHERE id = ?""",
                (
                    status,
                    stats_dict["total_assets"],
                    stats_dict["total_assets"],
                    stats_dict["matched_assets"],
                    stats_dict["unmatched_assets"],
                    stats_dict["dimension_matches"],
                    datetime.now().isoformat(),
                    job_id,
                ),
            )

            # Update daily stats
            self._update_daily_stats(start_date, stats_dict)

            result = {
                "job_id": job_id,
                "status": status,
                "start_date": start_date,
                "end_date": end_date,
                "output_file": output_file,
                "stats": stats_dict,
            }

            log_timing(f"Week sync complete: {stats_dict['total_assets']} assets")
            return result

        except Exception as e:
            # Update job with error
            execute_write(
                """UPDATE tagging_jobs SET
                status = 'error',
                error_message = ?,
                completed_at = ?,
                updated_at = CURRENT_TIMESTAMP
                WHERE id = ?""",
                (str(e), datetime.now().isoformat(), job_id),
            )
            log_timing(f"Week sync error: {e}")
            raise

        finally:
            self._current_engine = None
            self._current_job_id = None

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
        Run a sync for an entire month, broken into weekly chunks.

        Args:
            umbrella_client: Client for Umbrella API.
            mapping_engine: Mapping engine with loaded dimensions.
            account_key: Umbrella account key.
            month: Month string (YYYY-MM).
            progress_callback: Optional progress callback.
            vtag_filter_dimensions: Optional dimension filter list.

        Returns:
            Dict with aggregated month sync results.
        """
        log_timing(f"Starting month sync: {month}")

        start_date, end_date = _get_month_ranges(month)
        week_ranges = _get_week_ranges(start_date, end_date)

        # Create month sync record
        sync_id = execute_write(
            """INSERT INTO month_syncs
            (month, status, total_weeks, started_at, created_at)
            VALUES (?, 'running', ?, ?, CURRENT_TIMESTAMP)""",
            (month, len(week_ranges), datetime.now().isoformat()),
        )

        # Create week records
        week_ids = []
        for week_start, week_end in week_ranges:
            week_id = execute_write(
                """INSERT INTO month_sync_weeks
                (sync_id, week_start, week_end, status, created_at)
                VALUES (?, ?, ?, 'pending', CURRENT_TIMESTAMP)""",
                (sync_id, week_start, week_end),
            )
            week_ids.append(week_id)

        total_stats = {
            "total_assets": 0,
            "matched_assets": 0,
            "unmatched_assets": 0,
            "dimension_matches": 0,
        }
        completed_weeks = 0
        week_results = []
        all_output_files = []

        try:
            for i, (week_start, week_end) in enumerate(week_ranges):
                if self._current_engine and self._current_engine.is_cancelled():
                    log_timing(f"Month sync cancelled at week {i + 1}")
                    break

                # Update week status
                execute_write(
                    """UPDATE month_sync_weeks SET
                    status = 'running', started_at = ?
                    WHERE id = ?""",
                    (datetime.now().isoformat(), week_ids[i]),
                )

                try:
                    result = self.run_week_sync(
                        umbrella_client=umbrella_client,
                        mapping_engine=mapping_engine,
                        account_key=account_key,
                        start_date=week_start,
                        end_date=week_end,
                        progress_callback=progress_callback,
                        vtag_filter_dimensions=vtag_filter_dimensions,
                    )

                    week_stats = result.get("stats", {})
                    total_stats["total_assets"] += week_stats.get("total_assets", 0)
                    total_stats["matched_assets"] += week_stats.get("matched_assets", 0)
                    total_stats["unmatched_assets"] += week_stats.get("unmatched_assets", 0)
                    total_stats["dimension_matches"] += week_stats.get("dimension_matches", 0)

                    # Update week record
                    execute_write(
                        """UPDATE month_sync_weeks SET
                        status = 'completed',
                        statement_count = ?,
                        completed_at = ?
                        WHERE id = ?""",
                        (
                            week_stats.get("total_assets", 0),
                            datetime.now().isoformat(),
                            week_ids[i],
                        ),
                    )

                    completed_weeks += 1
                    week_results.append(result)

                    if result.get("output_file"):
                        all_output_files.append(result["output_file"])

                except Exception as e:
                    execute_write(
                        """UPDATE month_sync_weeks SET
                        status = 'error',
                        error_message = ?,
                        completed_at = ?
                        WHERE id = ?""",
                        (str(e), datetime.now().isoformat(), week_ids[i]),
                    )
                    log_timing(f"Week {i + 1} error: {e}")

                # Update month sync progress
                execute_write(
                    """UPDATE month_syncs SET
                    completed_weeks = ?,
                    total_statements = ?,
                    fetched_statements = ?,
                    updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?""",
                    (
                        completed_weeks,
                        total_stats["total_assets"],
                        total_stats["total_assets"],
                        sync_id,
                    ),
                )

            # Final status
            final_status = "completed"
            if self._current_engine and self._current_engine.is_cancelled():
                final_status = "cancelled"
            elif completed_weeks < len(week_ranges):
                final_status = "partial"

            execute_write(
                """UPDATE month_syncs SET
                status = ?,
                completed_at = ?,
                updated_at = CURRENT_TIMESTAMP
                WHERE id = ?""",
                (final_status, datetime.now().isoformat(), sync_id),
            )

            result = {
                "sync_id": sync_id,
                "month": month,
                "status": final_status,
                "total_weeks": len(week_ranges),
                "completed_weeks": completed_weeks,
                "stats": total_stats,
                "week_results": week_results,
                "output_files": all_output_files,
            }

            log_timing(f"Month sync {final_status}: {month}")
            return result

        except Exception as e:
            execute_write(
                """UPDATE month_syncs SET
                status = 'error',
                error_message = ?,
                completed_at = ?,
                updated_at = CURRENT_TIMESTAMP
                WHERE id = ?""",
                (str(e), datetime.now().isoformat(), sync_id),
            )
            log_timing(f"Month sync error: {e}")
            raise

    def run_range_sync(
        self,
        umbrella_client,
        mapping_engine,
        account_key: str,
        start_date: str,
        end_date: str,
        progress_callback: Optional[Callable] = None,
        vtag_filter_dimensions: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Run a sync for an arbitrary date range, broken into weekly chunks.

        Args:
            umbrella_client: Client for Umbrella API.
            mapping_engine: Mapping engine with loaded dimensions.
            account_key: Umbrella account key.
            start_date: Range start date (YYYY-MM-DD).
            end_date: Range end date (YYYY-MM-DD).
            progress_callback: Optional progress callback.
            vtag_filter_dimensions: Optional dimension filter list.

        Returns:
            Dict with aggregated range sync results.
        """
        log_timing(f"Starting range sync: {start_date} to {end_date}")

        week_ranges = _get_week_ranges(start_date, end_date)

        total_stats = {
            "total_assets": 0,
            "matched_assets": 0,
            "unmatched_assets": 0,
            "dimension_matches": 0,
        }
        completed_weeks = 0
        week_results = []
        all_output_files = []

        for i, (week_start, week_end) in enumerate(week_ranges):
            if self._current_engine and self._current_engine.is_cancelled():
                log_timing(f"Range sync cancelled at week {i + 1}")
                break

            try:
                result = self.run_week_sync(
                    umbrella_client=umbrella_client,
                    mapping_engine=mapping_engine,
                    account_key=account_key,
                    start_date=week_start,
                    end_date=week_end,
                    progress_callback=progress_callback,
                    vtag_filter_dimensions=vtag_filter_dimensions,
                )

                week_stats = result.get("stats", {})
                total_stats["total_assets"] += week_stats.get("total_assets", 0)
                total_stats["matched_assets"] += week_stats.get("matched_assets", 0)
                total_stats["unmatched_assets"] += week_stats.get("unmatched_assets", 0)
                total_stats["dimension_matches"] += week_stats.get("dimension_matches", 0)

                completed_weeks += 1
                week_results.append(result)

                if result.get("output_file"):
                    all_output_files.append(result["output_file"])

            except Exception as e:
                log_timing(f"Range week {i + 1} error: {e}")
                week_results.append({
                    "week_start": week_start,
                    "week_end": week_end,
                    "status": "error",
                    "error": str(e),
                })

        final_status = "completed"
        if self._current_engine and self._current_engine.is_cancelled():
            final_status = "cancelled"
        elif completed_weeks < len(week_ranges):
            final_status = "partial"

        result = {
            "status": final_status,
            "start_date": start_date,
            "end_date": end_date,
            "total_weeks": len(week_ranges),
            "completed_weeks": completed_weeks,
            "stats": total_stats,
            "week_results": week_results,
            "output_files": all_output_files,
        }

        log_timing(f"Range sync {final_status}: {start_date} to {end_date}")
        return result

    def _update_daily_stats(self, date_str: str, stats: dict):
        """
        Update the daily_stats table with results from a tagging run.

        Uses UPSERT to accumulate stats for the same date across multiple runs.
        """
        total = stats.get("total_assets", 0)
        matched = stats.get("matched_assets", 0)
        unmatched = stats.get("unmatched_assets", 0)
        dimension_matches = stats.get("dimension_matches", 0)

        match_rate = 0.0
        if total > 0:
            match_rate = (matched / total) * 100

        with get_db() as conn:
            # Check if entry exists for this date
            existing = conn.execute(
                "SELECT id, total_statements FROM daily_stats WHERE stat_date = ?",
                (date_str,),
            ).fetchone()

            if existing:
                # Accumulate stats
                conn.execute(
                    """UPDATE daily_stats SET
                    total_statements = total_statements + ?,
                    tagged_statements = tagged_statements + ?,
                    dimension_matches = dimension_matches + ?,
                    unmatched_statements = unmatched_statements + ?,
                    match_rate = ?,
                    api_calls = api_calls + 1,
                    updated_at = CURRENT_TIMESTAMP
                    WHERE stat_date = ?""",
                    (total, matched, dimension_matches, unmatched, match_rate, date_str),
                )
            else:
                conn.execute(
                    """INSERT INTO daily_stats
                    (stat_date, total_statements, tagged_statements,
                     dimension_matches, unmatched_statements, match_rate,
                     api_calls, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)""",
                    (date_str, total, matched, dimension_matches, unmatched, match_rate),
                )


# Global instance
sync_service = SyncService()
