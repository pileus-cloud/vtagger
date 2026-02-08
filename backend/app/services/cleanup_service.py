"""
VTagger Cleanup Service.

Manages cleanup of runtime data, output files, and database tables.
Supports soft cleanup (runtime data only) and hard cleanup (everything except
dimensions and api_keys).

Ported from BPVtagger with VTagger-specific table names.
"""

import glob
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.config import settings
from app.database import execute_query, execute_write, get_db
from app.services.agent_logger import log_timing


# Runtime tables that can be safely cleaned
RUNTIME_TABLES = [
    "tagging_jobs",
    "daily_stats",
    "vtag_uploads",
    "month_syncs",
    "month_sync_weeks",
]

# Tables preserved during hard cleanup
PRESERVED_TABLES = [
    "dimensions",
    "api_keys",
]


class CleanupService:
    """Manages cleanup of runtime data and output files."""

    def soft_cleanup(
        self,
        older_than_days: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Soft cleanup: remove runtime data older than specified days.

        Cleans runtime tables and output files, preserving dimensions and api_keys.

        Args:
            older_than_days: If specified, only clean data older than this many days.
                           If None, uses settings.retention_days.

        Returns:
            Dict with cleanup statistics.
        """
        days = older_than_days if older_than_days is not None else settings.retention_days
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        log_timing(f"Soft cleanup: removing data older than {days} days (before {cutoff_date})")

        results = {
            "type": "soft",
            "cutoff_date": cutoff_date,
            "tables_cleaned": {},
            "files_cleaned": 0,
            "bytes_freed": 0,
        }

        with get_db() as conn:
            # Clean tagging_jobs
            cursor = conn.execute(
                "DELETE FROM tagging_jobs WHERE created_at < ?", (cutoff_date,)
            )
            results["tables_cleaned"]["tagging_jobs"] = cursor.rowcount

            # Clean daily_stats
            cursor = conn.execute(
                "DELETE FROM daily_stats WHERE stat_date < ?", (cutoff_date,)
            )
            results["tables_cleaned"]["daily_stats"] = cursor.rowcount

            # Clean vtag_uploads
            cursor = conn.execute(
                "DELETE FROM vtag_uploads WHERE created_at < ?", (cutoff_date,)
            )
            results["tables_cleaned"]["vtag_uploads"] = cursor.rowcount

            # Clean month_sync_weeks (referencing month_syncs)
            cursor = conn.execute(
                """DELETE FROM month_sync_weeks WHERE sync_id IN
                (SELECT id FROM month_syncs WHERE created_at < ?)""",
                (cutoff_date,),
            )
            results["tables_cleaned"]["month_sync_weeks"] = cursor.rowcount

            # Clean month_syncs
            cursor = conn.execute(
                "DELETE FROM month_syncs WHERE created_at < ?", (cutoff_date,)
            )
            results["tables_cleaned"]["month_syncs"] = cursor.rowcount

        # Clean output files
        file_stats = self._clean_output_files(days)
        results["files_cleaned"] = file_stats["files_removed"]
        results["bytes_freed"] = file_stats["bytes_freed"]

        log_timing(
            f"Soft cleanup complete: "
            f"{sum(results['tables_cleaned'].values())} rows removed, "
            f"{results['files_cleaned']} files deleted"
        )

        return results

    def hard_cleanup(self) -> Dict[str, Any]:
        """
        Hard cleanup: remove ALL runtime data.

        Preserves dimensions and api_keys tables.
        Removes all output files.

        Returns:
            Dict with cleanup statistics.
        """
        log_timing("Hard cleanup: removing all runtime data")

        results = {
            "type": "hard",
            "tables_cleaned": {},
            "files_cleaned": 0,
            "bytes_freed": 0,
        }

        with get_db() as conn:
            for table in RUNTIME_TABLES:
                try:
                    cursor = conn.execute(f"DELETE FROM {table}")
                    results["tables_cleaned"][table] = cursor.rowcount
                except Exception as e:
                    results["tables_cleaned"][table] = f"error: {e}"

            # Also clean discovered_tags and config (runtime data)
            for table in ["discovered_tags", "config", "umbrella_accounts"]:
                try:
                    cursor = conn.execute(f"DELETE FROM {table}")
                    results["tables_cleaned"][table] = cursor.rowcount
                except Exception:
                    pass

            # Clean dimension_history
            try:
                cursor = conn.execute("DELETE FROM dimension_history")
                results["tables_cleaned"]["dimension_history"] = cursor.rowcount
            except Exception:
                pass

        # Remove all output files
        file_stats = self._clean_all_output_files()
        results["files_cleaned"] = file_stats["files_removed"]
        results["bytes_freed"] = file_stats["bytes_freed"]

        log_timing(
            f"Hard cleanup complete: "
            f"{sum(v for v in results['tables_cleaned'].values() if isinstance(v, int))} "
            f"rows removed, {results['files_cleaned']} files deleted"
        )

        return results

    def reset_all(self) -> Dict[str, Any]:
        """
        Full reset: remove EVERYTHING including dimensions and api_keys.

        WARNING: This is destructive and cannot be undone.

        Returns:
            Dict with reset statistics.
        """
        log_timing("Full reset: removing ALL data")

        results = {
            "type": "reset",
            "tables_cleaned": {},
            "files_cleaned": 0,
            "bytes_freed": 0,
        }

        with get_db() as conn:
            # Get all tables
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
            tables = [row[0] for row in cursor.fetchall()]

            for table in tables:
                try:
                    cursor = conn.execute(f"DELETE FROM {table}")
                    results["tables_cleaned"][table] = cursor.rowcount
                except Exception as e:
                    results["tables_cleaned"][table] = f"error: {e}"

        # Remove all output files
        file_stats = self._clean_all_output_files()
        results["files_cleaned"] = file_stats["files_removed"]
        results["bytes_freed"] = file_stats["bytes_freed"]

        log_timing(f"Full reset complete")
        return results

    def _clean_output_files(self, older_than_days: int) -> Dict[str, int]:
        """
        Clean output files older than the specified number of days.

        Returns:
            Dict with files_removed and bytes_freed counts.
        """
        output_dir = settings.output_dir
        if not os.path.exists(output_dir):
            return {"files_removed": 0, "bytes_freed": 0}

        cutoff_time = datetime.now().timestamp() - (older_than_days * 86400)
        files_removed = 0
        bytes_freed = 0

        for pattern in ["*.jsonl", "*.csv", "*.json"]:
            for filepath in glob.glob(os.path.join(output_dir, pattern)):
                try:
                    file_stat = os.stat(filepath)
                    if file_stat.st_mtime < cutoff_time:
                        file_size = file_stat.st_size
                        os.remove(filepath)
                        files_removed += 1
                        bytes_freed += file_size
                except OSError as e:
                    log_timing(f"Error removing file {filepath}: {e}")

        return {"files_removed": files_removed, "bytes_freed": bytes_freed}

    def _clean_all_output_files(self) -> Dict[str, int]:
        """
        Remove all output files.

        Returns:
            Dict with files_removed and bytes_freed counts.
        """
        output_dir = settings.output_dir
        if not os.path.exists(output_dir):
            return {"files_removed": 0, "bytes_freed": 0}

        files_removed = 0
        bytes_freed = 0

        for pattern in ["*.jsonl", "*.csv", "*.json"]:
            for filepath in glob.glob(os.path.join(output_dir, pattern)):
                try:
                    file_size = os.path.getsize(filepath)
                    os.remove(filepath)
                    files_removed += 1
                    bytes_freed += file_size
                except OSError as e:
                    log_timing(f"Error removing file {filepath}: {e}")

        return {"files_removed": files_removed, "bytes_freed": bytes_freed}

    def get_cleanup_stats(self) -> Dict[str, Any]:
        """
        Get current data sizes and counts for cleanup planning.

        Returns:
            Dict with table row counts and output directory size.
        """
        stats = {"tables": {}, "output_dir": {}}

        with get_db() as conn:
            for table in RUNTIME_TABLES + PRESERVED_TABLES:
                try:
                    cursor = conn.execute(f"SELECT COUNT(*) as cnt FROM {table}")
                    row = cursor.fetchone()
                    stats["tables"][table] = row[0] if row else 0
                except Exception:
                    stats["tables"][table] = 0

        # Output directory stats
        output_dir = settings.output_dir
        if os.path.exists(output_dir):
            total_size = 0
            file_count = 0
            for pattern in ["*.jsonl", "*.csv", "*.json"]:
                for filepath in glob.glob(os.path.join(output_dir, pattern)):
                    try:
                        total_size += os.path.getsize(filepath)
                        file_count += 1
                    except OSError:
                        pass

            stats["output_dir"] = {
                "path": output_dir,
                "file_count": file_count,
                "total_size_bytes": total_size,
                "total_size_mb": round(total_size / (1024 * 1024), 2),
            }
        else:
            stats["output_dir"] = {
                "path": output_dir,
                "file_count": 0,
                "total_size_bytes": 0,
                "total_size_mb": 0,
            }

        return stats


# Global instance
cleanup_service = CleanupService()
