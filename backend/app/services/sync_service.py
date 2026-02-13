"""
VTagger Sync Service.

Coordinates sync operations: fetch assets, map dimensions, upload vtags.
Sync = Simulation + Upload to Umbrella.

Follows the same pattern as simulation_service for fetch_and_map,
then adds the upload phase using presigned S3 URLs per payer account.
"""

import csv
import gzip
import json
import os
import time
from typing import Any, Callable, Dict, List, Optional

from app.config import settings
from app.database import execute_query, execute_write
from app.services.agent_logger import log_timing
from app.services.tagging_engine import TaggingEngine, MappingStats


class SyncService:
    """Coordinates fetch-map-upload sync operations."""

    LAST_RESULT_FILE = os.path.join(settings.output_dir, "last_sync_result.json")
    UPLOAD_HISTORY_FILE = os.path.join(settings.output_dir, "upload_history.json")
    MAX_UPLOAD_HISTORY = 30

    def __init__(self):
        self._engine: Optional[TaggingEngine] = None
        self._cancelled = False
        self._upload_phase = ""
        self._upload_progress: Dict[str, Any] = {}
        self._starting = False  # Set before background task runs
        self._starting_time: Optional[float] = None
        self._starting_info: Dict[str, str] = {}
        self._import_status_cache: Dict[str, Dict[str, Any]] = {}  # upload_id -> cached result
        # Persisted after completion so UI can display results
        self._last_result: Optional[Dict[str, Any]] = self._load_last_result()
        self._upload_history: List[Dict[str, Any]] = self._load_upload_history()
        # Seed daily_stats from last result if table is empty
        self._seed_stats_if_empty()

    def _load_last_result(self) -> Optional[Dict[str, Any]]:
        """Load last sync result from disk (survives server restarts)."""
        try:
            if os.path.exists(self.LAST_RESULT_FILE):
                with open(self.LAST_RESULT_FILE, "r") as f:
                    return json.load(f)
        except Exception:
            pass
        return None

    def _save_last_result(self):
        """Persist last sync result to disk."""
        if self._last_result:
            try:
                os.makedirs(os.path.dirname(self.LAST_RESULT_FILE), exist_ok=True)
                with open(self.LAST_RESULT_FILE, "w") as f:
                    json.dump(self._last_result, f)
            except Exception:
                pass
            # Also append uploads to history
            uploads = self._last_result.get("uploads", [])
            if uploads:
                import datetime
                ts = datetime.datetime.now().isoformat()
                sync_type = self._last_result.get("sync_type", "")
                start_date = self._last_result.get("start_date", "")
                end_date = self._last_result.get("end_date", "")
                for u in uploads:
                    u["timestamp"] = ts
                    u["sync_type"] = sync_type
                    u["start_date"] = start_date
                    u["end_date"] = end_date
                self._append_upload_history(uploads)

    def _load_upload_history(self) -> List[Dict[str, Any]]:
        """Load upload history from disk."""
        try:
            if os.path.exists(self.UPLOAD_HISTORY_FILE):
                with open(self.UPLOAD_HISTORY_FILE, "r") as f:
                    return json.load(f)
        except Exception:
            pass
        return []

    def _append_upload_history(self, uploads: List[Dict[str, Any]]):
        """Append uploads to history and persist (keep last N)."""
        # Deduplicate by upload_id
        existing_ids = {u["upload_id"] for u in self._upload_history}
        for u in uploads:
            if u["upload_id"] not in existing_ids:
                self._upload_history.append(u)
        # Trim to max
        self._upload_history = self._upload_history[-self.MAX_UPLOAD_HISTORY:]
        try:
            os.makedirs(os.path.dirname(self.UPLOAD_HISTORY_FILE), exist_ok=True)
            with open(self.UPLOAD_HISTORY_FILE, "w") as f:
                json.dump(self._upload_history, f)
        except Exception:
            pass

    def _seed_stats_if_empty(self):
        """Seed daily_stats from last sync result if the table is empty."""
        try:
            from app.database import execute_query
            rows = execute_query("SELECT COUNT(*) as cnt FROM daily_stats")
            if rows and rows[0]["cnt"] > 0:
                return  # Already has data
            if self._last_result and self._last_result.get("total_assets", 0) > 0:
                self._record_daily_stats(self._last_result)
                log_timing("[STATS] Seeded daily_stats from last sync result")
        except Exception as e:
            log_timing(f"[STATS] Error seeding stats: {e}")

    def mark_starting(self, sync_type: str = "", start_date: str = "", end_date: str = ""):
        """Mark that a sync has been requested (before background task runs)."""
        self._starting = True
        self._starting_time = time.time()
        self._starting_info = {
            "sync_type": sync_type,
            "start_date": start_date,
            "end_date": end_date,
        }
        self._cancelled = False
        log_timing(f"[SYNC] mark_starting: type={sync_type}, {start_date} to {end_date}")

    def cancel(self):
        """Cancel the current sync."""
        self._cancelled = True
        if self._engine:
            self._engine.cancel()
        # Also clear starting flag so cancel works during auth phase
        if self._starting:
            self._starting = False
            self._last_result = {
                "status": "cancelled",
                "sync_type": self._starting_info.get("sync_type", ""),
                "start_date": self._starting_info.get("start_date", ""),
                "end_date": self._starting_info.get("end_date", ""),
                "total_assets": 0, "matched_assets": 0, "unmatched_assets": 0,
                "elapsed_seconds": 0, "error_message": "Cancelled before sync started.",
            }
            self._save_last_result()
        log_timing("Sync cancellation requested")

    def get_progress(self) -> dict:
        """Get current sync progress."""
        if self._engine:
            progress = self._engine.get_progress()
            if self._upload_phase:
                # Override status to "running" during upload phase
                # (engine status is "completed" but sync is still uploading)
                progress["status"] = "running"
                progress["phase"] = self._upload_phase
                progress.update(self._upload_progress)
            return progress
        # Background task requested but not yet started
        if self._starting:
            info = self._starting_info
            elapsed = round(time.time() - self._starting_time, 1) if self._starting_time else 0
            return {
                "status": "running",
                "phase": "authenticating",
                "processed_assets": 0,
                "matched_assets": 0,
                "unmatched_assets": 0,
                "dimension_matches": 0,
                "elapsed_seconds": elapsed,
                "sync_type": info.get("sync_type", ""),
                "start_date": info.get("start_date", ""),
                "end_date": info.get("end_date", ""),
            }
        # When idle, include last result summary so UI can show it
        if self._last_result:
            return {
                "status": self._last_result.get("status", "idle"),
                "last_sync": {
                    "sync_type": self._last_result.get("sync_type", ""),
                    "start_date": self._last_result.get("start_date", ""),
                    "end_date": self._last_result.get("end_date", ""),
                    "total_assets": self._last_result.get("total_assets", 0),
                    "matched_assets": self._last_result.get("matched_assets", 0),
                    "unmatched_assets": self._last_result.get("unmatched_assets", 0),
                    "uploaded_count": self._last_result.get("uploaded_count", 0),
                    "upload_count": len(self._last_result.get("uploads", [])),
                    "elapsed_seconds": self._last_result.get("elapsed_seconds", 0),
                    "error_message": self._last_result.get("error_message", ""),
                },
            }
        return {"status": "idle"}

    def get_last_result(self) -> Optional[Dict[str, Any]]:
        """Get the result of the last sync/upload operation."""
        return self._last_result

    def get_import_status(self, umbrella_client) -> Optional[Dict[str, Any]]:
        """Poll Umbrella for the import processing status of recent uploads.

        Uses caching: completed/failed uploads are cached and not re-fetched.
        Only active uploads are polled from Umbrella.
        """
        # Use full upload history (last 30)
        uploads = list(self._upload_history)

        # Fallback to last_result if history is empty
        if not uploads and self._last_result:
            uploads = self._last_result.get("uploads", [])
            if not uploads:
                upload_ids = self._last_result.get("upload_ids", [])
                if not upload_ids:
                    return None
                uploads = [{"upload_id": uid, "account_id": "", "total_rows": 0}
                           for uid in upload_ids]

        if not uploads:
            return None

        results = []
        uploads_to_fetch = []

        # Use cached results for terminal uploads, collect active ones to fetch
        for upload_info in uploads:
            upload_id = upload_info["upload_id"]
            cached = self._import_status_cache.get(upload_id)
            if cached and cached.get("phase") in ("completed", "failed", "error"):
                results.append(cached)
            else:
                uploads_to_fetch.append(upload_info)

        # If all cached, return immediately
        if not uploads_to_fetch:
            return {"last_result": self._last_result, "import_statuses": results}

        # Authenticate once and get headers
        try:
            umbrella_client._ensure_authenticated()
            aggregate_accounts, individual_accounts = umbrella_client.get_accounts()
            all_accounts = individual_accounts + aggregate_accounts
            if not all_accounts:
                return None
            account_key = all_accounts[0].get("accountKey")
            headers = umbrella_client._build_headers(account_key)
        except Exception as e:
            return {"last_result": self._last_result, "import_statuses": results,
                    "error": str(e)}

        import httpx

        # Single client for all requests, short timeout
        with httpx.Client(timeout=10.0) as client:
            for upload_info in uploads_to_fetch:
                upload_id = upload_info["upload_id"]
                account_id = upload_info.get("account_id", "")
                local_rows = upload_info.get("total_rows", 0)
                timestamp = upload_info.get("timestamp", "")
                base_info = {
                    "upload_id": upload_id,
                    "account_id": account_id,
                    "timestamp": timestamp,
                    "sync_type": upload_info.get("sync_type", ""),
                    "start_date": upload_info.get("start_date", ""),
                    "end_date": upload_info.get("end_date", ""),
                }

                try:
                    url = (
                        f"{umbrella_client.base_url.replace('/v1', '')}"
                        f"/v2/governance-tags/resources/import/status/{upload_id}"
                    )
                    response = client.get(url, headers=headers)
                    if response.status_code == 200:
                        status_data = response.json() or {}
                        ops = status_data.get("operations") or {}
                        entry = {
                            **base_info,
                            "total_rows": status_data.get("totalRows") or local_rows,
                            "processed_rows": status_data.get("processedRows"),
                            "phase": status_data.get("phase", "unknown"),
                            "phase_description": status_data.get("phaseDescription", ""),
                            "errors": status_data.get("errors", 0),
                            "status": status_data.get("status", "unknown"),
                            "import_mode": status_data.get("importMode", ""),
                            "inserted": ops.get("inserted", 0),
                            "updated": ops.get("updated", 0),
                            "deleted": ops.get("deleted", 0),
                        }
                    else:
                        entry = {
                            **base_info,
                            "total_rows": local_rows,
                            "phase": "unknown",
                            "error": f"HTTP {response.status_code}",
                        }
                except Exception as e:
                    entry = {
                        **base_info,
                        "total_rows": local_rows,
                        "phase": "fetch_error",
                        "error": str(e),
                    }

                # Only cache truly terminal states from Umbrella (not transient fetch errors)
                if entry.get("phase") in ("completed", "failed"):
                    self._import_status_cache[upload_id] = entry
                results.append(entry)

        return {
            "last_result": self._last_result,
            "import_statuses": results,
        }

    def run_week_sync(
        self,
        umbrella_client,
        mapping_engine,
        account_key: str = "0",
        start_date: str = "",
        end_date: str = "",
        vtag_filter_dimensions: Optional[List[str]] = None,
        filter_mode: str = "not_vtagged",
        account_keys: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Run a full week sync: fetch assets, map dimensions, upload vtags.

        This is a synchronous method meant to run in a BackgroundTask.
        """
        return self._run_sync(
            umbrella_client=umbrella_client,
            mapping_engine=mapping_engine,
            account_key=account_key,
            account_keys=account_keys,
            start_date=start_date,
            end_date=end_date,
            vtag_filter_dimensions=vtag_filter_dimensions,
            filter_mode=filter_mode,
            sync_type="week",
        )

    def run_range_sync(
        self,
        umbrella_client,
        mapping_engine,
        account_key: str = "0",
        start_date: str = "",
        end_date: str = "",
        vtag_filter_dimensions: Optional[List[str]] = None,
        filter_mode: str = "not_vtagged",
        account_keys: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Run a range sync: fetch assets for date range, map, upload.
        """
        return self._run_sync(
            umbrella_client=umbrella_client,
            mapping_engine=mapping_engine,
            account_key=account_key,
            account_keys=account_keys,
            start_date=start_date,
            end_date=end_date,
            vtag_filter_dimensions=vtag_filter_dimensions,
            filter_mode=filter_mode,
            sync_type="range",
        )

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
        Run a month sync. Parses month (YYYY-MM) into date range.
        """
        import calendar

        parts = month.split("-")
        year, mon = int(parts[0]), int(parts[1])
        last_day = calendar.monthrange(year, mon)[1]
        start_date = f"{year}-{mon:02d}-01"
        end_date = f"{year}-{mon:02d}-{last_day:02d}"

        return self._run_sync(
            umbrella_client=umbrella_client,
            mapping_engine=mapping_engine,
            account_key=account_key,
            account_keys=account_keys,
            start_date=start_date,
            end_date=end_date,
            vtag_filter_dimensions=vtag_filter_dimensions,
            filter_mode=filter_mode,
            sync_type="month",
        )

    def _run_sync(
        self,
        umbrella_client,
        mapping_engine,
        account_key: str = "0",
        start_date: str = "",
        end_date: str = "",
        vtag_filter_dimensions: Optional[List[str]] = None,
        filter_mode: str = "not_vtagged",
        account_keys: Optional[List[str]] = None,
        sync_type: str = "week",
    ) -> Dict[str, Any]:
        """Core sync implementation: fetch -> map -> upload."""
        log_timing(f"[SYNC] _run_sync started: type={sync_type}, {start_date} to {end_date}, filter={filter_mode}")

        # Check if cancelled during auth/starting phase
        if self._cancelled:
            log_timing("[SYNC] Cancelled before engine started")
            self._starting = False
            self._cancelled = False
            return {
                "status": "cancelled", "sync_type": sync_type,
                "start_date": start_date, "end_date": end_date,
                "total_assets": 0, "matched_assets": 0, "unmatched_assets": 0,
                "elapsed_seconds": 0, "error_message": "Cancelled before sync started.",
            }

        self._cancelled = False
        self._starting = False
        self._upload_phase = ""
        self._upload_progress = {}

        engine = TaggingEngine()
        self._engine = engine
        start_time = time.time()

        result: Dict[str, Any] = {
            "status": "running",
            "sync_type": sync_type,
            "start_date": start_date,
            "end_date": end_date,
            "total_assets": 0,
            "matched_assets": 0,
            "unmatched_assets": 0,
            "dimension_matches": 0,
            "uploaded_count": 0,
            "upload_ids": [],
            "elapsed_seconds": 0,
            "error_message": "",
        }

        try:
            # Resolve account keys
            resolved_keys = []
            if account_keys and len(account_keys) > 0:
                # Explicit list provided by the user
                resolved_keys = [str(k) for k in account_keys]
                log_timing(f"[SYNC] Using {len(resolved_keys)} selected account keys")
            elif account_key == "0" or not account_key:
                # Fetch all accounts
                aggregate_accounts, individual_accounts = umbrella_client.get_accounts()
                all_accounts = aggregate_accounts + individual_accounts
                if not all_accounts:
                    raise Exception("No accounts found.")
                resolved_keys = [
                    str(acc.get("accountKey")) for acc in all_accounts
                    if acc.get("accountKey")
                ]
                log_timing(f"[SYNC] Resolved {len(resolved_keys)} account keys")
            else:
                resolved_keys = [account_key]

            # --- Phase 1: Fetch and Map ---
            log_timing(
                f"[SYNC] Phase 1: Fetch & Map "
                f"({start_date} to {end_date}, filter={filter_mode})"
            )

            output_file, stats = engine.fetch_and_map(
                umbrella_client=umbrella_client,
                mapping_engine=mapping_engine,
                account_keys=resolved_keys,
                start_date=start_date,
                end_date=end_date,
                output_dir=settings.output_dir,
                vtag_filter_dimensions=vtag_filter_dimensions,
                filter_mode=filter_mode,
            )

            stats_dict = stats.to_dict()
            result["total_assets"] = stats_dict["total_assets"]
            result["matched_assets"] = stats_dict["matched_assets"]
            result["unmatched_assets"] = stats_dict["unmatched_assets"]
            result["dimension_matches"] = stats_dict["dimension_matches"]

            if engine.is_cancelled():
                result["status"] = "cancelled"
                result["elapsed_seconds"] = round(time.time() - start_time, 1)
                log_timing("[SYNC] Cancelled after fetch phase")
                return result

            if stats.matched_assets == 0:
                log_timing("[SYNC] No matched assets, skipping upload")
                result["status"] = "completed"
                result["message"] = "No assets matched any dimension rules. Nothing uploaded."
                result["elapsed_seconds"] = round(time.time() - start_time, 1)
                return result

            # --- Phase 2: Upload vtags ---
            log_timing(
                f"[SYNC] Phase 2: Upload "
                f"({stats.matched_assets} matched assets)"
            )
            self._upload_phase = "uploading vtags"

            uploads = self._upload_vtags(
                umbrella_client=umbrella_client,
                jsonl_file=output_file,
            )

            result["uploads"] = uploads
            result["upload_ids"] = [u["upload_id"] for u in uploads]
            result["uploaded_count"] = stats.matched_assets
            result["status"] = "completed"
            result["elapsed_seconds"] = round(time.time() - start_time, 1)

            log_timing(
                f"[SYNC] Complete: {stats.total_assets} assets, "
                f"{stats.matched_assets} matched, "
                f"{len(uploads)} payer uploads"
            )
            return result

        except Exception as e:
            result["status"] = "error"
            result["error_message"] = str(e)
            result["elapsed_seconds"] = round(time.time() - start_time, 1)
            log_timing(f"[SYNC] Error: {e}")
            return result

        finally:
            self._last_result = result
            self._save_last_result()
            self._record_daily_stats(result)
            self._engine = None
            self._upload_phase = ""
            self._upload_progress = {}

    def upload_file(
        self,
        umbrella_client,
        jsonl_file: str,
    ) -> Dict[str, Any]:
        """
        Upload vtags from an existing JSONL file (standalone, no fetch phase).

        This is a synchronous method meant to run in a BackgroundTask.
        Exposes the upload phase with progress tracking via get_progress().
        """
        self._cancelled = False
        self._upload_phase = "uploading vtags"
        self._upload_progress = {}

        # Create a dummy engine so get_progress() returns "running" status
        engine = TaggingEngine()
        engine.progress.status = "running"
        engine.progress.phase = "uploading vtags"
        engine.progress.start_time = time.time()
        self._engine = engine

        start_time = time.time()

        try:
            log_timing(f"[UPLOAD] Starting upload from {jsonl_file}")

            uploads = self._upload_vtags(
                umbrella_client=umbrella_client,
                jsonl_file=jsonl_file,
            )

            elapsed = round(time.time() - start_time, 1)
            log_timing(f"[UPLOAD] Complete: {len(uploads)} payer uploads in {elapsed}s")

            result = {
                "status": "completed",
                "uploads": uploads,
                "upload_ids": [u["upload_id"] for u in uploads],
                "upload_count": len(uploads),
                "elapsed_seconds": elapsed,
            }
            self._last_result = result
            self._save_last_result()
            return result

        except Exception as e:
            log_timing(f"[UPLOAD] Error: {e}")
            result = {
                "status": "error",
                "error_message": str(e),
                "elapsed_seconds": round(time.time() - start_time, 1),
            }
            self._last_result = result
            self._save_last_result()
            return result

        finally:
            self._engine = None
            self._upload_phase = ""
            self._upload_progress = {}

    def _upload_vtags(
        self,
        umbrella_client,
        jsonl_file: str,
    ) -> List[str]:
        """
        Upload vtags from JSONL file to Umbrella.

        Groups records by payer account, creates CSV per payer,
        compresses and uploads via presigned URL.

        Returns list of upload IDs.
        """
        uploads: List[Dict[str, Any]] = []

        # Group records by payer account
        payer_groups = self._group_by_payer(jsonl_file)
        total_payers = len(payer_groups)

        if total_payers == 0:
            log_timing("[SYNC] No records with vtags to upload")
            return uploads

        total_records = sum(len(recs) for recs in payer_groups.values())
        log_timing(
            f"[SYNC] Uploading {total_records} records "
            f"to {total_payers} payer accounts"
        )

        # Build account lookup once (avoid repeated get_accounts calls)
        aggregate_accounts, individual_accounts = umbrella_client.get_accounts()
        account_lookup: Dict[str, str] = {}
        for acc in individual_accounts + aggregate_accounts:
            acc_id = acc.get("accountId") or acc.get("accountName")
            acc_key = acc.get("accountKey")
            if acc_id and acc_key:
                account_lookup[acc_id] = acc_key

        output_dir = os.path.dirname(jsonl_file)

        for payer_idx, (payer_id, records) in enumerate(payer_groups.items(), 1):
            if self._cancelled:
                log_timing("[SYNC] Upload cancelled")
                break

            self._upload_progress = {
                "upload_payer": f"{payer_idx}/{total_payers}",
                "upload_payer_id": payer_id,
                "upload_records": len(records),
            }

            # Look up account_key for this payer
            account_key = account_lookup.get(payer_id)
            if not account_key:
                log_timing(
                    f"[SYNC] WARNING: No account key for payer {payer_id}, skipping"
                )
                continue

            log_timing(
                f"[SYNC] Uploading payer {payer_idx}/{total_payers}: "
                f"{payer_id} ({len(records)} records)"
            )

            csv_path = None
            gz_path = None
            try:
                # Write CSV for this payer
                csv_path = os.path.join(
                    output_dir,
                    f"upload_{payer_id}_{len(records)}.csv",
                )
                self._write_upload_csv(records, csv_path)

                # Compress
                gz_path = csv_path + ".gz"
                with open(csv_path, "rb") as f_in:
                    with gzip.open(gz_path, "wb") as f_out:
                        while True:
                            chunk = f_in.read(1024 * 1024)
                            if not chunk:
                                break
                            f_out.write(chunk)

                # Upload via presigned URL
                upload_id = umbrella_client.upload_virtual_tags(
                    csv_path=gz_path,
                    account_id=payer_id,
                    account_key=account_key,
                    compressed=True,
                    mode="upsert",
                )
                uploads.append({
                    "upload_id": upload_id,
                    "account_id": payer_id,
                    "total_rows": len(records),
                })
                log_timing(f"[SYNC] Payer {payer_id} uploaded: {upload_id}")

            except Exception as e:
                log_timing(f"[SYNC] Error uploading payer {payer_id}: {e}")

            finally:
                # Cleanup temp files
                for path in [csv_path, gz_path]:
                    if path:
                        try:
                            os.unlink(path)
                        except OSError:
                            pass

        log_timing(
            f"[SYNC] Upload complete: "
            f"{len(uploads)}/{total_payers} payer accounts"
        )
        return uploads

    def _record_daily_stats(self, result: Dict[str, Any]):
        """Record sync results into daily_stats table for the statistics page."""
        try:
            import datetime
            stat_date = datetime.date.today().isoformat()
            total = result.get("total_assets", 0)
            matched = result.get("matched_assets", 0)
            unmatched = result.get("unmatched_assets", 0)
            dim_matches = result.get("dimension_matches", 0)
            match_rate = (matched / total * 100) if total > 0 else 0.0
            has_error = 1 if result.get("status") == "error" else 0

            # Upsert: add to existing day's stats or create new row
            execute_write(
                """INSERT INTO daily_stats (stat_date, total_statements, tagged_statements,
                    dimension_matches, unmatched_statements, match_rate, api_calls, errors)
                VALUES (?, ?, ?, ?, ?, ?, 1, ?)
                ON CONFLICT(stat_date) DO UPDATE SET
                    total_statements = total_statements + excluded.total_statements,
                    tagged_statements = tagged_statements + excluded.tagged_statements,
                    dimension_matches = dimension_matches + excluded.dimension_matches,
                    unmatched_statements = unmatched_statements + excluded.unmatched_statements,
                    match_rate = CASE WHEN (total_statements + excluded.total_statements) > 0
                        THEN (tagged_statements + excluded.tagged_statements) * 100.0 / (total_statements + excluded.total_statements)
                        ELSE 0 END,
                    api_calls = api_calls + 1,
                    errors = errors + excluded.errors,
                    updated_at = CURRENT_TIMESTAMP""",
                (stat_date, total, matched, dim_matches, unmatched, round(match_rate, 2), has_error),
            )
            log_timing(f"[STATS] Recorded daily stats for {stat_date}")
        except Exception as e:
            log_timing(f"[STATS] Error recording stats: {e}")

    def _group_by_payer(self, jsonl_file: str) -> Dict[str, List[Dict]]:
        """
        Group JSONL records by payer account, building upload-ready records.

        Deduplicates by resource_id within each payer, skips resources with
        no vtags (all dimensions = Unallocated), and skips invalid resource IDs.
        """
        groups: Dict[str, List[Dict]] = {}
        seen_resources: Dict[str, set] = {}

        with open(jsonl_file, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                resource_id = record.get("resourceid", "")
                if not resource_id or resource_id == "Not Available":
                    continue
                # Umbrella DB limit
                if len(resource_id) > 255:
                    continue

                payer_account = (
                    record.get("payeraccount", "")
                    or record.get("linkedaccid", "")
                )
                if not payer_account:
                    continue

                # Skip duplicates within same payer
                if payer_account not in seen_resources:
                    seen_resources[payer_account] = set()
                if resource_id in seen_resources[payer_account]:
                    continue
                seen_resources[payer_account].add(resource_id)

                # Build vtag string from dimensions (dynamic, not hardcoded)
                dims = record.get("dimensions", {})
                vtags = []
                for dim_name, dim_value in dims.items():
                    if dim_value and dim_value != "Unallocated":
                        vtags.append(f"{dim_name}:{dim_value}")

                if not vtags:
                    continue

                if payer_account not in groups:
                    groups[payer_account] = []

                groups[payer_account].append({
                    "resource_id": resource_id,
                    "linked_account": record.get("linkedaccid", ""),
                    "vtags": ";".join(vtags),
                })

        return groups

    def _write_upload_csv(self, records: List[Dict], csv_path: str):
        """
        Write CSV in Umbrella's expected format.

        Format: Resource Cost,Resource Name,Resource ID,Service,Region,
                Linked Account,Virtual Tags,Tags

        Uses manual writes (not csv.writer) to match BPVtagger's format
        with Unix line endings.
        """
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write("Resource Cost,Resource Name,Resource ID,Service,Region,Linked Account,Virtual Tags,Tags\n")

            for record in records:
                resource_id = record["resource_id"]
                linked_account = record["linked_account"]
                vtags_str = record["vtags"]

                # Escape fields containing commas
                if "," in resource_id:
                    resource_id = f'"{resource_id}"'
                if "," in vtags_str:
                    vtags_str = f'"{vtags_str}"'

                f.write(f",,{resource_id},,,{linked_account},{vtags_str},\n")


# Global instance
sync_service = SyncService()
