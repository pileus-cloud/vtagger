"""
VTagger Status API.

Endpoints for health checks, simulation, sync operations, SSE streaming,
cleanup, file download, upload, and reset.

Ported from BPVtagger with:
- vtag_filter_dimensions parameter on simulation and sync requests
- dimension_matches naming (replaces bizmapping_matches)
- Imports from mapping_engine instead of mapping_service
- No legacy month-sync endpoints (only /sync/* endpoints)
"""

import asyncio
import glob
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field

from app.config import settings
from app.database import execute_query, execute_write, get_db
from app.services.agent_logger import log_timing
from app.services.mapping_engine import mapping_engine
from app.services.umbrella_client import umbrella_client
from app.services.simulation_service import simulation_service
from app.services.sync_service import sync_service
from app.services.month_sync_service import month_sync_service
from app.services.vtag_upload_service import vtag_upload_service
from app.services.cleanup_service import cleanup_service
from app.services.progress_tracker import progress_tracker, AgentState
from app.services.credential_manager import has_credentials

router = APIRouter(prefix="/status", tags=["status"])


# ---------------------------------------------------------------------------
# Request / Response Models
# ---------------------------------------------------------------------------


class SimulationRequest(BaseModel):
    """Request to run a simulation."""
    account_key: str
    start_date: str
    end_date: str
    vtag_filter_dimensions: Optional[List[str]] = None
    max_records: int = 0  # 0 = unlimited
    filter_mode: str = "all"  # "all" or "not_vtagged"


class WeekSyncRequest(BaseModel):
    """Request to run a weekly sync.

    Accepts either week_number+year OR start_date+end_date.
    If week_number/year are provided, start_date/end_date are computed.
    """
    account_key: str = "0"
    account_keys: Optional[List[str]] = None
    week_number: Optional[int] = None
    year: Optional[int] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    vtag_filter_dimensions: Optional[List[str]] = None
    filter_mode: str = "not_vtagged"  # "all" or "not_vtagged"
    force_all: bool = False  # If true, overrides filter_mode to "all"


class MonthSyncRequest(BaseModel):
    """Request to run a month sync."""
    account_key: str = "0"
    account_keys: Optional[List[str]] = None
    month: str
    vtag_filter_dimensions: Optional[List[str]] = None
    filter_mode: str = "not_vtagged"


class RangeSyncRequest(BaseModel):
    """Request to run a range sync."""
    account_key: str = "0"
    account_keys: Optional[List[str]] = None
    start_date: str
    end_date: str
    vtag_filter_dimensions: Optional[List[str]] = None
    filter_mode: str = "not_vtagged"


class UploadRequest(BaseModel):
    """Request to upload vtags from a JSONL file."""
    account_key: str
    jsonl_file: str
    group_by_payer: bool = False
    description: str = ""


class CleanupRequest(BaseModel):
    """Request for cleanup operation."""
    cleanup_type: str = "soft"  # "soft", "hard", or "reset"
    older_than_days: Optional[int] = None


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    version: str
    database: str
    dimensions_loaded: int
    api_key_configured: bool
    uptime_seconds: Optional[float] = None


# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------

_start_time = time.time()


# ---------------------------------------------------------------------------
# Health Check
# ---------------------------------------------------------------------------


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    db_status = "ok"
    try:
        execute_query("SELECT 1")
    except Exception:
        db_status = "error"

    return HealthResponse(
        status="healthy" if db_status == "ok" else "degraded",
        version="1.0.0",
        database=db_status,
        dimensions_loaded=len(mapping_engine.dimensions),
        api_key_configured=has_credentials(),
        uptime_seconds=round(time.time() - _start_time, 1),
    )


# ---------------------------------------------------------------------------
# SSE Progress Streaming
# ---------------------------------------------------------------------------


@router.get("/events")
async def sse_events(request: Request):
    """Server-Sent Events endpoint for real-time progress updates."""

    async def event_generator():
        queue = progress_tracker.subscribe()
        try:
            # Send initial state
            initial = progress_tracker.to_dict()
            yield f"data: {json.dumps(initial)}\n\n"

            while True:
                # Check if client disconnected
                if await request.is_disconnected():
                    break

                try:
                    message = await asyncio.wait_for(queue.get(), timeout=30.0)
                    event_type = message.get("event", "progress")
                    data = message.get("data", "{}")
                    yield f"event: {event_type}\ndata: {data}\n\n"
                except asyncio.TimeoutError:
                    # Send keepalive
                    yield f": keepalive\n\n"

        finally:
            progress_tracker.unsubscribe(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/progress")
async def get_progress():
    """Get current progress state (polling alternative to SSE).

    Merges live progress from sync/simulation engines into the
    progress_tracker response so the Monitor page always reflects
    the actual running state.
    """
    result = progress_tracker.to_dict()

    # Check if a sync engine is actively running
    sync_progress = sync_service.get_progress()
    if sync_progress.get("status") == "running":
        result["state"] = "fetching_resources"
        result["message"] = "Sync running"
        result["detail"] = sync_progress.get("phase", "")
        pct = sync_progress.get("progress_pct", 0)
        result["progress"] = pct
        result["sub_progress"] = pct
        result["sub_message"] = sync_progress.get("phase", "")
        result["elapsed_seconds"] = sync_progress.get("elapsed_seconds")
        result["stats"] = {
            "processed_assets": sync_progress.get("processed_assets", 0),
            "matched_assets": sync_progress.get("matched_assets", 0),
            "unmatched_assets": sync_progress.get("unmatched_assets", 0),
            "dimension_matches": sync_progress.get("dimension_matches", 0),
        }
        return result

    # Check if a simulation engine is actively running
    sim_results = simulation_service.get_results()
    if sim_results and sim_results.get("status") == "running":
        result["state"] = "fetching_resources"
        result["message"] = "Simulation running"
        result["detail"] = sim_results.get("phase", "")
        result["elapsed_seconds"] = sim_results.get("elapsed_seconds")
        result["stats"] = {
            "processed_assets": sim_results.get("total_assets", 0),
            "matched_assets": sim_results.get("matched_assets", 0),
            "unmatched_assets": sim_results.get("unmatched_assets", 0),
            "dimension_matches": sim_results.get("dimension_matches", 0),
        }
        return result

    # When idle, include last sync result summary if available
    last_sync = sync_progress.get("last_sync")
    if last_sync:
        status = last_sync.get("error_message") and "error" or "complete"
        result["state"] = status
        sync_type = last_sync.get("sync_type", "sync")
        start_date = last_sync.get("start_date", "")
        end_date = last_sync.get("end_date", "")
        result["message"] = f"Last {sync_type} sync: {start_date} to {end_date}"
        result["elapsed_seconds"] = last_sync.get("elapsed_seconds", 0)
        result["stats"] = {
            "total_assets": last_sync.get("total_assets", 0),
            "matched_assets": last_sync.get("matched_assets", 0),
            "unmatched_assets": last_sync.get("unmatched_assets", 0),
            "uploaded": last_sync.get("uploaded_count", 0),
            "payer_accounts": last_sync.get("upload_count", 0),
        }
        if last_sync.get("error_message"):
            result["detail"] = last_sync["error_message"]

    return result


# ---------------------------------------------------------------------------
# Simulation
# ---------------------------------------------------------------------------


@router.post("/simulate")
async def start_simulation(
    request: SimulationRequest,
    background_tasks: BackgroundTasks,
):
    """Start a simulation/dry-run tagging."""
    if progress_tracker.is_running:
        raise HTTPException(
            status_code=409, detail="An operation is already running."
        )

    # Ensure authenticated via credential manager
    try:
        umbrella_client._ensure_authenticated()
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))

    # Ensure dimensions loaded
    if not mapping_engine.dimensions:
        mapping_engine.load_dimensions()

    if not mapping_engine.dimensions:
        raise HTTPException(
            status_code=400, detail="No dimensions loaded. Create dimensions first."
        )

    def run_sim():
        try:
            simulation_service.run_simulation(
                umbrella_client=umbrella_client,
                mapping_engine=mapping_engine,
                account_key=request.account_key,
                start_date=request.start_date,
                end_date=request.end_date,
                vtag_filter_dimensions=request.vtag_filter_dimensions,
                max_records=request.max_records,
                filter_mode=request.filter_mode,
            )
        except Exception as e:
            log_timing(f"Simulation background task error: {e}")

    background_tasks.add_task(run_sim)

    return {
        "status": "started",
        "message": "Simulation started. Use /status/events for progress.",
        "start_date": request.start_date,
        "end_date": request.end_date,
    }


@router.get("/simulate/results")
async def get_simulation_results():
    """Get the results of the last simulation."""
    results = simulation_service.get_results()
    if results is None:
        raise HTTPException(status_code=404, detail="No simulation results available.")
    return results


# ---------------------------------------------------------------------------
# Sync Operations
# ---------------------------------------------------------------------------


@router.post("/sync/week")
async def start_week_sync(
    request: WeekSyncRequest,
    background_tasks: BackgroundTasks,
):
    """Start a weekly sync operation.

    Accepts week_number+year OR start_date+end_date.
    If force_all=true, overrides filter_mode to "all".
    """
    # Resolve dates from week_number/year if provided
    start_date = request.start_date
    end_date = request.end_date
    filter_mode = "all" if request.force_all else request.filter_mode

    if request.week_number is not None and request.year is not None:
        from datetime import datetime as dt, timedelta
        # ISO week: Jan 4 is always in week 1
        jan4 = dt(request.year, 1, 4)
        start_of_week1 = jan4 - timedelta(days=jan4.isoweekday() - 1)
        monday = start_of_week1 + timedelta(weeks=request.week_number - 1)
        sunday = monday + timedelta(days=6)
        start_date = monday.strftime("%Y-%m-%d")
        end_date = sunday.strftime("%Y-%m-%d")
    elif not start_date or not end_date:
        raise HTTPException(status_code=400, detail="Provide week_number+year or start_date+end_date.")

    log_timing(f"[API] POST /sync/week: {start_date} to {end_date}, filter={filter_mode}")

    if sync_service._engine or sync_service._starting:
        raise HTTPException(status_code=409, detail="A sync is already running.")

    effective_key = request.account_key
    effective_keys = request.account_keys

    # Mark starting immediately so progress shows right away
    sync_service.mark_starting("week", start_date, end_date)

    def run_sync():
        try:
            if sync_service._cancelled:
                log_timing("[API] Week sync cancelled before auth")
                sync_service._starting = False
                return

            umbrella_client._ensure_authenticated()

            if sync_service._cancelled:
                log_timing("[API] Week sync cancelled after auth")
                sync_service._starting = False
                return

            if not mapping_engine.dimensions:
                mapping_engine.load_dimensions()
            if not mapping_engine.dimensions:
                raise Exception("No dimensions loaded. Create dimensions first.")

            sync_service.run_week_sync(
                umbrella_client=umbrella_client,
                mapping_engine=mapping_engine,
                account_key=effective_key,
                account_keys=effective_keys,
                start_date=start_date,
                end_date=end_date,
                vtag_filter_dimensions=request.vtag_filter_dimensions,
                filter_mode=filter_mode,
            )
        except Exception as e:
            log_timing(f"Week sync error: {e}")
            sync_service._starting = False
            sync_service._last_result = {"status": "error", "error_message": str(e),
                "sync_type": "week", "start_date": start_date, "end_date": end_date,
                "total_assets": 0, "matched_assets": 0, "unmatched_assets": 0, "elapsed_seconds": 0}
            sync_service._save_last_result()
            sync_service._engine = None

    background_tasks.add_task(run_sync)

    return {
        "status": "started",
        "message": f"Week sync started.",
        "week_number": request.week_number,
        "year": request.year,
        "start_date": start_date,
        "end_date": end_date,
    }


@router.post("/sync/month")
async def start_month_sync(
    request: MonthSyncRequest,
    background_tasks: BackgroundTasks,
):
    """Start a month sync operation."""
    log_timing(f"[API] POST /sync/month: {request.month}, filter={request.filter_mode}")

    if sync_service._engine or sync_service._starting:
        raise HTTPException(status_code=409, detail="A sync is already running.")

    effective_key = request.account_key
    effective_keys = request.account_keys

    sync_service.mark_starting("month", request.month, request.month)

    def run_sync():
        try:
            if sync_service._cancelled:
                sync_service._starting = False
                return

            umbrella_client._ensure_authenticated()

            if sync_service._cancelled:
                sync_service._starting = False
                return

            if not mapping_engine.dimensions:
                mapping_engine.load_dimensions()
            if not mapping_engine.dimensions:
                raise Exception("No dimensions loaded. Create dimensions first.")

            month_sync_service.run_month_sync(
                umbrella_client=umbrella_client,
                mapping_engine=mapping_engine,
                account_key=effective_key,
                account_keys=effective_keys,
                month=request.month,
                vtag_filter_dimensions=request.vtag_filter_dimensions,
                filter_mode=request.filter_mode,
            )
        except Exception as e:
            log_timing(f"Month sync error: {e}")
            sync_service._starting = False
            sync_service._last_result = {"status": "error", "error_message": str(e),
                "sync_type": "month", "start_date": request.month, "end_date": request.month,
                "total_assets": 0, "matched_assets": 0, "unmatched_assets": 0, "elapsed_seconds": 0}
            sync_service._save_last_result()
            sync_service._engine = None

    background_tasks.add_task(run_sync)

    return {
        "status": "started",
        "message": f"Month sync started for {request.month}.",
        "month": request.month,
    }


@router.post("/sync/range")
async def start_range_sync(
    request: RangeSyncRequest,
    background_tasks: BackgroundTasks,
):
    """Start a range sync operation."""
    log_timing(f"[API] POST /sync/range: {request.start_date} to {request.end_date}, filter={request.filter_mode}")

    if sync_service._engine or sync_service._starting:
        raise HTTPException(status_code=409, detail="A sync is already running.")

    effective_key = request.account_key
    effective_keys = request.account_keys

    sync_service.mark_starting("range", request.start_date, request.end_date)

    def run_sync():
        try:
            if sync_service._cancelled:
                sync_service._starting = False
                return

            umbrella_client._ensure_authenticated()

            if sync_service._cancelled:
                sync_service._starting = False
                return

            if not mapping_engine.dimensions:
                mapping_engine.load_dimensions()
            if not mapping_engine.dimensions:
                raise Exception("No dimensions loaded. Create dimensions first.")

            sync_service.run_range_sync(
                umbrella_client=umbrella_client,
                mapping_engine=mapping_engine,
                account_key=effective_key,
                account_keys=effective_keys,
                start_date=request.start_date,
                end_date=request.end_date,
                vtag_filter_dimensions=request.vtag_filter_dimensions,
                filter_mode=request.filter_mode,
            )
        except Exception as e:
            log_timing(f"Range sync error: {e}")
            sync_service._starting = False
            sync_service._last_result = {"status": "error", "error_message": str(e),
                "sync_type": "range", "start_date": request.start_date, "end_date": request.end_date,
                "total_assets": 0, "matched_assets": 0, "unmatched_assets": 0, "elapsed_seconds": 0}
            sync_service._save_last_result()
            sync_service._engine = None

    background_tasks.add_task(run_sync)

    return {
        "status": "started",
        "message": "Range sync started. Use /status/events for progress.",
        "start_date": request.start_date,
        "end_date": request.end_date,
    }


@router.post("/sync/cancel")
async def cancel_sync():
    """Cancel the currently running operation."""
    log_timing("[API] POST /sync/cancel")
    sync_service.cancel()
    month_sync_service.cancel()
    simulation_service.cancel()
    return {"status": "cancelled", "message": "Cancellation requested."}


@router.get("/sync/progress")
async def get_sync_progress():
    """Get the current sync progress."""
    progress = sync_service.get_progress()
    log_timing(f"[API] GET /sync/progress -> status={progress.get('status')}, starting={sync_service._starting}, engine={'yes' if sync_service._engine else 'no'}")
    return progress


@router.get("/sync/last-result")
async def get_last_sync_result():
    """Get the result of the last sync/upload operation."""
    result = sync_service.get_last_result()
    if not result:
        return {"status": "none", "message": "No sync or upload has been run yet."}
    return result


@router.get("/sync/import-status")
async def get_import_status():
    """Poll Umbrella for the import processing status of the last upload."""
    import asyncio
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None, sync_service.get_import_status, umbrella_client
    )
    if not result:
        return {"status": "none", "message": "No upload IDs to check."}
    return result


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------


@router.post("/upload")
async def upload_vtags(
    request: UploadRequest,
    background_tasks: BackgroundTasks,
):
    """Upload vtags from a JSONL output file to Umbrella.

    Uses the sync_service upload logic with progress tracking
    visible via /status/sync/progress and the Monitor page.
    """
    try:
        umbrella_client._ensure_authenticated()
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))

    if not os.path.exists(request.jsonl_file):
        raise HTTPException(
            status_code=404, detail=f"JSONL file not found: {request.jsonl_file}"
        )

    def run_upload():
        try:
            sync_service.upload_file(
                umbrella_client=umbrella_client,
                jsonl_file=request.jsonl_file,
            )
        except Exception as e:
            log_timing(f"Upload background task error: {e}")

    background_tasks.add_task(run_upload)

    return {
        "status": "started",
        "message": "Upload started. Monitor via /status/sync/progress.",
        "file": request.jsonl_file,
    }


@router.get("/uploads")
async def list_uploads(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """List recent vtag uploads."""
    uploads = vtag_upload_service.list_uploads(limit=limit, offset=offset)
    return {"uploads": uploads, "count": len(uploads)}


@router.get("/uploads/{upload_id}")
async def get_upload(upload_id: int):
    """Get details for a specific upload."""
    upload = vtag_upload_service.get_upload(upload_id)
    if not upload:
        raise HTTPException(status_code=404, detail="Upload not found.")
    return upload


# ---------------------------------------------------------------------------
# File Downloads
# ---------------------------------------------------------------------------


@router.get("/files")
async def list_output_files():
    """List available output files."""
    output_dir = settings.output_dir
    files = []

    if os.path.exists(output_dir):
        for pattern in ["*.jsonl", "*.csv", "*.json"]:
            for filepath in glob.glob(os.path.join(output_dir, pattern)):
                stat = os.stat(filepath)
                files.append({
                    "name": os.path.basename(filepath),
                    "path": filepath,
                    "size_bytes": stat.st_size,
                    "size_mb": round(stat.st_size / (1024 * 1024), 2),
                    "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                })

    files.sort(key=lambda x: x["modified_at"], reverse=True)
    return {"files": files, "count": len(files), "output_dir": output_dir}


@router.get("/files/{filename}")
async def download_file(filename: str):
    """Download an output file."""
    filepath = os.path.join(settings.output_dir, filename)

    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail=f"File not found: {filename}")

    # Determine media type
    if filename.endswith(".jsonl"):
        media_type = "application/x-ndjson"
    elif filename.endswith(".csv"):
        media_type = "text/csv"
    elif filename.endswith(".json"):
        media_type = "application/json"
    else:
        media_type = "application/octet-stream"

    return FileResponse(
        filepath,
        media_type=media_type,
        filename=filename,
    )


# ---------------------------------------------------------------------------
# Cleanup & Reset
# ---------------------------------------------------------------------------


@router.post("/cleanup")
async def run_cleanup(request: CleanupRequest):
    """Run a cleanup operation."""
    if request.cleanup_type == "soft":
        result = cleanup_service.soft_cleanup(
            older_than_days=request.older_than_days
        )
    elif request.cleanup_type == "hard":
        result = cleanup_service.hard_cleanup()
    elif request.cleanup_type == "reset":
        result = cleanup_service.reset_all()
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid cleanup type: {request.cleanup_type}. "
                   f"Use 'soft', 'hard', or 'reset'.",
        )

    return result


@router.get("/cleanup/stats")
async def get_cleanup_stats():
    """Get current data sizes for cleanup planning."""
    return cleanup_service.get_cleanup_stats()


@router.post("/reset")
async def reset_all():
    """Full reset: clear stuck agent state. Does not delete data."""
    log_timing("[API] POST /reset - force reset")
    # Clear all sync state flags
    sync_service._engine = None
    sync_service._starting = False
    sync_service._starting_time = None
    sync_service._cancelled = False
    sync_service._upload_phase = ""
    sync_service._upload_progress = {}
    # Clear progress tracker
    progress_tracker.reset()
    return {"message": "Agent state reset successfully.", "state": "idle"}


# ---------------------------------------------------------------------------
# Month Sync Status
# ---------------------------------------------------------------------------


@router.get("/sync/months")
async def list_month_syncs(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """List recent month syncs."""
    syncs = month_sync_service.list_month_syncs(limit=limit, offset=offset)
    return {"syncs": syncs, "count": len(syncs)}


@router.get("/sync/months/{sync_id}")
async def get_month_sync_status(sync_id: int):
    """Get status of a specific month sync."""
    status = month_sync_service.get_month_sync_status(sync_id)
    if not status:
        raise HTTPException(status_code=404, detail="Month sync not found.")
    return status
