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
from app.api.auth import get_login_key

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


class WeekSyncRequest(BaseModel):
    """Request to run a weekly sync."""
    account_key: str
    start_date: str
    end_date: str
    vtag_filter_dimensions: Optional[List[str]] = None


class MonthSyncRequest(BaseModel):
    """Request to run a month sync."""
    account_key: str
    month: str
    vtag_filter_dimensions: Optional[List[str]] = None


class RangeSyncRequest(BaseModel):
    """Request to run a range sync."""
    account_key: str
    start_date: str
    end_date: str
    vtag_filter_dimensions: Optional[List[str]] = None


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

    has_key = get_login_key() is not None

    return HealthResponse(
        status="healthy" if db_status == "ok" else "degraded",
        version="1.0.0",
        database=db_status,
        dimensions_loaded=len(mapping_engine.dimensions),
        api_key_configured=has_key,
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
    """Get current progress state (polling alternative to SSE)."""
    return progress_tracker.to_dict()


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

    # Ensure authenticated
    login_key = get_login_key()
    if not login_key:
        raise HTTPException(
            status_code=401, detail="No login key configured."
        )

    if not umbrella_client.is_authenticated():
        success = umbrella_client.authenticate(login_key)
        if not success:
            raise HTTPException(
                status_code=401, detail="Authentication failed."
            )

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
    """Start a weekly sync operation."""
    if progress_tracker.is_running:
        raise HTTPException(
            status_code=409, detail="An operation is already running."
        )

    login_key = get_login_key()
    if not login_key:
        raise HTTPException(status_code=401, detail="No login key configured.")

    if not umbrella_client.is_authenticated():
        success = umbrella_client.authenticate(login_key)
        if not success:
            raise HTTPException(status_code=401, detail="Authentication failed.")

    if not mapping_engine.dimensions:
        mapping_engine.load_dimensions()

    if not mapping_engine.dimensions:
        raise HTTPException(
            status_code=400, detail="No dimensions loaded. Create dimensions first."
        )

    def run_sync():
        try:
            sync_service.run_week_sync(
                umbrella_client=umbrella_client,
                mapping_engine=mapping_engine,
                account_key=request.account_key,
                start_date=request.start_date,
                end_date=request.end_date,
                vtag_filter_dimensions=request.vtag_filter_dimensions,
            )
        except Exception as e:
            log_timing(f"Week sync background task error: {e}")

    background_tasks.add_task(run_sync)

    return {
        "status": "started",
        "message": "Week sync started. Use /status/events for progress.",
        "start_date": request.start_date,
        "end_date": request.end_date,
    }


@router.post("/sync/month")
async def start_month_sync(
    request: MonthSyncRequest,
    background_tasks: BackgroundTasks,
):
    """Start a month sync operation."""
    if progress_tracker.is_running:
        raise HTTPException(
            status_code=409, detail="An operation is already running."
        )

    login_key = get_login_key()
    if not login_key:
        raise HTTPException(status_code=401, detail="No login key configured.")

    if not umbrella_client.is_authenticated():
        success = umbrella_client.authenticate(login_key)
        if not success:
            raise HTTPException(status_code=401, detail="Authentication failed.")

    if not mapping_engine.dimensions:
        mapping_engine.load_dimensions()

    if not mapping_engine.dimensions:
        raise HTTPException(
            status_code=400, detail="No dimensions loaded. Create dimensions first."
        )

    def run_sync():
        try:
            month_sync_service.run_month_sync(
                umbrella_client=umbrella_client,
                mapping_engine=mapping_engine,
                account_key=request.account_key,
                month=request.month,
                vtag_filter_dimensions=request.vtag_filter_dimensions,
            )
        except Exception as e:
            log_timing(f"Month sync background task error: {e}")

    background_tasks.add_task(run_sync)

    return {
        "status": "started",
        "message": f"Month sync started for {request.month}. Use /status/events for progress.",
        "month": request.month,
    }


@router.post("/sync/range")
async def start_range_sync(
    request: RangeSyncRequest,
    background_tasks: BackgroundTasks,
):
    """Start a range sync operation."""
    if progress_tracker.is_running:
        raise HTTPException(
            status_code=409, detail="An operation is already running."
        )

    login_key = get_login_key()
    if not login_key:
        raise HTTPException(status_code=401, detail="No login key configured.")

    if not umbrella_client.is_authenticated():
        success = umbrella_client.authenticate(login_key)
        if not success:
            raise HTTPException(status_code=401, detail="Authentication failed.")

    if not mapping_engine.dimensions:
        mapping_engine.load_dimensions()

    if not mapping_engine.dimensions:
        raise HTTPException(
            status_code=400, detail="No dimensions loaded. Create dimensions first."
        )

    def run_sync():
        try:
            sync_service.run_range_sync(
                umbrella_client=umbrella_client,
                mapping_engine=mapping_engine,
                account_key=request.account_key,
                start_date=request.start_date,
                end_date=request.end_date,
                vtag_filter_dimensions=request.vtag_filter_dimensions,
            )
        except Exception as e:
            log_timing(f"Range sync background task error: {e}")

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
    sync_service.cancel()
    month_sync_service.cancel()
    simulation_service.cancel()
    return {"status": "cancelled", "message": "Cancellation requested."}


@router.get("/sync/progress")
async def get_sync_progress():
    """Get the current sync progress."""
    return sync_service.get_progress()


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------


@router.post("/upload")
async def upload_vtags(
    request: UploadRequest,
    background_tasks: BackgroundTasks,
):
    """Upload vtags from a JSONL output file to Umbrella."""
    login_key = get_login_key()
    if not login_key:
        raise HTTPException(status_code=401, detail="No login key configured.")

    if not umbrella_client.is_authenticated():
        success = umbrella_client.authenticate(login_key)
        if not success:
            raise HTTPException(status_code=401, detail="Authentication failed.")

    if not os.path.exists(request.jsonl_file):
        raise HTTPException(
            status_code=404, detail=f"JSONL file not found: {request.jsonl_file}"
        )

    def run_upload():
        try:
            vtag_upload_service.upload_from_jsonl(
                umbrella_client=umbrella_client,
                account_key=request.account_key,
                jsonl_file=request.jsonl_file,
                group_by_payer=request.group_by_payer,
                description=request.description,
            )
        except Exception as e:
            log_timing(f"Upload background task error: {e}")

    background_tasks.add_task(run_upload)

    return {
        "status": "started",
        "message": "Upload started.",
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
    """Full reset: remove ALL data including dimensions and API keys."""
    result = cleanup_service.reset_all()
    return result


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
