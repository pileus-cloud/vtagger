"""
VTagger Jobs API.

Endpoints for viewing and managing tagging jobs.

Ported from BPVtagger with:
- bizmapping_matches -> dimension_matches in JobResponse and SQL queries
"""

import math
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.database import execute_query, execute_write, get_db

router = APIRouter(prefix="/jobs", tags=["jobs"])


# ---------------------------------------------------------------------------
# Response Models
# ---------------------------------------------------------------------------


class JobResponse(BaseModel):
    """Response model for a tagging job."""
    id: int
    job_date: str
    status: str
    total_statements: int = 0
    processed_statements: int = 0
    matched_statements: int = 0
    unmatched_statements: int = 0
    dimensions_applied: int = 0
    dimension_matches: int = 0
    match_rate: float = 0.0
    error_message: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class JobListResponse(BaseModel):
    """Response model for a list of jobs."""
    jobs: List[JobResponse]
    count: int
    total: int
    page: int
    page_size: int
    total_pages: int


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/")
async def list_jobs(
    status: Optional[str] = Query(None, description="Filter by status"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Jobs per page"),
):
    """List tagging jobs with optional status filter and pagination."""
    # Build query
    where_clause = ""
    params: list = []

    if status:
        where_clause = "WHERE status = ?"
        params.append(status)

    # Get total count
    count_rows = execute_query(
        f"SELECT COUNT(*) as cnt FROM tagging_jobs {where_clause}",
        tuple(params),
    )
    total = count_rows[0]["cnt"] if count_rows else 0
    total_pages = max(1, math.ceil(total / page_size))

    # Get paginated results
    offset = (page - 1) * page_size
    rows = execute_query(
        f"""SELECT id, job_date, status, total_statements, processed_statements,
        matched_statements, unmatched_statements, dimensions_applied,
        error_message, started_at, completed_at, created_at, updated_at
        FROM tagging_jobs
        {where_clause}
        ORDER BY created_at DESC
        LIMIT ? OFFSET ?""",
        tuple(params + [page_size, offset]),
    )

    jobs = []
    for row in rows:
        total_stmts = row["total_statements"] or 0
        matched = row["matched_statements"] or 0
        dims_applied = row["dimensions_applied"] or 0
        match_rate = (matched / total_stmts * 100) if total_stmts > 0 else 0.0

        jobs.append(JobResponse(
            id=row["id"],
            job_date=row["job_date"],
            status=row["status"],
            total_statements=total_stmts,
            processed_statements=row["processed_statements"] or 0,
            matched_statements=matched,
            unmatched_statements=row["unmatched_statements"] or 0,
            dimensions_applied=dims_applied,
            dimension_matches=dims_applied,  # dimensions_applied is the dimension match count
            match_rate=round(match_rate, 2),
            error_message=row["error_message"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        ))

    return JobListResponse(
        jobs=jobs,
        count=len(jobs),
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
    )


@router.get("/{job_id}")
async def get_job(job_id: int):
    """Get details for a specific tagging job."""
    rows = execute_query(
        """SELECT id, job_date, status, total_statements, processed_statements,
        matched_statements, unmatched_statements, dimensions_applied,
        error_message, started_at, completed_at, created_at, updated_at
        FROM tagging_jobs WHERE id = ?""",
        (job_id,),
    )

    if not rows:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found.")

    row = rows[0]
    total_stmts = row["total_statements"] or 0
    matched = row["matched_statements"] or 0
    dims_applied = row["dimensions_applied"] or 0
    match_rate = (matched / total_stmts * 100) if total_stmts > 0 else 0.0

    return JobResponse(
        id=row["id"],
        job_date=row["job_date"],
        status=row["status"],
        total_statements=total_stmts,
        processed_statements=row["processed_statements"] or 0,
        matched_statements=matched,
        unmatched_statements=row["unmatched_statements"] or 0,
        dimensions_applied=dims_applied,
        dimension_matches=dims_applied,
        match_rate=round(match_rate, 2),
        error_message=row["error_message"],
        started_at=row["started_at"],
        completed_at=row["completed_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


@router.delete("/{job_id}")
async def delete_job(job_id: int):
    """Delete a tagging job record."""
    rows = execute_query(
        "SELECT id FROM tagging_jobs WHERE id = ?", (job_id,)
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found.")

    execute_write("DELETE FROM tagging_jobs WHERE id = ?", (job_id,))
    return {"deleted": job_id, "message": f"Job {job_id} deleted."}


@router.get("/{job_id}/stats")
async def get_job_stats(job_id: int):
    """Get statistics for a specific job, including dimension matches from daily_stats."""
    # Get job
    rows = execute_query(
        """SELECT id, job_date, status, total_statements, matched_statements,
        unmatched_statements, dimensions_applied
        FROM tagging_jobs WHERE id = ?""",
        (job_id,),
    )

    if not rows:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found.")

    job = rows[0]
    job_date = job["job_date"]

    # Get daily stats for the job's date
    daily_rows = execute_query(
        """SELECT stat_date, total_statements, tagged_statements,
        dimension_matches, unmatched_statements, match_rate
        FROM daily_stats WHERE stat_date = ?""",
        (job_date,),
    )

    daily_stat = None
    if daily_rows:
        dr = daily_rows[0]
        total = dr["total_statements"] or 0
        dim_matches = dr["dimension_matches"] or 0
        dim_pct = (dim_matches / total * 100) if total > 0 else 0.0

        daily_stat = {
            "stat_date": dr["stat_date"],
            "total_statements": total,
            "tagged_statements": dr["tagged_statements"] or 0,
            "dimension_matches": dim_matches,
            "unmatched_statements": dr["unmatched_statements"] or 0,
            "match_rate": dr["match_rate"] or 0.0,
            "dimension_percentage": round(dim_pct, 2),
        }

    total_stmts = job["total_statements"] or 0
    matched = job["matched_statements"] or 0
    dims = job["dimensions_applied"] or 0
    match_rate = (matched / total_stmts * 100) if total_stmts > 0 else 0.0

    return {
        "job": {
            "id": job["id"],
            "job_date": job_date,
            "status": job["status"],
            "total_statements": total_stmts,
            "matched_statements": matched,
            "unmatched_statements": job["unmatched_statements"] or 0,
            "dimensions_applied": dims,
            "dimension_matches": dims,
            "match_rate": round(match_rate, 2),
        },
        "daily_stat": daily_stat,
    }


@router.post("/cleanup")
async def cleanup_old_jobs(
    older_than_days: int = Query(90, ge=1, le=365),
):
    """Delete jobs older than specified number of days."""
    cutoff_date = (
        datetime.now() - timedelta(days=older_than_days)
    ).strftime("%Y-%m-%d")

    with get_db() as conn:
        cursor = conn.execute(
            "DELETE FROM tagging_jobs WHERE created_at < ?",
            (cutoff_date,),
        )
        deleted_count = cursor.rowcount

    return {
        "deleted": deleted_count,
        "cutoff_date": cutoff_date,
        "message": f"Deleted {deleted_count} jobs older than {older_than_days} days.",
    }
