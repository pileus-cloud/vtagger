"""
VTagger Stats API.

Endpoints for daily statistics, weekly trends, mapping breakdowns,
and summary metrics.

Ported from BPVtagger with:
- bizmapping_matches -> dimension_matches in all SQL queries and response models
- bizmapping_percentage -> dimension_percentage
- avg_bizmapping_percentage -> avg_dimension_percentage
- "bizmapping" -> "dimension_match" in mapping breakdown source names
"""

import math
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.database import execute_query, get_db

router = APIRouter(prefix="/stats", tags=["stats"])


# ---------------------------------------------------------------------------
# Response Models
# ---------------------------------------------------------------------------


class DailyStat(BaseModel):
    """Single daily statistics record."""
    stat_date: str
    total_statements: int = 0
    tagged_statements: int = 0
    dimension_matches: int = 0
    unmatched_statements: int = 0
    match_rate: float = 0.0
    dimension_percentage: float = 0.0
    api_calls: int = 0
    errors: int = 0


class StatsSummary(BaseModel):
    """Summary statistics across a date range."""
    start_date: str
    end_date: str
    total_days: int = 0
    total_statements: int = 0
    total_tagged: int = 0
    total_dimension_matches: int = 0
    total_unmatched: int = 0
    avg_match_rate: float = 0.0
    avg_dimension_percentage: float = 0.0
    total_api_calls: int = 0
    total_errors: int = 0


class WeeklyTrend(BaseModel):
    """Weekly trend data point."""
    week_start: str
    week_end: str
    total_statements: int = 0
    tagged_statements: int = 0
    dimension_matches: int = 0
    unmatched_statements: int = 0
    match_rate: float = 0.0
    dimension_percentage: float = 0.0


class MappingBreakdown(BaseModel):
    """Breakdown of mapping sources."""
    source: str
    count: int = 0
    percentage: float = 0.0


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/daily")
async def get_daily_stats(
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(30, ge=1, le=365),
):
    """Get daily statistics for a date range."""
    if start_date and end_date:
        rows = execute_query(
            """SELECT stat_date, total_statements, tagged_statements,
            dimension_matches, unmatched_statements, match_rate,
            api_calls, errors
            FROM daily_stats
            WHERE stat_date >= ? AND stat_date <= ?
            ORDER BY stat_date DESC""",
            (start_date, end_date),
        )
    elif start_date:
        rows = execute_query(
            """SELECT stat_date, total_statements, tagged_statements,
            dimension_matches, unmatched_statements, match_rate,
            api_calls, errors
            FROM daily_stats
            WHERE stat_date >= ?
            ORDER BY stat_date DESC
            LIMIT ?""",
            (start_date, limit),
        )
    else:
        rows = execute_query(
            """SELECT stat_date, total_statements, tagged_statements,
            dimension_matches, unmatched_statements, match_rate,
            api_calls, errors
            FROM daily_stats
            ORDER BY stat_date DESC
            LIMIT ?""",
            (limit,),
        )

    stats = []
    for row in rows:
        total = row["total_statements"] or 0
        dim_matches = row["dimension_matches"] or 0
        dim_pct = (dim_matches / total * 100) if total > 0 else 0.0

        stats.append(DailyStat(
            stat_date=row["stat_date"],
            total_statements=total,
            tagged_statements=row["tagged_statements"] or 0,
            dimension_matches=dim_matches,
            unmatched_statements=row["unmatched_statements"] or 0,
            match_rate=row["match_rate"] or 0.0,
            dimension_percentage=round(dim_pct, 2),
            api_calls=row["api_calls"] or 0,
            errors=row["errors"] or 0,
        ))

    return {"daily_stats": [s.model_dump() for s in stats], "count": len(stats)}


@router.get("/summary")
async def get_stats_summary(
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    days: int = Query(30, ge=1, le=365, description="Number of days to look back"),
):
    """Get summary statistics for a date range."""
    if not start_date:
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    rows = execute_query(
        """SELECT
            COUNT(*) as total_days,
            COALESCE(SUM(total_statements), 0) as total_statements,
            COALESCE(SUM(tagged_statements), 0) as total_tagged,
            COALESCE(SUM(dimension_matches), 0) as total_dimension_matches,
            COALESCE(SUM(unmatched_statements), 0) as total_unmatched,
            COALESCE(AVG(match_rate), 0) as avg_match_rate,
            COALESCE(SUM(api_calls), 0) as total_api_calls,
            COALESCE(SUM(errors), 0) as total_errors
        FROM daily_stats
        WHERE stat_date >= ? AND stat_date <= ?""",
        (start_date, end_date),
    )

    if not rows:
        return StatsSummary(start_date=start_date, end_date=end_date).model_dump()

    row = rows[0]
    total_stmts = row["total_statements"] or 0
    total_dim = row["total_dimension_matches"] or 0
    avg_dim_pct = (total_dim / total_stmts * 100) if total_stmts > 0 else 0.0

    summary = StatsSummary(
        start_date=start_date,
        end_date=end_date,
        total_days=row["total_days"] or 0,
        total_statements=total_stmts,
        total_tagged=row["total_tagged"] or 0,
        total_dimension_matches=total_dim,
        total_unmatched=row["total_unmatched"] or 0,
        avg_match_rate=round(row["avg_match_rate"] or 0.0, 2),
        avg_dimension_percentage=round(avg_dim_pct, 2),
        total_api_calls=row["total_api_calls"] or 0,
        total_errors=row["total_errors"] or 0,
    )

    return summary.model_dump()


@router.get("/weekly-trends")
async def get_weekly_trends(
    weeks: int = Query(12, ge=1, le=52, description="Number of weeks to look back"),
):
    """Get weekly trend data."""
    end_date = datetime.now()
    start_date = end_date - timedelta(weeks=weeks)

    rows = execute_query(
        """SELECT stat_date, total_statements, tagged_statements,
        dimension_matches, unmatched_statements, match_rate
        FROM daily_stats
        WHERE stat_date >= ? AND stat_date <= ?
        ORDER BY stat_date ASC""",
        (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")),
    )

    # Group by week (Monday start)
    weekly_data: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        row_date = datetime.strptime(row["stat_date"], "%Y-%m-%d")
        # Monday of this week
        week_start = row_date - timedelta(days=row_date.weekday())
        week_end = week_start + timedelta(days=6)
        week_key = week_start.strftime("%Y-%m-%d")

        if week_key not in weekly_data:
            weekly_data[week_key] = {
                "week_start": week_key,
                "week_end": week_end.strftime("%Y-%m-%d"),
                "total_statements": 0,
                "tagged_statements": 0,
                "dimension_matches": 0,
                "unmatched_statements": 0,
            }

        wd = weekly_data[week_key]
        wd["total_statements"] += row["total_statements"] or 0
        wd["tagged_statements"] += row["tagged_statements"] or 0
        wd["dimension_matches"] += row["dimension_matches"] or 0
        wd["unmatched_statements"] += row["unmatched_statements"] or 0

    # Calculate percentages
    trends = []
    for week_key in sorted(weekly_data.keys()):
        wd = weekly_data[week_key]
        total = wd["total_statements"]
        tagged = wd["tagged_statements"]
        dim_matches = wd["dimension_matches"]

        match_rate = (tagged / total * 100) if total > 0 else 0.0
        dim_pct = (dim_matches / total * 100) if total > 0 else 0.0

        trends.append(WeeklyTrend(
            week_start=wd["week_start"],
            week_end=wd["week_end"],
            total_statements=total,
            tagged_statements=tagged,
            dimension_matches=dim_matches,
            unmatched_statements=wd["unmatched_statements"],
            match_rate=round(match_rate, 2),
            dimension_percentage=round(dim_pct, 2),
        ))

    return {
        "weekly_trends": [t.model_dump() for t in trends],
        "count": len(trends),
    }


@router.get("/mapping-breakdown")
async def get_mapping_breakdown(
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    days: int = Query(30, ge=1, le=365),
):
    """Get breakdown of mapping sources (tagged vs dimension_match vs unmatched)."""
    if not start_date:
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    rows = execute_query(
        """SELECT
            COALESCE(SUM(total_statements), 0) as total,
            COALESCE(SUM(tagged_statements), 0) as tagged,
            COALESCE(SUM(dimension_matches), 0) as dimension_match,
            COALESCE(SUM(unmatched_statements), 0) as unmatched
        FROM daily_stats
        WHERE stat_date >= ? AND stat_date <= ?""",
        (start_date, end_date),
    )

    if not rows or rows[0]["total"] == 0:
        return {
            "breakdown": [],
            "total": 0,
            "start_date": start_date,
            "end_date": end_date,
        }

    row = rows[0]
    total = row["total"] or 1  # avoid division by zero

    breakdown = [
        MappingBreakdown(
            source="tagged",
            count=row["tagged"] or 0,
            percentage=round((row["tagged"] or 0) / total * 100, 2),
        ),
        MappingBreakdown(
            source="dimension_match",
            count=row["dimension_match"] or 0,
            percentage=round((row["dimension_match"] or 0) / total * 100, 2),
        ),
        MappingBreakdown(
            source="unmatched",
            count=row["unmatched"] or 0,
            percentage=round((row["unmatched"] or 0) / total * 100, 2),
        ),
    ]

    return {
        "breakdown": [b.model_dump() for b in breakdown],
        "total": row["total"],
        "start_date": start_date,
        "end_date": end_date,
    }


@router.get("/recent")
async def get_recent_activity(
    limit: int = Query(10, ge=1, le=50),
):
    """Get recent tagging activity (jobs + stats combined)."""
    jobs = execute_query(
        """SELECT id, job_date, status, total_statements, matched_statements,
        unmatched_statements, dimensions_applied, started_at, completed_at
        FROM tagging_jobs
        ORDER BY created_at DESC
        LIMIT ?""",
        (limit,),
    )

    activity = []
    for job in jobs:
        total = job["total_statements"] or 0
        matched = job["matched_statements"] or 0
        match_rate = (matched / total * 100) if total > 0 else 0.0

        activity.append({
            "job_id": job["id"],
            "date": job["job_date"],
            "status": job["status"],
            "total_statements": total,
            "matched_statements": matched,
            "unmatched_statements": job["unmatched_statements"] or 0,
            "dimensions_applied": job["dimensions_applied"] or 0,
            "match_rate": round(match_rate, 2),
            "started_at": job["started_at"],
            "completed_at": job["completed_at"],
        })

    return {"recent_activity": activity, "count": len(activity)}
