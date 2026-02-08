"""
VTagger Dimensions API.

CRUD operations for virtual tag dimensions (mapping rules).
Replaces the legacy bizmapping.py and mapping.py endpoints.
"""

import hashlib
import json
import math
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.database import execute_query, execute_write
from app.services.mapping_engine import mapping_engine
from app.services.dsl_parser import validate_dimension_json
from app.services.tag_discovery import tag_discovery_service

router = APIRouter(prefix="/dimensions", tags=["dimensions"])


# ---------------------------------------------------------------------------
# Request / Response Models
# ---------------------------------------------------------------------------


class StatementModel(BaseModel):
    """A single mapping statement (match -> value)."""
    matchExpression: str
    valueExpression: str


class DimensionCreate(BaseModel):
    """Payload for creating a new dimension."""
    vtag_name: str
    index: int
    kind: str = "TAG_MAPPING"
    defaultValue: str = "Unallocated"
    source: str = "TAGS"
    statements: List[StatementModel] = Field(default_factory=list)


class DimensionUpdate(BaseModel):
    """Payload for updating an existing dimension."""
    index: Optional[int] = None
    kind: Optional[str] = None
    defaultValue: Optional[str] = None
    statements: Optional[List[StatementModel]] = None


class ResolveRequest(BaseModel):
    """Payload for resolving tags through all dimensions."""
    tags: Dict[str, str]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_dimension_json(
    vtag_name: str,
    index: int,
    kind: str,
    default_value: str,
    source: str,
    statements: List[StatementModel],
) -> dict:
    """Build the canonical JSON blob stored in dimensions.content."""
    return {
        "vtagName": vtag_name,
        "index": index,
        "kind": kind,
        "defaultValue": default_value,
        "source": source,
        "statements": [s.model_dump() for s in statements],
    }


def _compute_checksum(content: dict) -> str:
    """MD5 hex digest of the canonical JSON representation."""
    raw = json.dumps(content, sort_keys=True, separators=(",", ":"))
    return hashlib.md5(raw.encode()).hexdigest()


def _record_history(
    vtag_name: str,
    action: str,
    previous_content: Optional[str] = None,
    new_content: Optional[str] = None,
    source: str = "web",
):
    """Insert a row into dimension_history."""
    execute_write(
        "INSERT INTO dimension_history (vtag_name, action, previous_content, new_content, source) "
        "VALUES (?, ?, ?, ?, ?)",
        (vtag_name, action, previous_content, new_content, source),
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/discovered-tags")
async def list_discovered_tags():
    """Return all discovered physical tag keys with sample values."""
    tags = tag_discovery_service.get_discovered_tags()
    return {"discovered_tags": tags, "count": len(tags)}


@router.post("/validate")
async def validate_dimension(payload: DimensionCreate):
    """Validate a dimension JSON blob without persisting it."""
    content = _build_dimension_json(
        payload.vtag_name,
        payload.index,
        payload.kind,
        payload.defaultValue,
        payload.source,
        payload.statements,
    )
    errors = validate_dimension_json(content)
    if errors:
        return {"valid": False, "errors": errors}
    return {"valid": True, "errors": []}


@router.post("/resolve")
async def resolve_tags(request: ResolveRequest):
    """Resolve a set of physical tags through all loaded dimensions."""
    results = {}
    for dim in mapping_engine.dimensions:
        vtag_name = dim.get("vtagName", "")
        value = mapping_engine.resolve(vtag_name, request.tags)
        results[vtag_name] = value
    return {"resolved": results}


@router.get("/")
async def list_dimensions():
    """List all dimensions (without full statement bodies)."""
    rows = execute_query(
        "SELECT vtag_name, index_number, kind, default_value, source, "
        "statement_count, checksum, created_at, updated_at "
        "FROM dimensions ORDER BY index_number ASC"
    )
    dimensions = []
    for row in rows:
        dimensions.append({
            "vtag_name": row["vtag_name"],
            "index": row["index_number"],
            "kind": row["kind"],
            "defaultValue": row["default_value"],
            "source": row["source"],
            "statement_count": row["statement_count"],
            "checksum": row["checksum"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        })
    return {"dimensions": dimensions, "count": len(dimensions)}


@router.post("/", status_code=201)
async def create_dimension(payload: DimensionCreate):
    """Create a new dimension."""
    # Check for duplicate
    existing = execute_query(
        "SELECT id FROM dimensions WHERE vtag_name = ?", (payload.vtag_name,)
    )
    if existing:
        raise HTTPException(status_code=409, detail=f"Dimension '{payload.vtag_name}' already exists")

    # Build and validate
    content = _build_dimension_json(
        payload.vtag_name,
        payload.index,
        payload.kind,
        payload.defaultValue,
        payload.source,
        payload.statements,
    )
    errors = validate_dimension_json(content)
    if errors:
        raise HTTPException(status_code=422, detail={"validation_errors": errors})

    checksum = _compute_checksum(content)
    content_json = json.dumps(content)

    # Persist
    execute_write(
        "INSERT INTO dimensions (vtag_name, index_number, kind, default_value, source, "
        "content, statement_count, checksum) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            payload.vtag_name,
            payload.index,
            payload.kind,
            payload.defaultValue,
            payload.source,
            content_json,
            len(payload.statements),
            checksum,
        ),
    )

    # History
    _record_history(payload.vtag_name, "created", new_content=content_json)

    # Reload engine
    mapping_engine.load_dimensions()

    return {
        "vtag_name": payload.vtag_name,
        "index": payload.index,
        "statement_count": len(payload.statements),
        "checksum": checksum,
    }


@router.get("/{vtag_name}")
async def get_dimension(
    vtag_name: str,
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=500, description="Statements per page"),
):
    """Get a dimension with paginated statements."""
    rows = execute_query(
        "SELECT vtag_name, index_number, kind, default_value, source, "
        "content, statement_count, checksum, created_at, updated_at "
        "FROM dimensions WHERE vtag_name = ?",
        (vtag_name,),
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"Dimension '{vtag_name}' not found")

    row = rows[0]

    # Parse content for statements
    try:
        content = json.loads(row["content"] or "{}")
    except (json.JSONDecodeError, TypeError):
        content = {}

    all_statements = content.get("statements", [])
    total = len(all_statements)
    total_pages = max(1, math.ceil(total / page_size))

    start = (page - 1) * page_size
    end = start + page_size
    page_statements = all_statements[start:end]

    return {
        "vtag_name": row["vtag_name"],
        "index": row["index_number"],
        "kind": row["kind"],
        "defaultValue": row["default_value"],
        "source": row["source"],
        "statement_count": row["statement_count"],
        "checksum": row["checksum"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "statements": page_statements,
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total_statements": total,
            "total_pages": total_pages,
        },
    }


@router.put("/{vtag_name}")
async def update_dimension(vtag_name: str, payload: DimensionUpdate):
    """Update an existing dimension."""
    rows = execute_query(
        "SELECT id, content, index_number, kind, default_value, source "
        "FROM dimensions WHERE vtag_name = ?",
        (vtag_name,),
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"Dimension '{vtag_name}' not found")

    row = rows[0]
    previous_content = row["content"]

    # Parse existing content
    try:
        existing = json.loads(previous_content or "{}")
    except (json.JSONDecodeError, TypeError):
        existing = {}

    # Merge updates
    new_index = payload.index if payload.index is not None else row["index_number"]
    new_kind = payload.kind if payload.kind is not None else row["kind"]
    new_default = payload.defaultValue if payload.defaultValue is not None else row["default_value"]
    source = row["source"]

    if payload.statements is not None:
        new_statements = payload.statements
    else:
        # Keep existing statements
        raw_stmts = existing.get("statements", [])
        new_statements = [StatementModel(**s) for s in raw_stmts]

    # Build and validate
    content = _build_dimension_json(
        vtag_name, new_index, new_kind, new_default, source, new_statements
    )
    errors = validate_dimension_json(content)
    if errors:
        raise HTTPException(status_code=422, detail={"validation_errors": errors})

    checksum = _compute_checksum(content)
    content_json = json.dumps(content)

    # Persist
    execute_write(
        "UPDATE dimensions SET index_number = ?, kind = ?, default_value = ?, "
        "content = ?, statement_count = ?, checksum = ?, updated_at = CURRENT_TIMESTAMP "
        "WHERE vtag_name = ?",
        (
            new_index,
            new_kind,
            new_default,
            content_json,
            len(new_statements),
            checksum,
            vtag_name,
        ),
    )

    # History
    _record_history(vtag_name, "updated", previous_content=previous_content, new_content=content_json)

    # Reload engine
    mapping_engine.load_dimensions()

    return {
        "vtag_name": vtag_name,
        "index": new_index,
        "statement_count": len(new_statements),
        "checksum": checksum,
    }


@router.delete("/{vtag_name}")
async def delete_dimension(vtag_name: str):
    """Delete a dimension."""
    rows = execute_query(
        "SELECT id, content FROM dimensions WHERE vtag_name = ?", (vtag_name,)
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"Dimension '{vtag_name}' not found")

    previous_content = rows[0]["content"]

    execute_write("DELETE FROM dimensions WHERE vtag_name = ?", (vtag_name,))

    # History
    _record_history(vtag_name, "deleted", previous_content=previous_content)

    # Reload engine
    mapping_engine.load_dimensions()

    return {"deleted": vtag_name}


@router.get("/{vtag_name}/search")
async def search_statements(
    vtag_name: str,
    q: str = Query("", description="Search query for match or value expressions"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    """Search statements within a dimension by match or value expression."""
    rows = execute_query(
        "SELECT content FROM dimensions WHERE vtag_name = ?", (vtag_name,)
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"Dimension '{vtag_name}' not found")

    try:
        content = json.loads(rows[0]["content"] or "{}")
    except (json.JSONDecodeError, TypeError):
        content = {}

    all_statements = content.get("statements", [])

    # Filter by query
    query_lower = q.lower()
    if query_lower:
        filtered = [
            s for s in all_statements
            if query_lower in s.get("matchExpression", "").lower()
            or query_lower in s.get("valueExpression", "").lower()
        ]
    else:
        filtered = all_statements

    total = len(filtered)
    total_pages = max(1, math.ceil(total / page_size))

    start = (page - 1) * page_size
    end = start + page_size
    page_statements = filtered[start:end]

    return {
        "vtag_name": vtag_name,
        "query": q,
        "statements": page_statements,
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total_statements": total,
            "total_pages": total_pages,
        },
    }


@router.get("/{vtag_name}/history")
async def get_dimension_history(
    vtag_name: str,
    limit: int = Query(50, ge=1, le=200),
):
    """Get change history for a dimension."""
    rows = execute_query(
        "SELECT id, vtag_name, action, previous_content, new_content, source, created_at "
        "FROM dimension_history WHERE vtag_name = ? ORDER BY created_at DESC LIMIT ?",
        (vtag_name, limit),
    )
    entries = []
    for row in rows:
        entries.append({
            "id": row["id"],
            "vtag_name": row["vtag_name"],
            "action": row["action"],
            "previous_content": row["previous_content"],
            "new_content": row["new_content"],
            "source": row["source"],
            "created_at": row["created_at"],
        })
    return {"history": entries, "count": len(entries)}
