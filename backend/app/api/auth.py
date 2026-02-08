"""
VTagger Auth API.

Endpoints for API key management and validation.
"""

import os
import secrets
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Header, Security
from fastapi.security import APIKeyHeader
from pydantic import BaseModel

from app.database import execute_query, execute_write

router = APIRouter(prefix="/auth", tags=["auth"])

# Security scheme
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


# --- Models ---

class APIKeyCreate(BaseModel):
    """Request model for creating an API key."""
    name: str
    key: Optional[str] = None  # If not provided, one will be generated


class APIKeyResponse(BaseModel):
    """Response model for API key operations."""
    name: str
    key: str
    message: str


class APIKeyValidation(BaseModel):
    """Response model for key validation."""
    valid: bool
    name: Optional[str] = None
    message: str


class APIKeyInfo(BaseModel):
    """Response model for key existence check."""
    exists: bool
    name: Optional[str] = None


# --- Dependency functions ---

async def get_api_key(
    api_key: Optional[str] = Security(api_key_header),
) -> str:
    """Require a valid API key. Raises 401 if invalid or missing."""
    if not api_key:
        raise HTTPException(
            status_code=401,
            detail="Missing API key. Provide X-API-Key header.",
        )

    rows = execute_query(
        "SELECT name FROM api_keys WHERE key = ?",
        (api_key,),
    )

    if not rows:
        raise HTTPException(
            status_code=401,
            detail="Invalid API key.",
        )

    return api_key


async def optional_api_key(
    api_key: Optional[str] = Security(api_key_header),
) -> Optional[str]:
    """Get API key if provided, but don't require it."""
    if not api_key:
        return None

    rows = execute_query(
        "SELECT name FROM api_keys WHERE key = ?",
        (api_key,),
    )

    if not rows:
        return None

    return api_key


async def ensure_api_key_exists() -> str:
    """Ensure at least one API key exists. Create default if none found."""
    rows = execute_query("SELECT key FROM api_keys LIMIT 1")

    if rows:
        return rows[0]["key"]

    # Create a default key
    default_key = secrets.token_urlsafe(32)
    execute_write(
        "INSERT INTO api_keys (name, key) VALUES (?, ?)",
        ("default", default_key),
    )
    print(f"[AUTH] Created default API key: {default_key}")
    return default_key


def get_login_key() -> Optional[str]:
    """Get the login/default API key for frontend authentication."""
    rows = execute_query(
        "SELECT key FROM api_keys WHERE name = 'default' OR name = 'login' LIMIT 1"
    )
    if rows:
        return rows[0]["key"]
    return None


# --- Endpoints ---

@router.post("/keys", response_model=APIKeyResponse)
async def create_api_key(body: APIKeyCreate):
    """Create a new API key."""
    # Check if name already exists
    existing = execute_query(
        "SELECT key FROM api_keys WHERE name = ?",
        (body.name,),
    )

    if existing:
        raise HTTPException(
            status_code=409,
            detail=f"API key with name '{body.name}' already exists.",
        )

    key = body.key if body.key else secrets.token_urlsafe(32)

    execute_write(
        "INSERT INTO api_keys (name, key) VALUES (?, ?)",
        (body.name, key),
    )

    return APIKeyResponse(
        name=body.name,
        key=key,
        message=f"API key '{body.name}' created successfully.",
    )


@router.get("/validate", response_model=APIKeyValidation)
async def validate_api_key(
    api_key: Optional[str] = Security(api_key_header),
):
    """Validate an API key."""
    if not api_key:
        return APIKeyValidation(
            valid=False,
            message="No API key provided.",
        )

    rows = execute_query(
        "SELECT name FROM api_keys WHERE key = ?",
        (api_key,),
    )

    if not rows:
        return APIKeyValidation(
            valid=False,
            message="Invalid API key.",
        )

    return APIKeyValidation(
        valid=True,
        name=rows[0]["name"],
        message="API key is valid.",
    )


@router.get("/key-exists", response_model=APIKeyInfo)
async def check_key_exists():
    """Check if any API key exists."""
    rows = execute_query("SELECT name FROM api_keys LIMIT 1")

    if rows:
        return APIKeyInfo(exists=True, name=rows[0]["name"])

    return APIKeyInfo(exists=False)


@router.delete("/keys/{key_name}")
async def delete_api_key(
    key_name: str,
    _api_key: str = Depends(get_api_key),
):
    """Delete an API key by name. Requires authentication."""
    existing = execute_query(
        "SELECT key FROM api_keys WHERE name = ?",
        (key_name,),
    )

    if not existing:
        raise HTTPException(
            status_code=404,
            detail=f"API key with name '{key_name}' not found.",
        )

    execute_write(
        "DELETE FROM api_keys WHERE name = ?",
        (key_name,),
    )

    return {"message": f"API key '{key_name}' deleted successfully."}
