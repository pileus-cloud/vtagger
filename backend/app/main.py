"""
VTagger FastAPI Application.

Main entry point for the web API.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.database import init_database
from app.api import (
    auth_router,
    status_router,
    dimensions_router,
    jobs_router,
    stats_router,
)
from app.services.mapping_engine import mapping_engine
from app.services.credential_manager import has_credentials


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events."""
    print("\n" + "="*60)
    print("  VTagger - Virtual Tagging Agent")
    print("="*60)

    print("\nInitializing...")
    init_database()
    print(f"  Database: {settings.database_path}")

    if has_credentials():
        print("  Credentials: (configured)")
    else:
        print("  Credentials: (not set - use CLI or environment variables)")

    print("\nLoading configuration...")
    mapping_engine.load_dimensions()
    print(f"  Dimensions: {len(mapping_engine.dimensions)}")

    print("\n" + "="*60)
    print(f"  VTagger ready at http://{settings.api_host}:{settings.api_port}")
    print("="*60 + "\n")

    yield

    print("\nShutting down VTagger...")


app = FastAPI(
    title="VTagger API",
    description="Virtual Tagging Agent",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router)
app.include_router(status_router)
app.include_router(dimensions_router)
app.include_router(jobs_router)
app.include_router(stats_router)


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "name": "VTagger API",
        "version": "1.0.0",
        "description": "Virtual Tagging Agent",
        "docs_url": "/docs",
        "health_check": "/status/health"
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.dev_mode
    )
