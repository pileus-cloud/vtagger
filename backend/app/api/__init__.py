"""
VTagger API Module.
"""

from app.api.auth import router as auth_router
from app.api.status import router as status_router
from app.api.dimensions import router as dimensions_router
from app.api.jobs import router as jobs_router
from app.api.stats import router as stats_router

__all__ = [
    "auth_router",
    "status_router",
    "dimensions_router",
    "jobs_router",
    "stats_router",
]
