"""
VTagger Progress Tracker.

Tracks agent state and progress, broadcasts updates via SSE.
"""

import asyncio
import json
import time
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

from app.services.agent_logger import log_timing


class AgentState(str, Enum):
    """Agent processing states."""
    IDLE = "idle"
    STARTING = "starting"
    AUTHENTICATING = "authenticating"
    FETCHING_ACCOUNTS = "fetching_accounts"
    FETCHING_RESOURCES = "fetching_resources"
    FETCHING_TAGS = "fetching_tags"
    MAPPING = "mapping"
    WRITING = "writing"
    COMPLETE = "complete"
    ERROR = "error"
    CANCELLED = "cancelled"


class ProgressTracker:
    """Tracks progress and broadcasts updates to SSE clients."""

    def __init__(self):
        self._state: AgentState = AgentState.IDLE
        self._progress: float = 0.0
        self._message: str = ""
        self._detail: str = ""
        self._started_at: Optional[float] = None
        self._completed_at: Optional[float] = None
        self._error: Optional[str] = None
        self._subscribers: Set[asyncio.Queue] = set()
        self._step_count: int = 0
        self._total_steps: int = 0
        self._sub_progress: float = 0.0
        self._sub_message: str = ""
        self._stats: Dict[str, Any] = {}
        self._lock = asyncio.Lock() if asyncio.get_event_loop().is_running() else None

    @property
    def state(self) -> AgentState:
        return self._state

    @property
    def progress(self) -> float:
        return self._progress

    @property
    def is_running(self) -> bool:
        return self._state not in (
            AgentState.IDLE,
            AgentState.COMPLETE,
            AgentState.ERROR,
            AgentState.CANCELLED,
        )

    def subscribe(self) -> asyncio.Queue:
        """Subscribe to progress updates. Returns a queue."""
        queue: asyncio.Queue = asyncio.Queue()
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue):
        """Unsubscribe from progress updates."""
        self._subscribers.discard(queue)

    async def _broadcast(self, event_type: str = "progress"):
        """Send current state to all subscribers."""
        data = self.to_dict()
        message = json.dumps(data)

        dead_queues = []
        for queue in self._subscribers:
            try:
                queue.put_nowait({"event": event_type, "data": message})
            except asyncio.QueueFull:
                dead_queues.append(queue)

        for q in dead_queues:
            self._subscribers.discard(q)

    async def set_state(
        self,
        state: AgentState,
        message: str = "",
        detail: str = "",
        progress: Optional[float] = None,
    ):
        """Update state and broadcast."""
        self._state = state
        self._message = message
        self._detail = detail

        if progress is not None:
            self._progress = progress

        if state == AgentState.STARTING:
            self._started_at = time.time()
            self._completed_at = None
            self._error = None
            self._stats = {}
            self._progress = 0.0

        elif state in (AgentState.COMPLETE, AgentState.ERROR, AgentState.CANCELLED):
            self._completed_at = time.time()
            if state == AgentState.ERROR:
                self._error = message

        log_timing(f"STATE -> {state.value}: {message} {detail}")
        await self._broadcast()

    async def set_progress(
        self,
        progress: float,
        message: str = "",
        detail: str = "",
    ):
        """Update progress percentage and broadcast."""
        self._progress = min(100.0, max(0.0, progress))
        if message:
            self._message = message
        if detail:
            self._detail = detail
        await self._broadcast()

    async def set_sub_progress(
        self,
        sub_progress: float,
        sub_message: str = "",
    ):
        """Update sub-step progress."""
        self._sub_progress = min(100.0, max(0.0, sub_progress))
        if sub_message:
            self._sub_message = sub_message
        await self._broadcast()

    async def increment_step(self, message: str = "", detail: str = ""):
        """Increment step counter and recalculate progress."""
        self._step_count += 1
        if self._total_steps > 0:
            self._progress = (self._step_count / self._total_steps) * 100.0
        if message:
            self._message = message
        if detail:
            self._detail = detail
        await self._broadcast()

    def set_total_steps(self, total: int):
        """Set total number of steps for progress calculation."""
        self._total_steps = total
        self._step_count = 0

    def set_stat(self, key: str, value: Any):
        """Set a statistics value."""
        self._stats[key] = value

    def get_stat(self, key: str, default: Any = None) -> Any:
        """Get a statistics value."""
        return self._stats.get(key, default)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize current state to dict."""
        elapsed = None
        if self._started_at:
            end = self._completed_at or time.time()
            elapsed = round(end - self._started_at, 1)

        return {
            "state": self._state.value,
            "progress": round(self._progress, 1),
            "message": self._message,
            "detail": self._detail,
            "sub_progress": round(self._sub_progress, 1),
            "sub_message": self._sub_message,
            "step": self._step_count,
            "total_steps": self._total_steps,
            "elapsed_seconds": elapsed,
            "error": self._error,
            "stats": self._stats,
        }

    def reset(self):
        """Reset tracker to idle state."""
        self._state = AgentState.IDLE
        self._progress = 0.0
        self._message = ""
        self._detail = ""
        self._started_at = None
        self._completed_at = None
        self._error = None
        self._step_count = 0
        self._total_steps = 0
        self._sub_progress = 0.0
        self._sub_message = ""
        self._stats = {}


# Global instance
progress_tracker = ProgressTracker()
