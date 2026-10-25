"""Async-local correlation state for one MCP tool call and its HTTP work."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field


@dataclass(slots=True)
class UpstreamWork:
    attempts: int = 0
    negative_cache_hit: bool = False
    queue_wait_ms: float = 0.0
    cache_lookups: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    persistent_cache_hits: int = 0
    coalesced_cache_reads: int = 0
    media_bytes: int = 0
    media_read_bytes: int = 0
    decode_ms: float = 0.0
    outcome: str | None = None
    call_cache_hit: bool = False

    def send(self) -> None:
        """Count one actual upstream send for this call."""
        self.attempts += 1


@dataclass(frozen=True, slots=True)
class ToolCallLogContext:
    request_id: str
    client_request_id: str | None
    tool: str
    started: float
    upstream_work: UpstreamWork = field(default_factory=UpstreamWork)


_TOOL_CALL_CONTEXT: ContextVar[ToolCallLogContext | None] = ContextVar(
    "fxbilibili_tool_call_context",
    default=None,
)


@contextmanager
def bind_tool_call(context: ToolCallLogContext) -> Iterator[None]:
    """Bind one call while preserving isolation across concurrent async tasks."""
    token = _TOOL_CALL_CONTEXT.set(context)
    try:
        yield
    finally:
        _TOOL_CALL_CONTEXT.reset(token)


def current_tool_call() -> ToolCallLogContext | None:
    """Return the active MCP call correlation state, if this task has one."""
    return _TOOL_CALL_CONTEXT.get()
