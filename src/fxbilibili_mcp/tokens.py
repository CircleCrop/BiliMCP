"""Opaque six-character cursor tokens backed by a bounded process-local KV."""

from __future__ import annotations

import secrets
import string
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from pydantic import Field

from .models import ErrorCode, ServiceError, StrictModel

BASE62 = string.digits + string.ascii_uppercase + string.ascii_lowercase


class VideoCursorState(StrictModel):
    tool: Literal["get_video"] = "get_video"
    bvid: str
    next_page: int = Field(ge=1)
    page_size: Literal[10] = 10
    subtitle_mode: Literal["default", "all"]
    content_version: str


class SearchCursorState(StrictModel):
    tool: Literal["search"] = "search"
    query_fingerprint: str
    account_scope: str
    generation: str
    next_page: int = Field(ge=1)
    page_offset: int = Field(default=0, ge=0)
    page_loaded: bool = False
    seen_ids: tuple[str, ...] = ()
    scanned_items: int = Field(default=0, ge=0)
    unmappable_items: int = Field(default=0, ge=0)


class CursorUserHistoryState(StrictModel):
    tool: Literal["search_user_content"] = "search_user_content"
    account_scope: str
    query_fingerprint: str
    generation: str
    position: int = Field(default=0, ge=0)


class LiveGuardsCursorState(StrictModel):
    tool: Literal["list_live_guards"] = "list_live_guards"
    requested_room_id: str | None = None
    requested_uid: str | None = None
    room_id: str
    uid: str
    next_page: int = Field(ge=2)


@dataclass(frozen=True, slots=True)
class CommentSearchCursorState:
    query_fingerprint: str
    account_scope: str
    generation: str
    position: int = 1
    scanned_comments: int = 0
    tool: Literal["search_comments"] = "search_comments"


@dataclass(slots=True)
class _TokenEntry:
    value: (
        VideoCursorState
        | SearchCursorState
        | CursorUserHistoryState
        | LiveGuardsCursorState
        | CommentSearchCursorState
    )
    expires_at: float


class CursorTokenStore:
    """Issue collision-checked Base62 tokens with fixed TTL and LRU eviction."""

    def __init__(
        self,
        *,
        max_items: int = 10_000,
        ttl_seconds: float = 1_800,
        clock: Callable[[], float] = time.monotonic,
        token_factory: Callable[[], str] | None = None,
        collision_retries: int = 8,
    ) -> None:
        """Own token state without storing credentials, keys, or upstream URLs."""
        if max_items <= 0 or ttl_seconds <= 0 or collision_retries <= 0:
            raise ValueError("token store limits must be positive")
        self.max_items = max_items
        self.ttl_seconds = ttl_seconds
        self._clock = clock
        self._token_factory = token_factory or _random_token
        self._collision_retries = collision_retries
        self._entries: OrderedDict[str, _TokenEntry] = OrderedDict()

    def issue(
        self,
        state: VideoCursorState
        | SearchCursorState
        | CursorUserHistoryState
        | LiveGuardsCursorState
        | CommentSearchCursorState,
    ) -> str:
        """Create one unpredictable token, pruning expired and least-recent entries."""
        self._prune_expired()
        for _ in range(self._collision_retries):
            token = self._token_factory()
            if len(token) != 6 or any(char not in BASE62 for char in token):
                raise RuntimeError("cursor token factory returned an invalid token")
            if token in self._entries:
                continue
            self._entries[token] = _TokenEntry(
                value=state,
                expires_at=self._clock() + self.ttl_seconds,
            )
            while len(self._entries) > self.max_items:
                self._entries.popitem(last=False)
            return token
        raise RuntimeError("could not allocate a unique cursor token")

    def resolve(
        self,
        token: str,
        *,
        bvid: str,
        subtitle_mode: Literal["default", "all"],
    ) -> VideoCursorState:
        """Validate token syntax, TTL, tool binding, and caller-visible parameters."""
        if len(token) != 6 or any(char not in BASE62 for char in token):
            raise _invalid_cursor()
        entry = self._entries.get(token)
        if entry is None:
            raise _invalid_cursor()
        if entry.expires_at <= self._clock():
            del self._entries[token]
            raise _invalid_cursor()
        state = entry.value
        if (
            not isinstance(state, VideoCursorState)
            or state.bvid != bvid
            or state.subtitle_mode != subtitle_mode
        ):
            raise _invalid_cursor()
        self._entries.move_to_end(token)
        return state

    def issue_search(self, state: SearchCursorState) -> str:
        """Allocate a search continuation in the shared bounded token store."""
        return self.issue(state)

    def resolve_search(
        self, token: str, *, query_fingerprint: str, account_scope: str
    ) -> SearchCursorState:
        """Bind search continuation to its exact parameters and server-side account."""
        if len(token) != 6 or any(char not in BASE62 for char in token):
            raise _invalid_cursor()
        entry = self._entries.get(token)
        if entry is None:
            raise _invalid_cursor()
        if entry.expires_at <= self._clock():
            del self._entries[token]
            raise _invalid_cursor()
        state = entry.value
        if (
            not isinstance(state, SearchCursorState)
            or state.query_fingerprint != query_fingerprint
            or state.account_scope != account_scope
        ):
            raise _invalid_cursor()
        self._entries.move_to_end(token)
        return state

    def size(self) -> int:
        """Return the fresh token count for readiness metrics."""
        self._prune_expired()
        return len(self._entries)

    def issue_user_history(self, state: CursorUserHistoryState) -> str:
        """Keep user-history continuation in the same bounded token store."""
        return self.issue(state)

    def resolve_user_history(
        self, token: str, *, account_scope: str, query_fingerprint: str
    ) -> CursorUserHistoryState:
        """Bind a history cursor to its account and complete search parameters."""
        if len(token) != 6 or any(char not in BASE62 for char in token):
            raise _invalid_cursor()
        entry = self._entries.get(token)
        if entry is None:
            raise _invalid_cursor()
        if entry.expires_at <= self._clock():
            del self._entries[token]
            raise _invalid_cursor()
        state = entry.value
        if (
            not isinstance(state, CursorUserHistoryState)
            or state.account_scope != account_scope
            or state.query_fingerprint != query_fingerprint
        ):
            raise _invalid_cursor()
        self._entries.move_to_end(token)
        return state

    def resolve_comment_search(
        self, token: str, *, query_fingerprint: str, account_scope: str
    ) -> CommentSearchCursorState:
        """Keep comment searches bound to their account and validated filters."""
        if len(token) != 6 or any(char not in BASE62 for char in token):
            raise _invalid_cursor()
        entry = self._entries.get(token)
        if entry is None or entry.expires_at <= self._clock():
            if entry is not None:
                del self._entries[token]
            raise _invalid_cursor()
        state = entry.value
        if (
            not isinstance(state, CommentSearchCursorState)
            or state.query_fingerprint != query_fingerprint
            or state.account_scope != account_scope
        ):
            raise _invalid_cursor()
        self._entries.move_to_end(token)
        return state

    def resolve_live_guards(
        self, token: str, *, requested_room_id: str | None, requested_uid: str | None
    ) -> LiveGuardsCursorState:
        """Bind current-member continuation to the caller's exact room or UID input."""
        if len(token) != 6 or any(char not in BASE62 for char in token):
            raise _invalid_cursor()
        entry = self._entries.get(token)
        if entry is None:
            raise _invalid_cursor()
        if entry.expires_at <= self._clock():
            del self._entries[token]
            raise _invalid_cursor()
        state = entry.value
        if (
            not isinstance(state, LiveGuardsCursorState)
            or state.requested_room_id != requested_room_id
            or state.requested_uid != requested_uid
        ):
            raise _invalid_cursor()
        self._entries.move_to_end(token)
        return state

    def clear(self) -> None:
        """Discard all process-local continuation state during shutdown."""
        self._entries.clear()

    def _prune_expired(self) -> None:
        """Remove expired entries before reads, writes, and readiness metrics."""
        now = self._clock()
        expired = [token for token, entry in self._entries.items() if entry.expires_at <= now]
        for token in expired:
            del self._entries[token]


def _random_token() -> str:
    return "".join(secrets.choice(BASE62) for _ in range(6))


def _invalid_cursor() -> ServiceError:
    return ServiceError(ErrorCode.INVALID_CURSOR, "cursor is invalid, expired, or mismatched")
