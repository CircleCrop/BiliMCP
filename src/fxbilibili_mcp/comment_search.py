"""Bounded literal and time filtering over native full-text root-comment ranges."""

from __future__ import annotations

import asyncio
import hashlib
import json
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Literal
from uuid import uuid4

from pydantic import Field

from .cache import AsyncTTLCache, CachePolicy
from .comments import CommentPaginationLimitReached, CommentService
from .models import (
    Author,
    CommentItem,
    CommentListOutput,
    CommentTarget,
    CommentTargetInput,
    ErrorCode,
    ServiceError,
    StrictModel,
)
from .normalization import TAIPEI
from .search_service import _validate_published_bounds, _validate_search
from .tokens import CommentSearchCursorState
from .transport import BilibiliTransport, BusinessBudgetExhausted

if TYPE_CHECKING:
    from .tokens import CursorTokenStore

_VERSION = "comment-root-search-v1"
_WINDOW_SIZE = 20
_MAX_WINDOWS = 3
_MAX_TOTAL_ROOTS = 1000
_MAX_SENDS = 6
_SCAN_SECONDS = 20.0
_EXCERPT_SIZE = 300


class CommentSearchInput(StrictModel):
    target: CommentTargetInput
    query: str | None = Field(default=None, max_length=100)
    published_after: str | None = None
    cursor: str | None = Field(default=None, pattern=r"^[0-9A-Za-z]{6}$")


class CommentSearchMatch(StrictModel):
    """Identify a root that list_comments can expand using the enclosing target."""

    comment_id: str
    author: Author
    created_at: str
    excerpt: str = Field(max_length=_EXCERPT_SIZE)
    like_count: int = Field(ge=0)
    reply_count: int = Field(ge=0)
    ip_location: str | None = None


type CommentSearchStopReason = Literal[
    "source_exhausted",
    "result_limit",
    "page_limit",
    "budget_exhausted",
    "time_limit",
    "coverage_limit",
]


class CommentSearchCoverage(StrictModel):
    """Only visible roots are scanned; attached replies are not searched."""

    scope: Literal["root_comments"] = "root_comments"
    complete: bool
    scanned_root_comments: int = Field(ge=0)
    scanned_this_call: int = Field(ge=0, le=_MAX_WINDOWS * _WINDOW_SIZE)
    reason: CommentSearchStopReason


class CommentSearchOutput(StrictModel):
    schema_version: Literal[1] = 1
    target: CommentTarget
    query: str | None
    published_after: str | None
    matches: list[CommentSearchMatch] = Field(max_length=10)
    coverage: CommentSearchCoverage
    next_cursor: str | None
    fetched_at: str


@dataclass(frozen=True, slots=True)
class _CommentSearchGeneration:
    version: str
    fetched_at: str


@dataclass(frozen=True, slots=True)
class _CommentSearchWindow:
    comments: CommentListOutput


class CommentSearchService:
    """Reuse comment pagination, full text, public cache and account-bound cursor storage."""

    def __init__(
        self,
        transport: BilibiliTransport,
        comments: CommentService,
        cache: AsyncTTLCache,
        policy: CachePolicy,
        tokens: CursorTokenStore,
    ) -> None:
        """Bind shared services without introducing another pagination or HTTP implementation."""
        self._transport = transport
        self._comments = comments
        self._cache = cache
        self._policy = policy
        self._tokens = tokens

    async def search_comments(
        self,
        target: CommentTargetInput,
        query: str | None = None,
        published_after: str | None = None,
        cursor: str | None = None,
    ) -> tuple[CommentSearchOutput, bool]:
        """Convert references on the server and share the scan's actual-send budget."""
        _validate_selection(target, query, published_after)
        try:
            async with self._transport.scoped_work(_MAX_SENDS):
                resolved, identity_hit = await self._comments.resolve_target(target)
                output, hit = await self._search_target(resolved, query, published_after, cursor)
                return output, identity_hit and hit
        except BusinessBudgetExhausted as exc:
            raise ServiceError(
                ErrorCode.RATE_LIMITED, "comment target resolution work budget is exhausted"
            ) from exc

    async def _search_target(
        self,
        target: CommentTarget,
        query: str | None = None,
        published_after: str | None = None,
        cursor: str | None = None,
    ) -> tuple[CommentSearchOutput, bool]:
        """Read at most sixty visible roots per call and six actual provider sends."""
        normalized, after = _validate_selection(target, query, published_after)
        account_scope = self._transport.account_uid
        if not account_scope:
            raise ServiceError(
                ErrorCode.CREDENTIALS_INVALID, "comment search requires a verified account"
            )
        after_text = after.astimezone(TAIPEI).isoformat() if after is not None else None
        fingerprint = _selection_fingerprint(target, normalized, after_text)
        state = (
            self._tokens.resolve_comment_search(
                cursor, query_fingerprint=fingerprint, account_scope=account_scope
            )
            if cursor is not None
            else None
        )
        generation, anchor_hit = await self._generation(target, account_scope, state)
        position = state.position if state is not None else 1
        scanned = state.scanned_comments if state is not None else 0
        scanned_now = 0
        matches: list[CommentSearchMatch] = []
        cache_hit = anchor_hit
        complete = False
        reason: CommentSearchStopReason = "page_limit"
        try:
            async with self._transport.scoped_work(_MAX_SENDS):
                async with asyncio.timeout(_SCAN_SECONDS):
                    for _ in range(_MAX_WINDOWS):
                        # Native ranges walk at most fifty source pages; stop this search before
                        # attempting unlimited deep ranges or claiming the whole room was read.
                        if position > _MAX_TOTAL_ROOTS:
                            reason = "coverage_limit"
                            break
                        end = min(position + _WINDOW_SIZE - 1, _MAX_TOTAL_ROOTS)
                        window, window_hit = await self._window(
                            target, account_scope, generation.version, position, end
                        )
                        cache_hit = cache_hit and window_hit
                        output = window.comments
                        consumed_count = 0
                        for item in output.items:
                            consumed_count += 1
                            position += 1
                            scanned += 1
                            scanned_now += 1
                            match = _match(item, normalized, after)
                            if match is not None:
                                matches.append(match)
                            if len(matches) == 10:
                                reason = "result_limit"
                                break
                        # Short terminal ranges are legitimate; completion requires consuming
                        # every returned root, including roots left after the tenth match.
                        consumed_window = consumed_count == len(output.items)
                        if consumed_window and output.next_range is None:
                            complete = True
                            reason = "source_exhausted"
                            break
                        if len(matches) == 10:
                            break
                    if not complete and position > _MAX_TOTAL_ROOTS:
                        reason = "coverage_limit"
        except BusinessBudgetExhausted:
            reason, cache_hit = "budget_exhausted", False
        except TimeoutError:
            reason, cache_hit = "time_limit", False
        except CommentPaginationLimitReached:
            reason, cache_hit = "coverage_limit", False
        next_cursor = None
        if not complete and reason != "coverage_limit":
            next_cursor = self._tokens.issue(
                CommentSearchCursorState(
                    query_fingerprint=fingerprint,
                    account_scope=account_scope,
                    generation=generation.version,
                    position=position,
                    scanned_comments=scanned,
                )
            )
        return CommentSearchOutput(
            target=target,
            query=normalized,
            published_after=after_text,
            matches=matches,
            coverage=CommentSearchCoverage(
                complete=complete,
                scanned_root_comments=scanned,
                scanned_this_call=scanned_now,
                reason=reason,
            ),
            next_cursor=next_cursor,
            fetched_at=generation.fetched_at,
        ), cache_hit

    async def _generation(
        self, target: CommentTarget, account_scope: str, state: CommentSearchCursorState | None
    ) -> tuple[_CommentSearchGeneration, bool]:
        """Bind continuation to a live cache generation; expiry requires an explicit restart."""

        async def produce() -> _CommentSearchGeneration:
            if state is not None:
                raise ServiceError(
                    ErrorCode.INVALID_CURSOR, "comment search cache expired; restart without cursor"
                )
            return _CommentSearchGeneration(uuid4().hex, datetime.now(TAIPEI).isoformat())

        generation, hit = await self._cache.get_or_create(
            ("comment-search-generation", account_scope, _target_key(target), _VERSION),
            self._policy.comments_time,
            produce,
        )
        if state is not None and generation.version != state.generation:
            raise ServiceError(
                ErrorCode.INVALID_CURSOR, "comment search cache changed; restart without cursor"
            )
        return generation, hit

    async def _window(
        self, target: CommentTarget, account_scope: str, generation: str, start: int, end: int
    ) -> tuple[_CommentSearchWindow, bool]:
        """Reuse native root ranges and retain their full text for all query variants."""

        async def produce() -> _CommentSearchWindow:
            """Cache complete root-comment text before applying caller filters."""
            output, _ = await self._comments.list_comments(
                None,
                None,
                "time",
                f"{start}..{end}",
                None,
                "full",
                target=target,
            )
            if not isinstance(output, CommentListOutput):
                raise ServiceError(
                    ErrorCode.UPSTREAM_SCHEMA_CHANGED, "comment search expected root comments"
                )
            return _CommentSearchWindow(output)

        return await self._cache.get_or_create(
            (
                "comment-search-window",
                account_scope,
                _target_key(target),
                generation,
                start,
                end,
                _VERSION,
            ),
            self._policy.comments_time,
            produce,
        )


def _validate_selection(
    target: CommentTargetInput, query: str | None, published_after: str | None
) -> tuple[str | None, datetime | None]:
    """Reuse shared query rules and require at least one explicit keyword or time filter."""
    if not isinstance(target, str) and target.kind == "unknown":
        raise ServiceError(ErrorCode.INVALID_ARGUMENT, "unsupported indexed comment target")
    normalized = _validate_search(query, 10) if query is not None else None
    if normalized is None and published_after is None:
        raise ServiceError(ErrorCode.INVALID_ARGUMENT, "provide query or published_after")
    after = None
    if published_after is not None:
        try:
            after = datetime.fromisoformat(published_after)
        except ValueError as exc:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT, "published_after must be RFC 3339 with a timezone"
            ) from exc
        _validate_published_bounds(after, None)
    return normalized, after


def _target_key(target: CommentTarget) -> str:
    return json.dumps(target.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def _selection_fingerprint(target: CommentTarget, query: str | None, after: str | None) -> str:
    encoded = json.dumps(
        [_target_key(target), query, after, _VERSION], ensure_ascii=False, separators=(",", ":")
    )
    return hashlib.sha256(encoded.encode()).hexdigest()


def _match(
    item: CommentItem, query: str | None, after: datetime | None
) -> CommentSearchMatch | None:
    """Match all normalized terms against full root text, excluding attached reply previews."""
    if after is not None and datetime.fromisoformat(item.created_at) < after:
        return None
    if item.content_text is None:
        raise ServiceError(
            ErrorCode.UPSTREAM_SCHEMA_CHANGED, "comment search requires full root text"
        )
    text = item.content_text
    normalized = unicodedata.normalize("NFKC", text).casefold()
    terms = (
        [unicodedata.normalize("NFKC", term).casefold() for term in query.split()] if query else []
    )
    if any(term not in normalized for term in terms):
        return None
    # The excerpt only locates a match; the native full-text tool remains the quoting source.
    position = max(0, text.casefold().find(terms[0])) if terms else 0
    start = max(0, position - 80)
    return CommentSearchMatch(
        comment_id=item.comment_id,
        author=item.author,
        created_at=item.created_at,
        excerpt=text[start : start + _EXCERPT_SIZE],
        like_count=item.like_count,
        reply_count=item.reply_count,
        ip_location=item.ip_location,
    )
