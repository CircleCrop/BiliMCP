"""Bounded user-content filtering over persistent public history generations."""

from __future__ import annotations

import asyncio
import hashlib
import json
import unicodedata
from datetime import datetime
from typing import Annotated, Literal

from pydantic import Field, ValidationError

from .activities import ACTIVITY_NORMALIZATION_VERSION, normalize_activity
from .cache import KeyedLocks
from .models import (
    ActivityReferenceTarget,
    ErrorCode,
    OpusListItem,
    ServiceError,
    StrictModel,
    UnavailableActivityTarget,
    UserActivity,
    UserVideoSummary,
    VideoSearchFilters,
)
from .normalization import (
    TAIPEI,
    JsonValue,
    as_object,
    get_int,
    get_list,
    get_object,
    get_str,
    is_valid_bvid,
    optional_str,
    schema_error,
    validate_decimal_id,
)
from .opuses import IncompleteOpusError, OpusService
from .persistent_cache import HistoryBinding, HistoryItem, HistorySnapshot, PersistentHistoryCache
from .tokens import CursorTokenStore, CursorUserHistoryState
from .transport import BilibiliTransport, BusinessBudgetExhausted
from .users import UserService, normalize_user_video

type HistoryKind = Literal["videos", "opuses", "activities"]
type HistoryResult = UserVideoSummary | OpusListItem | UserActivity
type StopReason = Literal[
    "budget_exhausted", "scan_limit", "time_limit", "source_exhausted", "source_incomplete"
]
_VERSION = "public-history-v1"
_MAX_BATCHES = 8
_MAX_ATTEMPTS = 16
_SCAN_SECONDS = 20.0


class UserHistoryCoverage(StrictModel):
    scope: Literal["user_videos", "user_opuses", "user_activities"]
    complete: bool
    scanned_items: int = Field(ge=0)
    reason: StopReason | None = Field(
        default=None,
        description=(
            "source_incomplete preserves matches and pauses at an Opus with missing content. "
            "Retry later with next_cursor and unchanged parameters."
        ),
    )


class _UserContentSearchBase(StrictModel):
    """Expose only summaries from the selected public history source."""

    schema_version: Literal[1] = 1
    uid: str
    query: str | None
    matched_fields: list[Literal["title", "summary"]]
    next_cursor: str | None
    fetched_at: str
    snapshot_updated_at: str
    coverage: UserHistoryCoverage


class UserVideoSearchOutput(_UserContentSearchBase):
    kind: Literal["videos"] = "videos"
    items: list[UserVideoSummary] = Field(max_length=10)


class UserOpusSearchOutput(_UserContentSearchBase):
    kind: Literal["opuses"] = "opuses"
    items: list[OpusListItem] = Field(max_length=10)


class UserActivitySearchOutput(_UserContentSearchBase):
    kind: Literal["activities"] = "activities"
    items: list[UserActivity] = Field(max_length=10)


type UserContentSearchOutput = Annotated[
    UserVideoSearchOutput | UserOpusSearchOutput | UserActivitySearchOutput,
    Field(discriminator="kind"),
]


class _HistoryFrontier(StrictModel):
    """Persist only scan positions and public IDs awaiting bounded Opus enrichment."""

    kind: HistoryKind
    page: int = Field(default=1, ge=1)
    offset: str = ""
    pending_opus_ids: tuple[str, ...] = ()
    feed_has_more: bool = True
    cache_namespace: str | None = None


class UserHistorySearchService:
    """Reuse account-bound history while limiting each explicit scan's upstream work."""

    def __init__(
        self,
        transport: BilibiliTransport,
        users: UserService,
        history: PersistentHistoryCache,
        tokens: CursorTokenStore,
        opuses: OpusService,
    ) -> None:
        """Bind shared provider, history lifecycle, and the existing opaque cursor store."""
        self._transport = transport
        self._users = users
        self._opuses = opuses
        self._history = history
        self._tokens = tokens
        self._locks = KeyedLocks()

    async def search_user_content(
        self,
        uid: str,
        kind: HistoryKind = "videos",
        query: str | None = None,
        published_after: str | None = None,
        published_before: str | None = None,
        limit: int = 10,
        cursor: str | None = None,
        freshness: Literal["cached", "refresh"] = "cached",
    ) -> tuple[UserContentSearchOutput, bool]:
        """Filter committed summaries and continue uncompleted scans within one bounded call."""
        validate_decimal_id(uid, "uid")
        normalized, bounds = _validate_selection(
            kind, query, published_after, published_before, limit, freshness
        )
        account_scope = self._transport.account_uid
        if not account_scope:
            raise ServiceError(
                ErrorCode.CREDENTIALS_INVALID, "history search requires a verified account"
            )
        # Rebuild changed target/count projections while retaining unchanged Opus histories.
        version = (
            f"public-activities-{ACTIVITY_NORMALIZATION_VERSION}"
            if kind == "activities"
            else "public-videos-v2"
            if kind == "videos"
            else _VERSION
        )
        index_fingerprint = _fingerprint({"uid": uid, "kind": kind, "version": version})
        query_fingerprint = _fingerprint(
            {
                "uid": uid,
                "kind": kind,
                "query": normalized,
                "published_after": published_after,
                "published_before": published_before,
                "limit": limit,
                "freshness": freshness,
                "version": version,
            }
        )
        binding: HistoryBinding = {
            "scope": account_scope,
            "kind": kind,
            "query_fingerprint": index_fingerprint,
            "normalization_version": version,
        }
        # Filters share a frontier only within the same account and source index.
        deadline = asyncio.get_running_loop().time() + _SCAN_SECONDS
        async with self._locks.hold((account_scope, index_fingerprint), wait_seconds=_SCAN_SECONDS):
            snapshot, state = await self._load_snapshot(
                binding, query_fingerprint, cursor, freshness, kind
            )
            selected: list[HistoryResult] = []
            batches = 0
            cache_hit = True
            reason: StopReason | None = None
            async with self._transport.scoped_work(_MAX_ATTEMPTS):
                try:
                    async with asyncio.timeout_at(deadline):
                        while len(selected) < limit:
                            window = await self._history.read_generation(
                                state.generation,
                                **binding,
                                item_offset=state.position,
                                item_limit=128,
                            )
                            if window is None:
                                raise _invalid_cursor()
                            found, consumed = await asyncio.to_thread(
                                _select_items,
                                window.items,
                                kind,
                                normalized,
                                bounds,
                                0,
                                limit - len(selected),
                            )
                            state.position += consumed
                            selected.extend(found)
                            if len(selected) >= limit:
                                break
                            if state.position < window.item_count:
                                continue
                            if snapshot.source_exhausted:
                                break
                            if batches >= _MAX_BATCHES:
                                reason = "scan_limit"
                                break
                            snapshot, new_batch, hit = await self._append_next(
                                uid, kind, snapshot, binding
                            )
                            batches += new_batch
                            cache_hit = cache_hit and hit
                except BusinessBudgetExhausted:
                    reason = "budget_exhausted"
                    cache_hit = False
                except TimeoutError:
                    reason = "time_limit"
                    cache_hit = False
                except IncompleteOpusError:
                    # Keep committed matches and retry this pending ID on continuation.
                    reason = "source_incomplete"
                    cache_hit = False
            # Cancelled SQLite awaits settle their commits; reread the durable result.
            current = await self._history.read_generation(state.generation, **binding, item_limit=0)
            if current is None:
                raise _invalid_cursor()
            snapshot = current
            complete = snapshot.source_exhausted and state.position >= snapshot.item_count
            if complete:
                reason = "source_exhausted"
            next_cursor = None if complete else self._tokens.issue_user_history(state)
            return (
                _build_output(
                    _UserContentSearchBase(
                        uid=uid,
                        query=normalized,
                        matched_fields=["title", "summary"],
                        next_cursor=next_cursor,
                        fetched_at=datetime.now(TAIPEI).isoformat(timespec="seconds"),
                        snapshot_updated_at=snapshot.fetched_at,
                        coverage=UserHistoryCoverage(
                            scope=_coverage_scope(kind),
                            complete=complete,
                            scanned_items=state.position,
                            reason=reason,
                        ),
                    ),
                    kind,
                    selected,
                ),
                cache_hit,
            )

    async def _load_snapshot(
        self,
        binding: HistoryBinding,
        query_fingerprint: str,
        cursor: str | None,
        freshness: Literal["cached", "refresh"],
        kind: HistoryKind,
    ) -> tuple[HistorySnapshot, CursorUserHistoryState]:
        """Keep continuation pinned to its generation and reuse indexes across filters."""
        if cursor is not None:
            state = self._tokens.resolve_user_history(
                cursor, account_scope=binding["scope"], query_fingerprint=query_fingerprint
            ).model_copy(deep=True)
            snapshot = await self._history.read_generation(
                state.generation, **binding, item_limit=0
            )
            if snapshot is None or state.position > snapshot.item_count:
                raise _invalid_cursor()
            return snapshot, state
        snapshot = (
            await self._history.latest_generation(**binding, item_limit=0)
            if freshness == "cached"
            else None
        )
        if snapshot is None:
            generation = await self._history.create_generation(**binding)
            if freshness == "refresh":
                await self._history.commit_batch(
                    generation,
                    (),
                    _HistoryFrontier(kind=kind, cache_namespace=generation).model_dump_json(),
                    False,
                )
            snapshot = await self._history.read_generation(generation, **binding, item_limit=0)
        if snapshot is None:
            raise ServiceError(
                ErrorCode.UPSTREAM_UNAVAILABLE, "history generation could not be initialized"
            )
        return snapshot, CursorUserHistoryState(
            account_scope=binding["scope"],
            query_fingerprint=query_fingerprint,
            generation=snapshot.generation,
        )

    async def _append_next(
        self,
        uid: str,
        kind: HistoryKind,
        snapshot: HistorySnapshot,
        binding: HistoryBinding,
    ) -> tuple[HistorySnapshot, int, bool]:
        """Commit normalized data with its exact next frontier before making it visible."""
        frontier = _parse_frontier(snapshot.frontier, kind)
        if kind == "opuses" and frontier.pending_opus_ids:
            opus_id = frontier.pending_opus_ids[0]
            resource, hit = await self._opuses.get_resource(opus_id)
            item = resource.list_item()
            if item.opus_id != opus_id:
                raise schema_error("history Opus identity")
            frontier.pending_opus_ids = frontier.pending_opus_ids[1:]
            exhausted = not frontier.pending_opus_ids and not frontier.feed_has_more
            await self._history.commit_batch(
                snapshot.generation, (_history_item(item),), frontier.model_dump_json(), exhausted
            )
            new_batch = 0
        elif kind == "videos":
            data, hit = await self._users.video_page(
                uid, page=frontier.page, sort="newest", cache_namespace=frontier.cache_namespace
            )
            listing = get_object(data, "list", "history videos")
            items = tuple(
                _normalize_video_item(value)
                for value in get_list(listing, "vlist", "history videos")
            )
            count = get_int(get_object(data, "page", "history videos"), "count", "history videos")
            if count < 0 or (not items and frontier.page * 30 < count):
                raise schema_error("history videos source coverage")
            exhausted = frontier.page * 30 >= count
            frontier.page += 1
            await self._history.commit_batch(
                snapshot.generation, items, frontier.model_dump_json(), exhausted
            )
            new_batch = int(not hit)
        else:
            data, hit = await (
                self._users.opus_page(
                    uid, frontier.offset, cache_namespace=frontier.cache_namespace
                )
                if kind == "opuses"
                else self._users.activity_page(
                    uid, frontier.offset, cache_namespace=frontier.cache_namespace
                )
            )
            values = get_list(data, "items", "history feed")
            has_more = data.get("has_more")
            if not isinstance(has_more, (bool, int)) or has_more not in {0, 1}:
                raise schema_error("history feed has_more")
            offset = optional_str(data, "offset")
            if has_more and (not offset or offset == frontier.offset):
                raise schema_error("history feed offset")
            frontier.offset = offset
            frontier.feed_has_more = bool(has_more)
            if kind == "opuses":
                frontier.pending_opus_ids = tuple(
                    dict.fromkeys(
                        validate_decimal_id(
                            get_str(as_object(value, "history Opus"), "opus_id", "history Opus"),
                            "opus_id",
                        )
                        for value in values
                    )
                )
                items = ()
            else:
                items = tuple(_normalize_activity_item(value) for value in values)
            exhausted = not has_more and not frontier.pending_opus_ids
            await self._history.commit_batch(
                snapshot.generation, items, frontier.model_dump_json(), exhausted
            )
            new_batch = int(not hit)
        updated = await self._history.read_generation(snapshot.generation, **binding, item_limit=0)
        if updated is None:
            raise _invalid_cursor()
        return updated, new_batch, hit


def _validate_selection(
    kind: HistoryKind,
    query: str | None,
    published_after: str | None,
    published_before: str | None,
    limit: int,
    freshness: Literal["cached", "refresh"],
) -> tuple[str | None, tuple[datetime | None, datetime | None]]:
    """Require an explicit filter and validate raw query length and exact timestamp syntax."""
    if kind not in {"videos", "opuses", "activities"} or freshness not in {"cached", "refresh"}:
        raise ServiceError(ErrorCode.INVALID_ARGUMENT, "history kind or freshness is invalid")
    if isinstance(limit, bool) or not 1 <= limit <= 10:
        raise ServiceError(ErrorCode.INVALID_ARGUMENT, "limit must be between 1 and 10")
    normalized: str | None = None
    if query is not None:
        if len(query) > 100 or any(
            unicodedata.category(char) in {"Cc", "Cf", "Cs"} for char in query
        ):
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT, "query must contain at most 100 visible characters"
            )
        normalized = " ".join(query.split())
        if not normalized or len(normalized.split()) > 3:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT, "query must contain one to three nonempty terms"
            )
    if normalized is None and published_after is None and published_before is None:
        raise ServiceError(ErrorCode.INVALID_ARGUMENT, "provide query or a publication boundary")
    try:
        VideoSearchFilters(published_after=published_after, published_before=published_before)
    except ValidationError as exc:
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT,
            "publication boundaries must be a valid RFC 3339 interval with time zones",
        ) from exc
    bounds = tuple(
        datetime.fromisoformat(value.replace("Z", "+00:00")) if value is not None else None
        for value in (published_after, published_before)
    )
    return normalized, (bounds[0], bounds[1])


def _build_output(
    base: _UserContentSearchBase, kind: HistoryKind, items: list[HistoryResult]
) -> UserContentSearchOutput:
    """Validate final results against the closed branch exposed by the public output schema."""
    values = base.model_dump(mode="python") | {"kind": kind, "items": items}
    if kind == "videos":
        return UserVideoSearchOutput.model_validate(values)
    if kind == "opuses":
        return UserOpusSearchOutput.model_validate(values)
    return UserActivitySearchOutput.model_validate(values)


def _select_items(
    items: tuple[HistoryItem, ...],
    kind: HistoryKind,
    query: str | None,
    bounds: tuple[datetime | None, datetime | None],
    position: int,
    limit: int,
) -> tuple[list[HistoryResult], int]:
    """Inspect stable normalized positions without assuming upstream date monotonicity."""
    selected: list[HistoryResult] = []
    words = query.casefold().split() if query is not None else []
    after, before = bounds
    while position < len(items) and len(selected) < limit:
        saved = items[position]
        model = _restore_item(saved, kind)
        text = _match_text(model).casefold()
        matches_query = all(word in text for word in words)
        published = datetime.fromisoformat(model.published_at)
        matches_date = (after is None or published >= after) and (
            before is None or published < before
        )
        if matches_query and matches_date:
            selected.append(model)
        position += 1
    return selected, position


def _restore_item(item: HistoryItem, kind: HistoryKind) -> HistoryResult:
    """Validate persisted projections with the exact public model for this source."""
    try:
        if kind == "videos":
            return UserVideoSummary.model_validate(item.payload)
        if kind == "opuses":
            return OpusListItem.model_validate(item.payload)
        return UserActivity.model_validate(item.payload)
    except ValidationError as exc:
        raise ServiceError(
            ErrorCode.UPSTREAM_SCHEMA_CHANGED, "cached history projection schema changed"
        ) from exc


def _match_text(item: HistoryResult) -> str:
    """Match only the activity's public summary and its one-level referenced title/summary."""
    if isinstance(item, UserActivity):
        target = item.target
        if isinstance(target, ActivityReferenceTarget):
            return item.summary or ""
        title = "" if isinstance(target, UnavailableActivityTarget) else (target.title or "")
        return " ".join(value for value in (item.summary, title, target.summary) if value)
    return f"{item.title} {item.summary}"


def _history_item(item: HistoryResult) -> HistoryItem:
    """Persist the closed public model rather than the original upstream object."""
    key = (
        item.bvid
        if isinstance(item, UserVideoSummary)
        else (item.opus_id if isinstance(item, OpusListItem) else item.activity_id)
    )
    return HistoryItem(
        key=key,
        published_at=item.published_at,
        payload=as_object(item.model_dump(mode="python", exclude_none=True), "history projection"),
    )


def _normalize_video_item(value: JsonValue) -> HistoryItem:
    """Require real identity and publication fields before adding a video to a date index."""
    data = as_object(value, "history video")
    if get_int(data, "created", "history video") <= 0:
        raise schema_error("history video publication time")
    item = normalize_user_video(data)
    if not is_valid_bvid(item.bvid):
        raise schema_error("history video identity")
    return _history_item(item)


def _normalize_activity_item(value: JsonValue) -> HistoryItem:
    """Keep missing activity dates from silently becoming an epoch timestamp in the index."""
    data = as_object(value, "history activity")
    author = get_object(
        get_object(data, "modules", "history activity"), "module_author", "history activity"
    )
    if get_int(author, "pub_ts", "history activity") <= 0:
        raise schema_error("history activity publication time")
    return _history_item(normalize_activity(data))


def _parse_frontier(value: str | None, kind: HistoryKind) -> _HistoryFrontier:
    """Reject stale or invalid internal frontier versions instead of restarting their scan."""
    if value is None:
        return _HistoryFrontier(kind=kind)
    try:
        parsed = _HistoryFrontier.model_validate_json(value)
    except ValidationError as exc:
        raise _invalid_cursor() from exc
    if parsed.kind != kind:
        raise _invalid_cursor()
    return parsed


def _fingerprint(values: dict[str, str | int | None]) -> str:
    return hashlib.sha256(
        json.dumps(values, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _coverage_scope(kind: HistoryKind) -> Literal["user_videos", "user_opuses", "user_activities"]:
    if kind == "videos":
        return "user_videos"
    return "user_opuses" if kind == "opuses" else "user_activities"


def _invalid_cursor() -> ServiceError:
    return ServiceError(ErrorCode.INVALID_CURSOR, "history cursor is invalid, expired, or evicted")
