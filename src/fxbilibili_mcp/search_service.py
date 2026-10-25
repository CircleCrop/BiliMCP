"""Bounded video, user, article-index and live-room search with verified normalization."""

from __future__ import annotations

import hashlib
import json
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, cast
from uuid import uuid4

from pydantic import ValidationError

from .cache import AsyncTTLCache, CachePolicy
from .content import normalize_space, parse_duration, plain_text, truncate_summary
from .live import ExtendedLiveSearchOutput, LiveSearchItem, normalize_live_status
from .models import (
    Author,
    ErrorCode,
    ExtendedOpusSearchOutput,
    ExtendedSearchOutput,
    ExtendedUserSearchOutput,
    ExtendedVideoSearchOutput,
    OpusListItem,
    SampleVideo,
    SearchCoverage,
    ServiceError,
    UserSearchItem,
    VideoSearchFilters,
    VideoStats,
    VideoSummary,
)
from .normalization import (
    TAIPEI,
    JsonObject,
    as_object,
    format_timestamp,
    get_int,
    get_list,
    get_object,
    get_str,
    is_valid_bvid,
    optional_int,
    optional_list,
    optional_object,
    optional_str,
    schema_error,
)
from .opuses import OpusService
from .tokens import CursorTokenStore, SearchCursorState
from .transport import BilibiliTransport, BusinessBudgetExhausted
from .users import UserService


@dataclass(frozen=True, slots=True)
class _VideoCandidate:
    video: VideoSummary
    category_id: int | None


@dataclass(frozen=True, slots=True)
class _UserCandidate:
    uid: str
    name: str
    description: str
    level: int
    follower_count: int
    sample_videos: tuple[SampleVideo, ...]
    video_count: int | None = None


@dataclass(frozen=True, slots=True)
class _OpusCandidate:
    cvid: str
    category_id: int


@dataclass(frozen=True, slots=True)
class _LiveCandidate:
    room: LiveSearchItem


@dataclass(slots=True)
class _SearchGeneration:
    version: str
    fetched_through: int = 0


@dataclass(frozen=True, slots=True)
class _SearchPage:
    snapshot: _SearchGeneration
    items: tuple[_VideoCandidate | _UserCandidate | _OpusCandidate | _LiveCandidate | None, ...]
    source_exhausted: bool
    fetched_at: str


class _ScanLimitReached(Exception):
    """Stop before a fifth uncached provider page while retaining continuation."""


class SearchService:
    """Read bounded public search pages with parameter-bound continuation."""

    def __init__(
        self,
        transport: BilibiliTransport,
        cache: AsyncTTLCache,
        policy: CachePolicy,
        users: UserService,
        *,
        tokens: CursorTokenStore | None = None,
        opuses: OpusService | None = None,
    ) -> None:
        """Bind shared provider, cache, policy, and profile enrichment services."""
        self._transport = transport
        self._cache = cache
        self._policy = policy
        self._users = users
        self._tokens = tokens
        self._opuses = opuses

    async def search_extended(
        self,
        query: str,
        kind: Literal["videos", "users", "opuses", "live"] = "videos",
        sort: str = "relevance",
        filters: VideoSearchFilters | None = None,
        limit: int = 5,
        cursor: str | None = None,
    ) -> tuple[ExtendedSearchOutput | ExtendedLiveSearchOutput, bool]:
        """Scan signed, cached search pages with exact filtering and bounded enrichment."""
        normalized = _validate_search(query, limit)
        _validate_extended_selection(kind, sort, filters)
        bounds = _publication_bounds(filters)
        fingerprint = _search_fingerprint(normalized, kind, sort, filters, limit)
        account_scope = self._transport.account_uid
        if not account_scope:
            raise ServiceError(ErrorCode.CREDENTIALS_INVALID, "search requires a verified account")
        tokens = self._tokens
        if tokens is None:
            raise RuntimeError("extended search requires the shared cursor store")
        state = (
            tokens.resolve_search(
                cursor, query_fingerprint=fingerprint, account_scope=account_scope
            ).model_copy(deep=True)
            if cursor is not None
            else SearchCursorState(
                query_fingerprint=fingerprint,
                account_scope=account_scope,
                generation="",
                next_page=1,
            )
        )
        seen = set(state.seen_ids)
        videos: list[VideoSummary] = []
        users: list[UserSearchItem] = []
        opuses: list[OpusListItem] = []
        rooms: list[LiveSearchItem] = []
        new_batches = 0
        cache_hit = True
        complete = False
        reason: (
            Literal["budget_exhausted", "scan_limit", "source_exhausted", "unmappable_items"] | None
        ) = None
        source_exhausted = False
        fetched_at = datetime.now(TAIPEI).isoformat()
        async with self._transport.scoped_work(32):
            try:
                while len(videos) + len(users) + len(opuses) + len(rooms) < limit:
                    page, page_hit = await self._search_page(
                        normalized, kind, sort, filters, state, new_batches
                    )
                    state.page_loaded = True
                    state.generation = page.snapshot.version
                    new_batches += not page_hit
                    cache_hit = cache_hit and page_hit
                    fetched_at = page.fetched_at
                    while (
                        state.page_offset < len(page.items)
                        and len(videos) + len(users) + len(opuses) + len(rooms) < limit
                    ):
                        candidate = page.items[state.page_offset]
                        identity = _candidate_id(candidate)
                        if identity is not None and identity not in seen:
                            if isinstance(candidate, _VideoCandidate):
                                if _matches_video(candidate, filters, bounds):
                                    videos.append(candidate.video)
                            elif isinstance(candidate, _UserCandidate):
                                user, profile_hit = await self._enrich_candidate(candidate)
                                users.append(user)
                                cache_hit = cache_hit and profile_hit
                            elif isinstance(candidate, _LiveCandidate):
                                rooms.append(candidate.room)
                            elif isinstance(candidate, _OpusCandidate) and (
                                filters is None
                                or filters.category_id in {None, candidate.category_id}
                            ):
                                item, opus_hit = await self._read_opus_candidate(candidate)
                                cache_hit = cache_hit and opus_hit
                                if item is None:
                                    state.unmappable_items += 1
                                elif "opus:" + item.opus_id not in seen:
                                    if _within_publication_window(
                                        datetime.fromisoformat(item.published_at), *bounds
                                    ):
                                        opuses.append(item)
                                    seen.add("opus:" + item.opus_id)
                            seen.add(identity)
                        # Commit after enrichment succeeds so a budget stop can resume this entry.
                        state.page_offset += 1
                        state.scanned_items += 1
                    if state.page_offset == len(page.items):
                        if page.source_exhausted:
                            source_exhausted = True
                            complete = state.unmappable_items == 0
                            reason = "source_exhausted" if complete else "unmappable_items"
                            break
                        state.next_page += 1
                        state.page_offset = 0
                        state.page_loaded = False
            except _ScanLimitReached:
                reason = "scan_limit"
            except BusinessBudgetExhausted:
                reason = "budget_exhausted"
                cache_hit = False
        state.seen_ids = tuple(sorted(seen))
        if reason is None and state.unmappable_items:
            reason = "unmappable_items"
        next_cursor = None if source_exhausted else tokens.issue_search(state)
        coverage = SearchCoverage(
            scope="article_index" if kind == "opuses" else "search_index",
            complete=complete,
            scanned_items=state.scanned_items,
            unmappable_items=state.unmappable_items or None,
            reason=reason,
        )
        if kind == "live":
            return ExtendedLiveSearchOutput(
                query=normalized,
                sort=cast(Literal["relevance", "popularity", "recent_live"], sort),
                items=rooms,
                next_cursor=next_cursor,
                fetched_at=fetched_at,
                coverage=coverage,
            ), cache_hit
        if kind == "videos":
            return ExtendedVideoSearchOutput(
                query=normalized,
                sort=cast(Literal["relevance", "newest", "views"], sort),
                items=videos,
                next_cursor=next_cursor,
                fetched_at=fetched_at,
                coverage=coverage,
            ), cache_hit
        if kind == "opuses":
            return ExtendedOpusSearchOutput(
                query=normalized,
                sort=cast(Literal["relevance", "newest"], sort),
                items=opuses,
                next_cursor=next_cursor,
                fetched_at=fetched_at,
                coverage=coverage,
            ), cache_hit
        return ExtendedUserSearchOutput(
            query=normalized,
            sort=cast(Literal["relevance", "followers"], sort),
            items=users,
            next_cursor=next_cursor,
            fetched_at=fetched_at,
            coverage=coverage,
        ), cache_hit

    async def _search_page(
        self,
        query: str,
        kind: Literal["videos", "users", "opuses", "live"],
        sort: str,
        filters: VideoSearchFilters | None,
        state: SearchCursorState,
        new_batches: int,
    ) -> tuple[_SearchPage, bool]:
        """Reuse fixed pages and reject lost pending content rather than fetching it again."""

        async def missing_generation() -> _SearchGeneration:
            raise ServiceError(
                ErrorCode.INVALID_CURSOR, "search content version is no longer available"
            )

        snapshot = (
            (
                await self._cache.get_or_create(
                    _generation_key(state), self._policy.search, missing_generation
                )
            )[0]
            if state.generation
            else None
        )
        key = (
            "search_extended",
            state.account_scope,
            # Upstream always returns a fixed page; changing the output limit needs no refetch.
            _search_fingerprint(query, kind, sort, filters, None),
            "first" if state.next_page == 1 else state.generation,
            state.next_page,
            "v2" if kind == "users" else "v1",
        )

        async def produce() -> _SearchPage:
            """Fetch one verified search endpoint page without retaining provider-only fields."""
            # A replayed frontier must also reuse pages read by another cursor from this generation.
            if state.page_loaded or (
                snapshot is not None and state.next_page <= snapshot.fetched_through
            ):
                raise ServiceError(
                    ErrorCode.INVALID_CURSOR, "cached search page is no longer available"
                )
            if new_batches >= 4:
                raise _ScanLimitReached()
            raw = await self._transport.request_data(
                "/x/web-interface/wbi/search/type",
                _search_parameters(query, kind, sort, filters, state.next_page),
                wbi=True,
            )
            data = as_object(raw, "search.extended.data")
            if kind == "live":
                # The combined response has independent room/user indexes; top-level pages mix them.
                result = get_object(data, "result", "search.live")
                values = get_list(result, "live_room", "search.live.result")
                page_info = get_object(data, "pageinfo", "search.live")
                rooms_info = get_object(page_info, "live_room", "search.live.pageinfo")
                page_count = get_int(rooms_info, "numPages", "search.live.pageinfo.live_room")
                room_count = get_int(rooms_info, "numResults", "search.live.pageinfo.live_room")
                if room_count < 0 or (room_count == 0 and (values or page_count)):
                    raise schema_error("search.live.pagination")
            else:
                values = (
                    []
                    if data.get("result") is None and optional_int(data, "numResults", -1) == 0
                    else get_list(data, "result", "search.extended")
                )
                page_count = get_int(data, "numPages", "search.extended")
            if page_count < 0 or len(values) > 10:
                raise schema_error("search.extended.pagination")
            if "page" in data and get_int(data, "page", "search.extended") != state.next_page:
                raise schema_error("search.extended.page")
            entries = tuple(
                _search_candidate(as_object(value, "search.extended.item"), kind)
                for value in values
            )
            return _SearchPage(
                snapshot=snapshot or _SearchGeneration(uuid4().hex),
                items=entries,
                source_exhausted=state.next_page >= page_count,
                fetched_at=datetime.now(TAIPEI).isoformat(),
            )

        page, hit = await self._cache.get_or_create(key, self._policy.search, produce)
        if state.generation and page.snapshot.version != state.generation:
            raise ServiceError(
                ErrorCode.INVALID_CURSOR, "search content version is no longer available"
            )
        if state.page_offset > len(page.items):
            raise ServiceError(ErrorCode.INVALID_CURSOR, "search continuation position is invalid")
        if snapshot is None:
            initial = state.model_copy(update={"generation": page.snapshot.version})

            async def retain_generation() -> _SearchGeneration:
                return page.snapshot

            await self._cache.get_or_create(
                _generation_key(initial), self._policy.search, retain_generation
            )
        # Advance only after the page producer has committed its cached value.
        page.snapshot.fetched_through = max(page.snapshot.fetched_through, state.next_page)
        return page, hit

    async def _read_opus_candidate(
        self, candidate: _OpusCandidate
    ) -> tuple[OpusListItem | None, bool]:
        """Return only readable Opus resources confirmed through an article conversion."""
        if self._opuses is None:
            raise RuntimeError("Opus search requires the shared Opus service")
        try:
            opus_id, mapping_hit = await self._opuses.resolve_article(candidate.cvid)
            if opus_id is None:
                return None, mapping_hit
            full, content_hit = await self._opuses.get_full(opus_id)
            return full.list_item(), mapping_hit and content_hit
        except ServiceError as exc:
            if exc.code is ErrorCode.NOT_FOUND:
                return None, False
            raise

    async def _enrich_candidate(self, candidate: _UserCandidate) -> tuple[UserSearchItem, bool]:
        """Fetch only the profiles of users selected for this response."""
        profile, profile_hit = await self._users.get_space_profile(candidate.uid)
        # The live user index already carries the public upload count, including zero.
        count, count_hit = (
            (candidate.video_count, True)
            if candidate.video_count is not None
            else await self._users.get_video_count(candidate.uid)
        )
        return UserSearchItem(
            uid=candidate.uid,
            name=candidate.name or profile.name,
            description=candidate.description,
            level=candidate.level,
            is_live=profile.is_live,
            follower_count=candidate.follower_count,
            video_count=count,
            sample_videos=list(candidate.sample_videos),
            verification=profile.verification,
        ), profile_hit and count_hit


def _validate_extended_selection(kind: str, sort: str, filters: VideoSearchFilters | None) -> None:
    """Reject sorts and filters that do not belong to the selected index."""
    if kind == "videos" and sort in {"relevance", "newest", "views"}:
        return
    if (
        kind == "opuses"
        and sort in {"relevance", "newest"}
        and (filters is None or filters.duration is None)
    ):
        return
    if kind == "users" and sort in {"relevance", "followers"} and filters is None:
        return
    if kind == "live" and sort in {"relevance", "popularity", "recent_live"} and filters is None:
        return
    raise ServiceError(
        ErrorCode.INVALID_ARGUMENT, "unsupported search kind, sort or filters combination"
    )


def _publication_bounds(
    filters: VideoSearchFilters | None,
) -> tuple[datetime | None, datetime | None]:
    """Parse already-validated timestamp boundaries without reducing timezone precision."""
    after = (
        datetime.fromisoformat(filters.published_after)
        if filters is not None and filters.published_after is not None
        else None
    )
    before = (
        datetime.fromisoformat(filters.published_before)
        if filters is not None and filters.published_before is not None
        else None
    )
    _validate_published_bounds(after, before)
    return after, before


def _search_fingerprint(
    query: str, kind: str, sort: str, filters: VideoSearchFilters | None, limit: int | None
) -> str:
    """Bind result selection; None excludes the local output limit from source-page identity."""
    serialized = json.dumps(
        {
            "query": query,
            "kind": kind,
            "sort": sort,
            "filters": filters.model_dump() if filters is not None else None,
            "limit": limit,
            "normalization": "v1",
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(serialized.encode()).hexdigest()


def _generation_key(state: SearchCursorState) -> tuple[object, ...]:
    return ("search_generation", state.account_scope, state.query_fingerprint, state.generation)


def _search_parameters(
    query: str,
    kind: Literal["videos", "users", "opuses", "live"],
    sort: str,
    filters: VideoSearchFilters | None,
    page: int,
) -> dict[str, str | int | bool]:
    """Map only verified upstream sorts and filters; timestamps are filtered locally."""
    params: dict[str, str | int | bool] = {
        "keyword": query,
        "search_type": {
            "videos": "video",
            "users": "bili_user",
            "opuses": "article",
            "live": "live",
        }[kind],
        "page": page,
        "page_size": 5 if kind == "opuses" else 10,
    }
    if kind == "live":
        if sort != "relevance":
            params["order"] = {"popularity": "online", "recent_live": "live_time"}[sort]
        return params
    if kind == "users":
        if sort == "followers":
            params.update({"order": "fans", "order_sort": 0})
        return params
    params["order"] = {"relevance": "totalrank", "newest": "pubdate", "views": "click"}[sort]
    if filters is not None:
        if filters.duration is not None:
            params["duration"] = {
                "under_10_minutes": 1,
                "10_to_30_minutes": 2,
                "30_to_60_minutes": 3,
                "over_60_minutes": 4,
            }[filters.duration]
        if filters.category_id is not None:
            params["category_id" if kind == "opuses" else "tids"] = filters.category_id
    # The live date probe returned out-of-window hits; sending those parameters would hide the scan.
    return params


def _search_candidate(
    data: JsonObject, kind: Literal["videos", "users", "opuses", "live"]
) -> _VideoCandidate | _UserCandidate | _OpusCandidate | _LiveCandidate | None:
    """Normalize provider candidates before caching, leaving user profiles for selected outputs."""
    if kind == "videos":
        if not is_valid_bvid(data.get("bvid")):
            return None
        try:
            video = _normalize_video(data)
        except ValidationError as exc:
            raise schema_error("search.extended.video") from exc
        return _VideoCandidate(
            video=video,
            category_id=(
                get_int(data, "typeid", "search.extended.video")
                if data.get("typeid") is not None
                else None
            ),
        )
    if kind == "live":
        return _LiveCandidate(_normalize_live_search(data))
    if kind == "opuses":
        cvid = get_int(data, "id", "search.extended.article")
        category_id = get_int(data, "category_id", "search.extended.article")
        if cvid <= 0 or category_id <= 0:
            raise schema_error("search.extended.article")
        return _OpusCandidate(str(cvid), category_id)
    sample_values = [
        value
        for value in (as_object(item, "search.user.sample") for item in optional_list(data, "res"))
        if is_valid_bvid(value.get("bvid"))
    ][:2]
    samples = tuple(
        SampleVideo(
            bvid=get_str(value, "bvid", "search.user.sample"),
            title=plain_text(get_str(value, "title", "search.user.sample")),
        )
        for value in sample_values
    )
    uid = get_int(data, "mid", "search.user.item")
    level = optional_int(data, "level")
    followers = optional_int(data, "fans")
    if uid <= 0 or level < 0 or followers < 0:
        raise schema_error("search.extended.user")
    return _UserCandidate(
        uid=str(uid),
        name=plain_text(optional_str(data, "uname")),
        description=normalize_space(optional_str(data, "usign")),
        level=level,
        follower_count=followers,
        sample_videos=samples,
        video_count=get_int(data, "videos", "search.user.videos")
        if data.get("videos") is not None
        else None,
    )


def _candidate_id(
    candidate: _VideoCandidate | _UserCandidate | _OpusCandidate | _LiveCandidate | None,
) -> str | None:
    """Return a resource-specific key for cross-page deduplication."""
    if isinstance(candidate, _VideoCandidate):
        return candidate.video.bvid
    if isinstance(candidate, _UserCandidate):
        return candidate.uid
    if isinstance(candidate, _OpusCandidate):
        return candidate.cvid
    if isinstance(candidate, _LiveCandidate):
        return "live:" + candidate.room.room_id
    return None


def _matches_video(
    candidate: _VideoCandidate,
    filters: VideoSearchFilters | None,
    bounds: tuple[datetime | None, datetime | None],
) -> bool:
    """Check verified native bins and exact half-open timestamp boundaries on each video."""
    if filters is not None:
        if filters.category_id is not None:
            if candidate.category_id is None:
                raise schema_error("search.extended.video.typeid")
            if candidate.category_id != filters.category_id:
                return False
        seconds = candidate.video.duration_seconds
        if filters.duration is not None:
            lower, upper = {
                "under_10_minutes": (-1, 600),
                "10_to_30_minutes": (600, 1800),
                "30_to_60_minutes": (1800, 3600),
                "over_60_minutes": (3600, None),
            }[filters.duration]
            if seconds <= lower or (upper is not None and seconds > upper):
                return False
    return _within_publication_window(datetime.fromisoformat(candidate.video.published_at), *bounds)


def _validate_search(query: str, limit: int) -> str:
    """Normalize whitespace without changing keyword order, case or silently dropping terms."""
    # Validate the submitted length before collapsing whitespace so padding cannot bypass the cap.
    if len(query) > 100:
        raise ServiceError(ErrorCode.INVALID_ARGUMENT, "query may contain at most 100 characters")
    if any(
        unicodedata.category(character) in {"Cc", "Cf", "Cs"} and character not in "\t\r\n"
        for character in query
    ):
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT,
            "query must not contain control, invisible format or surrogate characters",
        )
    normalized = " ".join(query.split())
    if not normalized:
        raise ServiceError(ErrorCode.INVALID_ARGUMENT, "query must not be blank")
    if not 1 <= limit <= 10:
        raise ServiceError(ErrorCode.INVALID_ARGUMENT, "limit must be between 1 and 10")
    if len(normalized.split()) > 3:
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT,
            "query must use at most 3 whitespace-separated terms; shorten the query to "
            "1 to 3 distinctive keywords and retry",
        )
    return normalized


def _validate_published_bounds(
    published_after: datetime | None, published_before: datetime | None
) -> None:
    """Require explicit timezones and a nonempty inclusive/exclusive publication window."""
    for name, value in (
        ("published_after", published_after),
        ("published_before", published_before),
    ):
        if value is not None and value.utcoffset() is None:
            raise ServiceError(ErrorCode.INVALID_ARGUMENT, f"{name} must include a timezone")
    if (
        published_after is not None
        and published_before is not None
        and published_after >= published_before
    ):
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT, "published_after must be earlier than published_before"
        )


def _within_publication_window(
    published_at: datetime,
    published_after: datetime | None,
    published_before: datetime | None,
) -> bool:
    # Compare instants instead of calendar dates to preserve the requested half-open interval.
    return (published_after is None or published_at >= published_after) and (
        published_before is None or published_at < published_before
    )


def _normalize_video(data: JsonObject) -> VideoSummary:
    """Normalize a video search hit into the compact public summary."""
    raw_summary = optional_str(data, "description") or optional_str(data, "desc")
    summary, _ = truncate_summary(plain_text(raw_summary), 240)
    return VideoSummary(
        bvid=get_str(data, "bvid", "search.video"),
        title=plain_text(get_str(data, "title", "search.video")),
        summary=summary,
        author=Author(
            uid=str(get_int(data, "mid", "search.video")),
            name=plain_text(optional_str(data, "author") or optional_str(data, "uname")),
        ),
        duration_seconds=parse_duration(optional_str(data, "duration")),
        published_at=format_timestamp(get_int(data, "pubdate", "search.video")),
        stats=VideoStats(
            view_count=optional_int(data, "play"),
            danmaku_count=optional_int(data, "danmaku"),
            favorite_count=optional_int(data, "favorites"),
            like_count=optional_int(data, "like"),
        ),
    )


def _normalize_live_search(data: JsonObject) -> LiveSearchItem:
    """Normalize only verified room-index entries and separate popularity from watched counts."""
    if get_str(data, "type", "search.live.room") != "live_room":
        raise schema_error("search.live.room.type")
    room_id = get_int(data, "roomid", "search.live.room")
    uid = get_int(data, "uid", "search.live.room")
    status = get_int(data, "live_status", "search.live.room")
    population = get_int(data, "online", "search.live.room")
    if room_id <= 0 or uid <= 0 or population < 0:
        raise schema_error("search.live.room.identity_or_state")
    raw_start = get_str(data, "live_time", "search.live.room")
    started_at = None
    # Offline/replay entries can retain an old timestamp or use the source's zero-date placeholder.
    if raw_start != "0000-00-00 00:00:00":
        try:
            started_at = (
                datetime.strptime(raw_start, "%Y-%m-%d %H:%M:%S").replace(tzinfo=TAIPEI).isoformat()
            )
        except ValueError as exc:
            raise schema_error("search.live.room.live_time") from exc
    watched = optional_object(data, "watched_show")
    watched_count = (
        get_int(watched, "num", "search.live.room.watched_show") if watched is not None else None
    )
    if watched_count is not None and watched_count < 0:
        raise schema_error("search.live.room.watched_show.num")
    return LiveSearchItem(
        room_id=str(room_id),
        author=Author(uid=str(uid), name=plain_text(get_str(data, "uname", "search.live.room"))),
        title=plain_text(get_str(data, "title", "search.live.room")),
        live_status=normalize_live_status(status, "search.live.room.live_status"),
        population=population,
        watched_count=watched_count,
        started_at=started_at,
        area_name=plain_text(get_str(data, "cate_name", "search.live.room")),
    )
