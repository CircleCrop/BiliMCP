"""User profiles, public activities, and range-based content aggregation."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Literal

from .activities import normalize_activity
from .cache import AsyncTTLCache, CachePolicy
from .content import (
    normalize_space,
    parse_duration,
    plain_text,
    truncate_summary,
)
from .models import (
    Author,
    ErrorCode,
    OpusListItem,
    ServiceError,
    UserActivitiesOutput,
    UserActivity,
    UserContentOutput,
    UserOutput,
    UserStats,
    UserVideoSummary,
    Verification,
    VideoStats,
)
from .normalization import (
    JsonObject,
    as_object,
    format_timestamp,
    get_list,
    get_object,
    get_str,
    optional_bool,
    optional_count,
    optional_int,
    optional_object,
    optional_str,
    schema_error,
    validate_decimal_id,
    verification_from_profile,
)
from .opuses import OpusService
from .ranges import next_item_range, parse_range
from .transport import BilibiliTransport


@dataclass(frozen=True, slots=True)
class SpaceProfile:
    uid: str
    name: str
    description: str
    avatar_url: str
    level: int
    is_live: bool
    verification: Verification | None


class UserService:
    """Hide native pages/offsets while caching normalized public ranges."""

    def __init__(
        self,
        transport: BilibiliTransport,
        cache: AsyncTTLCache,
        policy: CachePolicy,
        opuses: OpusService,
    ) -> None:
        """Bind shared provider, cache policy, and Opus normalization service."""
        self._transport = transport
        self._cache = cache
        self._policy = policy
        self._opuses = opuses

    async def get_space_profile(self, uid: str) -> tuple[SpaceProfile, bool]:
        """Fetch the current space profile used by get_user and user search."""
        validate_decimal_id(uid, "uid")
        return await self._cache.get_or_create(
            ("space_profile", uid, "v1"),
            self._policy.profile,
            lambda: self._fetch_space_profile(uid),
        )

    async def get_user(self, uid: str) -> tuple[UserOutput, bool]:
        """Join profile, public relation counts, and video count without private account data."""
        profile_result, relation_result, video_count_result = await asyncio.gather(
            self.get_space_profile(uid),
            self._relation(uid),
            self.get_video_count(uid),
        )
        profile, profile_hit = profile_result
        relation, relation_hit = relation_result
        video_count, videos_hit = video_count_result
        return (
            UserOutput(
                uid=profile.uid,
                name=profile.name,
                description=profile.description,
                avatar_url=profile.avatar_url,
                verification=profile.verification,
                level=profile.level,
                stats=UserStats(
                    follower_count=optional_int(relation, "follower"),
                    following_count=optional_int(relation, "following"),
                    video_count=video_count,
                ),
                original_url=f"https://space.bilibili.com/{uid}",
            ),
            profile_hit and relation_hit and videos_hit,
        )

    async def get_video_count(self, uid: str) -> tuple[int, bool]:
        """Read the authoritative space upload count shared by profile and user search."""
        validate_decimal_id(uid, "uid")
        data, cache_hit = await self.video_page(uid, page=1, sort="newest")
        page = get_object(data, "page", "user.videos")
        return optional_int(page, "count"), cache_hit

    async def list_activities(
        self,
        uid: str,
        range_value: str,
    ) -> tuple[UserActivitiesOutput, bool]:
        """Pull dynamic offsets from the beginning until the requested public range is known."""
        validate_decimal_id(uid, "uid")
        requested = parse_range(range_value, max_span=10)
        items: list[UserActivity] = []
        offset = ""
        has_more = True
        pages = 0
        cache_hit = True
        while len(items) < requested.end and has_more:
            pages += 1
            if pages > 50:
                raise ServiceError(
                    ErrorCode.UPSTREAM_UNAVAILABLE,
                    "Bilibili activity pagination did not converge",
                )
            data, page_hit = await self.activity_page(uid, offset)
            cache_hit = cache_hit and page_hit
            for value in get_list(data, "items", "activities"):
                items.append(normalize_activity(as_object(value, "activities.item")))
            has_more = bool(data.get("has_more"))
            offset = optional_str(data, "offset")
            if has_more and not offset:
                raise schema_error("activities.offset")
        # A valid empty first page is a result, not an out-of-range continuation.
        if requested.start > len(items) and not (requested.start == 1 and not items):
            raise ServiceError(ErrorCode.INVALID_RANGE, "range starts beyond available activities")
        selected = items[requested.start - 1 : requested.end]
        more = has_more or len(items) > requested.end
        return (
            UserActivitiesOutput(
                uid=uid,
                range=range_value,
                items=selected,
                next_range=next_item_range(requested.end, has_more=more, max_span=requested.span),
            ),
            cache_hit,
        )

    async def list_content(
        self,
        uid: str,
        kind: Literal["videos", "opuses"],
        sort: Literal["newest", "popular"],
        range_value: str,
    ) -> tuple[UserContentOutput, bool]:
        """Dispatch to page- or offset-backed content while preserving one range contract."""
        validate_decimal_id(uid, "uid")
        requested = parse_range(range_value, max_span=10)
        if kind == "opuses" and sort == "popular":
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT, "popular sort is available only for videos"
            )
        if kind == "videos":
            return await self._list_videos(
                uid, sort, range_value, requested.start, requested.end, requested.span
            )
        return await self._list_opuses(
            uid, range_value, requested.start, requested.end, requested.span
        )

    async def _fetch_space_profile(self, uid: str) -> SpaceProfile:
        """Normalize the WBI2 space response and its authoritative attestation text."""
        raw = await self._transport.request_data(
            "/x/space/wbi/acc/info",
            {"mid": uid},
            wbi=True,
            wbi2=True,
            not_found_codes=frozenset({-400, -404}),
        )
        data = as_object(raw, "user.data")
        live_room = optional_object(data, "live_room")
        return SpaceProfile(
            uid=str(optional_int(data, "mid")),
            name=get_str(data, "name", "user"),
            description=normalize_space(optional_str(data, "sign")),
            avatar_url=get_str(data, "face", "user"),
            level=optional_int(data, "level"),
            is_live=live_room is not None and optional_int(live_room, "liveStatus") == 1,
            verification=verification_from_profile(data),
        )

    async def _relation(self, uid: str) -> tuple[JsonObject, bool]:
        async def produce() -> JsonObject:
            raw = await self._transport.request_data("/x/relation/stat", {"vmid": uid})
            return as_object(raw, "relation.data")

        # Follower totals can tolerate more staleness than the profile used for Lv2 eligibility.
        return await self._cache.get_or_create(("relation", uid), self._policy.relation, produce)

    async def video_page(
        self,
        uid: str,
        *,
        page: int,
        sort: Literal["newest", "popular"],
        cache_namespace: str | None = None,
    ) -> tuple[JsonObject, bool]:
        """Fetch and cache one internal WBI2 video page for range aggregation."""

        async def produce() -> JsonObject:
            """Request one signed native page and validate its data object."""
            raw = await self._transport.request_data(
                "/x/space/wbi/arc/search",
                {
                    "mid": uid,
                    "ps": 30,
                    "tid": 0,
                    "pn": page,
                    "keyword": "",
                    "order": "pubdate" if sort == "newest" else "click",
                    "order_avoided": "true",
                    "platform": "web",
                },
                wbi=True,
                wbi2=True,
            )
            return as_object(raw, "user.videos.data")

        key: tuple[object, ...] = ("user_video_page", uid, sort, page, "v1")
        # A requested history refresh owns fresh pages without invalidating ordinary list reads.
        if cache_namespace is not None:
            key += (cache_namespace,)
        return await self._cache.get_or_create(
            key,
            self._policy.user_content,
            produce,
        )

    async def activity_page(
        self, uid: str, offset: str, *, cache_namespace: str | None = None
    ) -> tuple[JsonObject, bool]:
        """Fetch and cache one native activity offset for overlapping public ranges."""

        async def produce() -> JsonObject:
            """Request one signed activity page and validate its data object."""
            raw = await self._transport.request_data(
                "/x/polymer/web-dynamic/v1/feed/space",
                {
                    "host_mid": uid,
                    "offset": offset,
                    "features": (
                        "itemOpusStyle,listOnlyfans,opusBigCover,onlyfansVote,forwardListHidden,"
                        "decorationCard,commentsNewVersion,onlyfansAssetsV2,ugcDelete,onlyfansQaCard"
                    ),
                    "timezone_offset": -480,
                    "x-bili-device-req-json": '{"platform":"web","device":"pc"}',
                    "x-bili-web-req-json": '{"spm_id":"333.1387"}',
                },
                wbi=True,
                wbi2=True,
            )
            return as_object(raw, "activities.data")

        key: tuple[object, ...] = ("activity_page", uid, offset, "v1")
        if cache_namespace is not None:
            key += (cache_namespace,)
        return await self._cache.get_or_create(
            key,
            self._policy.activities,
            produce,
        )

    async def _list_videos(
        self,
        uid: str,
        sort: Literal["newest", "popular"],
        range_value: str,
        start: int,
        end: int,
        span: int,
    ) -> tuple[UserContentOutput, bool]:
        """Fetch only enough 30-item pages to satisfy a public video range."""
        # Numbered pages support direct access; fetching every preceding page amplifies input.
        first_data, first_hit = await self.video_page(uid, page=1, sort=sort)
        available = optional_int(get_object(first_data, "page", "user.videos"), "count")
        if start > available and not (start == 1 and available == 0):
            raise ServiceError(ErrorCode.INVALID_RANGE, "range starts beyond available user videos")
        start_page = (start - 1) // 30 + 1
        end_page = (min(end, available) - 1) // 30 + 1
        results = []
        for page in range(start_page, end_page + 1):
            results.append(
                (first_data, first_hit)
                if page == 1
                else await self.video_page(uid, page=page, sort=sort)
            )
        values = []
        for data, _ in results:
            listing = get_object(data, "list", "user.videos")
            values.extend(get_list(listing, "vlist", "user.videos.list"))
        offset = (start_page - 1) * 30
        items: list[UserVideoSummary | OpusListItem] = [
            normalize_user_video(as_object(value, "user.videos.item"))
            for value in values[start - 1 - offset : min(end, available) - offset]
        ]
        return (
            UserContentOutput(
                uid=uid,
                kind="videos",
                sort=sort,
                range=range_value,
                available_count=available,
                items=items,
                next_range=next_item_range(end, has_more=end < available, max_span=span),
            ),
            first_hit and all(cache_hit for _, cache_hit in results),
        )

    async def opus_page(
        self, uid: str, offset: str, *, cache_namespace: str | None = None
    ) -> tuple[JsonObject, bool]:
        """Fetch and cache one native Opus offset for overlapping public ranges."""

        async def produce() -> JsonObject:
            """Request one signed Opus listing page and validate its data object."""
            raw = await self._transport.request_data(
                "/x/polymer/web-dynamic/v1/opus/feed/space",
                {
                    "host_mid": uid,
                    "offset": offset,
                    "type": "all",
                    "web_location": "333.1387",
                },
                wbi=True,
            )
            return as_object(raw, "user.opuses.data")

        key: tuple[object, ...] = ("user_opus_page", uid, offset, "v1")
        if cache_namespace is not None:
            key += (cache_namespace,)
        return await self._cache.get_or_create(
            key,
            self._policy.user_content,
            produce,
        )

    async def _list_opuses(
        self,
        uid: str,
        range_value: str,
        start: int,
        end: int,
        span: int,
    ) -> tuple[UserContentOutput, bool]:
        """Follow hidden Opus offsets and enrich only the selected public summaries."""
        values: list[JsonObject] = []
        offset = ""
        has_more = True
        pages = 0
        cache_hit = True
        while len(values) < end and has_more:
            pages += 1
            if pages > 50:
                raise ServiceError(
                    ErrorCode.UPSTREAM_UNAVAILABLE, "Bilibili Opus pagination did not converge"
                )
            data, page_hit = await self.opus_page(uid, offset)
            cache_hit = cache_hit and page_hit
            values.extend(
                as_object(value, "user.opuses.item")
                for value in get_list(data, "items", "user.opuses")
            )
            has_more = bool(data.get("has_more"))
            offset = optional_str(data, "offset")
            if has_more and not offset:
                raise schema_error("user.opuses.offset")
        if start > len(values) and not (start == 1 and not values):
            raise ServiceError(ErrorCode.INVALID_RANGE, "range starts beyond available user Opuses")
        selected = values[start - 1 : end]
        full_results = await asyncio.gather(
            *(
                self._opuses.get_resource(get_str(value, "opus_id", "user.opuses.item"))
                for value in selected
            )
        )
        return (
            UserContentOutput(
                uid=uid,
                kind="opuses",
                sort="newest",
                range=range_value,
                items=[full.list_item() for full, _ in full_results],
                next_range=next_item_range(
                    end,
                    has_more=has_more or len(values) > end,
                    max_span=span,
                ),
            ),
            cache_hit and all(resource_hit for _, resource_hit in full_results),
        )


def normalize_user_video(data: JsonObject) -> UserVideoSummary:
    """Normalize one space video without returning its cover or URL."""
    summary, _ = truncate_summary(optional_str(data, "description"), 240)
    return UserVideoSummary(
        bvid=get_str(data, "bvid", "user.video"),
        title=plain_text(get_str(data, "title", "user.video")),
        summary=summary,
        author=Author(
            uid=str(optional_int(data, "mid")),
            name=get_str(data, "author", "user.video"),
        ),
        duration_seconds=parse_duration(optional_str(data, "length")),
        published_at=format_timestamp(optional_int(data, "created")),
        stats=VideoStats(
            view_count=optional_count(data, "play"),
            danmaku_count=optional_count(data, "video_review"),
            comment_count=optional_count(data, "comment"),
        ),
        relationship=("collaboration" if optional_bool(data, "is_union_video") else None),
    )
