"""Video detail, page cursors, related videos, and subtitle inventory."""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from .cache import AsyncTTLCache, CachePolicy
from .content import normalize_space, plain_text
from .models import (
    Author,
    ErrorCode,
    RelatedVideo,
    RelatedVideosOutput,
    ServiceError,
    SubtitleInfo,
    SubtitleTrack,
    VideoOutput,
    VideoPage,
    VideoRights,
    VideoSeasonTarget,
    VideoStats,
)
from .normalization import (
    JsonObject,
    JsonValue,
    as_list,
    as_object,
    format_timestamp,
    get_int,
    get_list,
    get_object,
    get_str,
    normalize_language,
    optional_bool,
    optional_int,
    optional_list,
    optional_str,
    schema_error,
    validate_bvid,
)
from .tokens import CursorTokenStore, VideoCursorState
from .transport import BilibiliTransport


@dataclass(frozen=True, slots=True)
class VideoPageCore:
    page_number: int
    cid: str
    title: str
    duration_seconds: int


@dataclass(frozen=True, slots=True)
class VideoCore:
    bvid: str
    aid: int
    title: str
    description: str
    author: Author
    duration_seconds: int
    published_at: str
    cover_url: str
    stats: VideoStats
    rights: VideoRights
    pages: tuple[VideoPageCore, ...]
    content_version: str
    collection: VideoSeasonTarget | None = None


@dataclass(frozen=True, slots=True)
class VideoViewSnapshot:
    """Share only public view fields and their acquisition time across projections."""

    data: JsonObject
    stats: VideoStats
    fetched_at: float


def require_public_playback(core: VideoCore) -> None:
    """Keep account-entitled paid content outside public full-text and frame tools."""
    if core.rights.paid:
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT,
            "Full subtitles and exact frames are limited to publicly playable videos",
        )


async def get_video_view(
    transport: BilibiliTransport,
    cache: AsyncTTLCache,
    bvid: str,
    ttl: int,
    *,
    clock: Callable[[], float] = time.time,
) -> tuple[VideoViewSnapshot, bool]:
    """Cache one safe view snapshot without giving derived projections a second TTL."""
    validate_bvid(bvid)

    async def produce() -> VideoViewSnapshot:
        """Verify common counters before caching, keeping detail-only checks independent."""
        raw = await transport.request_data(
            "/x/web-interface/view",
            {"bvid": bvid},
            not_found_codes=frozenset({-400, -404, 62002}),
        )
        data = as_object(raw, "video view")
        if get_str(data, "bvid", "video view") != bvid:
            raise ServiceError(
                ErrorCode.UPSTREAM_SCHEMA_CHANGED, "Bilibili returned a different video identifier"
            )
        public: JsonObject = {
            key: data[key]
            for key in ("bvid", "aid", "title", "desc", "duration", "pubdate", "pic")
            if key in data
        }
        for name, fields in (
            ("owner", ("mid", "name")),
            ("rights", ("pay", "ugc_pay", "arc_pay", "is_stein_gate", "is_cooperation")),
            ("ugc_season", ("id", "mid")),
        ):
            if data.get(name) is not None:
                item = data[name]
                public[name] = (
                    {key: item[key] for key in fields if key in item}
                    if isinstance(item, dict)
                    else item
                )
        if "pages" in data:
            pages = data["pages"]
            public["pages"] = (
                [
                    {key: page[key] for key in ("page", "cid", "part", "duration") if key in page}
                    if isinstance(page, dict)
                    else page
                    for page in pages
                ]
                if isinstance(pages, list)
                else pages
            )
        return VideoViewSnapshot(
            public, normalize_video_stats(get_object(data, "stat", "video")), clock()
        )

    # Different configured freshness policies must not reuse a longer-lived snapshot.
    return await cache.get_or_create(("video_view", bvid, ttl, "v1"), ttl, produce)


@dataclass(frozen=True, slots=True)
class SubtitleCue:
    start: float
    end: float
    content: str


@dataclass(frozen=True, slots=True)
class SubtitleTrackData:
    public: SubtitleTrack
    cues: tuple[SubtitleCue, ...]


@dataclass(frozen=True, slots=True)
class SubtitleInventory:
    tracks: tuple[SubtitleTrackData, ...]

    @property
    def default(self) -> SubtitleTrackData | None:
        for track in self.tracks:
            if track.public.ai_kind != "translation" and track.public.line_count > 3:
                return track
        return None


class VideoService:
    """Normalize video APIs and keep pagination/subtitle state internal."""

    def __init__(
        self,
        transport: BilibiliTransport,
        cache: AsyncTTLCache,
        policy: CachePolicy,
        tokens: CursorTokenStore,
    ) -> None:
        """Bind shared provider, cache policy, and opaque continuation store."""
        self._transport = transport
        self._cache = cache
        self._policy = policy
        self._tokens = tokens

    async def get_video(
        self,
        bvid: str,
        cursor: str | None,
        subtitle: Literal["all"] | None,
    ) -> tuple[VideoOutput, bool]:
        """Return one ten-page window and issue only opaque continuation state."""
        validate_bvid(bvid)
        mode: Literal["default", "all"] = "all" if subtitle == "all" else "default"
        core, cache_hit = await self.get_core(bvid)
        if cursor is None:
            start = 0
        else:
            state = self._tokens.resolve(cursor, bvid=bvid, subtitle_mode=mode)
            if state.content_version != core.content_version:
                raise ServiceError(
                    ErrorCode.INVALID_CURSOR, "cursor content version is no longer valid"
                )
            start = state.next_page - 1
        if start >= len(core.pages):
            raise ServiceError(ErrorCode.INVALID_CURSOR, "cursor has no remaining video pages")
        selected = core.pages[start : start + 10]
        inventories = await asyncio.gather(
            *(self.get_subtitle_inventory(core, page) for page in selected)
        )
        pages = [
            VideoPage(
                page_number=page.page_number,
                cid=page.cid,
                title=page.title,
                duration_seconds=page.duration_seconds,
                subtitle_info=_public_subtitle_info(inventory, mode),
            )
            for page, (inventory, _) in zip(selected, inventories, strict=True)
        ]
        next_index = start + len(selected)
        next_cursor = None
        if next_index < len(core.pages):
            next_cursor = self._tokens.issue(
                VideoCursorState(
                    bvid=bvid,
                    next_page=next_index + 1,
                    subtitle_mode=mode,
                    content_version=core.content_version,
                )
            )
        output = VideoOutput(
            bvid=core.bvid,
            collection=core.collection,
            title=core.title,
            description=core.description,
            author=core.author,
            duration_seconds=core.duration_seconds,
            published_at=core.published_at,
            cover_url=core.cover_url,
            stats=core.stats,
            rights=core.rights,
            page_count=len(core.pages),
            pages=pages,
            next_cursor=next_cursor,
            original_url=f"https://www.bilibili.com/video/{core.bvid}",
        )
        return output, cache_hit and all(hit for _, hit in inventories)

    async def get_core(self, bvid: str) -> tuple[VideoCore, bool]:
        """Refresh video metadata, statistics and availability together within the detail TTL."""
        snapshot, hit = await get_video_view(
            self._transport, self._cache, bvid, self._policy.detail
        )
        return _normalize_core(snapshot), hit

    async def get_subtitle_inventory(
        self,
        core: VideoCore,
        page: VideoPageCore,
    ) -> tuple[SubtitleInventory, bool]:
        """Fetch player metadata and all subtitle bodies to produce reliable line counts."""
        return await self._cache.get_or_create(
            ("subtitle_inventory", core.bvid, page.cid, core.content_version, "v2"),
            self._policy.subtitle_inventory,
            lambda: self._fetch_subtitle_inventory(core, page),
        )

    async def list_related(self, bvid: str, limit: int) -> tuple[RelatedVideosOutput, bool]:
        """Return a bounded related-video list without public images or URLs."""
        validate_bvid(bvid)
        if not 1 <= limit <= 10:
            raise ServiceError(ErrorCode.INVALID_ARGUMENT, "limit must be between 1 and 10")

        async def produce() -> RelatedVideosOutput:
            raw = await self._transport.request_data(
                "/x/web-interface/archive/related",
                {"bvid": bvid},
            )
            values = as_list(raw, "related.data")
            items = [_normalize_related(as_object(value, "related.item")) for value in values[:10]]
            return RelatedVideosOutput(bvid=bvid, items=items)

        page, cache_hit = await self._cache.get_or_create(
            ("related", bvid, "v2"),
            self._policy.detail,
            produce,
        )
        return page.model_copy(update={"items": page.items[:limit]}), cache_hit

    async def _fetch_subtitle_inventory(
        self,
        core: VideoCore,
        page: VideoPageCore,
    ) -> SubtitleInventory:
        """Refresh track availability while reusing previously downloaded subtitle bodies."""
        raw = await self._transport.request_data(
            "/x/player/wbi/v2",
            {
                "aid": core.aid,
                "cid": int(page.cid),
                "isGaiaAvoided": "false",
                "web_location": 1315873,
            },
            wbi=True,
            wbi2=True,
        )
        data = as_object(raw, "player.data")
        subtitle = get_object(data, "subtitle", "player")
        values = optional_list(subtitle, "subtitles")
        metadata = [as_object(value, "player.subtitle.item") for value in values]
        cue_sets = await asyncio.gather(
            *(self._download_cues(core, page, item) for item in metadata)
        )
        tracks = tuple(
            _normalize_track(item, cues) for item, cues in zip(metadata, cue_sets, strict=True)
        )
        return SubtitleInventory(tracks=tracks)

    async def _download_cues(
        self, core: VideoCore, page: VideoPageCore, item: JsonObject
    ) -> tuple[SubtitleCue, ...]:
        """Cache source-bound cues without retaining a temporary URL in cache keys or values."""
        # subtitle_url is the current directly downloadable JSON location. The v2 field may
        # contain an encoded transport path and is only a compatibility fallback.
        url = optional_str(item, "subtitle_url") or optional_str(item, "subtitle_url_v2")
        if not url:
            return ()
        # The complete address is fingerprinted: unknown query parameters may select content.
        # A new locator or track ID must not reuse an older body's long cache lifetime.
        source_version = hashlib.sha256(url.encode()).hexdigest()
        cues, _ = await self._cache.get_or_create(
            (
                "subtitle_body",
                core.bvid,
                page.cid,
                core.content_version,
                _subtitle_track_id(item),
                source_version,
                "v1",
            ),
            self._policy.subtitle_body,
            lambda: self._fetch_cues(url),
        )
        return cues

    async def _fetch_cues(self, url: str) -> tuple[SubtitleCue, ...]:
        """Download and normalize one subtitle body, retaining only timing and text."""
        payload = await self._transport.download_json(url)
        values = get_list(payload, "body", "subtitle")
        return tuple(_normalize_cue(as_object(value, "subtitle.body.item")) for value in values)


def _normalize_core(snapshot: VideoViewSnapshot) -> VideoCore:
    """Derive detail from the shared view without changing its source freshness."""
    data = snapshot.data
    owner = get_object(data, "owner", "video")
    rights_raw = get_object(data, "rights", "video")
    page_values = get_list(data, "pages", "video")
    pages = tuple(
        _normalize_page(
            as_object(value, "video.pages.item"), fallback_title=get_str(data, "title", "video")
        )
        for value in page_values
    )
    if not pages:
        raise schema_error("video.pages")
    version_source = "|".join(f"{page.cid}:{page.duration_seconds}" for page in pages)
    collection = None
    if data.get("ugc_season") is not None:
        season = get_object(data, "ugc_season", "video")
        collection = VideoSeasonTarget(
            kind="video_season",
            season_id=str(get_int(season, "id", "video season")),
            owner_uid=str(get_int(season, "mid", "video season")),
        )
    return VideoCore(
        collection=collection,
        bvid=get_str(data, "bvid", "video"),
        aid=get_int(data, "aid", "video"),
        title=plain_text(get_str(data, "title", "video")),
        description=normalize_space(optional_str(data, "desc")),
        author=Author(
            uid=str(get_int(owner, "mid", "video.owner")),
            name=get_str(owner, "name", "video.owner"),
        ),
        duration_seconds=get_int(data, "duration", "video"),
        published_at=format_timestamp(get_int(data, "pubdate", "video")),
        cover_url=get_str(data, "pic", "video"),
        stats=snapshot.stats,
        rights=VideoRights(
            paid=True
            if any(optional_bool(rights_raw, key) for key in ("pay", "ugc_pay", "arc_pay"))
            else None,
            interactive=True if optional_bool(rights_raw, "is_stein_gate") else None,
            collaboration=True if optional_bool(rights_raw, "is_cooperation") else None,
        ),
        pages=pages,
        content_version=hashlib.sha256(version_source.encode()).hexdigest()[:16],
    )


def normalize_video_stats(data: JsonObject) -> VideoStats:
    """Reuse public counter mapping and preserve unavailable counts instead of inventing zero."""
    counts: dict[str, int | None] = {}
    for field, source in (
        ("view_count", "view"),
        ("danmaku_count", "danmaku"),
        ("comment_count", "reply"),
        ("favorite_count", "favorite"),
        ("coin_count", "coin"),
        ("share_count", "share"),
        ("like_count", "like"),
    ):
        value = data.get(source)
        # Bilibili hides selected view counts as "--"; hidden is different from zero.
        if value is None or (source == "view" and value == "--"):
            counts[field] = None
        elif type(value) is int and value >= 0:
            counts[field] = value
        else:
            raise schema_error(f"video.stat.{source}")
    return VideoStats.model_validate(counts)


def _normalize_page(data: JsonObject, *, fallback_title: str) -> VideoPageCore:
    return VideoPageCore(
        page_number=get_int(data, "page", "video.page"),
        cid=str(get_int(data, "cid", "video.page")),
        title=plain_text(optional_str(data, "part", fallback_title)),
        duration_seconds=get_int(data, "duration", "video.page"),
    )


def _normalize_track(data: JsonObject, cues: tuple[SubtitleCue, ...]) -> SubtitleTrackData:
    """Normalize subtitle metadata and attach already-validated cue content."""
    track_id = _subtitle_track_id(data)
    is_ai = optional_int(data, "type") == 1
    ai_type = optional_int(data, "ai_type", -1)
    ai_kind: Literal["transcription", "translation", "other"] | None = None
    if is_ai:
        ai_kind = "translation" if ai_type == 1 else "transcription" if ai_type == 0 else "other"
    return SubtitleTrackData(
        public=SubtitleTrack(
            track_id=track_id,
            language=normalize_language(get_str(data, "lan", "subtitle.track")),
            language_label=get_str(data, "lan_doc", "subtitle.track"),
            is_ai_generated=is_ai,
            ai_kind=ai_kind,
            line_count=len(cues),
        ),
        cues=cues,
    )


def _subtitle_track_id(data: JsonObject) -> str:
    return optional_str(data, "id_str") or str(get_int(data, "id", "subtitle.track"))


def _normalize_cue(data: JsonObject) -> SubtitleCue:
    start = _number(data.get("from"), "subtitle.body.from")
    end = _number(data.get("to"), "subtitle.body.to")
    return SubtitleCue(start=start, end=end, content=get_str(data, "content", "subtitle.body"))


def _number(value: JsonValue | None, context: str) -> float:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    raise schema_error(context)


def _public_subtitle_info(
    inventory: SubtitleInventory,
    mode: Literal["default", "all"],
) -> SubtitleInfo:
    """Apply public translation filtering while preserving complete inventory counts."""
    all_tracks = [track.public for track in inventory.tracks]
    if mode == "all":
        selected = all_tracks
    else:
        selected = [track for track in all_tracks if track.ai_kind != "translation"][:3]
    default = inventory.default
    return SubtitleInfo(
        track_count=len(all_tracks),
        language_count=len({track.language for track in all_tracks}),
        default_track_id=default.public.track_id if default is not None else None,
        tracks=selected,
        tracks_truncated=True if len(selected) < len(all_tracks) else None,
    )


def _normalize_related(data: JsonObject) -> RelatedVideo:
    """Normalize one related video without cover, avatar, or reconstructable URL."""
    owner = get_object(data, "owner", "related")
    stat = get_object(data, "stat", "related")
    return RelatedVideo(
        bvid=get_str(data, "bvid", "related"),
        title=plain_text(get_str(data, "title", "related")),
        summary=normalize_space(optional_str(data, "desc")),
        author=Author(
            uid=str(get_int(owner, "mid", "related.owner")),
            name=get_str(owner, "name", "related.owner"),
        ),
        duration_seconds=get_int(data, "duration", "related"),
        published_at=format_timestamp(get_int(data, "pubdate", "related")),
        view_count=optional_int(stat, "view"),
    )
