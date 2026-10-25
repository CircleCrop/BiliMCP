"""Runtime-owned facade wiring shared transport, cache, tokens, and domain services."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import httpx

from .cache import AsyncTTLCache, CachePolicy
from .collections import (
    CollectionDirectoryOutput,
    CollectionOutput,
    CollectionService,
    CollectionTarget,
)
from .comment_search import CommentSearchOutput, CommentSearchService
from .comments import CommentService
from .content_search import ContentSearchOutput, ContentSearchService, ContentSearchTarget
from .danmaku import DanmakuService
from .images import ImageMedia, ImageService
from .live import ExtendedLiveSearchOutput, LiveRoomOutput, LiveService
from .live_guards import LiveGuardsService
from .medals import MedalService, UserMedalsOutput
from .models import (
    ActivityOutput,
    ActivityResourceRef,
    ArticleOutput,
    CommentListOutput,
    CommentTargetInput,
    CommentThreadOutput,
    DanmakuKind,
    DanmakuListOutput,
    DanmakuSampling,
    ExtendedSearchOutput,
    ImageOutput,
    ImagePart,
    ImageRegion,
    ImageTarget,
    LiveGuardsOutput,
    OpusResourceRef,
    PlaybackWindow,
    RelatedVideosOutput,
    ResolveResourceOutput,
    TranscriptOutput,
    UserActivitiesOutput,
    UserContentOutput,
    UserOutput,
    VideoFrameOutput,
    VideoFramesOutput,
    VideoOutput,
    VideoSearchFilters,
)
from .opuses import OpusService
from .persistent_cache import PersistentHistoryCache
from .resource_resolution import ResourceResolutionService
from .scheduler import SendBudget
from .search_service import SearchService
from .supporters import SupporterService, UserSupportersOutput
from .tokens import CursorTokenStore
from .transcripts import TranscriptService
from .transport import BilibiliTransport
from .user_history import UserContentSearchOutput, UserHistorySearchService
from .users import UserService
from .video_frames import VideoFrameService
from .video_stats import VideoStatsBatchOutput, VideoStatsService
from .videos import VideoService


class BilibiliClient:
    """Expose the read-only operations over one bounded process-local runtime."""

    def __init__(
        self,
        cookie: str | None,
        *,
        ac_time_value: str | None = None,
        credential_state_path: Path | None = None,
        qr_code_path: Path | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        cache_policy: CachePolicy | None = None,
        token_store: CursorTokenStore | None = None,
        debug_log_http: bool = False,
        upstream_requests_per_minute: int = 0,
        upstream_burst: int = 60,
        upstream_concurrency: int = 8,
        upstream_requests_per_call: int = 0,
        upstream_spacing_seconds: float = 0.0,
        upstream_send_budget: SendBudget | None = None,
        upstream_queue_timeout_seconds: float = 15.0,
        upstream_queue_capacity: int = 64,
        history_path: Path | None = None,
        history_max_bytes: int = 4 * 1024**3,
        history_ttl_seconds: int = 14 * 86400,
        video_cache_path: Path | None = None,
        video_cache_max_bytes: int = 20 * 1024**3,
        video_cache_ttl_seconds: int = 7 * 86400,
        image_media: ImageMedia | None = None,
    ) -> None:
        """Create resources only for this runtime; imports remain side-effect free."""
        self.cache_policy = cache_policy or CachePolicy()
        self.cache = AsyncTTLCache(
            self.cache_policy.max_items, max_bytes=self.cache_policy.max_bytes
        )
        self.tokens = token_store or CursorTokenStore()
        self.history = PersistentHistoryCache(history_path, history_max_bytes, history_ttl_seconds)
        if history_path is not None:
            self.cache.set_persistent_store(self.history)
        self.transport = BilibiliTransport(
            cookie,
            ac_time_value=ac_time_value,
            credential_state_path=credential_state_path,
            qr_code_path=qr_code_path,
            transport=transport,
            debug_log_http=debug_log_http,
            upstream_requests_per_minute=upstream_requests_per_minute,
            upstream_burst=upstream_burst,
            upstream_concurrency=upstream_concurrency,
            upstream_requests_per_call=upstream_requests_per_call,
            upstream_spacing_seconds=upstream_spacing_seconds,
            upstream_send_budget=upstream_send_budget,
            upstream_queue_timeout_seconds=upstream_queue_timeout_seconds,
            upstream_queue_capacity=upstream_queue_capacity,
        )
        self.opuses = OpusService(self.transport, self.cache, self.cache_policy)
        self.videos = VideoService(self.transport, self.cache, self.cache_policy, self.tokens)
        self.video_stats = VideoStatsService(self.transport, self.cache)
        self.transcripts = TranscriptService(self.videos, self.cache, self.cache_policy)
        self.users = UserService(self.transport, self.cache, self.cache_policy, self.opuses)
        self.user_history = UserHistorySearchService(
            self.transport, self.users, self.history, self.tokens, self.opuses
        )
        self.search = SearchService(
            self.transport,
            self.cache,
            self.cache_policy,
            self.users,
            tokens=self.tokens,
            opuses=self.opuses,
        )
        self.collections = CollectionService(self.transport, self.history)
        self.live = LiveService(self.transport, self.cache, self.cache_policy)
        self.live_guards = LiveGuardsService(
            self.transport, self.cache, self.tokens, live_service=self.live
        )
        self.medals = MedalService(self.transport, self.cache)
        self.supporters = SupporterService(self.transport, self.cache)
        self.content_search = ContentSearchService(
            self.transport, self.transcripts, self.opuses, self.cache, self.cache_policy
        )
        self.resources = ResourceResolutionService(self.transport, self.cache)
        self.comments = CommentService(
            self.transport,
            self.cache,
            self.cache_policy,
            self.videos,
            self.opuses,
            self.resources,
        )
        self._owns_image_media = image_media is None
        media = image_media or ImageMedia(
            history_path.with_name(history_path.stem + "-images.sqlite3") if history_path else None,
            transport,
            upstream_send_budget,
            video_cache_path=video_cache_path,
            video_cache_max_bytes=video_cache_max_bytes,
            video_cache_ttl_seconds=video_cache_ttl_seconds,
        )
        self.images = ImageService(
            media, self.videos, self.users, self.opuses, self.comments, self.collections, self.cache
        )
        self.video_frames = VideoFrameService(self.videos, self.transport, self.cache, media)
        self.comment_search = CommentSearchService(
            self.transport, self.comments, self.cache, self.cache_policy, self.tokens
        )
        self.danmaku = DanmakuService(self.transport, self.videos, self.cache, self.cache_policy)

    async def start(self) -> None:
        """Preload signed-provider state for an evidence-based readiness result."""
        await self.transport.start()
        await self.history.start()
        if self._owns_image_media:
            await self.images.media.store.start()

    async def close(self) -> None:
        """Release HTTP resources and completed process-local state."""
        # Producers may still use media or SQLite during cancellation cleanup.
        await self.cache.close()
        if self._owns_image_media:
            await self.images.media.close()
        await self.history.close()
        self.tokens.clear()
        await self.transport.close()

    async def list_user_medals(
        self, uid: str, range: str = "1..10"
    ) -> tuple[UserMedalsOutput, bool]:
        """Read the user's visible fan medals through the official endpoint."""
        return await self.medals.list_user_medals(uid, range)

    async def list_user_supporters(self, uid: str) -> tuple[UserSupportersOutput, bool]:
        """Read only a creator's publicly displayed charging showcase."""
        return await self.supporters.list_user_supporters(uid)

    async def get_video_stats(self, bvids: list[str]) -> tuple[VideoStatsBatchOutput, bool]:
        """Read at most five videos' counters through an independent statistics cache."""
        return await self.video_stats.get_video_stats(bvids)

    async def get_video_frame(
        self,
        bvid: str,
        time_seconds: float | None = None,
        page_number: int | None = None,
        mode: Literal["preview", "exact"] = "preview",
        *,
        times_seconds: list[float] | None = None,
        window: PlaybackWindow | None = None,
        frame_ref: str | None = None,
        region: ImageRegion | None = None,
    ) -> tuple[VideoFrameOutput | VideoFramesOutput, bool]:
        """Read source-timed frames or a bounded visual-change contact sheet."""
        return await self.video_frames.get_frame(
            bvid,
            time_seconds,
            page_number,
            mode,
            times_seconds=times_seconds,
            window=window,
            frame_ref=frame_ref,
            region=region,
        )

    async def search_comments(
        self,
        target: CommentTargetInput,
        query: str | None = None,
        published_after: str | None = None,
        cursor: str | None = None,
    ) -> tuple[CommentSearchOutput, bool]:
        """Resume a bounded literal and publication-time search of visible root comments."""
        return await self.comment_search.search_comments(target, query, published_after, cursor)

    def readiness(self) -> dict[str, bool]:
        """Return redacted provider readiness without exposing runtime credentials."""
        return self.transport.readiness()

    async def resolve_resource(self, text: str) -> tuple[ResolveResourceOutput, bool]:
        """Resolve public resource identities through fixed, audited upstream paths."""
        return await self.resources.resolve_resource(text)

    async def search_extended(
        self,
        query: str,
        kind: Literal["videos", "users", "opuses", "live"] = "videos",
        sort: Literal[
            "relevance", "newest", "views", "followers", "popularity", "recent_live"
        ] = "relevance",
        filters: VideoSearchFilters | None = None,
        limit: int = 5,
        cursor: str | None = None,
    ) -> tuple[ExtendedSearchOutput | ExtendedLiveSearchOutput, bool]:
        """Delegate parameter-bound search with a shared continuation store and send budget."""
        return await self.search.search_extended(query, kind, sort, filters, limit, cursor)

    async def get_activity(
        self, activity_id: str, range_value: str = "1..3000"
    ) -> tuple[ActivityOutput, bool]:
        """Read this activity's own body and a single target reference."""
        output, hit = await self.opuses.get_activity(activity_id, range_value)
        if output.images:
            output.image_target = ActivityResourceRef(activity_id=output.activity_id)
        return output, hit

    async def search_content(
        self,
        target: ContentSearchTarget,
        query: str,
        range_value: str = "1..10",
        case_sensitive: bool = False,
        content_version: str | None = None,
    ) -> tuple[ContentSearchOutput, bool]:
        """Return version-bound literal matches with original text positions."""
        return await self.content_search.search_content(
            target, query, range_value, case_sensitive, content_version
        )

    async def search_user_content(
        self,
        uid: str,
        kind: Literal["videos", "opuses", "activities"] = "videos",
        query: str | None = None,
        published_after: str | None = None,
        published_before: str | None = None,
        limit: int = 10,
        cursor: str | None = None,
        freshness: Literal["cached", "refresh"] = "cached",
    ) -> tuple[UserContentSearchOutput, bool]:
        """Filter a committed user-history generation and resume bounded scanning."""
        return await self.user_history.search_user_content(
            uid, kind, query, published_after, published_before, limit, cursor, freshness
        )

    async def list_user_collections(
        self,
        uid: str,
        kind: Literal["public_favorites", "video_collections"],
        range_value: str,
    ) -> tuple[CollectionDirectoryOutput, bool]:
        """Discover public collection targets through the shared cache."""
        return await self.collections.list_user_collections(uid, kind, range_value, self.cache)

    async def list_collection(
        self,
        target: CollectionTarget,
        range_value: str = "1..10",
        collection_version: str | None = None,
    ) -> tuple[CollectionOutput, bool]:
        """Read ordered public season, series or favorite members."""
        return await self.collections.list_collection(
            target, range_value=range_value, collection_version=collection_version
        )

    async def get_live_room(
        self, room_id: str | None = None, uid: str | None = None
    ) -> tuple[LiveRoomOutput, bool]:
        """Read a public room by its displayed ID or owner UID."""
        return await self.live.get_live_room(room_id, uid)

    async def list_live_guards(
        self, room_id: str | None = None, uid: str | None = None, cursor: str | None = None
    ) -> tuple[LiveGuardsOutput, bool]:
        """Read one page of current public live subscriptions."""
        return await self.live_guards.list_live_guards(room_id, uid, cursor)

    async def get_video(
        self,
        bvid: str,
        cursor: str | None,
        subtitle: Literal["all"] | None,
    ) -> tuple[VideoOutput, bool]:
        """Read video detail and resolve any process-local page cursor."""
        return await self.videos.get_video(bvid, cursor, subtitle)

    async def get_video_transcript(
        self,
        bvid: str,
        page_number: int | None,
        track_id: str | None,
        range_value: str,
        *,
        include_timestamps: bool = False,
        window: PlaybackWindow | None = None,
    ) -> tuple[TranscriptOutput, bool]:
        """Read subtitle text with optional original cue timing and playback-window selection."""
        return await self.transcripts.get_transcript(
            bvid,
            page_number,
            track_id,
            range_value,
            include_timestamps=include_timestamps,
            window=window,
        )

    async def list_danmaku(
        self,
        bvid: str,
        page_number: int | None = None,
        time_range: str = "0..299",
        include_pattern: str | None = None,
        exclude_pattern: str | None = None,
        kinds: list[DanmakuKind] | None = None,
        sender_hashes: list[str] | None = None,
        sampling: DanmakuSampling = "representative",
        sample_limit: int = 40,
    ) -> tuple[DanmakuListOutput, bool]:
        """Filter and sample a playback-time window through the segmented danmaku cache."""
        return await self.danmaku.list_danmaku(
            bvid,
            page_number,
            time_range,
            include_pattern,
            exclude_pattern,
            kinds,
            sender_hashes,
            sampling,
            sample_limit,
        )

    async def list_comments(
        self,
        bvid: str | None,
        opus_id: str | None,
        order: Literal["hot", "time"],
        range_value: str,
        expand_comment_id: str | None,
        text_mode: Literal["summary", "full"] = "summary",
        target: CommentTargetInput | None = None,
    ) -> tuple[CommentListOutput | CommentThreadOutput, bool]:
        """Read roots or one discussion thread through the comment service."""
        return await self.comments.list_comments(
            bvid,
            opus_id,
            order,
            range_value,
            expand_comment_id,
            text_mode,
            target,
        )

    async def list_related_videos(self, bvid: str, limit: int) -> tuple[RelatedVideosOutput, bool]:
        """Read a bounded related-video list without media URLs."""
        return await self.videos.list_related(bvid, limit)

    async def get_article(self, opus_id: str, range_value: str) -> tuple[ArticleOutput, bool]:
        """Read a character range from one cached normalized Opus."""
        output, hit = await self.opuses.get_article(opus_id, range_value)
        if output.images:
            output.image_target = OpusResourceRef(opus_id=output.opus_id)
        return output, hit

    async def get_image(
        self, target: ImageTarget, index: int = 1, part: ImagePart = "overview"
    ) -> tuple[ImageOutput, bool]:
        """Read one selected public image as native image content."""
        return await self.images.get_image(target, index, part)

    async def get_user(self, uid: str) -> tuple[UserOutput, bool]:
        """Read one public user profile and its public aggregate counts."""
        return await self.users.get_user(uid)

    async def list_user_activities(
        self,
        uid: str,
        range_value: str,
    ) -> tuple[UserActivitiesOutput, bool]:
        """Read a public activity range while hiding native offsets."""
        return await self.users.list_activities(uid, range_value)

    async def list_user_content(
        self,
        uid: str,
        kind: Literal["videos", "opuses"],
        sort: Literal["newest", "popular"],
        range_value: str,
    ) -> tuple[UserContentOutput, bool]:
        """Read a public submission range while hiding native pagination."""
        return await self.users.list_content(uid, kind, sort, range_value)
