"""Strict public MCP schemas and stable service errors."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, model_validator


class StrictModel(BaseModel):
    """Reject undeclared fields in every public schema."""

    model_config = ConfigDict(extra="forbid", strict=True)


class ErrorCode(str, Enum):
    INVALID_ARGUMENT = "invalid_argument"
    INVALID_RANGE = "invalid_range"
    INVALID_CURSOR = "invalid_cursor"
    NOT_FOUND = "not_found"
    CREDENTIALS_INVALID = "credentials_invalid"
    RATE_LIMITED = "rate_limited"
    UPSTREAM_UNAVAILABLE = "upstream_unavailable"
    UPSTREAM_SCHEMA_CHANGED = "upstream_schema_changed"
    RESOURCE_LIMIT_EXCEEDED = "resource_limit_exceeded"


class ServiceError(Exception):
    """Carry a public error code without retaining sensitive upstream data."""

    def __init__(
        self,
        code: ErrorCode,
        message: str,
        *,
        http_status: int | None = None,
        retry_after_seconds: int | None = None,
    ) -> None:
        """Carry sanitized capacity metadata separately from the public error schema."""
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status
        self.retry_after_seconds = retry_after_seconds


class ErrorDetail(StrictModel):
    code: Literal[
        "invalid_argument",
        "invalid_range",
        "invalid_cursor",
        "not_found",
        "credentials_invalid",
        "rate_limited",
        "upstream_unavailable",
        "upstream_schema_changed",
        "resource_limit_exceeded",
    ]
    message: str
    request_id: str


class ErrorOutput(StrictModel):
    schema_version: Literal[1] = 1
    error: ErrorDetail


class Author(StrictModel):
    uid: str
    name: str


class Verification(StrictModel):
    color: Literal["blue", "yellow"]
    info: str


class VideoStats(StrictModel):
    view_count: int | None = Field(default=None, ge=0)
    danmaku_count: int | None = Field(default=None, ge=0)
    comment_count: int | None = Field(default=None, ge=0)
    favorite_count: int | None = Field(default=None, ge=0)
    coin_count: int | None = Field(default=None, ge=0)
    share_count: int | None = Field(default=None, ge=0)
    like_count: int | None = Field(default=None, ge=0)


class VideoSummary(StrictModel):
    bvid: str
    title: str
    summary: str
    author: Author
    duration_seconds: int = Field(ge=0)
    published_at: str
    stats: VideoStats


class UserVideoSummary(VideoSummary):
    relationship: Literal["collaboration"] | None = Field(
        default=None,
        description=(
            "Present only for an upstream-confirmed collaboration. The outer uid is the space "
            "owner; author.uid is the primary uploader."
        ),
    )


class SampleVideo(StrictModel):
    bvid: str
    title: str


class UserSearchItem(StrictModel):
    uid: str
    name: str
    description: str
    level: int = Field(ge=0)
    is_live: bool
    follower_count: int = Field(ge=0)
    video_count: int = Field(ge=0)
    sample_videos: list[SampleVideo] = Field(max_length=2)
    verification: Verification | None = None


class VideoResourceRef(StrictModel):
    kind: Literal["video"] = "video"
    bvid: str = Field(pattern=r"^BV[0-9A-Za-z]{10}$")


class UserResourceRef(StrictModel):
    kind: Literal["user"] = "user"
    uid: str = Field(pattern=r"^[1-9][0-9]*$")


class OpusResourceRef(StrictModel):
    kind: Literal["opus"] = "opus"
    opus_id: str = Field(pattern=r"^[1-9][0-9]*$")


class ActivityResourceRef(StrictModel):
    kind: Literal["activity"] = "activity"
    activity_id: str = Field(pattern=r"^[1-9][0-9]*$")


class LegacyArticleResourceRef(StrictModel):
    kind: Literal["legacy_article"] = "legacy_article"
    cvid: str = Field(pattern=r"^[1-9][0-9]*$")


class LiveRoomResourceRef(StrictModel):
    kind: Literal["live_room"] = "live_room"
    room_id: str = Field(pattern=r"^[1-9][0-9]*$")


class CommentVideoTarget(StrictModel):
    kind: Literal["video"] = "video"
    video_aid: str = Field(pattern=r"^[1-9][0-9]*$")


class CommentAlbumTarget(StrictModel):
    kind: Literal["album"] = "album"
    album_id: str = Field(pattern=r"^[1-9][0-9]*$")


class UnknownCommentTarget(StrictModel):
    kind: Literal["unknown"] = "unknown"
    object_id: str = Field(pattern=r"^[1-9][0-9]*$")
    object_type: int


type CommentTarget = Annotated[
    CommentVideoTarget
    | ActivityResourceRef
    | OpusResourceRef
    | CommentAlbumTarget
    | LegacyArticleResourceRef
    | UnknownCommentTarget,
    Field(discriminator="kind"),
]

type CommentTargetInput = CommentTarget | Annotated[str, Field(min_length=1, max_length=4000)]


class VideoSeasonTarget(StrictModel):
    kind: Literal["video_season"]
    season_id: str = Field(pattern=r"^[1-9][0-9]*$")
    owner_uid: str = Field(pattern=r"^[1-9][0-9]*$")


class VideoSeriesTarget(StrictModel):
    kind: Literal["video_series"]
    series_id: str = Field(pattern=r"^[1-9][0-9]*$")
    owner_uid: str = Field(pattern=r"^[1-9][0-9]*$")


class PublicFavoritesTarget(StrictModel):
    kind: Literal["public_favorites"]
    favorite_list_id: str = Field(pattern=r"^[1-9][0-9]*$")


type CollectionTarget = Annotated[
    VideoSeasonTarget | VideoSeriesTarget | PublicFavoritesTarget, Field(discriminator="kind")
]


ResourceRef = Annotated[
    VideoResourceRef
    | UserResourceRef
    | OpusResourceRef
    | ActivityResourceRef
    | LegacyArticleResourceRef
    | LiveRoomResourceRef
    | VideoSeasonTarget
    | VideoSeriesTarget
    | PublicFavoritesTarget,
    Field(discriminator="kind"),
]


class ResolveResourceOutput(StrictModel):
    schema_version: Literal[1] = 1
    resource: ResourceRef
    original_url: str
    readable: bool
    page_number: int | None = Field(default=None, ge=1)
    at_seconds: int | None = Field(default=None, ge=0)
    fetched_at: str


class VideoSearchFilters(StrictModel):
    duration: (
        Literal["under_10_minutes", "10_to_30_minutes", "30_to_60_minutes", "over_60_minutes"]
        | None
    ) = None
    category_id: int | None = Field(default=None, ge=1)
    published_after: str | None = Field(
        default=None,
        pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$",
    )
    published_before: str | None = Field(
        default=None,
        pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$",
    )

    @model_validator(mode="after")
    def validate_dates(self) -> VideoSearchFilters:
        """Require explicit time zones and a nonempty half-open publication interval."""
        parsed: list[datetime | None] = []
        for value in (self.published_after, self.published_before):
            if value is None:
                parsed.append(None)
                continue
            date = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if "T" not in value or date.tzinfo is None or date.utcoffset() is None:
                raise ValueError(
                    "publication boundaries require RFC 3339 timestamps with time zones"
                )
            parsed.append(date)
        after, before = parsed
        if after is not None and before is not None and after >= before:
            raise ValueError("published_after must precede published_before")
        return self


class SearchCoverage(StrictModel):
    scope: Literal["search_index", "article_index"] = "search_index"
    complete: bool
    scanned_items: int = Field(ge=0)
    unmappable_items: int | None = Field(default=None, ge=1)
    reason: (
        Literal["budget_exhausted", "scan_limit", "source_exhausted", "unmappable_items"] | None
    ) = None


class ExtendedVideoSearchOutput(StrictModel):
    schema_version: Literal[1] = 1
    query: str
    kind: Literal["videos"] = "videos"
    sort: Literal["relevance", "newest", "views"]
    items: list[VideoSummary] = Field(max_length=10)
    next_cursor: str | None = None
    coverage: SearchCoverage
    fetched_at: str


class ExtendedUserSearchOutput(StrictModel):
    schema_version: Literal[1] = 1
    query: str
    kind: Literal["users"] = "users"
    sort: Literal["relevance", "followers"]
    items: list[UserSearchItem] = Field(max_length=10)
    next_cursor: str | None = None
    coverage: SearchCoverage
    fetched_at: str


class VideoRights(StrictModel):
    paid: Literal[True] | None = None
    interactive: Literal[True] | None = None
    collaboration: Literal[True] | None = None


class SubtitleTrack(StrictModel):
    track_id: str
    language: str
    language_label: str
    is_ai_generated: bool
    ai_kind: Literal["transcription", "translation", "other"] | None = None
    line_count: int = Field(ge=0)


class SubtitleInfo(StrictModel):
    track_count: int = Field(
        ge=0,
        description=(
            "Complete track inventory count for this part, including tracks omitted from this "
            "response."
        ),
    )
    language_count: int = Field(
        ge=0,
        description=("Distinct languages in the complete inventory, including omitted tracks."),
    )
    default_track_id: str | None = Field(
        default=None,
        description=(
            "First qualifying non-translation track with more than three lines; absent when "
            "no default qualifies."
        ),
    )
    tracks: list[SubtitleTrack]
    tracks_truncated: Literal[True] | None = Field(
        default=None,
        description=(
            "More inventory exists than shown. Use subtitle=all only if the task needs "
            "omitted tracks; a shown default track is sufficient for ordinary transcript reading."
        ),
    )


class VideoPage(StrictModel):
    page_number: int = Field(ge=1)
    cid: str
    title: str
    duration_seconds: int = Field(ge=0)
    subtitle_info: SubtitleInfo


class VideoOutput(StrictModel):
    schema_version: Literal[1] = 1
    bvid: str
    title: str
    description: str
    author: Author
    duration_seconds: int = Field(ge=0)
    published_at: str
    cover_url: str
    stats: VideoStats
    rights: VideoRights
    page_count: int = Field(ge=1)
    pages: list[VideoPage] = Field(max_length=10)
    next_cursor: str | None = Field(
        description=(
            "Pass unchanged as cursor for the same BVID and subtitle mode; null means no more "
            "parts. On invalid_cursor restart get_video with cursor=null."
        ),
    )
    original_url: str
    collection: VideoSeasonTarget | None = Field(
        default=None, description="Verified containing season; pass to list_collection as target."
    )


class PlaybackWindow(StrictModel):
    start_seconds: float = Field(ge=0, allow_inf_nan=False)
    end_seconds: float = Field(gt=0, allow_inf_nan=False)


class TranscriptTiming(StrictModel):
    content_range: str
    start_seconds: float = Field(ge=0, allow_inf_nan=False)
    end_seconds: float = Field(ge=0, allow_inf_nan=False)


class TranscriptOutput(StrictModel):
    schema_version: Literal[1] = 1
    bvid: str
    page_number: int = Field(ge=1)
    cid: str
    track: SubtitleTrack
    content_range: str | None
    total_characters: int = Field(ge=0)
    content_text: str
    window: PlaybackWindow | None = None
    timings: list[TranscriptTiming] | None = Field(
        default=None,
        max_length=100,
        description=(
            "Original subtitle-cue playback seconds for these character spans, not word-level "
            "timing. Pass seconds to get_video_frame for the same BVID and part."
        ),
    )
    next_range: str | None = Field(
        description=(
            "Pass unchanged as range with the same resource and selection; null means no "
            "further content. On invalid_range use a known continuation or correct the "
            "request as instructed by the error."
        ),
    )


type DanmakuKind = Literal[
    "scrolling",
    "bottom",
    "top",
    "reverse",
    "advanced",
    "code",
    "special",
]
type DanmakuSampling = Literal["representative", "chronological"]


class DanmakuKindCount(StrictModel):
    kind: DanmakuKind
    count: int = Field(ge=1)


class DanmakuReaction(StrictModel):
    text: str
    count: int = Field(ge=2)
    sender_count: int = Field(ge=0)
    first_at_seconds: int = Field(ge=0)
    last_at_seconds: int = Field(ge=0)
    content_truncated: Literal[True] | None = None


class DanmakuSample(StrictModel):
    at_seconds: int = Field(ge=0)
    text: str
    kind: DanmakuKind
    sender_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{1,8}$")
    content_truncated: Literal[True] | None = None


class DanmakuMoment(StrictModel):
    start_seconds: int = Field(ge=0)
    end_seconds: int = Field(ge=0)
    count: int = Field(ge=0)
    kind_counts: list[DanmakuKindCount] = Field(max_length=7)


class DanmakuListOutput(StrictModel):
    schema_version: Literal[1] = 1
    bvid: str
    page_number: int = Field(ge=1)
    cid: str
    duration_seconds: int = Field(ge=1)
    requested_time_range: str
    resolved_time_range: str
    bucket_seconds: Literal[60] = 60
    sampling: DanmakuSampling
    source_count: int = Field(ge=0)
    matched_count: int = Field(ge=0)
    unique_text_count: int = Field(ge=0)
    sampled_count: int = Field(
        ge=0,
        le=50,
        description=(
            "Number of returned samples, not the total matching population; compare "
            "matched_count for volume."
        ),
    )
    moments: list[DanmakuMoment] = Field(max_length=5)
    top_reactions: list[DanmakuReaction] = Field(max_length=8)
    samples: list[DanmakuSample] = Field(max_length=50)
    next_time_range: str | None = Field(
        description=(
            "Pass unchanged as time_range for the same video part and filters; null means the "
            "part has no later playback window."
        ),
    )


class ArticleImage(StrictModel):
    index: int = Field(ge=1, description="Pass this index to get_image with the image_target.")
    url: str
    width: int = Field(ge=0)
    height: int = Field(ge=0)


class CommentImageTarget(StrictModel):
    kind: Literal["comment"] = "comment"
    subject: CommentSubject | CommentTarget
    comment_id: str = Field(pattern=r"^[1-9][0-9]*$")
    root_comment_id: str | None = Field(
        default=None,
        pattern=r"^[1-9][0-9]*$",
        description="Required for replies; omit for a root comment.",
    )


class CommentReply(StrictModel):
    comment_id: str
    author: Author
    summary: str | None = Field(default=None, description="Excerpt; omitted in full mode.")
    content_text: str | None = Field(
        default=None,
        description="Raw complete comment text; available in full mode.",
    )
    content_truncated: Literal[True] | None = None
    like_count: int = Field(ge=0)
    images: list[ArticleImage] | None = None
    image_target: CommentImageTarget | None = Field(
        default=None, description="Pass unchanged as get_image target; choose an images index."
    )
    ip_location: str | None = Field(
        default=None,
        description=(
            "Region label displayed by Bilibili, only when supplied upstream; absence does "
            "not imply any location."
        ),
    )


class CommentItem(StrictModel):
    comment_id: str
    author: Author
    summary: str | None = Field(default=None, description="Excerpt; omitted in full mode.")
    content_text: str | None = Field(
        default=None,
        description="Raw complete comment text; available in full mode.",
    )
    content_truncated: Literal[True] | None = None
    created_at: str
    like_count: int = Field(ge=0)
    reply_count: int = Field(ge=0)
    images: list[ArticleImage] | None = None
    image_target: CommentImageTarget | None = Field(
        default=None, description="Pass unchanged as get_image target; choose an images index."
    )
    replies_truncated: Literal[True] | None = Field(
        default=None,
        description=(
            "More replies exist than the at-most-two preview; use the same subject and "
            "expand_comment_id=comment_id to read the thread."
        ),
    )
    replies: list[CommentReply] | None = Field(default=None, max_length=2)
    ip_location: str | None = Field(
        default=None,
        description=(
            "Region label displayed by Bilibili, only when supplied upstream; absence does "
            "not imply any location."
        ),
    )


class CommentSubject(StrictModel):
    kind: Literal["video", "opus"]
    bvid: str | None = None
    opus_id: str | None = None

    @model_validator(mode="after")
    def validate_identifier(self) -> CommentSubject:
        """Keep the discriminator and its identifier synchronized."""
        if self.kind == "video" and self.bvid is not None and self.opus_id is None:
            return self
        if self.kind == "opus" and self.opus_id is not None and self.bvid is None:
            return self
        raise ValueError("comment subject identifier does not match kind")


class CommentListOutput(StrictModel):
    schema_version: Literal[1] = 1
    subject: CommentSubject | CommentTarget
    order: Literal["hot", "time"]
    returned_count: int = Field(ge=0, le=20)
    returned_range: str | None
    available_count: int = Field(ge=0)
    items: list[CommentItem] = Field(max_length=20)
    next_range: str | None = Field(
        description=(
            "Pass unchanged as range with the same resource and selection; null means no "
            "further content. On invalid_range use a known continuation or correct the "
            "request as instructed by the error."
        ),
    )


class CommentThreadOutput(StrictModel):
    schema_version: Literal[1] = 1
    subject: CommentSubject | CommentTarget
    root_comment_id: str
    root: CommentReply | None = Field(
        default=None, description="Root comment body; included when expanding its discussion."
    )
    returned_count: int = Field(ge=0, le=20)
    returned_range: str | None
    available_count: int = Field(ge=0)
    items: list[CommentReply] = Field(max_length=20)
    next_range: str | None = Field(
        description=(
            "Pass unchanged as range with the same resource and selection; null means no "
            "further content. On invalid_range use a known continuation or correct the "
            "request as instructed by the error."
        ),
    )


class RelatedVideo(StrictModel):
    bvid: str
    title: str
    summary: str
    author: Author
    duration_seconds: int = Field(ge=0)
    published_at: str
    view_count: int = Field(ge=0)


class RelatedVideosOutput(StrictModel):
    schema_version: Literal[1] = 1
    bvid: str
    items: list[RelatedVideo] = Field(max_length=10)


class ArticleStats(StrictModel):
    share_count: int = Field(ge=0)
    comment_count: int = Field(ge=0)
    like_count: int = Field(ge=0)
    favorite_count: int = Field(ge=0)


class ArticleOutput(StrictModel):
    schema_version: Literal[1] = 1
    opus_id: str
    title: str
    author: Author
    published_at: str
    requested_range: str
    resolved_range: str
    total_characters: int = Field(
        ge=1,
        description=(
            "Unicode character count of the complete normalized Markdown, including image Markdown."
        ),
    )
    content_markdown: str
    images: list[ArticleImage] = Field(
        description=(
            "Complete ordered image inventory, even for a partial text range; image Markdown "
            "also contributes to character positions."
        ),
    )
    image_target: OpusResourceRef | None = Field(
        default=None, description="Pass unchanged as get_image target with an images index."
    )
    next_range: str | None = Field(
        description=(
            "Pass unchanged as range to continue in the requested reading direction on the "
            "same Opus; null means stop."
        ),
    )
    stats: ArticleStats
    original_url: str


class UserStats(StrictModel):
    follower_count: int = Field(ge=0)
    following_count: int = Field(ge=0)
    video_count: int = Field(ge=0)


class UserOutput(StrictModel):
    schema_version: Literal[1] = 1
    uid: str
    name: str
    description: str
    avatar_url: str
    verification: Verification | None = None
    ip_location: str | None = Field(
        default=None,
        description="Public IP region label supplied by Bilibili; omitted when unavailable.",
    )
    level: int = Field(ge=0)
    stats: UserStats
    original_url: str


class ActivityStats(StrictModel):
    share_count: int | None = Field(default=None, ge=0)
    comment_count: int | None = Field(default=None, ge=0)
    like_count: int | None = Field(default=None, ge=0)


class VideoActivityTarget(VideoResourceRef):
    title: str
    summary: str
    duration_seconds: int = Field(ge=0)


class OpusActivityTarget(OpusResourceRef):
    title: str | None = None
    summary: str


class LiveActivityTarget(LiveRoomResourceRef):
    title: str
    summary: str
    author_name: str | None = Field(
        default=None, description="Broadcaster name from the live card, independent of the poster."
    )
    live_status: Literal["offline", "live"] | None = Field(
        default=None,
        description=(
            "State shown by the fetched activity card; use get_live_room for current status."
        ),
    )


class UnavailableActivityTarget(StrictModel):
    kind: Literal["unavailable"] = "unavailable"
    summary: str = Field(
        description=(
            "Final unavailable-target summary; do not retry a detail tool for this target."
        ),
    )


class ActivityReferenceTarget(StrictModel):
    kind: Literal["activity"] = "activity"
    activity_id: str


class UserActivity(StrictModel):
    activity_id: str = Field(
        description=(
            "Identifier of this timeline event. Read original content using target.opus_id, "
            "target.bvid or target.room_id with the corresponding detail tool."
        ),
    )
    kind: Literal["video", "opus", "live", "repost", "unavailable"]
    published_at: str
    summary: str | None = None
    target: (
        VideoActivityTarget
        | OpusActivityTarget
        | LiveActivityTarget
        | ActivityReferenceTarget
        | UnavailableActivityTarget
    )
    stats: ActivityStats


class UserActivitiesOutput(StrictModel):
    schema_version: Literal[1] = 1
    uid: str
    range: str
    items: list[UserActivity] = Field(max_length=10)
    next_range: str | None = Field(
        description=(
            "Pass unchanged as range with the same resource and selection; null means no "
            "further content. On invalid_range use a known continuation or correct the "
            "request as instructed by the error."
        ),
    )


class OpusListItem(StrictModel):
    opus_id: str
    title: str
    summary: str
    author: Author
    published_at: str
    stats: ArticleStats
    readable: Literal[False] | None = Field(
        default=None,
        description="False is a terminal restricted/unavailable summary; do not call get_article.",
    )


class ExtendedOpusSearchOutput(StrictModel):
    schema_version: Literal[1] = 1
    query: str
    kind: Literal["opuses"] = "opuses"
    sort: Literal["relevance", "newest"]
    items: list[OpusListItem] = Field(max_length=10)
    next_cursor: str | None = None
    coverage: SearchCoverage
    fetched_at: str


ExtendedSearchOutput = (
    ExtendedVideoSearchOutput | ExtendedUserSearchOutput | ExtendedOpusSearchOutput
)


class ActivityOutput(StrictModel):
    schema_version: Literal[1] = 1
    activity_id: str
    kind: Literal["video", "opus", "live", "repost", "unavailable"]
    author: Author
    published_at: str
    content_markdown: str
    requested_range: str
    resolved_range: str | None
    total_characters: int = Field(ge=0)
    images: list[ArticleImage]
    image_target: ActivityResourceRef | None = Field(
        default=None, description="This activity's own images; pass unchanged to get_image."
    )
    target: (
        VideoResourceRef
        | OpusResourceRef
        | LiveActivityTarget
        | ActivityReferenceTarget
        | UnavailableActivityTarget
    )
    stats: ActivityStats
    next_range: str | None
    original_url: str
    fetched_at: str


class UserContentOutput(StrictModel):
    schema_version: Literal[1] = 1
    uid: str = Field(
        description=(
            "Space owner UID; a collaboration item may have a different primary uploader in "
            "author.uid."
        ),
    )
    kind: Literal["videos", "opuses"]
    sort: Literal["newest", "popular"]
    range: str
    available_count: int | None = Field(default=None, ge=0)
    items: list[UserVideoSummary | OpusListItem] = Field(max_length=10)
    next_range: str | None = Field(
        description=(
            "Pass unchanged as range with the same resource and selection; null means no "
            "further content. On invalid_range use a known continuation or correct the "
            "request as instructed by the error."
        ),
    )


class LiveGuardMember(StrictModel):
    rank: int = Field(ge=1)
    uid: str | None = Field(default=None, pattern=r"^[1-9][0-9]*$")
    name: str
    guard_tier: Literal["governor", "admiral", "captain"]
    masked: Literal[True] | None = None


class LiveGuardsOutput(StrictModel):
    schema_version: Literal[1] = 1
    room_id: str = Field(pattern=r"^[1-9][0-9]*$")
    uid: str = Field(pattern=r"^[1-9][0-9]*$", description="Broadcaster UID, not a member UID.")
    total_count: int = Field(ge=0, description="All current governor, admiral and captain members.")
    items: list[LiveGuardMember] = Field(max_length=13)
    next_cursor: str | None = Field(pattern=r"^[0-9A-Za-z]{6}$")
    fetched_at: str


class CachedLiveGuardsPage(StrictModel):
    total_count: int = Field(ge=0)
    items: list[LiveGuardMember] = Field(max_length=13)
    has_next: bool
    fetched_at: str


type ImageTarget = Annotated[
    VideoResourceRef
    | UserResourceRef
    | OpusResourceRef
    | ActivityResourceRef
    | CommentImageTarget
    | VideoSeasonTarget
    | VideoSeriesTarget
    | PublicFavoritesTarget,
    Field(discriminator="kind"),
]
type ImagePart = Annotated[int, Field(ge=1)] | Literal["overview"]


class ImageRegion(StrictModel):
    left: int = Field(ge=0)
    top: int = Field(ge=0)
    width: int = Field(ge=1)
    height: int = Field(ge=1)


class ImageOutput(StrictModel):
    schema_version: Literal[1] = 1
    target: ImageTarget
    index: int = Field(ge=1)
    part: ImagePart
    part_count: int = Field(ge=1)
    next_part: int | None = Field(description="Read only needed detail parts; null means stop.")
    source_region: ImageRegion
    original_url: str | None = None
    image_url: str
    source_width: int = Field(ge=1)
    source_height: int = Field(ge=1)
    width: int = Field(ge=1)
    height: int = Field(ge=1)
    mime_type: Literal["image/jpeg", "image/png"]
    fetched_at: str
    first_frame: Literal[True] | None = None
    _image_data: bytes = PrivateAttr(default=b"")


class VideoFrameQuality(StrictModel):
    label: str
    width: int = Field(ge=1)
    height: int = Field(ge=1)
    frame_rate: str | None = None
    notice: str | None = None


class VideoFrameItem(StrictModel):
    index: int = Field(ge=1)
    actual_time_seconds: float = Field(ge=0)
    width: int = Field(ge=1)
    height: int = Field(ge=1)
    frame_ref: str | None = None
    requested_time_seconds: float | None = None


class VideoFrameCoverage(StrictModel):
    complete: bool
    reason: Literal["end_of_window", "end_of_video", "frame_limit", "time_limit", "media_limit"]
    decoded_frames: int = Field(ge=0)
    examined_start_seconds: float = Field(ge=0)
    examined_end_seconds: float = Field(ge=0)
    presentation: Literal["selected_frames", "requested_frames"]


class VideoFrameOutput(StrictModel):
    schema_version: Literal[1] = 1
    bvid: str = Field(pattern=r"^BV[0-9A-Za-z]{10}$")
    cid: str = Field(pattern=r"^[1-9][0-9]*$")
    page_number: int = Field(ge=1)
    source_kind: Literal["video_preview", "video_frame"]
    requested_time_seconds: float = Field(ge=0, allow_inf_nan=False)
    actual_time_seconds: float = Field(ge=0, allow_inf_nan=False)
    width: int = Field(ge=1)
    height: int = Field(ge=1)
    mime_type: Literal["image/jpeg", "image/png"]
    fetched_at: str
    canonical_url: str
    image_url: str | None = None
    timing_notice: str | None = None
    frame_ref: str | None = None
    source_quality: VideoFrameQuality | None = None
    region: ImageRegion | None = None
    _image_data: bytes = PrivateAttr(default=b"")


class VideoFramesOutput(StrictModel):
    schema_version: Literal[1] = 1
    bvid: str = Field(pattern=r"^BV[0-9A-Za-z]{10}$")
    cid: str = Field(pattern=r"^[1-9][0-9]*$")
    page_number: int = Field(ge=1)
    source_kind: Literal["video_preview", "video_frame"]
    frames: list[VideoFrameItem] = Field(min_length=1, max_length=12)
    coverage: VideoFrameCoverage
    selection: Literal["timestamps", "visual_change"]
    source_quality: VideoFrameQuality | None = None
    width: int = Field(ge=1)
    height: int = Field(ge=1)
    mime_type: Literal["image/jpeg"] = "image/jpeg"
    fetched_at: str
    canonical_url: str
    image_url: str | None = None
    notice: str
    _image_data: bytes = PrivateAttr(default=b"")
