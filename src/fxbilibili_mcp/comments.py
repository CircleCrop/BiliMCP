"""Native comments and indexed targets, discussion expansion, text, and ranges."""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import partial
from typing import Literal

from .cache import AsyncTTLCache, CachePolicy
from .content import truncate_summary
from .models import (
    ActivityResourceRef,
    ArticleImage,
    Author,
    CommentImageTarget,
    CommentItem,
    CommentListOutput,
    CommentReply,
    CommentSubject,
    CommentTarget,
    CommentTargetInput,
    CommentThreadOutput,
    CommentVideoTarget,
    ErrorCode,
    LegacyArticleResourceRef,
    OpusResourceRef,
    ServiceError,
    VideoResourceRef,
)
from .normalization import (
    JsonObject,
    as_object,
    format_timestamp,
    get_object,
    get_str,
    normalize_ip_location,
    optional_bool,
    optional_int,
    optional_list,
    optional_object,
    optional_str,
    schema_error,
    validate_bvid,
    validate_decimal_id,
)
from .opuses import OpusService
from .ranges import COMMENT_MAX_SPAN, next_item_range, parse_range, returned_item_range
from .resource_resolution import (
    ParsedActivity,
    ParsedAV,
    ParsedOpus,
    ParsedVideo,
    ResourceResolutionService,
    parse_resource_text,
)
from .transport import BilibiliTransport
from .videos import VideoService


@dataclass(frozen=True, slots=True)
class CommentIdentity:
    subject: CommentSubject | CommentTarget
    oid: int
    type_code: int


class CommentPaginationLimitReached(ServiceError):
    """Expose the existing native range scan limit to bounded search callers."""

    def __init__(self) -> None:
        super().__init__(
            ErrorCode.UPSTREAM_UNAVAILABLE, "Bilibili comment pagination did not converge"
        )


class CommentService:
    """Hide comment offsets/pages and normalize only public discussion data."""

    def __init__(
        self,
        transport: BilibiliTransport,
        cache: AsyncTTLCache,
        policy: CachePolicy,
        videos: VideoService,
        opuses: OpusService,
        resources: ResourceResolutionService,
    ) -> None:
        """Bind shared transport, cache policy, and subject identity services."""
        self._transport = transport
        self._cache = cache
        self._policy = policy
        self._videos = videos
        self._opuses = opuses
        self._resources = resources

    async def resolve_target(self, target: CommentTargetInput) -> tuple[CommentTarget, bool]:
        """Resolve a known content reference once for both comment search and reading."""
        if not isinstance(target, str):
            return target, True
        parsed = parse_resource_text(target)
        if isinstance(parsed.target, ParsedAV):
            return CommentVideoTarget(video_aid=parsed.target.aid), True
        if isinstance(parsed.target, ParsedVideo):
            # Reuse the video core cache; agents should not convert BV to AV themselves.
            core, hit = await self._videos.get_core(parsed.target.bvid)
            return CommentVideoTarget(video_aid=str(core.aid)), hit
        if isinstance(parsed.target, ParsedActivity):
            return ActivityResourceRef(activity_id=parsed.target.activity_id), True
        if isinstance(parsed.target, ParsedOpus):
            return OpusResourceRef(opus_id=parsed.target.opus_id), True
        resolved, resolution_hit = await self._resources.resolve_resource(target)
        resource = resolved.resource
        if isinstance(resource, VideoResourceRef):
            core, hit = await self._videos.get_core(resource.bvid)
            return CommentVideoTarget(video_aid=str(core.aid)), resolution_hit and hit
        if isinstance(resource, (ActivityResourceRef, OpusResourceRef, LegacyArticleResourceRef)):
            return resource, resolution_hit
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT,
            "target must identify a video, activity, Opus or article; "
            "copy indexed targets unchanged",
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
        """Resolve exactly one subject, then list roots or one root's replies."""
        requested = parse_range(
            range_value,
            max_span=COMMENT_MAX_SPAN,
            range_name="comment range",
            unit_name="comments or replies",
        )
        if text_mode not in {"summary", "full"}:
            raise ServiceError(ErrorCode.INVALID_ARGUMENT, "text_mode must be summary or full")
        identity, identity_hit = await self._resolve_identity(bvid, opus_id, target)
        if expand_comment_id is not None:
            validate_decimal_id(expand_comment_id, "expand_comment_id")
            thread, pages_hit = await self._expand(
                identity,
                expand_comment_id,
                requested.start,
                requested.end,
                requested.span,
                text_mode,
            )
            return thread, identity_hit and pages_hit
        comments, pages_hit = await self._list_roots(
            identity, order, requested.start, requested.end, requested.span, text_mode
        )
        return comments, identity_hit and pages_hit

    async def _resolve_identity(
        self, bvid: str | None, opus_id: str | None, target: CommentTargetInput | None = None
    ) -> tuple[CommentIdentity, bool]:
        """Convert public identifiers to provider comment IDs without exposing them."""
        if sum(value is not None for value in (bvid, opus_id, target)) != 1:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT,
                "exactly one of bvid, opus_id or target must be provided",
            )
        if target is not None:
            target, resolution_hit = await self.resolve_target(target)
            # Indexed object types identify comment rooms, not interchangeable Opus IDs.
            if target.kind == "video":
                return CommentIdentity(target, int(target.video_aid), 1), resolution_hit
            if target.kind == "activity":
                return CommentIdentity(target, int(target.activity_id), 17), resolution_hit
            if target.kind == "album":
                return CommentIdentity(target, int(target.album_id), 11), resolution_hit
            if target.kind == "legacy_article":
                return CommentIdentity(target, int(target.cvid), 12), resolution_hit
            if target.kind == "unknown":
                raise ServiceError(
                    ErrorCode.INVALID_ARGUMENT,
                    "unsupported indexed comment target; do not reinterpret its object ID",
                )
            opus, cache_hit = await self._opuses.get_full(target.opus_id)
            return (
                CommentIdentity(target, opus.comment_oid, opus.comment_type),
                resolution_hit and cache_hit,
            )
        if bvid is not None:
            validate_bvid(bvid)
            core, cache_hit = await self._videos.get_core(bvid)
            return (
                CommentIdentity(
                    subject=CommentSubject(kind="video", bvid=bvid),
                    oid=core.aid,
                    type_code=1,
                ),
                cache_hit,
            )
        assert opus_id is not None
        validate_decimal_id(opus_id, "opus_id")
        opus, cache_hit = await self._opuses.get_full(opus_id)
        return (
            CommentIdentity(
                subject=CommentSubject(kind="opus", opus_id=opus_id),
                oid=opus.comment_oid,
                type_code=opus.comment_type,
            ),
            cache_hit,
        )

    async def _root_page(
        self,
        identity: CommentIdentity,
        order: Literal["hot", "time"],
        offset: str,
    ) -> tuple[JsonObject, bool]:
        """Fetch and cache one native root-comment offset for overlapping public ranges."""

        async def produce() -> JsonObject:
            """Request one signed comment page and validate its data object."""
            raw = await self._transport.request_data(
                "/x/v2/reply/wbi/main",
                {
                    "oid": identity.oid,
                    "type": identity.type_code,
                    "mode": 3 if order == "hot" else 2,
                    "pagination_str": json.dumps({"offset": offset}, separators=(",", ":")),
                    "web_location": 1315875,
                },
                wbi=True,
            )
            return as_object(raw, "comments.data")

        return await self._cache.get_or_create(
            (
                "comment_root_page",
                identity.subject.kind,
                identity.oid,
                identity.type_code,
                order,
                offset,
                "v1",
            ),
            self._policy.comments if order == "hot" else self._policy.comments_time,
            produce,
        )

    async def _list_roots(
        self,
        identity: CommentIdentity,
        order: Literal["hot", "time"],
        start: int,
        end: int,
        span: int,
        text_mode: Literal["summary", "full"],
    ) -> tuple[CommentListOutput, bool]:
        """Follow comment session offsets until the requested root interval is complete."""
        values: list[JsonObject] = []
        offset = ""
        is_end = False
        available = 0
        pages = 0
        cache_hit = True
        seen_offsets: set[str] = set()
        while len(values) < end and not is_end:
            # A non-advancing upstream cursor must not spin on the same cached page.
            if offset in seen_offsets:
                # Evict the stalled session so retries can obtain a fresh source offset.
                for seen in seen_offsets:
                    await self._cache.invalidate_projection(
                        (
                            "comment_root_page",
                            identity.subject.kind,
                            identity.oid,
                            identity.type_code,
                            order,
                            seen,
                            "v1",
                        )
                    )
                raise ServiceError(
                    ErrorCode.UPSTREAM_UNAVAILABLE,
                    "Bilibili comment pagination did not advance after "
                    f"{len(values)} root entries; retry later or request order=time",
                )
            seen_offsets.add(offset)
            pages += 1
            if pages > 50:
                raise CommentPaginationLimitReached()
            data, page_hit = await self._root_page(identity, order, offset)
            cache_hit = cache_hit and page_hit
            values.extend(
                as_object(value, "comments.item") for value in optional_list(data, "replies")
            )
            cursor = get_object(data, "cursor", "comments")
            available = optional_int(cursor, "all_count")
            is_end = optional_bool(cursor, "is_end")
            pagination = optional_object(cursor, "pagination_reply")
            offset = optional_str(pagination, "next_offset") if pagination is not None else ""
            if not is_end and not offset:
                raise schema_error("comments.cursor.pagination_reply.next_offset")
        if available == 0 and start == 1:
            selected: list[JsonObject] = []
        elif start > available or start > len(values):
            raise ServiceError(ErrorCode.INVALID_RANGE, "range starts beyond available comments")
        else:
            selected = values[start - 1 : min(end, available)]
        await self._remember_images(identity, values)
        items = [_normalize_root(value, text_mode, identity.subject) for value in selected]
        more = not is_end or end < available
        return (
            CommentListOutput(
                subject=identity.subject,
                order=order,
                returned_count=len(items),
                returned_range=returned_item_range(
                    parse_range(f"{start}..{end}", max_span=COMMENT_MAX_SPAN), len(items)
                ),
                available_count=available,
                items=items,
                next_range=next_item_range(end, has_more=more, max_span=span),
            ),
            cache_hit,
        )

    async def _reply_page(
        self,
        identity: CommentIdentity,
        root_id: str,
        page_number: int,
    ) -> tuple[JsonObject, bool]:
        """Fetch and cache one native reply page shared by overlapping thread ranges."""

        async def produce() -> JsonObject:
            """Request one discussion page and validate its data object."""
            raw = await self._transport.request_data(
                "/x/v2/reply/reply",
                {
                    "oid": identity.oid,
                    "type": identity.type_code,
                    "root": int(root_id),
                    "pn": page_number,
                    "ps": 10,
                },
                not_found_codes=frozenset({-400, -404, 12002, 12022}),
            )
            return as_object(raw, "comment_thread.data")

        return await self._cache.get_or_create(
            (
                "comment_reply_page",
                identity.subject.kind,
                identity.oid,
                identity.type_code,
                root_id,
                page_number,
                "v1",
            ),
            # Replies can arrive or be removed independently of a root's hot ranking.
            self._policy.comments_time,
            produce,
        )

    async def _expand(
        self,
        identity: CommentIdentity,
        root_id: str,
        start: int,
        end: int,
        span: int,
        text_mode: Literal["summary", "full"],
    ) -> tuple[CommentThreadOutput, bool]:
        """Fetch reply pages for one root and never repeat unrelated root comments."""
        pages_needed = max(1, (end + 9) // 10)
        values: list[JsonObject] = []
        available = 0
        root_comment: CommentReply | None = None
        cache_hit = True
        for page_number in range(1, pages_needed + 1):
            data, page_hit = await self._reply_page(identity, root_id, page_number)
            cache_hit = cache_hit and page_hit
            root = optional_object(data, "root")
            if root is not None:
                _validate_thread_root(root, identity, root_id)
                if root_comment is None:
                    await self._remember_images(identity, [root])
                    root_comment = _normalize_reply(root, text_mode, identity.subject, root_id)
            page = get_object(data, "page", "comment_thread")
            available = optional_int(page, "count")
            values.extend(
                as_object(value, "comment_thread.item") for value in optional_list(data, "replies")
            )
        if root_comment is None:
            raise ServiceError(ErrorCode.NOT_FOUND, "root comment does not belong to this subject")
        if available == 0 and start == 1:
            selected: list[JsonObject] = []
        elif start > available:
            raise ServiceError(ErrorCode.INVALID_RANGE, "range starts beyond available replies")
        else:
            selected = values[start - 1 : min(end, available)]
        await self._remember_images(identity, values)
        items = [
            _normalize_reply(value, text_mode, identity.subject, root_id) for value in selected
        ]
        return (
            CommentThreadOutput(
                subject=identity.subject,
                root_comment_id=root_id,
                root=root_comment,
                returned_count=len(items),
                returned_range=returned_item_range(
                    parse_range(f"{start}..{end}", max_span=COMMENT_MAX_SPAN), len(items)
                ),
                available_count=available,
                items=items,
                next_range=next_item_range(end, has_more=end < available, max_span=span),
            ),
            cache_hit,
        )

    async def _remember_images(self, identity: CommentIdentity, values: list[JsonObject]) -> None:
        """Keep observed image inventories so subsequent read-image calls need no scan."""
        for value in values:
            candidates = [
                value,
                *(as_object(v, "comment.reply") for v in optional_list(value, "replies")),
            ]
            for candidate in candidates:
                images = _comment_images(candidate)
                if not images:
                    continue
                comment_id = _comment_id(candidate)
                await self._cache.get_or_create(
                    ("comment_images", identity.oid, identity.type_code, comment_id, "v1"),
                    self._policy.comments_time,
                    partial(_image_value, images),
                )

    async def image_inventory(self, target: CommentImageTarget) -> tuple[list[ArticleImage], bool]:
        """Reuse observed images, or refresh the containing discussion's first page."""
        subject = target.subject
        identity, identity_hit = await self._resolve_identity(
            subject.bvid if isinstance(subject, CommentSubject) else None,
            subject.opus_id if isinstance(subject, CommentSubject) else None,
            None if isinstance(subject, CommentSubject) else subject,
        )

        async def produce() -> list[ArticleImage]:
            """Refresh a known root and first-page replies without unbounded pagination."""
            root_id = target.root_comment_id or target.comment_id
            data, _ = await self._reply_page(identity, root_id, 1)
            root = optional_object(data, "root")
            if root is not None:
                _validate_thread_root(root, identity, root_id)
            candidates = ([] if root is None else [root]) + [
                as_object(value, "comment.reply") for value in optional_list(data, "replies")
            ]
            for value in candidates:
                if _comment_id(value) == target.comment_id:
                    return _comment_images(value)
            raise ServiceError(
                ErrorCode.NOT_FOUND,
                "comment image inventory expired; read its original discussion range again",
            )

        images, hit = await self._cache.get_or_create(
            ("comment_images", identity.oid, identity.type_code, target.comment_id, "v1"),
            self._policy.comments_time,
            produce,
        )
        return images, identity_hit and hit


def _validate_thread_root(root: JsonObject, identity: CommentIdentity, root_id: str) -> None:
    """Reject mismatched roots and native room metadata supplied by the provider."""
    actual_id = optional_str(root, "rpid_str") or str(optional_int(root, "rpid"))
    actual_oid = optional_str(root, "oid_str")
    if not actual_oid and root.get("oid") is not None:
        actual_oid = str(optional_int(root, "oid"))
    if (
        actual_id != root_id
        or (actual_oid and actual_oid != str(identity.oid))
        or (root.get("type") is not None and optional_int(root, "type") != identity.type_code)
    ):
        raise ServiceError(ErrorCode.NOT_FOUND, "root comment does not belong to this subject")


def _normalize_root(
    data: JsonObject,
    text_mode: Literal["summary", "full"] = "summary",
    subject: CommentSubject | CommentTarget | None = None,
) -> CommentItem:
    """Normalize one root and retain at most two provider-attached reply previews."""
    text = get_str(get_object(data, "content", "comment"), "message", "comment.content")
    summary, truncated = truncate_summary(text)
    reply_values = optional_list(data, "replies")
    replies = [
        _normalize_reply(as_object(value, "comment.reply"), text_mode, subject, _comment_id(data))
        for value in reply_values[:2]
    ]
    reply_count = optional_int(data, "rcount")
    return CommentItem(
        comment_id=optional_str(data, "rpid_str") or str(optional_int(data, "rpid")),
        author=_comment_author(data),
        summary=summary if text_mode == "summary" else None,
        # Full text preserves line breaks; only the compact summary is normalized.
        content_text=text if text_mode != "summary" else None,
        content_truncated=True if truncated and text_mode == "summary" else None,
        created_at=format_timestamp(optional_int(data, "ctime")),
        like_count=optional_int(data, "like"),
        reply_count=reply_count,
        images=_comment_images(data) or None,
        image_target=_comment_image_target(data, subject),
        replies_truncated=True if reply_count > len(replies) else None,
        replies=replies or None,
        ip_location=_ip_location(data),
    )


def _normalize_reply(
    data: JsonObject,
    text_mode: Literal["summary", "full"] = "summary",
    subject: CommentSubject | CommentTarget | None = None,
    root_id: str | None = None,
) -> CommentReply:
    """Normalize one reply into the compact public comment shape."""
    text = get_str(get_object(data, "content", "comment.reply"), "message", "comment.reply.content")
    summary, truncated = truncate_summary(text)
    return CommentReply(
        comment_id=optional_str(data, "rpid_str") or str(optional_int(data, "rpid")),
        author=_comment_author(data),
        summary=summary if text_mode == "summary" else None,
        content_text=text if text_mode != "summary" else None,
        content_truncated=True if truncated and text_mode == "summary" else None,
        like_count=optional_int(data, "like"),
        images=_comment_images(data) or None,
        image_target=_comment_image_target(data, subject, root_id),
        ip_location=_ip_location(data),
    )


async def _image_value(images: list[ArticleImage]) -> list[ArticleImage]:
    return images


def _comment_id(data: JsonObject) -> str:
    return optional_str(data, "rpid_str") or str(optional_int(data, "rpid"))


def _comment_images(data: JsonObject) -> list[ArticleImage]:
    """Retain the provider's ordered picture links, including attached reply images."""
    content = get_object(data, "content", "comment")
    result: list[ArticleImage] = []
    for value in optional_list(content, "pictures"):
        picture = as_object(value, "comment.picture")
        url = get_str(picture, "img_src", "comment.picture")
        if url.startswith("//"):
            url = "https:" + url
        elif url.startswith("http://"):
            url = "https://" + url.removeprefix("http://")
        result.append(
            ArticleImage(
                index=len(result) + 1,
                url=url,
                width=optional_int(picture, "img_width"),
                height=optional_int(picture, "img_height"),
            )
        )
    return result


def _comment_image_target(
    data: JsonObject, subject: CommentSubject | CommentTarget | None, root_id: str | None = None
) -> CommentImageTarget | None:
    """Make a ready-to-call image target without conflating room and comment IDs."""
    if subject is None or not _comment_images(data):
        return None
    root = root_id or optional_str(data, "root_str") or str(optional_int(data, "root"))
    if root == "0":
        root = _comment_id(data)
    return CommentImageTarget(
        subject=subject,
        comment_id=_comment_id(data),
        root_comment_id=None if root == _comment_id(data) else root,
    )


def _comment_author(data: JsonObject) -> Author:
    member = get_object(data, "member", "comment")
    return Author(
        uid=optional_str(member, "mid")
        or optional_str(data, "mid_str")
        or str(optional_int(data, "mid")),
        name=get_str(member, "uname", "comment.member"),
    )


def _ip_location(data: JsonObject) -> str | None:
    """Return a compact IP属地 region label for agents, without redundant UI prefixes."""
    control = optional_object(data, "reply_control")
    if control is None:
        return None
    return normalize_ip_location(optional_str(control, "location"))
