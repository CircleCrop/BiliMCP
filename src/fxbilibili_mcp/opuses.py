"""Opus detail normalization, Markdown rendering, comments identity, and ranges."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from urllib.parse import urlsplit

from .activities import (
    ACTIVITY_KINDS,
    ACTIVITY_NORMALIZATION_VERSION,
    activity_kind,
    activity_stat,
)
from .activities import (
    activity_target as _activity_target,
)
from .cache import AsyncTTLCache, CachePolicy
from .content import blocked_notice_fields, normalize_space, truncate_summary
from .models import (
    ActivityOutput,
    ActivityReferenceTarget,
    ActivityStats,
    ArticleImage,
    ArticleOutput,
    ArticleStats,
    Author,
    ErrorCode,
    LiveActivityTarget,
    OpusListItem,
    OpusResourceRef,
    ServiceError,
    UnavailableActivityTarget,
    VideoResourceRef,
)
from .normalization import (
    ARTICLE_OPUS_MAPPING_VERSION,
    TAIPEI,
    JsonObject,
    article_opus_id,
    as_object,
    format_timestamp,
    get_int,
    get_list,
    get_object,
    get_str,
    optional_bool,
    optional_int,
    optional_list,
    optional_object,
    optional_str,
    schema_error,
    validate_decimal_id,
)
from .ranges import ARTICLE_MAX_SPAN, TEXT_HARD_MAX_SPAN, parse_range, slice_text
from .transport import BilibiliTransport


class IncompleteOpusError(ServiceError):
    """Identify a missing content module while preserving strict detail errors."""


@dataclass(frozen=True, slots=True)
class FullOpus:
    opus_id: str
    title: str
    author: Author
    published_at: str
    content_markdown: str
    images: tuple[ArticleImage, ...]
    stats: ArticleStats
    comment_oid: int
    comment_type: int

    def list_item(self) -> OpusListItem:
        """Build a compact text-only list item without exposing image URLs."""
        text_only = "\n".join(
            line for line in self.content_markdown.splitlines() if not line.startswith("![image ")
        )
        summary, _ = truncate_summary(normalize_space(text_only), 240)
        return OpusListItem(
            opus_id=self.opus_id,
            title=self.title,
            summary=summary,
            author=self.author,
            published_at=self.published_at,
            stats=self.stats,
        )


@dataclass(frozen=True, slots=True)
class UnavailableOpus:
    opus_id: str
    title: str
    summary: str
    author: Author
    published_at: str
    stats: ArticleStats

    def list_item(self) -> OpusListItem:
        """Preserve an upstream status card as a compact Opus list item."""
        return OpusListItem(
            opus_id=self.opus_id,
            title=self.title,
            summary=self.summary,
            author=self.author,
            published_at=self.published_at,
            stats=self.stats,
            readable=False,
        )


@dataclass(frozen=True, slots=True)
class _ActivityDetail:
    activity_id: str
    kind: Literal["video", "opus", "live", "repost", "unavailable"]
    author: Author
    published_at: str
    content_markdown: str
    images: tuple[ArticleImage, ...]
    target: (
        VideoResourceRef
        | OpusResourceRef
        | LiveActivityTarget
        | ActivityReferenceTarget
        | UnavailableActivityTarget
    )
    stats: ActivityStats
    fetched_at: str


class OpusService:
    """Normalize current Opus modules while retaining one unified public model."""

    def __init__(
        self,
        transport: BilibiliTransport,
        cache: AsyncTTLCache,
        policy: CachePolicy,
    ) -> None:
        self._transport = transport
        self._cache = cache
        self._policy = policy

    async def resolve_article(self, cvid: str) -> tuple[str | None, bool]:
        """Cache a verified modern-article mapping without treating legacy CV IDs as Opus IDs."""
        validate_decimal_id(cvid, "cvid")

        async def produce() -> str | None:
            """Confirm one article identity and accept only the verified modern-Opus shape."""
            raw = await self._transport.request_data(
                "/x/article/view", {"id": cvid}, not_found_codes=frozenset({-400, -404})
            )
            data = as_object(raw, "article.mapping")
            if get_int(data, "id", "article.mapping") != int(cvid):
                raise schema_error("article.mapping.id")
            return article_opus_id(data)

        return await self._cache.get_or_create(
            ("article_opus_mapping", cvid, ARTICLE_OPUS_MAPPING_VERSION),
            self._policy.detail,
            produce,
        )

    async def get_activity(self, activity_id: str, range_value: str) -> tuple[ActivityOutput, bool]:
        """Read this event's own body and direct target, without following repost chains."""
        validate_decimal_id(activity_id, "activity_id")
        parsed = parse_range(range_value, max_span=TEXT_HARD_MAX_SPAN, allow_negative=True)
        async with self._transport.scoped_work(6):
            detail, hit = await self._cache.get_or_create(
                ("activity_detail", activity_id, ACTIVITY_NORMALIZATION_VERSION),
                self._policy.detail,
                lambda: self._fetch_activity(activity_id),
            )
        markdown, images = detail.content_markdown, list(detail.images)
        if markdown:
            content, resolved = slice_text(
                markdown,
                range_value,
                max_span=TEXT_HARD_MAX_SPAN,
                allow_negative=True,
                continuation_span=ARTICLE_MAX_SPAN,
            )
            resolved_range, next_range = resolved.resolved, resolved.next_range
        else:
            if parsed.relative_to_end or parsed.start != 1:
                raise ServiceError(ErrorCode.INVALID_RANGE, "range cannot address empty content")
            content, resolved_range, next_range = "", None, None
        return ActivityOutput(
            activity_id=detail.activity_id,
            kind=detail.kind,
            author=detail.author,
            published_at=detail.published_at,
            content_markdown=content,
            requested_range=range_value,
            resolved_range=resolved_range,
            total_characters=len(markdown),
            images=images,
            target=detail.target,
            stats=detail.stats,
            next_range=next_range,
            original_url=f"https://t.bilibili.com/{detail.activity_id}",
            fetched_at=detail.fetched_at,
        ), hit

    async def _fetch_activity(self, activity_id: str) -> _ActivityDetail:
        """Normalize one dynamic detail and expand only its own full Opus body if present."""
        raw = await self._transport.request_data(
            "/x/polymer/web-dynamic/v1/detail",
            {"id": activity_id, "features": "itemOpusStyle"},
            not_found_codes=frozenset({-400, -404}),
        )
        data = as_object(raw, "activity.data")
        item = get_object(data, "item", "activity")
        if get_str(item, "id_str", "activity.item") != activity_id:
            raise schema_error("activity.item.id_str")
        activity_type = get_str(item, "type", "activity.item")
        if activity_type not in ACTIVITY_KINDS:
            raise ServiceError(ErrorCode.NOT_FOUND, "this activity type is not supported")
        modules = get_object(item, "modules", "activity.item")
        author = get_object(modules, "module_author", "activity.modules")
        dynamic = get_object(modules, "module_dynamic", "activity.modules")
        stat = get_object(modules, "module_stat", "activity.modules")
        own_uid = get_int(author, "mid", "activity.author")
        if own_uid <= 0:
            raise schema_error("activity.author.mid")
        own_author = Author(uid=str(own_uid), name=optional_str(author, "name"))
        description = optional_object(dynamic, "desc")
        body = optional_str(description, "text") if description is not None else ""
        images = _activity_images(dynamic)
        kind = ACTIVITY_KINDS[activity_type]
        target_item = optional_object(item, "orig") if kind == "repost" else item
        target = _activity_target(target_item)
        if kind != "repost":
            kind = activity_kind(item, target)
        if (
            kind == "opus"
            and isinstance(target, OpusResourceRef)
            and optional_object(optional_object(dynamic, "major") or {}, "opus") is not None
        ):
            # The dynamic card contains only a summary; full body belongs to this same event.
            full, _ = await self.get_full(target.opus_id)
            body, images = full.content_markdown, full.images
        else:
            body = _activity_markdown(body, images)
        return _ActivityDetail(
            activity_id=activity_id,
            kind=kind,
            author=own_author,
            published_at=format_timestamp(optional_str(author, "pub_ts")),
            content_markdown=body,
            images=images,
            target=target,
            stats=ActivityStats(
                share_count=activity_stat(stat, "forward"),
                comment_count=activity_stat(stat, "comment"),
                like_count=activity_stat(stat, "like"),
            ),
            fetched_at=datetime.now(TAIPEI).isoformat(),
        )

    async def get_article(self, opus_id: str, range_value: str) -> tuple[ArticleOutput, bool]:
        """Slice one cached full Markdown document with positive or end-relative ranges."""
        parse_range(range_value, max_span=TEXT_HARD_MAX_SPAN, allow_negative=True)
        full, cache_hit = await self.get_full(opus_id, resolve_forward=True)
        markdown, images = full.content_markdown, list(full.images)
        content, resolved = slice_text(
            markdown,
            range_value,
            max_span=TEXT_HARD_MAX_SPAN,
            allow_negative=True,
            continuation_span=ARTICLE_MAX_SPAN,
        )
        return (
            ArticleOutput(
                opus_id=full.opus_id,
                title=full.title,
                author=full.author,
                published_at=full.published_at,
                requested_range=range_value,
                resolved_range=resolved.resolved,
                total_characters=len(markdown),
                content_markdown=content,
                images=images,
                next_range=resolved.next_range,
                stats=full.stats,
                original_url=f"https://www.bilibili.com/opus/{full.opus_id}",
            ),
            cache_hit,
        )

    async def get_full(
        self,
        opus_id: str,
        *,
        resolve_forward: bool = False,
    ) -> tuple[FullOpus, bool]:
        """Cache normalized Opus content before any caller-specific range is applied."""
        resource, cache_hit = await self.get_resource(opus_id, resolve_forward=resolve_forward)
        if isinstance(resource, UnavailableOpus):
            raise ServiceError(
                ErrorCode.NOT_FOUND,
                "This Opus cannot be read and retrying will not change the result: "
                + resource.summary,
            )
        return resource, cache_hit

    async def get_resource(
        self,
        opus_id: str,
        *,
        resolve_forward: bool = False,
    ) -> tuple[FullOpus | UnavailableOpus, bool]:
        """Cache either readable Opus content or its provider-supplied status card."""
        validate_decimal_id(opus_id, "opus_id")
        resource, hit = await self._cache.get_or_create(
            # A forward ID may resolve to a different source, so keep aliases separate.
            ("opus", opus_id, "markdown-v5", resolve_forward),
            self._policy.detail,
            lambda: self._fetch_resource(opus_id, resolve_forward=resolve_forward),
        )
        if isinstance(resource, FullOpus) and resource.opus_id == opus_id:
            # Direct content has identical identity under both modes; share it with article reads.
            async def direct_content() -> FullOpus:
                return resource

            await self._cache.get_or_create(
                ("opus", opus_id, "markdown-v5", not resolve_forward),
                self._policy.detail,
                direct_content,
            )
        return resource, hit

    async def _fetch_resource(
        self,
        opus_id: str,
        *,
        resolve_forward: bool,
    ) -> FullOpus | UnavailableOpus:
        """Normalize readable content or a provider status card without leaking metadata."""
        raw = await self._transport.request_data(
            "/x/polymer/web-dynamic/v1/opus/detail",
            {
                "timezone_offset": -480,
                "id": opus_id,
                "features": (
                    "onlyfansVote,onlyfansAssetsV2,decorationCard,htmlNewStyle,"
                    "ugcDelete,editable,opusPrivateVisible"
                ),
            },
            not_found_codes=frozenset({-400, -404, 4101131}),
        )
        data = as_object(raw, "opus.data")
        if data.get("fallback") is not None:
            # The Opus endpoint exposes forwarded dynamics as a fallback shell;
            # only article reads may follow the dynamic's original target.
            if not resolve_forward:
                raise ServiceError(ErrorCode.NOT_FOUND, "the requested Opus was not found")
            source_opus_id = await self._resolve_forward_opus_id(opus_id)
            if source_opus_id is None or source_opus_id == opus_id:
                raise ServiceError(ErrorCode.NOT_FOUND, "the requested Opus was not found")
            # Keep the source lookup strict so an alias cannot change comment identity.
            resource, _ = await self.get_resource(source_opus_id)
            return resource
        item = get_object(data, "item", "opus")
        basic = get_object(item, "basic", "opus.item")
        modules = [
            as_object(value, "opus.module") for value in get_list(item, "modules", "opus.item")
        ]
        title_module = _find_optional_module(modules, "module_title")
        author_module = _find_module(modules, "module_author")
        stat_module = _find_module(modules, "module_stat")
        author = Author(
            uid=str(get_int(author_module, "mid", "opus.author")),
            name=optional_str(author_module, "name"),
        )
        published_at = format_timestamp(optional_str(author_module, "pub_ts"))
        stats = _opus_stats(stat_module)
        module_title = optional_str(title_module, "text") if title_module is not None else ""
        title = normalize_space(module_title or optional_str(basic, "title"))
        blocked_module = _find_optional_module(modules, "module_blocked")
        if blocked_module is not None:
            blocked_title, blocked_summary = _blocked_notice(blocked_module)
            return UnavailableOpus(
                opus_id=optional_str(item, "id_str", opus_id),
                title=title or blocked_title,
                summary=blocked_summary,
                author=author,
                published_at=published_at,
                stats=stats,
            )
        content_module = _find_optional_module(modules, "module_content")
        if content_module is None:
            # Observed author/stat-only responses can recover on the next read.
            raise IncompleteOpusError(
                ErrorCode.UPSTREAM_SCHEMA_CHANGED,
                "Bilibili response schema changed at opus.module_content",
            )
        content, images = render_opus_markdown(content_module)
        if not content:
            raise ServiceError(ErrorCode.NOT_FOUND, "the requested Opus has no readable text")
        return FullOpus(
            opus_id=optional_str(item, "id_str", opus_id),
            title=title,
            author=author,
            published_at=published_at,
            content_markdown=content,
            images=images,
            stats=stats,
            comment_oid=int(
                optional_str(basic, "comment_id_str") or optional_str(basic, "rid_str")
            ),
            comment_type=get_int(basic, "comment_type", "opus.basic"),
        )

    async def _resolve_forward_opus_id(self, dynamic_id: str) -> str | None:
        """Resolve a forwarded dynamic ID to its readable source Opus when available."""
        raw = await self._transport.request_data(
            "/x/polymer/web-dynamic/v1/detail",
            {
                "id": dynamic_id,
                "timezone_offset": -480,
                "platform": "web",
                "gaia_source": "main_web",
                "features": (
                    "itemOpusStyle,opusBigCover,onlyfansVote,endFooterHidden,decorationCard,"
                    "onlyfansAssetsV2,ugcDelete"
                ),
                "web_location": "333.1368",
                "x-bili-device-req-json": '{"platform":"web","device":"pc"}',
                "x-bili-web-req-json": '{"spm_id":"333.1368"}',
            },
            not_found_codes=frozenset({-400, -404}),
        )
        data = as_object(raw, "dynamic.data")
        item = optional_object(data, "item")
        if item is None or get_str(item, "type", "dynamic.item") != "DYNAMIC_TYPE_FORWARD":
            return None
        original = optional_object(item, "orig")
        return _extract_opus_id(original) if original is not None else None


def render_opus_markdown(
    content_module: JsonObject,
) -> tuple[str, tuple[ArticleImage, ...]]:
    """Render ordered text and image paragraphs into Markdown plus typed image metadata."""
    rendered: list[str] = []
    images: list[ArticleImage] = []
    for value in get_list(content_module, "paragraphs", "opus.content"):
        paragraph = as_object(value, "opus.paragraph")
        line = _render_paragraph_text(paragraph)
        if line:
            rendered.append(line.strip())
        paragraph_images = _paragraph_images(paragraph, len(images) + 1)
        for image in paragraph_images:
            images.append(image)
            rendered.append(f"![image {image.index}]({image.url})")
    return "\n\n".join(rendered).strip(), tuple(images)


def _render_paragraph_text(paragraph: JsonObject) -> str:
    """Render one text, heading, code, and indentation combination without images."""
    code = optional_object(paragraph, "code")
    if code is not None:
        language = optional_str(code, "lang")
        code_text = optional_str(code, "code") or optional_str(code, "text")
        return f"```{language}\n{code_text}\n```"

    text = optional_object(paragraph, "text")
    line = (
        ""
        if text is None
        else "".join(
            _render_node(as_object(node, "opus.text.node")) for node in optional_list(text, "nodes")
        )
    )
    heading = optional_object(paragraph, "heading")
    if heading is not None and line:
        level = min(max(optional_int(heading, "level", 2), 1), 6)
        return f"{'#' * level} {line}"
    if line and _paragraph_is_indented(paragraph) and not line.startswith(("- ", "#", "```")):
        return "- " + line
    return line


def _paragraph_is_indented(paragraph: JsonObject) -> bool:
    paragraph_format = optional_object(paragraph, "format")
    if paragraph_format is None:
        return False
    indent = optional_object(paragraph_format, "indent")
    return indent is not None and optional_int(indent, "indent") > 0


def _paragraph_images(paragraph: JsonObject, first_index: int) -> list[ArticleImage]:
    """Normalize all valid images from one paragraph while assigning contiguous indexes."""
    picture = optional_object(paragraph, "pic")
    if picture is None:
        return []
    values = optional_list(picture, "pics")
    if not values and optional_str(picture, "url"):
        values = [picture]
    images: list[ArticleImage] = []
    for value in values:
        image = _normalize_image(as_object(value, "opus.image"), first_index + len(images))
        if image is not None:
            images.append(image)
    return images


def _normalize_image(data: JsonObject, index: int) -> ArticleImage | None:
    """Keep stable CDN metadata and ignore image slots without a usable URL."""
    url = optional_str(data, "url").strip()
    if not url:
        return None
    if url.startswith("//"):
        url = "https:" + url
    return ArticleImage(
        index=index,
        url=url,
        width=optional_int(data, "width"),
        height=optional_int(data, "height"),
    )


def _render_node(node: JsonObject) -> str:
    """Render one typed Opus inline node without leaking unsupported node metadata."""
    if optional_str(node, "type") == "TEXT_NODE_TYPE_WORD":
        return _render_word(get_object(node, "word", "opus.node"))
    rich = optional_object(node, "rich")
    if rich is not None:
        return _render_rich(rich)
    user = optional_object(node, "user")
    if user is not None:
        return _render_user(user)
    formula = optional_object(node, "formula")
    if formula is not None:
        return _render_formula(formula)
    return ""


def _render_word(word: JsonObject) -> str:
    """Render nested Markdown emphasis in the provider's declared style order."""
    text = optional_str(word, "words")
    style = optional_object(word, "style")
    if style is None or not text:
        return text
    if optional_bool(style, "strikethrough"):
        text = f"~~{text}~~"
    if optional_bool(style, "italic"):
        text = f"*{text}*"
    if optional_bool(style, "bold"):
        text = f"**{text}**"
    return text


def _render_rich(rich: JsonObject) -> str:
    text = optional_str(rich, "text") or optional_str(rich, "orig_text")
    url = optional_str(rich, "jump_url") or optional_str(rich, "url")
    return f"[{text}]({url})" if text and url else text


def _render_user(user: JsonObject) -> str:
    name = optional_str(user, "name") or optional_str(user, "text")
    return "@" + name


def _render_formula(formula: JsonObject) -> str:
    return optional_str(formula, "latex_content") or optional_str(formula, "text")


def _find_module(modules: list[JsonObject], key: str) -> JsonObject:
    value = _find_optional_module(modules, key)
    if value is not None:
        return value
    raise schema_error(f"opus.{key}")


def _find_optional_module(modules: list[JsonObject], key: str) -> JsonObject | None:
    """Treat null module slots as absent while still validating non-null shapes."""
    for module in modules:
        value = module.get(key)
        if value is not None:
            return as_object(value, f"opus.{key}")
    return None


def _extract_opus_id(data: JsonObject) -> str | None:
    """Extract a source Opus ID from a forwarded dynamic's modular target."""
    modules = optional_object(data, "modules")
    dynamic = optional_object(modules, "module_dynamic") if modules is not None else None
    major = optional_object(dynamic, "major") if dynamic is not None else None
    opus = optional_object(major, "opus") if major is not None else None
    if opus is None:
        return None
    # `orig.id_str` is the stable source key; jump_url is only a compatibility fallback.
    candidate = _positive_decimal_id(optional_str(data, "id_str"))
    if candidate is not None:
        return candidate
    basic = optional_object(data, "basic")
    for value in (
        optional_str(opus, "jump_url"),
        optional_str(basic, "jump_url") if basic is not None else "",
    ):
        candidate = _opus_id_from_url(value)
        if candidate is not None:
            return candidate
    raise schema_error("dynamic.forward.orig.opus")


def _positive_decimal_id(value: str) -> str | None:
    if not value or not value.isascii() or not value.isdecimal() or value.startswith("0"):
        return None
    return value


def _opus_id_from_url(value: str) -> str | None:
    if not value:
        return None
    normalized = "https:" + value if value.startswith("//") else value
    path = urlsplit(normalized).path
    if not path.startswith("/opus/"):
        return None
    return _positive_decimal_id(path.removeprefix("/opus/").split("/", 1)[0])


def _stat_count(module: JsonObject, key: str) -> int:
    value = get_object(module, key, "opus.stat")
    return optional_int(value, "count")


def _opus_stats(module: JsonObject) -> ArticleStats:
    return ArticleStats(
        share_count=_stat_count(module, "forward"),
        comment_count=_stat_count(module, "comment"),
        like_count=_stat_count(module, "like"),
        favorite_count=_stat_count(module, "favorite"),
    )


def _blocked_notice(module: JsonObject) -> tuple[str, str]:
    """Turn the user-visible parts of an Opus status card into compact prose."""
    title, message, action_text = blocked_notice_fields(module)
    summary = message or title or "该 Opus 当前没有可读取的正文。"
    if action_text:
        summary = f"{summary} ({action_text})"
    return title or "内容暂不可读", summary


def _activity_images(dynamic: JsonObject) -> tuple[ArticleImage, ...]:
    """Preserve the image inventory in this event's own draw or Opus card."""
    major = optional_object(dynamic, "major")
    draw = optional_object(major, "draw") if major is not None else None
    opus = optional_object(major, "opus") if major is not None else None
    values = (
        optional_list(draw, "items")
        if draw is not None
        else optional_list(opus, "pics")
        if opus is not None
        else []
    )
    images: list[ArticleImage] = []
    for value in values:
        item = as_object(value, "activity.image")
        image = _normalize_image(
            {
                "url": optional_str(item, "src" if draw is not None else "url"),
                "width": optional_int(item, "width"),
                "height": optional_int(item, "height"),
            },
            len(images) + 1,
        )
        if image is not None:
            images.append(image)
    return tuple(images)


def _activity_markdown(body: str, images: tuple[ArticleImage, ...]) -> str:
    parts = [body] if body else []
    parts.extend(f"![image {image.index}]({image.url})" for image in images)
    return "\n\n".join(parts)
