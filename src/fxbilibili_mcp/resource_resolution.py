"""Parse a single public Bilibili reference before bounded upstream resolution."""

from __future__ import annotations

import re
import unicodedata
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from urllib.parse import parse_qs, urljoin, urlsplit

from .cache import AsyncTTLCache
from .models import (
    ActivityResourceRef,
    ErrorCode,
    LegacyArticleResourceRef,
    LiveRoomResourceRef,
    OpusResourceRef,
    PublicFavoritesTarget,
    ResolveResourceOutput,
    ResourceRef,
    ServiceError,
    UserResourceRef,
    VideoResourceRef,
    VideoSeasonTarget,
    VideoSeriesTarget,
)
from .normalization import (
    ARTICLE_OPUS_MAPPING_VERSION,
    TAIPEI,
    JsonValue,
    article_opus_id,
    as_object,
    get_int,
    get_str,
    is_valid_bvid,
    schema_error,
    validate_bvid,
    validate_decimal_id,
)
from .transport import BusinessBudget, BusinessBudgetExhausted

_URL_RE = re.compile(
    r"https?://[^\s<>\"'\uff0c\u3002\uff01\uff1f\uff1b\u3001\u3010\u3011]+", re.IGNORECASE
)
_NAMED_RE = re.compile(
    r"(?P<prefix>BV|av|uid|opus|activity|cv|room):?(?P<value>[0-9A-Za-z]+)", re.IGNORECASE
)
_SHORT_PATH_RE = re.compile(r"/[0-9A-Za-z]{1,32}/?")
_DECIMAL_RE = re.compile(r"[1-9][0-9]*")
_SECONDS_RE = re.compile(r"(?:0|[1-9][0-9]*)")
_HOSTS = frozenset(
    {
        "www.bilibili.com",
        "bilibili.com",
        "m.bilibili.com",
        "space.bilibili.com",
        "t.bilibili.com",
        "live.bilibili.com",
        "b23.tv",
    }
)


@dataclass(frozen=True, slots=True)
class ParsedVideo:
    bvid: str


@dataclass(frozen=True, slots=True)
class ParsedAV:
    aid: str


@dataclass(frozen=True, slots=True)
class ParsedUser:
    uid: str


@dataclass(frozen=True, slots=True)
class ParsedOpus:
    opus_id: str


@dataclass(frozen=True, slots=True)
class ParsedActivity:
    activity_id: str


@dataclass(frozen=True, slots=True)
class ParsedArticle:
    cvid: str


@dataclass(frozen=True, slots=True)
class ParsedShortLink:
    url: str


@dataclass(frozen=True, slots=True)
class ParsedLiveRoom:
    room_id: str


type ParsedTarget = (
    ParsedVideo
    | ParsedAV
    | ParsedUser
    | ParsedOpus
    | ParsedActivity
    | ParsedArticle
    | ParsedShortLink
    | ParsedLiveRoom
    | PublicFavoritesTarget
    | VideoSeasonTarget
    | VideoSeriesTarget
)


@dataclass(frozen=True, slots=True)
class ParsedResource:
    """Keep URL selection separate from resources requiring verified conversion."""

    target: ParsedTarget
    original_url: str | None = None
    page_number: int | None = None
    at_seconds: int | None = None


class ShortLinkResponseProtocol(Protocol):
    """Consume only redirect metadata from the credential-free transport path."""

    @property
    def status_code(self) -> int: ...

    @property
    def location(self) -> str | None: ...


class ResolutionTransport(Protocol):
    """Require audited sends and the shared actual-attempt budget."""

    def scoped_work(self, max_attempts: int) -> AbstractAsyncContextManager[BusinessBudget]: ...

    async def request_data(
        self, path: str, params: dict[str, str | int | bool] | None = None
    ) -> JsonValue: ...

    async def request_short_link(self, url: str) -> ShortLinkResponseProtocol: ...


class ResourceResolutionService:
    """Resolve supported identities using at most five audited business exchanges."""

    def __init__(self, transport: ResolutionTransport, cache: AsyncTTLCache | None = None) -> None:
        """Reuse the pool cache, or own a small cache for standalone resolver callers."""
        self._transport = transport
        self._cache = (
            cache if cache is not None else AsyncTTLCache(10000, max_bytes=8 * 1024 * 1024)
        )

    async def resolve_resource(self, text: str) -> tuple[ResolveResourceOutput, bool]:
        """Resolve one target without fetching final web pages or guessing legacy IDs."""
        parsed = parse_resource_text(text)
        if isinstance(parsed.target, (ParsedAV, ParsedArticle, ParsedShortLink)):
            # Preserve playback selection while coalescing only conversions that need HTTP.
            key = (
                "resource_resolution",
                repr(parsed.target),
                parsed.page_number,
                parsed.at_seconds,
                # Cached CV and short-link results may have classified readable notes as legacy.
                ARTICLE_OPUS_MAPPING_VERSION
                if isinstance(parsed.target, (ParsedArticle, ParsedShortLink))
                else "v1",
            )
            return await self._cache.get_or_create(key, 3600, lambda: self._resolve_parsed(parsed))
        return await self._resolve_parsed(parsed), False

    async def _resolve_parsed(self, parsed: ParsedResource) -> ResolveResourceOutput:
        """Resolve and cache only verified successful identities with their original timestamp."""
        try:
            async with self._transport.scoped_work(5):
                if isinstance(parsed.target, ParsedShortLink):
                    parsed = await self._resolve_short_link(parsed.target)
                target = parsed.target
                if isinstance(target, ParsedAV):
                    target = await self._resolve_av(target)
                elif isinstance(target, ParsedArticle):
                    target = await self._resolve_article(target)
        except BusinessBudgetExhausted as exc:
            raise ServiceError(
                ErrorCode.RATE_LIMITED, "resource resolution upstream work budget is exhausted"
            ) from exc
        resource, original_url, readable = _public_reference(target)
        if isinstance(target, PublicFavoritesTarget) and parsed.original_url is not None:
            # Collected folders need not belong to the space in the path; retain its valid link.
            original_url = parsed.original_url
        return ResolveResourceOutput(
            resource=resource,
            original_url=original_url,
            readable=readable,
            page_number=parsed.page_number,
            at_seconds=parsed.at_seconds,
            fetched_at=datetime.now(TAIPEI).isoformat(timespec="seconds"),
        )

    async def _resolve_av(self, target: ParsedAV) -> ParsedVideo:
        """Confirm the AV identity against the fixed video-detail JSON endpoint."""
        source = as_object(
            await self._transport.request_data("/x/web-interface/view", {"aid": int(target.aid)}),
            "resource video",
        )
        bvid = get_str(source, "bvid", "resource video")
        if not is_valid_bvid(bvid) or get_int(source, "aid", "resource video") != int(target.aid):
            raise schema_error("resource video identity")
        return ParsedVideo(bvid)

    async def _resolve_short_link(self, target: ParsedShortLink) -> ParsedResource:
        """Validate each redirect before requesting another audited short-link hop."""
        url = target.url
        seen = {url}
        for _ in range(3):
            response = await self._transport.request_short_link(url)
            if response.status_code == 404:
                raise ServiceError(ErrorCode.NOT_FOUND, "Bilibili short link was not found")
            if response.status_code not in {301, 302, 303, 307, 308} or not response.location:
                raise ServiceError(
                    ErrorCode.UPSTREAM_SCHEMA_CHANGED,
                    "Bilibili short link omitted a supported redirect",
                )
            location = urljoin(url, response.location)
            parsed = parse_resource_url(location)
            if not isinstance(parsed.target, ParsedShortLink):
                return parsed
            location = parsed.target.url
            if location in seen:
                raise _invalid("short link contains a redirect loop")
            seen.add(location)
            url = location
        raise _invalid("short link exceeds the three-redirect limit")

    async def _resolve_article(self, target: ParsedArticle) -> ParsedArticle | ParsedOpus:
        """Use the validated JSON article type and dynamic ID to confirm Opus mapping."""

        async def produce() -> str | None:
            """Share the search mapping cache so CV resolution does not repeat the same send."""
            source = as_object(
                await self._transport.request_data("/x/article/view", {"id": int(target.cvid)}),
                "resource article",
            )
            if get_int(source, "id", "resource article") != int(target.cvid):
                raise schema_error("resource article identity")
            return article_opus_id(source)

        opus_id, _ = await self._cache.get_or_create(
            ("article_opus_mapping", target.cvid, ARTICLE_OPUS_MAPPING_VERSION), 3600, produce
        )
        return ParsedOpus(opus_id) if opus_id is not None else target


def _public_reference(target: ParsedTarget) -> tuple[ResourceRef, str, bool]:
    """Expose current readable kinds and preserve unverified legacy identities."""
    if isinstance(target, ParsedVideo):
        return (
            VideoResourceRef(bvid=target.bvid),
            f"https://www.bilibili.com/video/{target.bvid}",
            True,
        )
    if isinstance(target, ParsedUser):
        return UserResourceRef(uid=target.uid), f"https://space.bilibili.com/{target.uid}", True
    if isinstance(target, ParsedOpus):
        return (
            OpusResourceRef(opus_id=target.opus_id),
            f"https://www.bilibili.com/opus/{target.opus_id}",
            True,
        )
    if isinstance(target, ParsedActivity):
        # Activity detail preserves the event identity instead of guessing an Opus body.
        return (
            ActivityResourceRef(activity_id=target.activity_id),
            f"https://t.bilibili.com/{target.activity_id}",
            True,
        )
    if isinstance(target, ParsedLiveRoom):
        # The live reader resolves displayed short room numbers to canonical IDs.
        return (
            LiveRoomResourceRef(room_id=target.room_id),
            f"https://live.bilibili.com/{target.room_id}",
            True,
        )
    if isinstance(target, ParsedArticle):
        # An unverified CV mapping must remain distinguishable from an Opus identifier.
        return (
            LegacyArticleResourceRef(cvid=target.cvid),
            f"https://www.bilibili.com/read/cv{target.cvid}",
            False,
        )
    if isinstance(target, PublicFavoritesTarget):
        return (
            target,
            f"https://www.bilibili.com/medialist/detail/ml{target.favorite_list_id}",
            True,
        )
    if isinstance(target, (VideoSeasonTarget, VideoSeriesTarget)):
        identifier = target.season_id if isinstance(target, VideoSeasonTarget) else target.series_id
        kind = "season" if isinstance(target, VideoSeasonTarget) else "series"
        return (
            target,
            f"https://space.bilibili.com/{target.owner_uid}/lists/{identifier}?type={kind}",
            True,
        )
    raise schema_error("unresolved resource identity")


def parse_resource_text(text: str) -> ParsedResource:
    """Extract exactly one HTTPS link or one complete named identifier."""
    if not text or len(text) > 4000:
        raise _invalid("text must contain 1..4000 characters")
    if any(
        unicodedata.category(char) in {"Cc", "Cf", "Cs"} and char not in "\n\r\t" for char in text
    ):
        raise _invalid("text contains unsupported control or invisible characters")
    value = text.strip()
    urls = [_trim_share_punctuation(match.group()) for match in _URL_RE.finditer(value)]
    if len(urls) > 1:
        raise _invalid("text contains multiple resource links; resolve each separately")
    if urls:
        # A second named ID outside the link makes the requested target ambiguous.
        remainder = _URL_RE.sub(" ", value)
        if re.search(
            r"(?<![0-9A-Za-z])(?:BV[0-9A-Za-z]{10}|(?:av|uid|opus|activity|cv|room):?[1-9][0-9]*)(?![0-9A-Za-z])",
            remainder,
            re.IGNORECASE,
        ):
            raise _invalid("text contains multiple resource references; resolve each separately")
        return parse_resource_url(urls[0])
    if value.isdecimal():
        raise _invalid("bare numbers are ambiguous; use a named identifier or complete link")
    match = _NAMED_RE.fullmatch(value)
    if match is None:
        raise _invalid("text must contain one named identifier or one Bilibili resource link")
    return ParsedResource(_parse_named(match.group("prefix"), match.group("value")))


def parse_resource_url(url: str) -> ParsedResource:
    """Accept only known HTTPS resource paths and strict playback selections."""
    if len(url) > 4000 or any(unicodedata.category(char) in {"Cc", "Cf", "Cs"} for char in url):
        raise _invalid("resource link must contain at most 4000 visible characters")
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as exc:
        raise _invalid("resource link has an invalid host or port") from exc
    if parsed.scheme != "https" or parsed.hostname not in _HOSTS or port not in {None, 443}:
        raise _invalid("resource link must use an approved Bilibili HTTPS host and port")
    if parsed.username is not None or parsed.password is not None or "\\" in url:
        raise _invalid("resource link must not contain credentials or backslashes")
    # Encoded paths admit alternate route interpretations between URL parsers.
    if "%" in parsed.path or "//" in parsed.path:
        raise _invalid("resource link must use a known unencoded path")
    host = parsed.hostname
    path = parsed.path.rstrip("/")
    target: ParsedTarget
    if host == "b23.tv":
        if _SHORT_PATH_RE.fullmatch(parsed.path) is None or parsed.query or parsed.fragment:
            raise _invalid("short link must use a known b23.tv token path")
        target = ParsedShortLink(f"https://b23.tv{parsed.path}")
    elif host == "space.bilibili.com":
        query = parse_qs(parsed.query, keep_blank_values=True)
        match = re.fullmatch(r"/([1-9][0-9]*)/favlist", path)
        lists_match = re.fullmatch(r"/([1-9][0-9]*)/lists/([1-9][0-9]*)", path)
        if match:
            fid = _query_integer(query, "fid", _DECIMAL_RE)
            if fid is None:
                raise _invalid("favorite link must contain one fid")
            # A space can collect another owner's folder; its path UID is not ownership proof.
            target = PublicFavoritesTarget(kind="public_favorites", favorite_list_id=str(fid))
        elif lists_match:
            if query.get("type") == ["season"]:
                target = VideoSeasonTarget(
                    kind="video_season",
                    owner_uid=lists_match.group(1),
                    season_id=lists_match.group(2),
                )
            elif query.get("type") == ["series"]:
                target = VideoSeriesTarget(
                    kind="video_series",
                    owner_uid=lists_match.group(1),
                    series_id=lists_match.group(2),
                )
            else:
                raise _invalid("collection link type must be season or series")
        elif match := re.fullmatch(r"/([1-9][0-9]*)(?:/(?:video|dynamic))?", path):
            target = ParsedUser(match.group(1))
        else:
            raise _invalid("resource link must name a user space")
    elif match := re.fullmatch(r"/medialist/detail/ml([1-9][0-9]*)", path):
        target = PublicFavoritesTarget(kind="public_favorites", favorite_list_id=match.group(1))
    elif host == "t.bilibili.com":
        target = ParsedActivity(_path_decimal(path, "activity_id"))
    elif host == "live.bilibili.com":
        target = ParsedLiveRoom(_path_decimal(path, "room_id"))
    elif (match := re.fullmatch(r"/video/(BV[0-9A-Za-z]{10}|av[1-9][0-9]*)", path)) is not None:
        identifier = match.group(1)
        target = (
            ParsedVideo(validate_bvid(identifier))
            if identifier.startswith("BV")
            else ParsedAV(identifier[2:])
        )
    elif (match := re.fullmatch(r"/opus/([1-9][0-9]*)", path)) is not None:
        target = ParsedOpus(match.group(1))
    elif (match := re.fullmatch(r"/read/cv([1-9][0-9]*)", path)) is not None:
        target = ParsedArticle(match.group(1))
    else:
        raise _invalid("resource link path is not supported")
    page_number: int | None = None
    at_seconds: int | None = None
    if isinstance(target, (ParsedVideo, ParsedAV)):
        query = parse_qs(parsed.query, keep_blank_values=True)
        page_number = _query_integer(query, "p", _DECIMAL_RE)
        at_seconds = _query_integer(query, "t", _SECONDS_RE)
    return ParsedResource(target, url, page_number, at_seconds)


def _parse_named(prefix: str, value: str) -> ParsedTarget:
    """Validate each named ID using the existing domain identifier rules."""
    if prefix.lower() == "bv":
        return ParsedVideo(validate_bvid(prefix + value))
    prefix = prefix.lower()
    validate_decimal_id(value, prefix)
    if prefix == "av":
        return ParsedAV(value)
    if prefix == "uid":
        return ParsedUser(value)
    if prefix == "opus":
        return ParsedOpus(value)
    if prefix == "activity":
        return ParsedActivity(value)
    if prefix == "room":
        return ParsedLiveRoom(value)
    return ParsedArticle(value)


def _path_decimal(path: str, field: str) -> str:
    return validate_decimal_id(path.removeprefix("/"), field)


def _query_integer(query: dict[str, list[str]], key: str, pattern: re.Pattern[str]) -> int | None:
    if key not in query:
        return None
    values = query[key]
    if len(values) != 1 or pattern.fullmatch(values[0]) is None:
        raise _invalid(f"resource link {key} must be one valid integer")
    return int(values[0])


def _trim_share_punctuation(url: str) -> str:
    """Remove share-text terminators while retaining balanced URL parentheses."""
    # ASCII punctuation can be part of an invalid playback value, so retain it.
    url = url.rstrip("\uff09\u3011")
    while url.endswith(")") and url.count(")") > url.count("("):
        url = url[:-1]
    while url.endswith("]") and url.count("]") > url.count("["):
        url = url[:-1]
    return url


def _invalid(message: str) -> ServiceError:
    return ServiceError(ErrorCode.INVALID_ARGUMENT, message)
