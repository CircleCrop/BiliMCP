"""Public image sources, byte-bounded media caching, and native image rendering."""

from __future__ import annotations

import asyncio
import io
import math
import time
import warnings
from collections.abc import Callable
from contextlib import aclosing
from http.cookiejar import Cookie, CookieJar, DefaultCookiePolicy
from pathlib import Path
from urllib.parse import urljoin, urlsplit
from urllib.request import Request

import httpx
from PIL import Image, ImageCms, ImageOps, UnidentifiedImageError

from .cache import AsyncTTLCache, BinaryCacheValue
from .collections import CollectionService
from .comments import CommentService
from .logging_context import current_tool_call
from .models import (
    ArticleImage,
    CommentVideoTarget,
    ErrorCode,
    ImageOutput,
    ImagePart,
    ImageRegion,
    ImageTarget,
    OpusResourceRef,
    PublicFavoritesTarget,
    ServiceError,
    VideoSeasonTarget,
    VideoSeriesTarget,
)
from .normalization import JsonObject, format_timestamp
from .opuses import OpusService
from .persistent_cache import PersistentHistoryCache
from .scheduler import SendBudget, UpstreamScheduler
from .transport import _retry_after_seconds
from .users import UserService
from .videos import VideoService

SOURCE_MAX_BYTES = 8 * 1024**2
OUTPUT_MAX_BYTES = 2 * 1024**2
RESPONSE_MAX_BYTES = 3 * 1024**2
IMAGE_TTL = 7 * 86400
COVER_KINDS = frozenset({"video", "video_season", "video_series", "public_favorites"})


class _NoResponseCookies(DefaultCookiePolicy):
    """Public media has no session state; reject CDN cookies before the shared jar grows."""

    def set_ok(self, cookie: Cookie, request: Request) -> bool:
        return False


CDN_HOSTS = frozenset(
    {
        "i0.hdslb.com",
        "i1.hdslb.com",
        "i2.hdslb.com",
        "archive.biliimg.com",
        "bimp.hdslb.com",
        "boss.hdslb.com",
    }
)


def public_image_url(url: str) -> str:
    """Accept the same stable, credential-free CDN destinations for every image path."""
    if url.startswith("//"):
        url = "https:" + url
    if url.startswith("http://"):
        url = "https://" + url.removeprefix("http://")
    try:
        parsed = urlsplit(url)
        allowed = (
            parsed.scheme == "https"
            and parsed.hostname in CDN_HOSTS
            and parsed.port in {None, 443}
            and not parsed.username
            and not parsed.password
            and not parsed.query
            and not parsed.fragment
        )
    except ValueError:
        allowed = False
    if not allowed:
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT, "image must use a stable approved Bilibili CDN URL"
        )
    return url


_public_image_url = public_image_url


class ImageMedia:
    """Manage credential-free downloads and bounded rendering."""

    def __init__(
        self,
        path: Path | None,
        transport: httpx.AsyncBaseTransport | None,
        send_budget: SendBudget | None,
        *,
        video_cache_path: Path | None = None,
        video_cache_max_bytes: int = 20 * 1024**3,
        video_cache_ttl_seconds: int = 7 * 86400,
    ) -> None:
        """Create one media cache and HTTP client; credentials are never copied here."""
        self.cache = AsyncTTLCache(max_bytes=64 * 1024**2)
        self.store = PersistentHistoryCache(path)
        self.video_cache = AsyncTTLCache(max_bytes=64 * 1024**2)
        self.video_cache_ttl = video_cache_ttl_seconds
        video_path = video_cache_path or (
            path.with_name(path.stem.removesuffix("-images") + "-video-media.sqlite3")
            if path is not None
            else None
        )
        self.video_store = PersistentHistoryCache(
            video_path, video_cache_max_bytes, video_cache_ttl_seconds
        )
        if video_path is not None:
            self.video_cache.set_persistent_store(self.video_store, "video-ranges-v1", binary=True)
        if path is not None:
            self.cache.set_persistent_store(self.store, "public-images-v1", binary=True)
        self.http = httpx.AsyncClient(
            cookies=CookieJar(policy=_NoResponseCookies()),
            transport=transport,
            timeout=10,
            follow_redirects=False,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "image/*"},
            trust_env=False,
        )
        self.scheduler = UpstreamScheduler(
            concurrency=2, requests_per_minute=0, burst=60, send_budget=send_budget
        )
        self.render_slots = asyncio.Semaphore(2)
        self.frame_slot = asyncio.Semaphore(1)
        self.frame_waiters = 0

    async def close(self) -> None:
        """Wait for producers before closing media stores and HTTP resources."""
        await self.cache.close()
        await self.store.close()
        await self.video_cache.close()
        await self.video_store.close()
        await self.scheduler.close()
        await self.http.aclose()

    async def source(
        self, url: str, *, max_bytes: int = SOURCE_MAX_BYTES
    ) -> tuple[BinaryCacheValue, bool]:
        """Fetch a public source once; disk reads retain original expiry and fetch time."""
        url = _public_image_url(url)

        async def produce() -> BinaryCacheValue:
            """Read bounded CDN bytes without cookie headers, redirects, or body logging."""
            current = url
            context = current_tool_call()

            def admit() -> None:
                """Count each actual image request."""
                if context is not None:
                    context.upstream_work.send()

            async with asyncio.timeout(10):
                for hop in range(3):
                    request = self.http.build_request("GET", current)
                    # Public CDN redirects must never propagate a stored cookie.
                    request.headers.pop("cookie", None)
                    async with self.scheduler.slot(
                        owner=context.request_id if context else "images",
                        endpoint="image-cdn",
                        on_admit=admit,
                    ):
                        response = await self.http.send(request, stream=True)
                        async with aclosing(response):
                            if response.is_redirect:
                                location = response.headers.get("location")
                                if not location or hop == 2:
                                    raise ServiceError(
                                        ErrorCode.UPSTREAM_UNAVAILABLE,
                                        "image redirect limit exceeded",
                                    )
                                current = _public_image_url(urljoin(current, location))
                                continue
                            if response.status_code in {401, 403, 429}:
                                raise ServiceError(
                                    ErrorCode.RATE_LIMITED,
                                    "image source rejected this request",
                                    http_status=429,
                                    retry_after_seconds=_retry_after_seconds(response),
                                )
                            if response.status_code == 404:
                                raise ServiceError(
                                    ErrorCode.NOT_FOUND, "image is no longer available"
                                )
                            if response.status_code != 200:
                                raise ServiceError(
                                    ErrorCode.UPSTREAM_UNAVAILABLE, "image source is unavailable"
                                )
                            data = bytearray()
                            async for chunk in response.aiter_bytes():
                                if len(data) + len(chunk) > max_bytes:
                                    raise ServiceError(
                                        ErrorCode.INVALID_ARGUMENT,
                                        "source exceeds download byte limit",
                                    )
                                data.extend(chunk)
                            return BinaryCacheValue(
                                bytes(data),
                                {
                                    "fetched_at": format_timestamp(int(time.time())),
                                },
                                time.time() + IMAGE_TTL,
                            )
            raise ServiceError(ErrorCode.UPSTREAM_UNAVAILABLE, "image download did not complete")

        try:
            result, hit = await self.cache.get_or_create(
                ("image-source", url, "v1"), IMAGE_TTL, produce
            )
            if len(result.data) > max_bytes:
                raise ServiceError(ErrorCode.INVALID_ARGUMENT, "source exceeds download byte limit")
            return result, hit
        except (httpx.HTTPError, TimeoutError) as exc:
            raise ServiceError(
                ErrorCode.UPSTREAM_UNAVAILABLE, "image download failed or timed out"
            ) from exc

    async def render(
        self, source: BinaryCacheValue, url: str, kind: str, part: ImagePart
    ) -> tuple[BinaryCacheValue, bool]:
        """Cache each rendering separately while sharing the original source bytes."""

        async def produce() -> BinaryCacheValue:
            """Wait for bounded Pillow work off-loop before releasing its slot."""
            return await self.render_work(source, kind, part)

        # A rendering of an old source must not receive a fresh seven-day lifetime.
        expires_at = source.expires_at
        remaining = (
            max(0.0, float(expires_at) - time.time())
            if isinstance(expires_at, (int, float))
            else IMAGE_TTL
        )
        return await self.cache.get_or_create(
            ("image-render", url, kind, part, expires_at, "v2"), remaining, produce
        )

    async def render_work(
        self, source: BinaryCacheValue, kind: str, part: ImagePart, crop: ImageRegion | None = None
    ) -> BinaryCacheValue:
        """Keep bounded decoder capacity until off-loop Pillow work has finished, even on cancel."""
        return await self.render_task(lambda: _render(source, kind, part, crop))

    async def render_task[T](self, work: Callable[[], T]) -> T:
        """Share Pillow slots across images, contact sheets and cached frame crops."""
        async with self.render_slots:
            task = asyncio.create_task(asyncio.to_thread(work))
            try:
                return await asyncio.shield(task)
            except asyncio.CancelledError:
                completion = asyncio.gather(task, return_exceptions=True)
                while not completion.done():
                    try:
                        await asyncio.shield(completion)
                    except asyncio.CancelledError:
                        continue
                completion.result()
                raise


class ImageService:
    """Resolve public resource identities into one selected source image."""

    def __init__(
        self,
        media: ImageMedia,
        videos: VideoService,
        users: UserService,
        opuses: OpusService,
        comments: CommentService,
        collections: CollectionService,
        cache: AsyncTTLCache,
    ) -> None:
        """Share existing resource services and their account-specific metadata cache."""
        self.media, self.videos, self.users = media, videos, users
        self.opuses, self.comments = opuses, comments
        self.collections, self.cache = collections, cache

    async def get_image(
        self, target: ImageTarget, index: int = 1, part: ImagePart = "overview"
    ) -> tuple[ImageOutput, bool]:
        """Return a native image and its source region, never image base64 in JSON."""
        if index < 1 or isinstance(index, bool):
            raise ServiceError(ErrorCode.INVALID_ARGUMENT, "image index must be a positive integer")
        if part != "overview" and (not isinstance(part, int) or isinstance(part, bool) or part < 1):
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT, "part must be overview or a positive integer"
            )
        metadata_hit = True
        original_url: str | None
        if target.kind == "video":
            core, metadata_hit = await self.videos.get_core(target.bvid)
            images = [ArticleImage(index=1, url=core.cover_url, width=0, height=0)]
            original_url = f"https://www.bilibili.com/video/{target.bvid}"
        elif target.kind == "user":
            profile, metadata_hit = await self.users.get_space_profile(target.uid)
            images = [ArticleImage(index=1, url=profile.avatar_url, width=0, height=0)]
            original_url = f"https://space.bilibili.com/{target.uid}"
        elif target.kind == "opus":
            opus, metadata_hit = await self.opuses.get_full(target.opus_id)
            images = list(opus.images)
            target = OpusResourceRef(opus_id=opus.opus_id)
            original_url = f"https://www.bilibili.com/opus/{opus.opus_id}"
        elif target.kind == "activity":
            # Resource resolution reads source URLs, not the caller-specific delivery projection.
            activity, metadata_hit = await self.opuses.get_activity(target.activity_id, "1..3000")
            images = activity.images
            original_url = activity.original_url
        elif isinstance(target, (VideoSeasonTarget, VideoSeriesTarget, PublicFavoritesTarget)):
            (cover, original_url), metadata_hit = await self.collections.get_cover(
                target, self.cache
            )
            images = [cover]
        else:
            images, metadata_hit = await self.comments.image_inventory(target)
            subject = target.subject
            if subject.kind == "video":
                identifier = (
                    "av" + subject.video_aid
                    if isinstance(subject, CommentVideoTarget)
                    else subject.bvid
                )
                original_url = f"https://www.bilibili.com/video/{identifier}"
            elif subject.kind == "opus":
                original_url = f"https://www.bilibili.com/opus/{subject.opus_id}"
            elif subject.kind == "activity":
                original_url = f"https://t.bilibili.com/{subject.activity_id}"
            elif subject.kind == "legacy_article":
                original_url = f"https://www.bilibili.com/read/cv{subject.cvid}"
            else:
                original_url = None
            if original_url is not None:
                original_url += f"#reply{target.comment_id}"
        selected = next((image for image in images if image.index == index), None)
        if selected is None:
            raise ServiceError(ErrorCode.NOT_FOUND, "resource has no image at this index")
        url = _public_image_url(selected.url)
        source, source_hit = await self.media.source(url)
        rendered, render_hit = await self.media.render(source, url, target.kind, part)
        output = ImageOutput.model_validate(
            {
                "target": target,
                "index": index,
                "part": part,
                "original_url": original_url,
                "image_url": url,
                **rendered.metadata,
            }
        )
        output._image_data = rendered.data
        return output, metadata_hit and source_hit and render_hit


def _axis(length: int, maximum: int, overlap: int = 64) -> list[tuple[int, int]]:
    count = max(1, math.ceil((length - overlap) / (maximum - overlap)))
    size = math.ceil((length + (count - 1) * overlap) / count)
    return [(i * (size - overlap), min(size, length - i * (size - overlap))) for i in range(count)]


def _render(
    source: BinaryCacheValue, kind: str, part: ImagePart, crop: ImageRegion | None = None
) -> BinaryCacheValue:
    """Validate, orient, crop and encode one image without restoring lost text detail."""
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(source.data)) as opened:
                if opened.format not in {"JPEG", "PNG", "WEBP", "GIF"}:
                    raise ServiceError(
                        ErrorCode.UPSTREAM_SCHEMA_CHANGED, "source is not a supported image"
                    )
                sw, sh = opened.size
                if sw * sh > 32_000_000 or max(sw, sh) > 32768:
                    raise ServiceError(
                        ErrorCode.INVALID_ARGUMENT, "source image exceeds 32 MP or dimension limit"
                    )
                png = (
                    opened.format == "PNG"
                    or opened.mode in {"RGBA", "LA"}
                    or "transparency" in opened.info
                )
                first_frame = getattr(opened, "n_frames", 1) > 1
                image = ImageOps.exif_transpose(opened)
                icc = image.info.get("icc_profile")
                if isinstance(icc, bytes):
                    alpha = (
                        image.convert("RGBA").getchannel("A")
                        if "A" in image.getbands() or "transparency" in image.info
                        else None
                    )
                    profile = ImageCms.ImageCmsProfile(io.BytesIO(icc))
                    converted = ImageCms.profileToProfile(
                        image.convert("RGB"),
                        profile,
                        ImageCms.createProfile("sRGB"),
                        outputMode="RGB",
                    )
                    if converted is None:
                        raise ValueError("image color conversion failed")
                    image = converted
                    if alpha is not None:
                        image.putalpha(alpha)
                image = image.convert(
                    "RGBA"
                    if png and ("A" in image.getbands() or "transparency" in image.info)
                    else "RGB"
                )
                sw, sh = image.size
                if (
                    part != "overview"
                    and kind not in COVER_KINDS | {"user"}
                    and (sw > 1280 or sh > 720)
                ):
                    # Detail crops must not add JPEG loss to small text in the source.
                    png = True
                region = ImageRegion(left=0, top=0, width=sw, height=sh)
                pieces = [(x, y, w, h) for y, h in _axis(sh, 1536) for x, w in _axis(sw, 1280)]
                if kind in COVER_KINDS | {"user"}:
                    pieces = [(0, 0, sw, sh)]
                if crop is not None:
                    if crop.left + crop.width > sw or crop.top + crop.height > sh:
                        raise ServiceError(
                            ErrorCode.UPSTREAM_SCHEMA_CHANGED, "preview grid exceeds source image"
                        )
                    region = crop
                    pieces = [(crop.left, crop.top, crop.width, crop.height)]
                if part != "overview":
                    if part > len(pieces):
                        raise ServiceError(
                            ErrorCode.INVALID_ARGUMENT, "part exceeds image part_count"
                        )
                    x, y, w, h = pieces[part - 1]
                    region = ImageRegion(left=x, top=y, width=w, height=h)
                if kind == "user":
                    side = min(sw, sh)
                    region = ImageRegion(
                        left=(sw - side) // 2, top=(sh - side) // 2, width=side, height=side
                    )
                image = image.crop(
                    (
                        region.left,
                        region.top,
                        region.left + region.width,
                        region.top + region.height,
                    )
                )
                if kind == "user":
                    image.thumbnail((256, 256), Image.Resampling.LANCZOS)
                elif part == "overview" or kind in COVER_KINDS:
                    image.thumbnail((1280, 720), Image.Resampling.LANCZOS)
                # Re-encoding removes EXIF/GPS and comments; retain alpha only in PNG.
                image.info.clear()
                buffer = io.BytesIO()
                image.save(buffer, format="PNG" if png else "JPEG", quality=85, optimize=True)
                data = buffer.getvalue()
                if len(data) > OUTPUT_MAX_BYTES:
                    raise ServiceError(ErrorCode.INVALID_ARGUMENT, "encoded image exceeds 2 MiB")
                next_part = None
                if part == "overview":
                    if (
                        crop is None
                        and kind not in COVER_KINDS | {"user"}
                        and (len(pieces) > 1 or image.size != (sw, sh))
                    ):
                        next_part = 1
                elif part < len(pieces):
                    next_part = part + 1
                metadata: JsonObject = {
                    "source_width": sw,
                    "source_height": sh,
                    "width": image.width,
                    "height": image.height,
                    "source_region": {
                        "left": region.left,
                        "top": region.top,
                        "width": region.width,
                        "height": region.height,
                    },
                    "part_count": len(pieces),
                    "next_part": next_part,
                    "mime_type": "image/png" if png else "image/jpeg",
                    "fetched_at": source.metadata["fetched_at"],
                }
                if first_frame:
                    metadata["first_frame"] = True
                return BinaryCacheValue(data, metadata, source.expires_at)
    except (
        UnidentifiedImageError,
        OSError,
        ValueError,
        Image.DecompressionBombError,
        Image.DecompressionBombWarning,
        ImageCms.PyCMSError,
    ) as exc:
        raise ServiceError(
            ErrorCode.UPSTREAM_SCHEMA_CHANGED, "source image is invalid or undecodable"
        ) from exc
