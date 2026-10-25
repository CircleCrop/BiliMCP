"""Official video preview grids and bounded real-frame extraction for public videos."""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import math
import re
import secrets
import struct
import time
from bisect import bisect_left
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from itertools import pairwise
from typing import Literal, cast

from PIL import Image, ImageDraw

from .cache import AsyncTTLCache, BinaryCacheValue
from .images import IMAGE_TTL, OUTPUT_MAX_BYTES, ImageMedia, _public_image_url
from .logging_context import current_tool_call
from .models import (
    ErrorCode,
    ImageRegion,
    PlaybackWindow,
    ServiceError,
    VideoFrameCoverage,
    VideoFrameItem,
    VideoFrameOutput,
    VideoFrameQuality,
    VideoFramesOutput,
)
from .normalization import (
    JsonObject,
    JsonValue,
    as_object,
    get_int,
    get_list,
    get_object,
    schema_error,
)
from .transport import BilibiliTransport
from .video_media import decode_frame, decode_frames, decoder_codecs, media_url
from .videos import VideoCore, VideoPageCore, VideoService, require_public_playback

FRAME_VERSION = "1080p-sdr-v3"
_QUALITY = {
    16: "360p",
    32: "480p",
    64: "720p",
    74: "720p60",
    80: "1080p",
    112: "1080p_high_bitrate",
    116: "1080p60",
}


@dataclass(frozen=True, slots=True)
class PlaybackTrack:
    url: str = field(repr=False)
    identity: str
    quality: VideoFrameQuality


class _PartialSheet(Exception):
    """Return incomplete evidence without caching a transient stopping point."""

    def __init__(self, value: BinaryCacheValue) -> None:
        self.value = value


def choose_track(data: JsonObject, codecs: frozenset[str]) -> PlaybackTrack:
    """Choose actual authorized SDR tracks, preferring temporal fidelity then proven codecs."""
    choices: list[tuple[tuple[int, int, int], PlaybackTrack]] = []
    highest = 0
    for raw in get_list(get_object(data, "dash", "video playback"), "video", "video playback"):
        track = as_object(raw, "video track")
        quality = get_int(track, "id", "video track")
        width, height = (
            get_int(track, "width", "video track"),
            get_int(track, "height", "video track"),
        )
        if (
            quality not in _QUALITY
            or min(width, height) < 1
            or min(width, height) > 1080
            or max(width, height) > 1920
        ):
            continue
        highest = max(highest, quality)
        name = track.get("codecs")
        codec = (
            "avc"
            if isinstance(name, str) and name.startswith("avc1.")
            else "hevc"
            if isinstance(name, str) and name.startswith(("hev1.", "hvc1."))
            else "av1"
            if isinstance(name, str) and name.startswith("av01.")
            else ""
        )
        if codec not in codecs:
            continue
        bandwidth = track.get("bandwidth", 0)
        if type(bandwidth) is not int or bandwidth < 0:
            raise schema_error("video track bandwidth")
        rate = track.get("frameRate", track.get("frame_rate"))
        rate = rate if isinstance(rate, str) else None
        urls: list[object] = [track.get("baseUrl"), track.get("base_url")]
        for key in ("backupUrl", "backup_url"):
            if isinstance(track.get(key), list):
                urls.extend(cast(list[object], track[key]))
        for url in urls:
            if not isinstance(url, str):
                continue
            try:
                checked = media_url(url)
            except ServiceError:
                continue
            identity = hashlib.sha256(
                json.dumps([quality, name, width, height, rate, bandwidth]).encode()
            ).hexdigest()[:24]
            choices.append(
                (
                    (quality, {"avc": 2, "hevc": 1, "av1": 0}[codec], bandwidth),
                    PlaybackTrack(
                        checked,
                        identity,
                        VideoFrameQuality(
                            label=_QUALITY[quality], width=width, height=height, frame_rate=rate
                        ),
                    ),
                )
            )
            break
    if not choices:
        raise ServiceError(ErrorCode.NOT_FOUND, "no decodable public SDR track; use mode=preview")
    priority, selected = max(choices, key=lambda choice: choice[0])
    if priority[0] < highest:
        selected.quality.notice = (
            "A higher tier has no supported decoder or validated media location; "
            "using the highest supported SDR tier."
        )
    return selected


def _contact_sheet(values: tuple[BinaryCacheValue, ...]) -> tuple[bytes, int, int]:
    """Compose at most twelve labeled thumbnails without duplicating native image blocks."""
    columns = min(4, len(values))
    rows = math.ceil(len(values) / columns)
    sheet = Image.new("RGB", (columns * 480, rows * 298), "#10141b")
    draw = ImageDraw.Draw(sheet)
    for index, value in enumerate(values):
        with Image.open(io.BytesIO(value.data)) as opened:
            thumb = opened.convert("RGB")
            thumb.thumbnail((480, 270), Image.Resampling.LANCZOS)
        x, y = (index % columns) * 480, (index // columns) * 298
        sheet.paste(thumb, (x + (480 - thumb.width) // 2, y + (270 - thumb.height) // 2))
        draw.text(
            (x + 8, y + 277),
            f"{index + 1} | {float(cast(float, value.metadata['actual_time_seconds'])):.6f}s",
            fill="white",
        )
    for quality in (85, 75, 65):
        buffer = io.BytesIO()
        sheet.save(buffer, "JPEG", quality=quality, optimize=True)
        if buffer.tell() <= OUTPUT_MAX_BYTES:
            return buffer.getvalue(), sheet.width, sheet.height
    raise ServiceError(
        ErrorCode.INVALID_ARGUMENT, "contact sheet exceeds 2 MiB; request fewer frames"
    )


def _crop_frame(value: BinaryCacheValue, region: ImageRegion) -> BinaryCacheValue:
    """Crop an already decoded frame without changing its temporal identity or enlarging it."""
    with Image.open(io.BytesIO(value.data)) as opened:
        if region.left + region.width > opened.width or region.top + region.height > opened.height:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT, "region exceeds the returned frame dimensions"
            )
        image = opened.crop(
            (region.left, region.top, region.left + region.width, region.top + region.height)
        )
        output = io.BytesIO()
        image.save(output, "PNG")
    if output.tell() > OUTPUT_MAX_BYTES:
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT, "cropped frame exceeds 2 MiB; use a smaller region"
        )
    return BinaryCacheValue(
        output.getvalue(),
        {
            **value.metadata,
            "width": region.width,
            "height": region.height,
            "mime_type": "image/png",
        },
        value.expires_at,
    )


@dataclass(frozen=True, slots=True)
class PreviewGrid:
    columns: int
    rows: int
    width: int
    height: int
    images: tuple[str, ...]
    times: tuple[int, ...]


def preview_times(values: list[object], capacity: int, duration: int) -> tuple[int, ...]:
    """Drop the protocol's single sentinel, keeping the first image's zero timestamp."""
    if len(values) < 2 or values[0] != 0 or values[1] != 0:
        raise schema_error("video preview time origin")
    result: list[int] = []
    for value in values[1:]:
        if type(value) is not int or value < 0 or value >= duration:
            raise schema_error("video preview time")
        if result and value <= result[-1]:
            raise schema_error("video preview time order")
        result.append(value)
    if len(result) > capacity:
        raise schema_error("video preview grid capacity")
    return tuple(result)


def preview_position(times: tuple[int, ...], requested: float) -> int:
    """Choose the nearest preview, preferring the earlier image on an equal distance."""
    right = bisect_left(times, requested)
    if right == 0:
        return 0
    if right == len(times):
        return right - 1
    return right - 1 if requested - times[right - 1] <= times[right] - requested else right


class VideoFrameService:
    """Reuse resource identities, persistent binary caching and the shared decoder slots."""

    def __init__(
        self,
        videos: VideoService,
        transport: BilibiliTransport,
        cache: AsyncTTLCache,
        media: ImageMedia,
    ) -> None:
        """Keep shared media resources and account-scoped playback selections for this service."""
        self.videos, self.transport, self.cache, self.media = videos, transport, cache, media
        self._tracks: dict[tuple[str, str, str], tuple[float, PlaybackTrack]] = {}
        self._track_lock = asyncio.Lock()
        self._codecs: frozenset[str] | None = None
        self._unverified_scope = secrets.token_hex(16)

    def _scope(self) -> str:
        """Partition frames by verified account without storing credentials."""
        identity = self.transport.account_uid or self._unverified_scope
        return hashlib.sha256(identity.encode()).hexdigest()[:24]

    @asynccontextmanager
    async def _slot(self) -> AsyncIterator[None]:
        """Share one cancellable decoder slot across both account pools."""
        if self.media.frame_waiters >= 8:
            raise ServiceError(
                ErrorCode.RATE_LIMITED,
                "video decoder queue is full",
                http_status=429,
                retry_after_seconds=3,
            )
        self.media.frame_waiters += 1
        acquired = False
        try:
            queued = time.monotonic()
            try:
                await asyncio.wait_for(self.media.frame_slot.acquire(), 3)
                acquired = True
            except TimeoutError as error:
                raise ServiceError(
                    ErrorCode.RATE_LIMITED,
                    "video decoder is busy",
                    http_status=429,
                    retry_after_seconds=3,
                ) from error
            context = current_tool_call()
            if context:
                context.upstream_work.queue_wait_ms += (time.monotonic() - queued) * 1000
            yield
        finally:
            self.media.frame_waiters -= 1
            if acquired:
                self.media.frame_slot.release()

    async def _track(self, core: VideoCore, page: VideoPageCore) -> PlaybackTrack:
        """Keep signed playback locations in bounded account-local memory for five minutes."""
        async with self._track_lock:
            key = (self._scope(), page.cid, core.content_version)
            cached = self._tracks.get(key)
            if cached and cached[0] > time.monotonic():
                return cached[1]
            if self._codecs is None:
                self._codecs = await decoder_codecs()
            data = as_object(
                await self.transport.request_data(
                    "/x/player/wbi/playurl",
                    {"bvid": core.bvid, "cid": page.cid, "qn": 116, "fnval": 4048, "fourk": 0},
                    wbi=True,
                ),
                "video playback",
            )
            if data.get("is_preview"):
                raise ServiceError(
                    ErrorCode.INVALID_ARGUMENT, "source only offers a paid playback preview"
                )
            track = choose_track(data, self._codecs)
            if len(self._tracks) >= 64:
                del self._tracks[next(iter(self._tracks))]
            self._tracks[(self._scope(), page.cid, core.content_version)] = (
                time.monotonic() + 300,
                track,
            )
            return track

    async def _remember(
        self, core: VideoCore, page: VideoPageCore, track: PlaybackTrack, value: BinaryCacheValue
    ) -> BinaryCacheValue:
        """Persist a frame reference bound to its account, source and integer PTS."""
        identity = [
            self._scope(),
            page.cid,
            core.content_version,
            track.identity,
            value.metadata.get("pts"),
            value.metadata.get("time_base"),
            value.metadata["actual_time_seconds"],
            FRAME_VERSION,
        ]
        reference = "vf_" + hashlib.sha256(json.dumps(identity).encode()).hexdigest()[:32]
        result = BinaryCacheValue(
            value.data,
            {
                **value.metadata,
                "frame_ref": reference,
                "bvid": core.bvid,
                "cid": page.cid,
                "content_version": core.content_version,
                "source_quality": track.quality.model_dump(mode="json", exclude_none=True),
            },
            value.expires_at,
        )

        async def produce() -> BinaryCacheValue:
            return result

        saved, _ = await self.media.cache.get_or_create(
            ("video-frame-reference", self._scope(), reference), IMAGE_TTL, produce
        )
        return saved

    async def get_frame(
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
        """Return a preview, exact image, batched contact sheet or bounded visual-change window."""
        if sum(item is not None for item in (time_seconds, times_seconds, window, frame_ref)) != 1:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT,
                "provide exactly one of time_seconds, times_seconds, window or frame_ref",
            )
        if mode not in {"preview", "exact"}:
            raise ServiceError(ErrorCode.INVALID_ARGUMENT, "mode must be preview or exact")
        if region is not None and (
            times_seconds is not None
            or window is not None
            or (frame_ref is None and mode != "exact")
        ):
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT, "region requires one exact frame or frame_ref"
            )
        times = (
            [time_seconds]
            if time_seconds is not None
            else times_seconds or ([window.start_seconds, window.end_seconds] if window else [])
        )
        if any(isinstance(value, bool) or not math.isfinite(value) or value < 0 for value in times):
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT, "frame times must be finite nonnegative seconds"
            )
        if times_seconds is not None and (
            not 1 <= len(times_seconds) <= 8
            or any(left >= right for left, right in pairwise(times_seconds))
        ):
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT,
                "times_seconds must contain 1..8 strictly increasing times",
            )
        if window and not 0 < window.end_seconds - window.start_seconds <= 10:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT,
                "window must have start < end and span at most 10 seconds",
            )
        if times_seconds and mode == "exact" and times_seconds[-1] - times_seconds[0] > 10:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT,
                "exact batch must span at most 10 seconds; use preview for wider scene discovery",
            )
        async with asyncio.timeout(20):
            core, core_hit = await self.videos.get_core(bvid)
            if len(core.pages) > 1 and page_number is None:
                raise ServiceError(
                    ErrorCode.INVALID_ARGUMENT, "page_number is required for a multi-page video"
                )
            number = 1 if page_number is None else page_number
            if isinstance(number, bool) or not 1 <= number <= len(core.pages):
                raise ServiceError(ErrorCode.INVALID_ARGUMENT, "page_number is outside the video")
            page = core.pages[number - 1]
            if times and (
                max(times) > page.duration_seconds
                or (window is None and max(times) >= page.duration_seconds)
            ):
                raise ServiceError(
                    ErrorCode.INVALID_ARGUMENT, "frame times must be within this video part"
                )
            exact = mode == "exact" or window is not None or frame_ref is not None
            if exact:
                require_public_playback(core)
            if frame_ref is not None:
                if not re.fullmatch(r"vf_[0-9a-f]{32}", frame_ref):
                    raise ServiceError(ErrorCode.INVALID_ARGUMENT, "invalid frame_ref")

                async def missing() -> BinaryCacheValue:
                    raise ServiceError(
                        ErrorCode.NOT_FOUND,
                        "frame_ref expired or belongs to another account; request a new frame",
                    )

                value, hit = await self.media.cache.get_or_create(
                    ("video-frame-reference", self._scope(), frame_ref), IMAGE_TTL, missing
                )
                if (
                    value.metadata.get("bvid") != bvid
                    or value.metadata.get("cid") != page.cid
                    or value.metadata.get("content_version") != core.content_version
                ):
                    raise ServiceError(
                        ErrorCode.INVALID_ARGUMENT,
                        "frame_ref does not match this video part or content version",
                    )
                requested = float(cast(float, value.metadata["actual_time_seconds"]))
            elif times_seconds is not None or window is not None:
                return await self._multiple(core, page, number, times_seconds, window, mode)
            else:
                assert time_seconds is not None
                requested = time_seconds
                value, hit = (
                    await self._exact(core, page, requested)
                    if exact
                    else await self._preview(core, page, requested)
                )
            reference = value.metadata.get("frame_ref")
            if region is not None:
                value = await self.media.render_task(lambda: _crop_frame(value, region))
            actual = float(cast(float, value.metadata["actual_time_seconds"]))
            output = VideoFrameOutput(
                bvid=bvid,
                cid=page.cid,
                page_number=number,
                source_kind="video_frame" if exact else "video_preview",
                requested_time_seconds=requested,
                actual_time_seconds=actual,
                width=get_int(value.metadata, "width", "video frame"),
                height=get_int(value.metadata, "height", "video frame"),
                mime_type="image/png"
                if value.metadata.get("mime_type") == "image/png"
                else "image/jpeg",
                fetched_at=str(value.metadata["fetched_at"]),
                canonical_url=f"https://www.bilibili.com/video/{bvid}?p={number}&t={int(actual)}",
                timing_notice="Preview timestamp may differ from the requested moment."
                if not exact
                else "Source frame timing has a gap after the requested moment."
                if actual - requested > 0.25
                else None,
                frame_ref=reference if isinstance(reference, str) else None,
                source_quality=VideoFrameQuality.model_validate(value.metadata["source_quality"])
                if "source_quality" in value.metadata
                else None,
                region=region,
            )
            output._image_data = value.data
            return output, core_hit and hit

    async def _grid(self, core: VideoCore, page: VideoPageCore) -> tuple[PreviewGrid, bool]:
        """Read JSON or current binary time tables, never an entire video or all sprite boards."""

        async def produce() -> PreviewGrid:
            """Normalize the validated official videoshot response before persistence."""
            data = as_object(
                await self.transport.request_data(
                    "/x/player/videoshot",
                    {"bvid": core.bvid, "cid": page.cid, "index": 1},
                    not_found_codes=frozenset({-404, 40003}),
                ),
                "video preview",
            )
            images = get_list(data, "image", "video preview")
            if not images:
                raise ServiceError(ErrorCode.NOT_FOUND, "this video part has no progress preview")
            urls: list[str] = []
            for image in images:
                if not isinstance(image, str):
                    raise schema_error("video preview image URL")
                urls.append(_public_image_url(image))
            columns, rows = (
                get_int(data, "img_x_len", "video preview"),
                get_int(data, "img_y_len", "video preview"),
            )
            width, height = (
                get_int(data, "img_x_size", "video preview"),
                get_int(data, "img_y_size", "video preview"),
            )
            if min(columns, rows, width, height) < 1 or columns * rows * len(urls) > 65536:
                raise schema_error("video preview dimensions")
            values: list[object] = list(get_list(data, "index", "video preview"))
            if not values:
                binary_url = data.get("pvdata")
                if not isinstance(binary_url, str):
                    raise ServiceError(
                        ErrorCode.NOT_FOUND, "this video part has no preview time table"
                    )
                binary, _ = await self.media.source(binary_url, max_bytes=128 * 1024)
                if not binary.data or len(binary.data) % 2:
                    raise schema_error("video preview binary table")
                values = list(struct.unpack(f">{len(binary.data) // 2}H", binary.data))
            return PreviewGrid(
                columns,
                rows,
                width,
                height,
                tuple(urls),
                preview_times(values, columns * rows * len(urls), page.duration_seconds),
            )

        return await self.cache.get_or_create(
            ("video-preview-grid", page.cid, "v1"), 86400, produce
        )

    async def _preview(
        self, core: VideoCore, page: VideoPageCore, requested: float
    ) -> tuple[BinaryCacheValue, bool]:
        """Cache only the selected grid region; nearby times share the same encoded frame."""
        grid, grid_hit = await self._grid(core, page)
        position = preview_position(grid.times, requested)
        board, cell = divmod(position, grid.columns * grid.rows)
        url = grid.images[board]
        source, source_hit = await self.media.source(url)
        region = ImageRegion(
            left=(cell % grid.columns) * grid.width,
            top=(cell // grid.columns) * grid.height,
            width=grid.width,
            height=grid.height,
        )

        async def produce() -> BinaryCacheValue:
            """Crop one original cell off-loop and persist its original source time and expiry."""
            rendered = await self.media.render_work(source, "video_preview", "overview", region)
            return BinaryCacheValue(
                rendered.data,
                {**rendered.metadata, "actual_time_seconds": grid.times[position]},
                source.expires_at,
            )

        remaining = max(0.0, (source.expires_at or time.time() + IMAGE_TTL) - time.time())
        value, hit = await self.media.cache.get_or_create(
            ("video-preview-frame", page.cid, url, position, source.expires_at, "v1"),
            remaining,
            produce,
        )
        return value, grid_hit and source_hit and hit

    def _media_identity(self, core: VideoCore, page: VideoPageCore, track: PlaybackTrack) -> str:
        """Bind reusable source bytes to authorization and content, never signed CDN URLs."""
        return hashlib.sha256(
            json.dumps(
                [
                    self._scope(),
                    page.cid,
                    core.content_version,
                    track.identity,
                    FRAME_VERSION,
                ],
                separators=(",", ":"),
            ).encode()
        ).hexdigest()

    async def _exact(
        self, core: VideoCore, page: VideoPageCore, requested: float
    ) -> tuple[BinaryCacheValue, bool]:
        """Revalidate account-local track selection before using durable, track-specific frames."""
        track = await self._track(core, page)

        async def produce() -> BinaryCacheValue:
            """Reserve decoding work and persist the exact frame with its source identity."""
            async with self._slot():
                value = await decode_frame(
                    self.media,
                    track.url,
                    requested,
                    cache_identity=self._media_identity(core, page, track),
                )
                return await self._remember(core, page, track, value)

        return await self.media.cache.get_or_create(
            (
                "video-exact-frame",
                self._scope(),
                page.cid,
                core.content_version,
                track.identity,
                requested,
                FRAME_VERSION,
            ),
            IMAGE_TTL,
            produce,
        )

    async def _multiple(
        self,
        core: VideoCore,
        page: VideoPageCore,
        number: int,
        times: list[float] | None,
        window: PlaybackWindow | None,
        mode: str,
    ) -> tuple[VideoFramesOutput, bool]:
        """Share one decoder run and return one contact sheet with explicit observed coverage."""
        exact = window is not None or mode == "exact"
        track = await self._track(core, page) if exact else None
        targets = tuple(times) if times else (window.start_seconds,) if window else ()
        selection: Literal["visual_change", "timestamps"] = (
            "visual_change" if window else "timestamps"
        )

        async def produce() -> BinaryCacheValue:
            """Decode a bounded selection and cache only a contact sheet with complete coverage."""
            if track is not None:
                async with self._slot():
                    decoded = await decode_frames(
                        self.media,
                        track.url,
                        targets,
                        window_end=window.end_seconds if window else None,
                        cache_identity=self._media_identity(core, page, track),
                    )
                    values = tuple(
                        [await self._remember(core, page, track, item) for item in decoded.frames]
                    )
                coverage = VideoFrameCoverage(
                    complete=decoded.complete,
                    reason=cast(
                        Literal[
                            "end_of_window",
                            "end_of_video",
                            "frame_limit",
                            "time_limit",
                            "media_limit",
                        ],
                        decoded.reason,
                    ),
                    decoded_frames=decoded.decoded,
                    examined_start_seconds=decoded.start,
                    examined_end_seconds=decoded.end,
                    presentation="selected_frames" if window else "requested_frames",
                )

            else:
                values = tuple([(await self._preview(core, page, target))[0] for target in targets])
                coverage = VideoFrameCoverage(
                    complete=True,
                    reason="end_of_window",
                    decoded_frames=0,
                    examined_start_seconds=float(
                        cast(float, values[0].metadata["actual_time_seconds"])
                    ),
                    examined_end_seconds=float(
                        cast(float, values[-1].metadata["actual_time_seconds"])
                    ),
                    presentation="requested_frames",
                )

            data, width, height = await self.media.render_task(lambda: _contact_sheet(values))
            frames = [
                VideoFrameItem(
                    index=i + 1,
                    actual_time_seconds=float(cast(float, value.metadata["actual_time_seconds"])),
                    width=int(cast(int, value.metadata["width"])),
                    height=int(cast(int, value.metadata["height"])),
                    frame_ref=cast(str | None, value.metadata.get("frame_ref")),
                    requested_time_seconds=targets[i] if window is None else None,
                ).model_dump(mode="json", exclude_none=True)
                for i, value in enumerate(values)
            ]
            result = BinaryCacheValue(
                data,
                {
                    "frames": cast(list[JsonValue], frames),
                    "coverage": coverage.model_dump(mode="json"),
                    "width": width,
                    "height": height,
                    "fetched_at": values[0].metadata["fetched_at"],
                },
                time.time() + IMAGE_TTL,
            )
            if not coverage.complete:
                raise _PartialSheet(result)
            return result

        key = (
            "video-frame-sheet",
            self._scope() if exact else "public",
            page.cid,
            core.content_version,
            track.identity if track else "preview",
            targets,
            window.end_seconds if window else None,
            FRAME_VERSION,
        )
        try:
            value, hit = await self.media.cache.get_or_create(key, IMAGE_TTL, produce)
        except _PartialSheet as partial:
            value, hit = partial.value, False

        output = VideoFramesOutput(
            bvid=core.bvid,
            cid=page.cid,
            page_number=number,
            source_kind="video_frame" if exact else "video_preview",
            frames=[
                VideoFrameItem.model_validate(item)
                for item in get_list(value.metadata, "frames", "frame sheet")
            ],
            coverage=VideoFrameCoverage.model_validate(value.metadata["coverage"]),
            selection=selection,
            source_quality=track.quality if track else None,
            width=get_int(value.metadata, "width", "frame sheet"),
            height=get_int(value.metadata, "height", "frame sheet"),
            fetched_at=str(value.metadata["fetched_at"]),
            canonical_url=f"https://www.bilibili.com/video/{core.bvid}?p={number}&t={int(targets[0])}",
            notice=(
                "Candidates are a visual-change subset, not semantic matches. "
                "Complete decoding does not prove an event absent; inspect frame_ref."
            )
            if window
            else "Thumbnails preserve source timestamps; inspect exact frame_ref for detail."
            if exact
            else "Sampled previews may miss brief events; use exact or a short window.",
        )
        output._image_data = value.data
        return output, hit
