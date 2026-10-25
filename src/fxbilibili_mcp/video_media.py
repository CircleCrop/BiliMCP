"""Budgeted seekable video input and cancellable, isolated FFmpeg frame decoding."""

from __future__ import annotations

import asyncio
import io
import math
import os
import re
import secrets
import signal
import time
from contextlib import suppress
from dataclasses import dataclass
from fractions import Fraction
from typing import cast
from urllib.parse import urljoin, urlsplit

import httpx
from PIL import Image, ImageChops

from .cache import BinaryCacheValue
from .images import IMAGE_TTL, OUTPUT_MAX_BYTES, ImageMedia
from .logging_context import current_tool_call
from .models import ErrorCode, ServiceError
from .normalization import format_timestamp
from .transport import _retry_after_seconds

BLOCK_BYTES = 2 * 1024**2
MAX_MEDIA_BYTES = 16 * 1024**2
MAX_MEDIA_SENDS = 8
MEDIA_HOST_SUFFIXES = (".bilivideo.com", ".bilivideo.cn", ".edge.mountaintoys.cn")


def media_url(value: str) -> str:
    """Allow only HTTPS playback CDN locations supplied by the authenticated API."""
    if value.startswith("http://"):
        value = "https://" + value[7:]
    try:
        parsed = urlsplit(value)
        valid = (
            parsed.scheme == "https"
            and parsed.hostname is not None
            and parsed.hostname.endswith(MEDIA_HOST_SUFFIXES)
            and parsed.port in {None, 443}
            and not parsed.username
            and not parsed.password
            and not parsed.fragment
        )
    except ValueError:
        valid = False
    if not valid:
        raise ServiceError(ErrorCode.INVALID_ARGUMENT, "unsupported video CDN location")
    return value


class VideoRangeReader:
    """Expose one task's validated CDN resource through a temporary loopback Range server."""

    def __init__(self, media: ImageMedia, url: str, cache_identity: str | None = None) -> None:
        """Keep signed URLs in task memory; persist validated ranges under an opaque identity."""
        self.media, self.url = media, media_url(url)
        self.cache_identity = cache_identity
        self.supplied_bytes = 0
        self.context = current_tool_call()
        self.blocks: dict[int, bytes] = {}
        self.total = 0
        self.downloaded = 0
        self.sends = 0
        self.server: asyncio.Server | None = None
        self.tasks: set[asyncio.Task[None]] = set()
        self.error: ServiceError | None = None
        self.lock = asyncio.Lock()
        self.path = "/" + secrets.token_urlsafe(24)

    async def start(self) -> str:
        """Learn the resource size with one bounded Range read before launching FFmpeg."""
        await self.block(0)
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 0, limit=8192)
        port = self.server.sockets[0].getsockname()[1]
        return f"http://127.0.0.1:{port}{self.path}"

    async def close(self) -> None:
        """Revoke the private endpoint and settle every active source reader."""
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.blocks.clear()

    async def block(self, number: int) -> bytes:
        """Serialize bounded cache fills so concurrent FFmpeg probes cannot overspend."""
        async with self.lock:
            if number in self.blocks:
                return self.blocks[number]
            needed = (
                min(BLOCK_BYTES, self.total - number * BLOCK_BYTES) if self.total else BLOCK_BYTES
            )
            if self.supplied_bytes + needed > MAX_MEDIA_BYTES:
                raise ServiceError(ErrorCode.INVALID_ARGUMENT, "video frame media budget exceeded")
            if self.cache_identity is None:
                value = await self._fetch_block(number)
            else:
                value, _ = await self.media.video_cache.get_or_create(
                    ("video-range", self.cache_identity, number, BLOCK_BYTES),
                    self.media.video_cache_ttl,
                    lambda: self._fetch_block(number),
                )
            total = int(cast(int, value.metadata["total"]))
            if self.total and self.total != total:
                raise ServiceError(ErrorCode.UPSTREAM_SCHEMA_CHANGED, "video resource size changed")
            self.total = total
            self.supplied_bytes += len(value.data)
            if self.supplied_bytes > MAX_MEDIA_BYTES:
                raise ServiceError(ErrorCode.INVALID_ARGUMENT, "video frame media budget exceeded")
            if self.context is not None:
                self.context.upstream_work.media_read_bytes += len(value.data)
            self.blocks[number] = value.data
            return value.data

    async def _fetch_block(self, number: int) -> BinaryCacheValue:
        """Fill one validated block; cache reads never count as network sends or bytes."""
        start = number * BLOCK_BYTES
        end = start + BLOCK_BYTES - 1
        if self.total:
            end = min(end, self.total - 1)
        current = self.url
        for _ in range(3):
            if self.sends >= MAX_MEDIA_SENDS or self.downloaded >= MAX_MEDIA_BYTES:
                raise ServiceError(ErrorCode.INVALID_ARGUMENT, "video frame media budget exceeded")

            def admit() -> None:
                """Count every actual CDN request in the current call."""
                if self.context is not None:
                    self.context.upstream_work.send()
                self.sends += 1

            request = self.media.http.build_request(
                "GET",
                current,
                headers={
                    "Range": f"bytes={start}-{end}",
                    "Accept": "application/octet-stream",
                    "Accept-Encoding": "identity",
                    "Referer": "https://www.bilibili.com/",
                },
            )
            request.headers.pop("cookie", None)
            async with self.media.scheduler.slot(
                owner=self.context.request_id if self.context else "video-frame",
                endpoint="/media/video",
                on_admit=admit,
            ):
                response = await self.media.http.send(request, stream=True)
                try:
                    if response.is_redirect:
                        location = response.headers.get("location")
                        if not location:
                            raise ServiceError(
                                ErrorCode.UPSTREAM_SCHEMA_CHANGED,
                                "video CDN redirect omitted location",
                            )
                        current = media_url(urljoin(current, location))
                        continue
                    if response.status_code in {401, 403, 429}:
                        raise ServiceError(
                            ErrorCode.RATE_LIMITED,
                            "video source rejected request",
                            http_status=429,
                            retry_after_seconds=_retry_after_seconds(response),
                        )
                    if response.status_code != 206:
                        raise ServiceError(
                            ErrorCode.UPSTREAM_UNAVAILABLE,
                            "video source does not provide bounded Range reads",
                        )
                    match = re.fullmatch(
                        r"bytes (\d+)-(\d+)/(\d+)", response.headers.get("content-range", "")
                    )
                    if match is None:
                        raise ServiceError(
                            ErrorCode.UPSTREAM_SCHEMA_CHANGED, "invalid video Content-Range"
                        )
                    first, last, total = map(int, match.groups())
                    if (
                        first != start
                        or last != min(end, total - 1)
                        or total <= last
                        or (self.total and total != self.total)
                    ):
                        raise ServiceError(
                            ErrorCode.UPSTREAM_SCHEMA_CHANGED,
                            "video Range does not match requested bytes",
                        )
                    if response.headers.get("content-encoding", "identity") != "identity":
                        raise ServiceError(
                            ErrorCode.UPSTREAM_SCHEMA_CHANGED,
                            "compressed video Range is unsupported",
                        )
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        self.downloaded += len(chunk)
                        if self.context is not None:
                            self.context.upstream_work.media_bytes += len(chunk)
                        if (
                            self.downloaded > MAX_MEDIA_BYTES
                            or len(body) + len(chunk) > last - first + 1
                        ):
                            raise ServiceError(
                                ErrorCode.INVALID_ARGUMENT,
                                "video frame media byte limit exceeded",
                            )
                        body.extend(chunk)
                    if len(body) != last - first + 1:
                        raise ServiceError(
                            ErrorCode.UPSTREAM_UNAVAILABLE, "video Range response was truncated"
                        )
                    self.total = total
                    return BinaryCacheValue(
                        bytes(body),
                        {"total": total},
                        time.time() + self.media.video_cache_ttl,
                    )
                finally:
                    await response.aclose()
        raise ServiceError(ErrorCode.UPSTREAM_UNAVAILABLE, "video CDN redirect limit exceeded")

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Serve only this resource; source failures abort FFmpeg and retain their stable error."""
        task = asyncio.current_task()
        assert task is not None
        self.tasks.add(task)
        try:
            header = (await reader.readuntil(b"\r\n\r\n")).decode("latin-1")
            lines = header.split("\r\n")
            method, path, _ = lines[0].split(" ")
            if path != self.path or method not in {"GET", "HEAD"}:
                writer.write(
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                await writer.drain()
                return
            headers = dict(line.split(":", 1) for line in lines[1:] if ":" in line)
            requested = next((v.strip() for k, v in headers.items() if k.lower() == "range"), None)
            start, end = 0, self.total - 1
            if requested is not None:
                match = re.fullmatch(r"bytes=(\d+)-(\d*)", requested)
                if match is None:
                    raise ServiceError(
                        ErrorCode.INVALID_ARGUMENT, "unsupported decoder Range request"
                    )
                start = int(match[1])
                end = min(int(match[2]), end) if match[2] else end
            if not 0 <= start <= end < self.total:
                writer.write(
                    (
                        "HTTP/1.1 416 Range Not Satisfiable\r\n"
                        f"Content-Range: bytes */{self.total}\r\n"
                        "Content-Length: 0\r\nConnection: close\r\n\r\n"
                    ).encode()
                )
                await writer.drain()
                return
            status = "206 Partial Content" if requested else "200 OK"
            writer.write(
                (
                    f"HTTP/1.1 {status}\r\nContent-Length: {end - start + 1}\r\n"
                    f"Content-Range: bytes {start}-{end}/{self.total}\r\n"
                    "Accept-Ranges: bytes\r\nContent-Type: video/mp4\r\nConnection: close\r\n\r\n"
                ).encode()
            )
            await writer.drain()
            if method == "GET":
                position = start
                while position <= end and not reader.at_eof():
                    body = await self.block(position // BLOCK_BYTES)
                    offset = position % BLOCK_BYTES
                    # Small writes apply backpressure before downloading another source block.
                    size = min(64 * 1024, len(body) - offset, end - position + 1)
                    writer.write(body[offset : offset + size])
                    await writer.drain()
                    position += size
        except ServiceError as error:
            self.error = error
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        except (httpx.HTTPError, TimeoutError):
            self.error = ServiceError(ErrorCode.UPSTREAM_UNAVAILABLE, "video media read failed")
        finally:
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()
            self.tasks.discard(task)


async def _bounded_pipe(reader: asyncio.StreamReader, limit: int) -> bytes:
    """Drain subprocess output without allowing image or diagnostics to grow without bounds."""
    result = bytearray()
    while chunk := await reader.read(65536):
        if len(result) + len(chunk) > limit:
            raise ServiceError(
                ErrorCode.UPSTREAM_UNAVAILABLE, "video decoder output exceeded limit"
            )
        result.extend(chunk)
    return bytes(result)


async def _stop(process: asyncio.subprocess.Process) -> None:
    """Terminate the isolated decoder group, then await it to avoid orphan processes."""

    async def discard(reader: asyncio.StreamReader) -> None:
        """Release pipe backpressure so an exited child can finish its asyncio transport."""
        while await reader.read(65536):
            pass

    drains = [
        asyncio.create_task(discard(reader))
        for reader in (process.stdout, process.stderr)
        if reader is not None
    ]
    if process.returncode is None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        except PermissionError:
            # macOS may reject group signaling after the short-lived decoder exits.
            with suppress(ProcessLookupError):
                process.terminate()
        try:
            await asyncio.wait_for(process.wait(), 1)
        except TimeoutError:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except PermissionError:
                with suppress(ProcessLookupError):
                    process.kill()
    await process.wait()
    await asyncio.gather(*drains)


@dataclass(frozen=True, slots=True)
class DecodedFrames:
    frames: tuple[BinaryCacheValue, ...]
    decoded: int
    start: float
    end: float
    complete: bool
    reason: str


async def decoder_codecs() -> frozenset[str]:
    """Check this installed FFmpeg's software decoders without decoding a remote source."""
    process = None
    try:
        process = await asyncio.create_subprocess_exec(
            "ffmpeg",
            "-hide_banner",
            "-decoders",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            start_new_session=True,
        )
        assert process.stdout is not None
        async with asyncio.timeout(3):
            body = await _bounded_pipe(process.stdout, 256 * 1024)
            await process.wait()
        names = set(re.findall(rb"^ V[^ ]*\s+(\w+)\s", body, re.MULTILINE))
        return frozenset(
            codec
            for codec, decoders in (
                ("avc", {b"h264"}),
                ("hevc", {b"hevc"}),
                ("av1", {b"av1", b"libdav1d", b"libaom-av1"}),
            )
            if names & decoders
        )
    except (OSError, TimeoutError) as error:
        raise ServiceError(
            ErrorCode.UPSTREAM_UNAVAILABLE, "video decoder is unavailable"
        ) from error
    finally:
        if process is not None:
            await _stop(process)


def _frame_difference(current: Image.Image, previous: Image.Image | None) -> float:
    """Score local visual changes; this ranks candidates without claiming semantic detection."""
    if previous is None:
        return 0.0
    difference = ImageChops.difference(current, previous).resize((16, 9), Image.Resampling.BOX)
    extrema = cast(tuple[tuple[int, int], ...], difference.getextrema())
    return max(high for _, high in extrema) / 255.0


def _inspect_frame(
    data: bytes,
    analyze: bool,
    previous: Image.Image | None,
) -> tuple[int, int, Image.Image | None, float]:
    """Decode and score a JPEG off-loop, retaining only a small analysis image."""
    with Image.open(io.BytesIO(data)) as opened:
        width, height = opened.size
        if not analyze:
            return width, height, None, 0.0
        opened.draft("RGB", (128, 72))
        small = opened.convert("RGB").resize((128, 72), Image.Resampling.BOX)
    return width, height, small, _frame_difference(small, previous)


async def decode_frames(
    media: ImageMedia,
    url: str,
    targets: tuple[float, ...],
    *,
    window_end: float | None = None,
    cache_identity: str | None = None,
) -> DecodedFrames:
    """Decode a bounded contiguous span once, streaming images and retaining finite candidates."""
    source = VideoRangeReader(media, url, cache_identity)
    process: asyncio.subprocess.Process | None = None
    diagnostics: asyncio.Task[None] | None = None
    metadata: asyncio.Queue[tuple[int, str, float] | None] = asyncio.Queue(64)
    selected: dict[int, tuple[float, BinaryCacheValue]] = {}
    previous: tuple[int, BinaryCacheValue, Image.Image, float] | None = None
    requested = targets[0]
    target_index = 0
    decoded = 0
    start = end = requested
    reason = "end_of_video"
    complete = False
    started = time.monotonic()

    async def read_metadata(reader: asyncio.StreamReader) -> None:
        """Drain bounded lines while preserving integer PTS and the stream time base."""
        origin: float | None = None
        time_base: str | None = None
        try:
            while line := await reader.readline():
                match = re.search(rb"Duration:.*?start:\s*([\d.eE+-]+)", line)
                if match:
                    origin = float(match[1])
                match = re.search(rb"config in time_base:\s*(\d+/\d+)", line)
                if match:
                    time_base = match[1].decode("ascii")
                match = re.search(rb"\bn:\s*\d+\s+pts:\s*(-?\d+)\s+pts_time:", line)
                if match:
                    await metadata.put(
                        (int(match[1]), time_base, origin)
                        if time_base is not None and origin is not None
                        else None
                    )
        finally:
            # Cancellation must never block cleanup on a producer that has filled the queue.
            with suppress(asyncio.QueueFull):
                metadata.put_nowait(None)

    def retain(index: int, value: BinaryCacheValue, score: float) -> None:
        """Keep only twelve encoded candidates, preserving chronology in the eventual output."""
        if index in selected and selected[index][0] >= score:
            return
        selected[index] = (score, value)
        if len(selected) > 12:
            lowest = min(selected, key=lambda item: (selected[item][0], -item))
            del selected[lowest]

    try:
        async with asyncio.timeout(12):
            loopback = await source.start()
            try:
                process = await asyncio.create_subprocess_exec(
                    "ffmpeg",
                    "-hide_banner",
                    "-nostdin",
                    "-loglevel",
                    "info",
                    "-threads",
                    "2",
                    "-protocol_whitelist",
                    "http,tcp",
                    "-rw_timeout",
                    "5000000",
                    "-ss",
                    str(requested),
                    "-copyts",
                    # Media tracks are MP4; auto-probing could interpret a malicious DASH playlist.
                    "-f",
                    "mov",
                    "-i",
                    loopback,
                    "-map",
                    "0:v:0",
                    "-an",
                    "-sn",
                    "-dn",
                    "-vf",
                    "showinfo,scale=w='trunc(iw*min(1,min(1920/max(iw*sar,ih),1080/min(iw*sar,ih)))*sar/2)*2':h='trunc(ih*min(1,min(1920/max(iw*sar,ih),1080/min(iw*sar,ih)))/2)*2',setsar=1",
                    "-frames:v",
                    "601" if window_end is not None or len(targets) > 1 else "1",
                    "-threads",
                    "2",
                    "-filter_threads",
                    "1",
                    "-fps_mode",
                    "passthrough",
                    "-q:v",
                    "3",
                    "-f",
                    "image2pipe",
                    "-vcodec",
                    "mjpeg",
                    "pipe:1",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    start_new_session=True,
                )
            except FileNotFoundError as error:
                raise ServiceError(
                    ErrorCode.UPSTREAM_UNAVAILABLE, "exact frame decoder is not installed"
                ) from error
            assert process.stdout is not None and process.stderr is not None
            diagnostics = asyncio.create_task(read_metadata(process.stderr))
            buffer = bytearray()
            finished = False
            while not finished:
                chunk = await process.stdout.read(65536)
                if not chunk:
                    break
                buffer.extend(chunk)
                while (boundary := buffer.find(b"\xff\xd9")) >= 0:
                    image = bytes(buffer[: boundary + 2])
                    del buffer[: boundary + 2]
                    if len(image) > OUTPUT_MAX_BYTES or not image.startswith(b"\xff\xd8"):
                        raise ServiceError(
                            ErrorCode.UPSTREAM_UNAVAILABLE, "video frame exceeds image limit"
                        )
                    stamp = await metadata.get()
                    if stamp is None:
                        raise ServiceError(
                            ErrorCode.UPSTREAM_SCHEMA_CHANGED, "decoder omitted frame timing"
                        )
                    pts, time_base, origin = stamp
                    actual = float(pts * Fraction(time_base)) - origin
                    if not math.isfinite(actual) or actual < requested - 0.001:
                        raise ServiceError(
                            ErrorCode.UPSTREAM_SCHEMA_CHANGED,
                            "decoder frame precedes requested time",
                        )
                    if window_end is not None and actual >= window_end:
                        complete, reason, end = True, "end_of_window", window_end
                        finished = True
                        break
                    if decoded >= 600:
                        reason = "frame_limit"
                        finished = True
                        break
                    if decoded == 0:
                        start = actual
                    end = actual
                    decoded += 1
                    width, height, small, score = await asyncio.to_thread(
                        _inspect_frame,
                        image,
                        window_end is not None,
                        previous[2] if previous else None,
                    )
                    value = BinaryCacheValue(
                        image,
                        {
                            "actual_time_seconds": actual,
                            "pts": str(pts),
                            "time_base": time_base,
                            "width": width,
                            "height": height,
                            "mime_type": "image/jpeg",
                            "fetched_at": format_timestamp(int(time.time())),
                        },
                        time.time() + IMAGE_TTL,
                    )
                    if window_end is not None:
                        assert small is not None
                        retain(decoded, value, max(score, previous[3] * 0.95 if previous else 0))
                        if previous is not None:
                            retain(previous[0], previous[1], score * 0.99)
                        previous = decoded, value, small, score
                    else:
                        while (
                            target_index < len(targets)
                            and actual >= targets[target_index] - 0.000001
                        ):
                            selected[target_index] = (0, value)
                            target_index += 1
                        if target_index == len(targets):
                            complete, reason, finished = True, "end_of_window", True
                            break
                if len(buffer) > OUTPUT_MAX_BYTES:
                    raise ServiceError(
                        ErrorCode.UPSTREAM_UNAVAILABLE, "decoder image buffer exceeded limit"
                    )
            if source.error is not None:
                if (
                    "budget exceeded" in source.error.message
                    or "byte limit exceeded" in source.error.message
                ):
                    reason = "media_limit"
                else:
                    raise source.error
            if not complete and reason == "end_of_video":
                await process.wait()
                if process.returncode:
                    raise ServiceError(
                        ErrorCode.UPSTREAM_UNAVAILABLE, "video decoder did not finish successfully"
                    )
                complete = window_end is not None
    except TimeoutError:
        reason = "time_limit"
    except ServiceError as error:
        if "budget exceeded" in error.message or "byte limit exceeded" in error.message:
            reason = "media_limit"
        else:
            raise
    except (httpx.HTTPError, OSError) as error:
        raise ServiceError(
            ErrorCode.UPSTREAM_UNAVAILABLE, "video media or decoder is unavailable"
        ) from error
    finally:
        if diagnostics is not None:
            diagnostics.cancel()
            await asyncio.gather(diagnostics, return_exceptions=True)
        if process is not None:
            await _stop(process)
        await source.close()
        context = current_tool_call()
        if context is not None:
            context.upstream_work.decode_ms += (time.monotonic() - started) * 1000
    if not selected and (complete or reason == "end_of_video"):
        raise ServiceError(
            ErrorCode.NOT_FOUND,
            "no source frame in requested times; widen the window or choose an earlier time",
        )
    if not selected:
        raise ServiceError(
            ErrorCode.UPSTREAM_UNAVAILABLE, "no timestamped frame within the media budget"
        )
    if window_end is None and not complete:
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT,
            "frame batch exceeds media limits; use fewer, closer timestamps",
        )
    frames = tuple(
        BinaryCacheValue(
            selected[index][1].data,
            {
                **selected[index][1].metadata,
                "source_bytes": source.downloaded,
                "source_read_bytes": source.supplied_bytes,
                "media_sends": source.sends,
            },
            selected[index][1].expires_at,
        )
        for index in sorted(selected)
    )
    return DecodedFrames(frames, decoded, start, end, complete, reason)


async def decode_frame(
    media: ImageMedia, url: str, requested: float, *, cache_identity: str | None = None
) -> BinaryCacheValue:
    """Keep the single-frame path on the same bounded decoder and timestamp handling."""
    result = await decode_frames(media, url, (requested,), cache_identity=cache_identity)
    return result.frames[0]
