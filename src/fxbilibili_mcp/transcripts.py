"""Subtitle selection, plain-text rendering, caching, and character ranges."""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass

from .cache import AsyncTTLCache, CachePolicy
from .content import normalize_space
from .models import (
    ErrorCode,
    PlaybackWindow,
    ServiceError,
    SubtitleTrack,
    TranscriptOutput,
    TranscriptTiming,
)
from .normalization import validate_decimal_id
from .ranges import TEXT_HARD_MAX_SPAN, TRANSCRIPT_MAX_SPAN, parse_range, resolve_range
from .videos import SubtitleCue, SubtitleTrackData, VideoService, require_public_playback

MAX_TIMING_SPANS = 100


@dataclass(frozen=True, slots=True)
class TranscriptCueSpan:
    """Map a half-open normalized character interval to one original subtitle cue."""

    start: int
    end: int
    start_seconds: float
    end_seconds: float


@dataclass(frozen=True, slots=True)
class RenderedTranscript:
    text: str
    spans: tuple[TranscriptCueSpan, ...]


@dataclass(frozen=True, slots=True)
class FullTranscript:
    bvid: str
    page_number: int
    cid: str
    track: SubtitleTrack
    rendered: RenderedTranscript
    duration_seconds: int = 0


class TranscriptService:
    """Convert one subtitle track into stable text before applying public ranges."""

    def __init__(
        self,
        videos: VideoService,
        cache: AsyncTTLCache,
        policy: CachePolicy,
    ) -> None:
        self._videos = videos
        self._cache = cache
        self._policy = policy

    async def get_transcript(
        self,
        bvid: str,
        page_number: int | None,
        track_id: str | None,
        range_value: str,
        *,
        include_timestamps: bool = False,
        window: PlaybackWindow | None = None,
    ) -> tuple[TranscriptOutput, bool]:
        """Reuse cue mapping for optional playback selection and bounded timestamp output."""
        parse_range(range_value, max_span=TEXT_HARD_MAX_SPAN)
        full, hit = await self.get_full_transcript(bvid, page_number, track_id)
        rendered = full.rendered
        if window is not None:
            if not 0 <= window.start_seconds < window.end_seconds <= full.duration_seconds:
                raise ServiceError(
                    ErrorCode.INVALID_ARGUMENT,
                    "window must satisfy 0 <= start_seconds < end_seconds <= video part duration",
                )
            rendered = await asyncio.to_thread(_select_window, rendered, window)
        if not rendered.text:
            # A valid silent window is a successful empty selection, not missing subtitles.
            resolve_range(
                range_value,
                total=1,
                max_span=TEXT_HARD_MAX_SPAN,
                continuation_span=TRANSCRIPT_MAX_SPAN,
            )
            return TranscriptOutput(
                bvid=full.bvid,
                page_number=full.page_number,
                cid=full.cid,
                track=full.track,
                content_range=None,
                total_characters=0,
                content_text="",
                next_range=None,
                window=window,
                timings=[] if include_timestamps else None,
            ), hit
        resolved = resolve_range(
            range_value,
            total=len(rendered.text),
            max_span=TEXT_HARD_MAX_SPAN,
            continuation_span=TRANSCRIPT_MAX_SPAN,
        )
        timings = None
        if include_timestamps:
            spans = [
                span
                for span in rendered.spans
                if span.start < resolved.end and span.end >= resolved.start
            ]
            if len(spans) > MAX_TIMING_SPANS:
                # Bound metadata too; next_range continues within this same selected text.
                resolved = resolve_range(
                    f"{resolved.start}..{spans[MAX_TIMING_SPANS - 1].end}",
                    total=len(rendered.text),
                    max_span=TEXT_HARD_MAX_SPAN,
                    continuation_span=TRANSCRIPT_MAX_SPAN,
                )
                spans = spans[:MAX_TIMING_SPANS]
            timings = [
                TranscriptTiming(
                    content_range=(
                        f"{max(span.start + 1, resolved.start)}..{min(span.end, resolved.end)}"
                    ),
                    start_seconds=span.start_seconds,
                    end_seconds=span.end_seconds,
                )
                for span in spans
            ]
        chunk = rendered.text[resolved.python_slice]
        return (
            TranscriptOutput(
                bvid=full.bvid,
                page_number=full.page_number,
                cid=full.cid,
                track=full.track,
                content_range=resolved.resolved,
                total_characters=len(rendered.text),
                content_text=chunk,
                next_range=resolved.next_range,
                window=window,
                timings=timings,
            ),
            hit,
        )

    async def get_full_transcript(
        self,
        bvid: str,
        page_number: int | None,
        track_id: str | None,
    ) -> tuple[FullTranscript, bool]:
        """Recheck inventory and cache normalized text with its original cue positions."""
        core, core_hit = await self._videos.get_core(bvid)
        require_public_playback(core)
        if len(core.pages) > 1 and page_number is None:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT,
                "page_number is required for a multi-page video",
            )
        resolved_page = 1 if page_number is None else page_number
        if not 1 <= resolved_page <= len(core.pages):
            raise ServiceError(ErrorCode.INVALID_ARGUMENT, "page_number is outside the video")
        page = core.pages[resolved_page - 1]
        inventory, inventory_hit = await self._videos.get_subtitle_inventory(core, page)
        selected = _select_track(inventory.tracks, track_id)
        content_hash = _cue_hash(selected.cues)
        rendered, text_hit = await self._cache.get_or_create(
            ("transcript", bvid, page.cid, selected.public.track_id, content_hash, "mapped-v1"),
            self._policy.transcript,
            lambda: _render_async(selected.cues),
        )
        if not rendered.text:
            raise ServiceError(ErrorCode.NOT_FOUND, "the selected subtitle track has no text")
        return (
            FullTranscript(
                bvid, resolved_page, page.cid, selected.public, rendered, page.duration_seconds
            ),
            core_hit and inventory_hit and text_hit,
        )


def render_transcript(cues: tuple[SubtitleCue, ...]) -> str:
    """Join adjacent cues with spaces and preserve pauses longer than five seconds."""
    return render_transcript_with_mapping(cues).text


def render_transcript_with_mapping(cues: tuple[SubtitleCue, ...]) -> RenderedTranscript:
    """Keep the existing text rules while recording only cue-owned character spans."""
    parts: list[str] = []
    spans: list[TranscriptCueSpan] = []
    previous_end: float | None = None
    position = 0
    for cue in cues:
        content = normalize_space(cue.content)
        if not content:
            continue
        if parts:
            parts.append("\n" if previous_end is not None and cue.start - previous_end > 5 else " ")
            position += 1
        parts.append(content)
        spans.append(TranscriptCueSpan(position, position + len(content), cue.start, cue.end))
        position += len(content)
        previous_end = cue.end
    return RenderedTranscript("".join(parts), tuple(spans))


async def _render_async(cues: tuple[SubtitleCue, ...]) -> RenderedTranscript:
    """Normalize potentially large subtitle bodies outside the event loop."""
    return await asyncio.to_thread(render_transcript_with_mapping, cues)


def _select_window(rendered: RenderedTranscript, window: PlaybackWindow) -> RenderedTranscript:
    """Rebuild selected text positions without interpolating original cue timestamps."""
    return render_transcript_with_mapping(
        tuple(
            SubtitleCue(span.start_seconds, span.end_seconds, rendered.text[span.start : span.end])
            for span in rendered.spans
            if span.start_seconds < window.end_seconds and span.end_seconds > window.start_seconds
        )
    )


def _select_track(
    tracks: tuple[SubtitleTrackData, ...],
    track_id: str | None,
) -> SubtitleTrackData:
    """Honor an explicit track or select the first valid non-translation track."""
    if not tracks:
        raise ServiceError(
            ErrorCode.NOT_FOUND,
            "the selected video page has no available subtitle tracks",
        )
    if track_id is not None:
        validate_decimal_id(track_id, "track_id")
        for track in tracks:
            if track.public.track_id == track_id:
                return track
        raise ServiceError(ErrorCode.NOT_FOUND, "track_id does not exist on the selected page")
    for track in tracks:
        if track.public.ai_kind != "translation" and track.public.line_count > 3:
            return track
    raise ServiceError(
        ErrorCode.INVALID_ARGUMENT,
        "no default subtitle track is available; call get_video with "
        "subtitle='all' and choose track_id",
    )


def _cue_hash(cues: tuple[SubtitleCue, ...]) -> str:
    """Include exact cue times so sub-millisecond changes cannot reuse stale mappings."""
    digest = hashlib.sha256()
    for cue in cues:
        digest.update(f"{cue.start!r}|{cue.end!r}|{cue.content}\n".encode())
    return digest.hexdigest()[:20]
