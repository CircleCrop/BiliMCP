"""Bounded literal search over one selected subtitle track or normalized Opus body."""

from __future__ import annotations

import asyncio
import hashlib
import math
import time
import unicodedata
from array import array
from bisect import bisect_left
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Literal

from pydantic import Field

from .cache import AsyncTTLCache, CachePolicy
from .models import ErrorCode, ServiceError, StrictModel
from .normalization import TAIPEI
from .opuses import OpusService
from .ranges import parse_range, resolve_range
from .transcripts import TranscriptCueSpan, TranscriptService
from .transport import BilibiliTransport, BusinessBudgetExhausted

MAX_CONTENT_CHARACTERS = 1_000_000
SCAN_TIMEOUT_SECONDS = 2.0
_NORMALIZATION_VERSION = "literal-casefold-v1-transcript-v1-opus-markdown-v5"
_RESTART = (
    "content or search results changed or expired; "
    "restart with range='1..10' and no content_version"
)


class TranscriptContentTarget(StrictModel):
    kind: Literal["video_transcript"]
    bvid: str = Field(pattern=r"^BV[0-9A-Za-z]{10}$")
    page_number: int | None = Field(default=None, ge=1)
    track_id: str | None = Field(default=None, pattern=r"^[1-9][0-9]*$")


class OpusContentTarget(StrictModel):
    kind: Literal["opus"]
    opus_id: str = Field(pattern=r"^[1-9][0-9]*$")


ContentSearchTarget = Annotated[
    TranscriptContentTarget | OpusContentTarget, Field(discriminator="kind")
]


class ContentMatch(StrictModel):
    excerpt: str = Field(max_length=300)
    content_range: str = Field(
        pattern=r"^[1-9][0-9]*\.\.[1-9][0-9]*$",
        description="1-based inclusive match positions in the normalized full text",
    )
    start_seconds: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    end_seconds: float | None = Field(default=None, ge=0, allow_inf_nan=False)


class ContentSearchOutput(StrictModel):
    schema_version: Literal[1] = 1
    target: ContentSearchTarget
    coverage: Literal["selected_subtitle_track", "opus_body"]
    content_version: str = Field(pattern=r"^[0-9a-f]{64}$")
    total_matches: int = Field(ge=0)
    matches: list[ContentMatch] = Field(max_length=10)
    next_range: str | None
    fetched_at: str


@dataclass(frozen=True, slots=True)
class _Corpus:
    target: ContentSearchTarget
    text: str
    spans: tuple[TranscriptCueSpan, ...] = ()
    # CID and track metadata distinguish tracks with identical visible text.
    source_identity: str = ""


@dataclass(frozen=True, slots=True)
class _MatchIndex:
    starts: array[int]
    ends: array[int]
    fetched_at: str


class ContentSearchService:
    """Read current normalized content, then reuse bounded version-bound match indices."""

    def __init__(
        self,
        transport: BilibiliTransport,
        transcripts: TranscriptService,
        opuses: OpusService,
        cache: AsyncTTLCache,
        policy: CachePolicy,
    ) -> None:
        """Share normalized content services and the bounded match-index cache."""
        self._transport = transport
        self._transcripts = transcripts
        self._opuses = opuses
        self._cache = cache
        self._policy = policy

    async def search_content(
        self,
        target: ContentSearchTarget,
        query: str,
        range_value: str = "1..10",
        case_sensitive: bool = False,
        content_version: str | None = None,
    ) -> tuple[ContentSearchOutput, bool]:
        """Search one resource with six actual sends and strict text, time, and output limits."""
        _validate_query(query)
        requested = parse_range(range_value, max_span=10)
        if requested.start > 1 and content_version is None:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT, "content_version is required to continue"
            )
        try:
            async with self._transport.scoped_work(6):
                corpus, source_hit = await self._load(target)
        except BusinessBudgetExhausted as exc:
            raise ServiceError(
                ErrorCode.RATE_LIMITED, "content search exceeded its upstream work budget"
            ) from exc
        if len(corpus.text) > MAX_CONTENT_CHARACTERS:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT,
                "content search supports at most 1000000 normalized characters",
            )
        version = await asyncio.to_thread(_content_version, corpus)
        if content_version is not None and version != content_version:
            raise ServiceError(ErrorCode.INVALID_ARGUMENT, _RESTART)
        key = (
            "content-search",
            self._transport.account_uid,
            version,
            query,
            case_sensitive,
            _NORMALIZATION_VERSION,
        )

        async def build_index() -> _MatchIndex:
            """Refuse stale continuations instead of silently rebuilding removed search state."""
            if content_version is not None:
                raise ServiceError(ErrorCode.INVALID_ARGUMENT, _RESTART)
            try:
                async with asyncio.timeout(SCAN_TIMEOUT_SECONDS):
                    return await asyncio.to_thread(_scan, corpus.text, query, case_sensitive)
            except TimeoutError as exc:
                raise ServiceError(
                    ErrorCode.RATE_LIMITED, "content search exceeded its local scan time limit"
                ) from exc

        index, index_hit = await self._cache.get_or_create(key, self._policy.search, build_index)
        total = len(index.starts)
        matches: list[ContentMatch] = []
        next_range: str | None = None
        if total:
            resolved = resolve_range(range_value, total=total, max_span=10)
            matches = [
                _build_match(corpus, index.starts[i], index.ends[i])
                for i in range(resolved.start - 1, resolved.end)
            ]
            next_range = resolved.next_range
        elif requested.start != 1:
            raise ServiceError(ErrorCode.INVALID_RANGE, "range starts beyond the known match count")
        return (
            ContentSearchOutput(
                target=corpus.target,
                coverage=(
                    "selected_subtitle_track"
                    if isinstance(corpus.target, TranscriptContentTarget)
                    else "opus_body"
                ),
                content_version=version,
                total_matches=total,
                matches=matches,
                next_range=next_range,
                fetched_at=index.fetched_at,
            ),
            source_hit and index_hit,
        )

    async def _load(self, target: ContentSearchTarget) -> tuple[_Corpus, bool]:
        """Reuse full-body services so chunks never cause repeated upstream downloads."""
        if isinstance(target, TranscriptContentTarget):
            full, hit = await self._transcripts.get_full_transcript(
                target.bvid, target.page_number, target.track_id
            )
            resolved = TranscriptContentTarget(
                kind="video_transcript",
                bvid=full.bvid,
                page_number=full.page_number,
                track_id=full.track.track_id,
            )
            return (
                _Corpus(
                    resolved,
                    full.rendered.text,
                    full.rendered.spans,
                    f"{full.cid}:{full.track.model_dump_json()}",
                ),
                hit,
            )
        opus, hit = await self._opuses.get_full(target.opus_id, resolve_forward=True)
        markdown = opus.content_markdown
        return (
            _Corpus(OpusContentTarget(kind="opus", opus_id=opus.opus_id), markdown),
            hit,
        )


def _validate_query(query: str) -> None:
    """Preserve literal spacing while rejecting empty or unsupported search characters."""
    if not 1 <= len(query) <= 100 or not query.strip():
        raise ServiceError(ErrorCode.INVALID_ARGUMENT, "query must contain 1..100 characters")
    if any(unicodedata.category(char) in {"Cc", "Cf", "Cs"} for char in query):
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT, "query contains unsupported control or format characters"
        )


def _content_version(corpus: _Corpus) -> str:
    """Bind positions to text, resolved resource, selected track, timing, and rendering rules."""
    digest = hashlib.sha256()
    for value in (
        _NORMALIZATION_VERSION,
        corpus.target.model_dump_json(),
        corpus.source_identity,
        corpus.text,
    ):
        encoded = value.encode()
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    for span in corpus.spans:
        digest.update(
            f"{span.start}:{span.end}:{span.start_seconds!r}:{span.end_seconds!r};".encode()
        )
    return digest.hexdigest()


def _scan(text: str, query: str, case_sensitive: bool) -> _MatchIndex:
    """Find non-overlapping original character spans with bounded casefold mapping storage."""
    deadline = time.monotonic() + SCAN_TIMEOUT_SECONDS
    positions = array("I")
    haystack = text
    needle = query
    if not case_sensitive:
        folded: list[str] = []
        for index, char in enumerate(text):
            if index % 4096 == 0 and time.monotonic() >= deadline:
                raise TimeoutError
            value = char.casefold()
            folded.append(value)
            positions.extend([index] * len(value))
        haystack = "".join(folded)
        needle = query.casefold()
    starts = array("I")
    ends = array("I")
    offset = 0
    while offset < len(haystack):
        if len(starts) % 1024 == 0 and time.monotonic() >= deadline:
            raise TimeoutError
        found = haystack.find(needle, offset)
        if found < 0:
            break
        folded_end = found + len(needle)
        start = found if case_sensitive else positions[found]
        end = folded_end if case_sensitive else positions[folded_end - 1] + 1
        starts.append(start)
        ends.append(end)
        # A casefold expansion such as ß -> ss must not yield two overlapping original hits.
        offset = folded_end if case_sensitive else bisect_left(positions, end, lo=folded_end)
    if time.monotonic() >= deadline:
        raise TimeoutError
    return _MatchIndex(starts, ends, datetime.now(TAIPEI).isoformat(timespec="seconds"))


def _build_match(corpus: _Corpus, start: int, end: int) -> ContentMatch:
    """Provide bounded context and actual cue boundaries for the exact original match."""
    excerpt_start = max(0, start - (300 - (end - start)) // 2)
    excerpt_end = min(len(corpus.text), excerpt_start + 300)
    excerpt_start = max(0, excerpt_end - 300)
    start_seconds: float | None = None
    end_seconds: float | None = None
    if corpus.spans:
        first = bisect_left(corpus.spans, start + 1, key=lambda span: span.end)
        last = bisect_left(corpus.spans, end, key=lambda span: span.start) - 1
        if first <= last:
            intersecting = corpus.spans[first : last + 1]
            if any(
                not math.isfinite(span.start_seconds)
                or not math.isfinite(span.end_seconds)
                or span.start_seconds < 0
                or span.end_seconds < span.start_seconds
                for span in intersecting
            ):
                raise ServiceError(ErrorCode.UPSTREAM_SCHEMA_CHANGED, "invalid subtitle cue times")
            # Overlapping cues can end out of order; cover all actual intersected cue times.
            start_seconds = min(span.start_seconds for span in intersecting)
            end_seconds = max(span.end_seconds for span in intersecting)
    return ContentMatch(
        excerpt=corpus.text[excerpt_start:excerpt_end],
        content_range=f"{start + 1}..{end}",
        start_seconds=start_seconds,
        end_seconds=end_seconds,
    )
