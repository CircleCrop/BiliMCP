"""Filter and sample time-bounded danmaku from Bilibili's segmented protobuf API."""

from __future__ import annotations

import asyncio
import math
import re
import unicodedata
from collections import Counter, defaultdict, deque
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from functools import partial
from typing import Literal, cast

import re2  # type: ignore[import-untyped]

from .cache import AsyncTTLCache, CachePolicy
from .content import normalize_space, truncate_summary
from .danmaku_proto import (
    ParsedDanmaku,
    parse_danmaku_segment,
)
from .models import (
    DanmakuKind,
    DanmakuKindCount,
    DanmakuListOutput,
    DanmakuMoment,
    DanmakuReaction,
    DanmakuSample,
    DanmakuSampling,
    ErrorCode,
    ServiceError,
)
from .transport import BilibiliTransport
from .videos import VideoService

_TIME_RANGE_RE = re.compile(r"^(0|[1-9][0-9]*)\.\.(0|[1-9][0-9]*)$")
_SENDER_HASH_RE = re.compile(r"^[0-9a-f]{1,8}$")
_REPEAT_RE = re.compile(r"(.)\1{2,}", re.DOTALL)
_WINDOW_MAX_SECONDS = 300
_SEGMENT_SECONDS = 360
_BUCKET_SECONDS: Literal[60] = 60
_NEAR_DUPLICATE_SECONDS = 5
_TEXT_LIMIT = 160
_PATTERN_LIMIT = 200
_SENDER_FILTER_LIMIT = 20
_SAMPLE_LIMIT_MAX = 50
_TOP_REACTION_LIMIT = 8
_KIND_ORDER: tuple[DanmakuKind, ...] = (
    "scrolling",
    "bottom",
    "top",
    "reverse",
    "advanced",
    "code",
    "special",
)
_KIND_INDEX = {kind: index for index, kind in enumerate(_KIND_ORDER)}
_KNOWN_KINDS = frozenset(_KIND_ORDER)
_DEFAULT_KINDS: frozenset[DanmakuKind] = frozenset({"scrolling", "bottom", "top", "reverse"})
_ORDINARY_KINDS: frozenset[DanmakuKind] = frozenset({"scrolling", "reverse"})
type _RegexSearch = Callable[[str], object | None]
_RawDanmaku = ParsedDanmaku
_parse_danmaku_segment = parse_danmaku_segment


@dataclass(frozen=True, slots=True)
class _ParsedTimeRange:
    start: int
    end: int

    @property
    def span(self) -> int:
        return self.end - self.start + 1


@dataclass(frozen=True, slots=True)
class _ResolvedTimeRange:
    requested: str
    resolved: str
    start: int
    end: int
    next_range: str | None


@dataclass(slots=True)
class _ReactionAggregate:
    representative: _RawDanmaku
    count: int = 1
    sender_hashes: set[str] = field(default_factory=set)
    last_at_seconds: int = 0


class DanmakuService:
    """Read cached segments, apply agent filters, and return bounded temporal samples."""

    def __init__(
        self,
        transport: BilibiliTransport,
        videos: VideoService,
        cache: AsyncTTLCache,
        policy: CachePolicy,
    ) -> None:
        """Bind transport, video identity, and the unfiltered segment cache."""
        self._transport = transport
        self._videos = videos
        self._cache = cache
        self._policy = policy

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
        """Fetch intersecting segments and apply all filters after the raw cache boundary."""
        parsed = _parse_time_range(time_range)
        include = _compile_pattern(include_pattern, "include_pattern")
        exclude = _compile_pattern(exclude_pattern, "exclude_pattern")
        selected_kinds = _validate_kinds(kinds)
        selected_senders = _validate_sender_hashes(sender_hashes)
        _validate_sampling(sampling, sample_limit)

        core, core_hit = await self._videos.get_core(bvid)
        if len(core.pages) > 1 and page_number is None:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT,
                "page_number is required for a multi-page video",
            )
        resolved_page = 1 if page_number is None else page_number
        if not 1 <= resolved_page <= len(core.pages):
            raise ServiceError(ErrorCode.INVALID_ARGUMENT, "page_number is outside the video")
        page = core.pages[resolved_page - 1]
        resolved = _resolve_time_range(parsed, time_range, page.duration_seconds)
        # A 300-second window intersects at most two bounded six-minute segments.
        segment_numbers = range(
            resolved.start // _SEGMENT_SECONDS + 1,
            resolved.end // _SEGMENT_SECONDS + 2,
        )
        segments = await asyncio.gather(
            *(
                self._cache.get_or_create(
                    ("danmaku_segment", bvid, page.cid, segment_number, "v5"),
                    self._policy.danmaku,
                    partial(self._load_segment, core.aid, page.cid, segment_number),
                )
                for segment_number in segment_numbers
            )
        )
        source_rows = tuple(
            row
            for segment, _ in segments
            for row in segment
            if resolved.start <= row.at_seconds <= resolved.end
        )
        matched_rows = _filter_rows(
            source_rows,
            include=include,
            exclude=exclude,
            kinds=selected_kinds,
            sender_hashes=selected_senders,
        )
        output = _build_output(
            bvid=bvid,
            page_number=resolved_page,
            cid=page.cid,
            duration_seconds=page.duration_seconds,
            resolved=resolved,
            source_count=len(source_rows),
            rows=matched_rows,
            sampling=sampling,
            sample_limit=sample_limit,
        )
        return output, core_hit and all(cache_hit for _, cache_hit in segments)

    async def _load_segment(
        self,
        aid: int,
        cid: str,
        segment_number: int,
    ) -> tuple[ParsedDanmaku, ...]:
        """Fetch and parse one current WBI-signed six-minute protobuf segment."""
        payload = await self._transport.request_bytes(
            "/x/v2/dm/wbi/web/seg.so",
            {
                "type": 1,
                "oid": int(cid),
                "pid": aid,
                "segment_index": segment_number,
            },
            wbi=True,
        )
        # Parsing can scan tens of thousands of records; keep it off the HTTP event loop.
        return await asyncio.to_thread(parse_danmaku_segment, payload)


def _parse_time_range(value: str) -> _ParsedTimeRange:
    """Parse zero-based inclusive playback seconds without accepting corrected forms."""
    match = _TIME_RANGE_RE.fullmatch(value)
    if match is None:
        raise ServiceError(
            ErrorCode.INVALID_RANGE,
            "time_range must use zero-based inclusive seconds such as 0..299",
        )
    start, end = (int(part) for part in match.groups())
    if start > end:
        raise ServiceError(ErrorCode.INVALID_RANGE, "time_range start must not exceed its end")
    parsed = _ParsedTimeRange(start, end)
    if parsed.span > _WINDOW_MAX_SECONDS:
        raise ServiceError(
            ErrorCode.INVALID_RANGE,
            "time_range may cover at most 300 seconds; use next_time_range to continue",
        )
    return parsed


def _resolve_time_range(
    parsed: _ParsedTimeRange,
    requested: str,
    duration_seconds: int,
) -> _ResolvedTimeRange:
    """Clip only the final legal window and calculate a directly reusable continuation."""
    if duration_seconds <= 0:
        raise ServiceError(ErrorCode.NOT_FOUND, "this video part has no readable playback window")
    if parsed.start >= duration_seconds:
        raise ServiceError(
            ErrorCode.INVALID_RANGE,
            "time_range starts at or beyond the video part duration",
        )
    end = min(parsed.end, duration_seconds - 1)
    next_start = end + 1
    next_range = (
        None
        if next_start >= duration_seconds
        else f"{next_start}..{min(next_start + parsed.span - 1, duration_seconds - 1)}"
    )
    return _ResolvedTimeRange(
        requested=requested,
        resolved=f"{parsed.start}..{end}",
        start=parsed.start,
        end=end,
        next_range=next_range,
    )


def _compile_pattern(value: str | None, field_name: str) -> _RegexSearch | None:
    """Compile one bounded RE2 expression and hide provider-specific parser details."""
    if value is None:
        return None
    if not value or len(value) > _PATTERN_LIMIT:
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT,
            f"{field_name} must contain between 1 and 200 characters",
        )
    try:
        compiled = re2.compile(value)
    except re2.error as exc:
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT,
            f"{field_name} is not a supported regular expression",
        ) from exc
    return cast(_RegexSearch, compiled.search)


def _validate_kinds(kinds: list[DanmakuKind] | None) -> frozenset[DanmakuKind]:
    """Normalize the optional public kind list while rejecting empty or oversized calls."""
    if kinds is None:
        return _DEFAULT_KINDS
    if (
        not kinds
        or len(kinds) > len(_KIND_ORDER)
        or any(kind not in _KNOWN_KINDS for kind in kinds)
    ):
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT,
            "kinds must contain between 1 and 7 supported danmaku kinds",
        )
    return frozenset(kinds)


def _validate_sender_hashes(sender_hashes: list[str] | None) -> frozenset[str] | None:
    """Validate stable provider sender summaries without attempting UID reversal."""
    if sender_hashes is None:
        return None
    if not sender_hashes or len(sender_hashes) > _SENDER_FILTER_LIMIT:
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT,
            "sender_hashes must contain between 1 and 20 values",
        )
    if any(_SENDER_HASH_RE.fullmatch(value) is None for value in sender_hashes):
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT,
            "each sender_hash must be 1 to 8 lowercase hexadecimal characters",
        )
    return frozenset(sender_hashes)


def _validate_sampling(sampling: DanmakuSampling, sample_limit: int) -> None:
    """Keep direct service calls within the same bounds as the MCP schema."""
    if sampling not in {"representative", "chronological"}:
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT,
            "sampling must be representative or chronological",
        )
    if not 1 <= sample_limit <= _SAMPLE_LIMIT_MAX:
        raise ServiceError(ErrorCode.INVALID_ARGUMENT, "sample_limit must be between 1 and 50")


def _filter_rows(
    rows: tuple[_RawDanmaku, ...],
    *,
    include: _RegexSearch | None,
    exclude: _RegexSearch | None,
    kinds: frozenset[DanmakuKind],
    sender_hashes: frozenset[str] | None,
) -> tuple[_RawDanmaku, ...]:
    """Apply type, sender, include, and exclude filters in a predictable order."""
    matched: list[_RawDanmaku] = []
    for row in rows:
        if row.kind not in kinds:
            continue
        if sender_hashes is not None and row.sender_hash not in sender_hashes:
            continue
        if include is not None and include(row.text) is None:
            continue
        if exclude is not None and exclude(row.text) is not None:
            continue
        matched.append(row)
    return tuple(matched)


def _build_output(
    *,
    bvid: str,
    page_number: int,
    cid: str,
    duration_seconds: int,
    resolved: _ResolvedTimeRange,
    source_count: int,
    rows: tuple[_RawDanmaku, ...],
    sampling: DanmakuSampling,
    sample_limit: int,
) -> DanmakuListOutput:
    """Build density, reaction aggregates, and one bounded deterministic sample list."""
    ordered = tuple(sorted(rows, key=_chronological_key))
    reaction_keys = {row.text: _reaction_key(row.text) for row in ordered}
    samples = _select_samples(ordered, resolved, sampling, sample_limit, reaction_keys)
    unique_text_count = len(set(reaction_keys.values()))
    return DanmakuListOutput(
        bvid=bvid,
        page_number=page_number,
        cid=cid,
        duration_seconds=duration_seconds,
        requested_time_range=resolved.requested,
        resolved_time_range=resolved.resolved,
        bucket_seconds=_BUCKET_SECONDS,
        sampling=sampling,
        source_count=source_count,
        matched_count=len(ordered),
        unique_text_count=unique_text_count,
        sampled_count=len(samples),
        moments=_build_moments(ordered, resolved),
        top_reactions=_top_reactions(ordered, reaction_keys),
        samples=[_public_sample(row) for row in samples],
        next_time_range=resolved.next_range,
    )


def _build_moments(
    rows: tuple[_RawDanmaku, ...],
    resolved: _ResolvedTimeRange,
) -> list[DanmakuMoment]:
    """Report all matched density in stable one-minute playback buckets."""
    bucket_count = math.ceil((resolved.end - resolved.start + 1) / _BUCKET_SECONDS)
    totals = [0] * bucket_count
    kind_counts: list[Counter[DanmakuKind]] = [Counter() for _ in range(bucket_count)]
    for row in rows:
        index = (row.at_seconds - resolved.start) // _BUCKET_SECONDS
        totals[index] += 1
        kind_counts[index][row.kind] += 1

    moments: list[DanmakuMoment] = []
    for index, start in enumerate(range(resolved.start, resolved.end + 1, _BUCKET_SECONDS)):
        end = min(resolved.end, start + _BUCKET_SECONDS - 1)
        moments.append(
            DanmakuMoment(
                start_seconds=start,
                end_seconds=end,
                count=totals[index],
                kind_counts=[
                    DanmakuKindCount(kind=kind, count=kind_counts[index][kind])
                    for kind in _KIND_ORDER
                    if kind_counts[index][kind]
                ],
            )
        )
    return moments


def _top_reactions(
    rows: tuple[_RawDanmaku, ...],
    reaction_keys: Mapping[str, str],
) -> list[DanmakuReaction]:
    """Aggregate repeated scrolling reactions without discarding punctuation-only text."""
    grouped: dict[str, _ReactionAggregate] = {}
    for row in rows:
        if row.kind not in _ORDINARY_KINDS:
            continue
        key = reaction_keys[row.text]
        aggregate = grouped.get(key)
        if aggregate is None:
            grouped[key] = _ReactionAggregate(
                representative=row,
                sender_hashes={row.sender_hash} if row.sender_hash is not None else set(),
                last_at_seconds=row.at_seconds,
            )
            continue
        aggregate.count += 1
        aggregate.last_at_seconds = row.at_seconds
        if row.sender_hash is not None:
            aggregate.sender_hashes.add(row.sender_hash)
    ranked = sorted(
        (aggregate for aggregate in grouped.values() if aggregate.count >= 2),
        key=lambda aggregate: (
            -aggregate.count,
            aggregate.representative.at_seconds,
            aggregate.representative.text,
        ),
    )
    reactions: list[DanmakuReaction] = []
    for aggregate in ranked[:_TOP_REACTION_LIMIT]:
        display, truncated = truncate_summary(aggregate.representative.text, _TEXT_LIMIT)
        reactions.append(
            DanmakuReaction(
                text=display,
                count=aggregate.count,
                sender_count=len(aggregate.sender_hashes),
                first_at_seconds=aggregate.representative.at_seconds,
                last_at_seconds=aggregate.last_at_seconds,
                content_truncated=True if truncated else None,
            )
        )
    return reactions


def _select_samples(
    rows: tuple[_RawDanmaku, ...],
    resolved: _ResolvedTimeRange,
    sampling: DanmakuSampling,
    limit: int,
    reaction_keys: Mapping[str, str],
) -> tuple[_RawDanmaku, ...]:
    """Select chronological or time-stratified samples after near-duplicate removal."""
    candidates = _near_deduplicate(rows, reaction_keys)
    if sampling == "chronological":
        return candidates[:limit]

    phrase_counts = Counter((row.kind, reaction_keys[row.text]) for row in rows)
    ordinary = _collapse_representative_candidates(
        (row for row in candidates if row.kind in _ORDINARY_KINDS),
        resolved,
        phrase_counts,
        reaction_keys,
        keep_per_bucket=False,
    )
    functional = _collapse_representative_candidates(
        (row for row in candidates if row.kind not in _ORDINARY_KINDS),
        resolved,
        phrase_counts,
        reaction_keys,
        keep_per_bucket=True,
    )
    functional_quota = limit if not ordinary else min(len(functional), max(1, math.ceil(limit / 3)))
    selected = _stratified_select(
        functional, resolved, functional_quota, phrase_counts, reaction_keys
    )
    selected.extend(
        _stratified_select(
            ordinary,
            resolved,
            limit - len(selected),
            phrase_counts,
            reaction_keys,
        )
    )
    if len(selected) < limit:
        chosen = set(selected)
        remainder = tuple(row for row in (*functional, *ordinary) if row not in chosen)
        selected.extend(
            _stratified_select(
                remainder,
                resolved,
                limit - len(selected),
                phrase_counts,
                reaction_keys,
            )
        )
    return tuple(sorted(selected, key=_chronological_key))


def _near_deduplicate(
    rows: tuple[_RawDanmaku, ...],
    reaction_keys: Mapping[str, str],
) -> tuple[_RawDanmaku, ...]:
    """Collapse only nearby repeats from the same sender, kind, and normalized text."""
    last_seen: dict[tuple[str | None, DanmakuKind, str], int] = {}
    selected: list[_RawDanmaku] = []
    for row in rows:
        key = (row.sender_hash, row.kind, reaction_keys[row.text])
        previous = last_seen.get(key)
        if previous is not None and row.at_seconds - previous <= _NEAR_DUPLICATE_SECONDS:
            last_seen[key] = row.at_seconds
            continue
        last_seen[key] = row.at_seconds
        selected.append(row)
    return tuple(selected)


def _collapse_representative_candidates(
    rows: Iterable[_RawDanmaku],
    resolved: _ResolvedTimeRange,
    phrase_counts: Counter[tuple[DanmakuKind, str]],
    reaction_keys: Mapping[str, str],
    *,
    keep_per_bucket: bool,
) -> tuple[_RawDanmaku, ...]:
    """Keep one strongest candidate per phrase globally or per functional minute."""
    selected: dict[tuple[object, ...], _RawDanmaku] = {}
    for row in rows:
        bucket = (row.at_seconds - resolved.start) // _BUCKET_SECONDS
        key: tuple[object, ...] = (
            (bucket, row.kind, reaction_keys[row.text])
            if keep_per_bucket
            else (row.kind, reaction_keys[row.text])
        )
        existing = selected.get(key)
        if existing is None or _sample_rank(row, phrase_counts, reaction_keys) < _sample_rank(
            existing, phrase_counts, reaction_keys
        ):
            selected[key] = row
    return tuple(selected.values())


def _stratified_select(
    rows: tuple[_RawDanmaku, ...],
    resolved: _ResolvedTimeRange,
    limit: int,
    phrase_counts: Counter[tuple[DanmakuKind, str]],
    reaction_keys: Mapping[str, str],
) -> list[_RawDanmaku]:
    """Reserve present kinds, then spread remaining ranked samples over playback minutes."""
    if limit <= 0:
        return []
    selected, available = _reserve_kind_samples(rows, limit, phrase_counts, reaction_keys)
    buckets = _ranked_minute_buckets(available, resolved, phrase_counts, reaction_keys)
    while buckets and len(selected) < limit:
        bucket_order = sorted(buckets)
        remaining = limit - len(selected)
        if remaining < len(bucket_order):
            bucket_order = _evenly_spaced(bucket_order, remaining)
        for bucket in bucket_order:
            selected.append(buckets[bucket].popleft())
            if not buckets[bucket]:
                del buckets[bucket]
            if len(selected) == limit:
                break
    return selected


def _reserve_kind_samples(
    rows: tuple[_RawDanmaku, ...],
    limit: int,
    phrase_counts: Counter[tuple[DanmakuKind, str]],
    reaction_keys: Mapping[str, str],
) -> tuple[list[_RawDanmaku], tuple[_RawDanmaku, ...]]:
    """Find the strongest candidate for each present kind in one pass."""
    best_by_kind: dict[DanmakuKind, _RawDanmaku] = {}
    for row in rows:
        existing = best_by_kind.get(row.kind)
        if existing is None or _sample_rank(row, phrase_counts, reaction_keys) < _sample_rank(
            existing, phrase_counts, reaction_keys
        ):
            best_by_kind[row.kind] = row
    selected = [best_by_kind[kind] for kind in _KIND_ORDER if kind in best_by_kind]
    if len(selected) > limit:
        return [], rows
    reserved = set(selected)
    return selected, tuple(row for row in rows if row not in reserved)


def _ranked_minute_buckets(
    rows: tuple[_RawDanmaku, ...],
    resolved: _ResolvedTimeRange,
    phrase_counts: Counter[tuple[DanmakuKind, str]],
    reaction_keys: Mapping[str, str],
) -> dict[int, deque[_RawDanmaku]]:
    """Group remaining candidates by minute and rank each group once."""
    buckets: dict[int, deque[_RawDanmaku]] = {}
    grouped: dict[int, list[_RawDanmaku]] = defaultdict(list)
    for row in rows:
        grouped[(row.at_seconds - resolved.start) // _BUCKET_SECONDS].append(row)
    for bucket, candidates in grouped.items():
        buckets[bucket] = deque(
            sorted(
                candidates,
                key=lambda row: _sample_rank(row, phrase_counts, reaction_keys),
            )
        )
    return buckets


def _evenly_spaced(values: list[int], limit: int) -> list[int]:
    """Choose stable positions across an ordered list, including both ends when possible."""
    if limit <= 0:
        return []
    if limit == 1:
        return [values[len(values) // 2]]
    last = len(values) - 1
    return [values[round(index * last / (limit - 1))] for index in range(limit)]


def _sample_rank(
    row: _RawDanmaku,
    phrase_counts: Counter[tuple[DanmakuKind, str]],
    reaction_keys: Mapping[str, str],
) -> tuple[int, int, int, str, str]:
    """Prefer provider weight and repeated relevance while keeping ties deterministic."""
    return (
        -row.weight,
        -phrase_counts[(row.kind, reaction_keys[row.text])],
        row.at_seconds,
        row.text,
        row.sender_hash or "",
    )


def _chronological_key(row: _RawDanmaku) -> tuple[int, int, str, str]:
    """Provide one stable ordering for temporal samples and aggregation representatives."""
    return (
        row.at_seconds,
        _KIND_INDEX[row.kind],
        row.text,
        row.sender_hash or "",
    )


def _public_sample(row: _RawDanmaku) -> DanmakuSample:
    """Bound public text while retaining the stable provider sender summary."""
    display, truncated = truncate_summary(row.text, _TEXT_LIMIT)
    return DanmakuSample(
        at_seconds=row.at_seconds,
        text=display,
        kind=row.kind,
        sender_hash=row.sender_hash,
        content_truncated=True if truncated else None,
    )


def _reaction_key(text: str) -> str:
    """Normalize width, case, whitespace, and long reaction runs for grouping only."""
    normalized = normalize_space(unicodedata.normalize("NFKC", text).casefold())
    return _REPEAT_RE.sub(r"\1\1", normalized)
