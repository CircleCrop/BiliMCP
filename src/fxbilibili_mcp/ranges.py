"""Deterministic parsing and slicing for every public range contract."""

from __future__ import annotations

import re
from dataclasses import dataclass

from .models import ErrorCode, ServiceError

_POSITIVE_RE = re.compile(r"^([1-9][0-9]*)\.\.([1-9][0-9]*)$")
_NEGATIVE_RE = re.compile(r"^(-[1-9][0-9]*)\.\.(-[1-9][0-9]*)$")
ITEM_MAX_SPAN = 10
COMMENT_MAX_SPAN = 20
TRANSCRIPT_MAX_SPAN = 5000
ARTICLE_MAX_SPAN = 3000
# Keep normal defaults and continuations small while accepting explicit larger reads.
TEXT_HARD_MAX_SPAN = 8001


@dataclass(frozen=True, slots=True)
class ParsedRange:
    start: int
    end: int
    relative_to_end: bool

    @property
    def span(self) -> int:
        return self.end - self.start + 1


@dataclass(frozen=True, slots=True)
class ResolvedRange:
    requested: str
    resolved: str
    start: int
    end: int
    next_range: str | None

    @property
    def python_slice(self) -> slice:
        return slice(self.start - 1, self.end)


def parse_range(
    value: str,
    *,
    max_span: int,
    allow_negative: bool = False,
    range_name: str = "range",
    unit_name: str = "units",
) -> ParsedRange:
    """Parse one closed range without accepting whitespace or corrected values."""
    match = _POSITIVE_RE.fullmatch(value)
    relative = False
    if match is None and allow_negative:
        match = _NEGATIVE_RE.fullmatch(value)
        relative = match is not None
    if match is None:
        raise ServiceError(ErrorCode.INVALID_RANGE, "range must use the documented inclusive form")
    start, end = (int(part) for part in match.groups())
    if start > end:
        raise ServiceError(ErrorCode.INVALID_RANGE, "range start must not exceed range end")
    parsed = ParsedRange(start=start, end=end, relative_to_end=relative)
    if parsed.span > max_span:
        raise ServiceError(
            ErrorCode.INVALID_RANGE,
            f"{range_name} may contain at most {max_span} {unit_name}; use next_range to continue",
        )
    return parsed


def resolve_range(
    value: str,
    *,
    total: int,
    max_span: int,
    allow_negative: bool = False,
    continuation_span: int | None = None,
) -> ResolvedRange:
    """Resolve a public range and calculate a directly reusable continuation."""
    parsed = parse_range(value, max_span=max_span, allow_negative=allow_negative)
    next_span = max_span if continuation_span is None else continuation_span
    if total <= 0:
        raise ServiceError(ErrorCode.INVALID_RANGE, "range cannot address empty content")
    if parsed.relative_to_end:
        start = total + parsed.start + 1
        end = total + parsed.end + 1
        if start < 1 or end < 1 or end > total:
            raise ServiceError(ErrorCode.INVALID_RANGE, "range exceeds the known content length")
        next_range = _next_negative(parsed.start, total=total, max_span=next_span)
    else:
        if parsed.start > total:
            raise ServiceError(
                ErrorCode.INVALID_RANGE, "range starts beyond the known content length"
            )
        start = parsed.start
        # The final legal chunk may be shorter than the requested maximum span.
        end = min(parsed.end, total)
        next_range = _next_positive(end, total=total, max_span=next_span)
    return ResolvedRange(
        requested=value,
        resolved=f"{start}..{end}",
        start=start,
        end=end,
        next_range=next_range,
    )


def slice_text(
    content: str,
    value: str,
    *,
    max_span: int,
    allow_negative: bool = False,
    continuation_span: int | None = None,
) -> tuple[str, ResolvedRange]:
    """Resolve and slice Python Unicode characters, never encoded bytes."""
    resolved = resolve_range(
        value,
        total=len(content),
        max_span=max_span,
        allow_negative=allow_negative,
        continuation_span=continuation_span,
    )
    return content[resolved.python_slice], resolved


def returned_item_range(requested: ParsedRange, returned_count: int) -> str | None:
    """Describe the actual closed item interval after a short final page."""
    if returned_count == 0:
        return None
    return f"{requested.start}..{requested.start + returned_count - 1}"


def next_item_range(end: int, *, has_more: bool, max_span: int) -> str | None:
    """Continue list reading with the same maximum span."""
    if not has_more:
        return None
    return f"{end + 1}..{end + max_span}"


def _next_positive(end: int, *, total: int, max_span: int) -> str | None:
    if end >= total:
        return None
    return f"{end + 1}..{min(end + max_span, total)}"


def _next_negative(start: int, *, total: int, max_span: int) -> str | None:
    first_index = -total
    if start <= first_index:
        return None
    next_end = start - 1
    next_start = max(first_index, next_end - max_span + 1)
    return f"{next_start}..{next_end}"
