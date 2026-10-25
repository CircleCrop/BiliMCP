"""Strict minimal decoder for Bilibili's segmented danmaku protobuf."""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from dataclasses import dataclass

from .content import normalize_space
from .models import DanmakuKind, ErrorCode, ServiceError

_SENDER_HASH_RE = re.compile(r"^[0-9a-f]{1,8}$")
SEGMENT_MAX_ELEMENTS = 50_000
SEGMENT_MAX_TEXT_BYTES = 4 * 1024**2
_MODE_KIND: dict[int, DanmakuKind] = {
    1: "scrolling",
    2: "scrolling",
    3: "scrolling",
    4: "bottom",
    5: "top",
    6: "reverse",
    7: "advanced",
    8: "code",
    9: "special",
}
type _ProtobufValue = int | bytes


@dataclass(frozen=True, slots=True)
class ParsedDanmaku:
    """Fields needed for filtering and sampling after the provider boundary."""

    at_seconds: int
    text: str
    kind: DanmakuKind
    sender_hash: str | None
    weight: int


@dataclass(slots=True)
class _DecodeBudget:
    """Count raw work before discarded elements or normalized text can hide its cost."""

    elements: int = 0
    text_bytes: int = 0

    def check(self) -> None:
        """Log only work counters and reject an incomplete decoded segment."""
        if self.elements > SEGMENT_MAX_ELEMENTS or self.text_bytes > SEGMENT_MAX_TEXT_BYTES:
            logging.getLogger("fxbilibili_mcp").warning(
                "danmaku_decode_limit reason=%s elements=%d text_bytes=%d",
                "elements" if self.elements > SEGMENT_MAX_ELEMENTS else "text_bytes",
                self.elements,
                self.text_bytes,
            )
            raise ServiceError(
                ErrorCode.RESOURCE_LIMIT_EXCEEDED,
                "This six-minute danmaku segment exceeds the decoding budget; "
                "no complete statistics were produced. Do not retry this segment. "
                "Reducing sample_limit or a time window inside it will not help.",
            )


def parse_danmaku_segment(payload: bytes) -> tuple[ParsedDanmaku, ...]:
    """Check raw element and text budgets before decoding and allocating result objects."""
    if payload == b"\x10\x01":
        raise ServiceError(
            ErrorCode.NOT_FOUND,
            "danmaku is disabled for this video part and retrying will not change the result",
        )
    rows: list[ParsedDanmaku] = []
    budget = _DecodeBudget()
    for field_number, wire_type, value in _protobuf_fields(payload):
        if field_number != 1 or wire_type != 2 or not isinstance(value, bytes):
            continue
        budget.elements += 1
        budget.check()
        parsed = _parse_danmaku_element(value, budget)
        if parsed is not None:
            rows.append(parsed)
    return tuple(rows)


def _parse_danmaku_element(payload: bytes, budget: _DecodeBudget) -> ParsedDanmaku | None:
    """Read progress, mode, text, sender summary, and display weight from one element."""
    progress_ms: int | None = None
    mode = 1
    text: str | None = None
    sender_hash: str | None = None
    weight = 0
    for field_number, wire_type, value in _protobuf_fields(payload):
        if field_number == 2 and wire_type == 0 and isinstance(value, int):
            progress_ms = value
        elif field_number == 3 and wire_type == 0 and isinstance(value, int):
            mode = value
        elif field_number == 6 and wire_type == 2 and isinstance(value, bytes):
            sender_hash = _decode_sender_hash(value)
        elif field_number == 7 and wire_type == 2 and isinstance(value, bytes):
            budget.text_bytes += len(value)
            budget.check()
            text = _decode_text(value)
        elif field_number == 9 and wire_type == 0 and isinstance(value, int):
            weight = value
    if progress_ms is None or not text:
        return None
    return ParsedDanmaku(
        at_seconds=progress_ms // 1000,
        text=text,
        kind=_MODE_KIND.get(mode, "special"),
        sender_hash=sender_hash,
        weight=weight,
    )


def _decode_text(value: bytes) -> str:
    """Decode and normalize provider text while mapping invalid UTF-8 to schema drift."""
    try:
        return normalize_space(value.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise ServiceError(
            ErrorCode.UPSTREAM_SCHEMA_CHANGED,
            "Bilibili danmaku protobuf contained invalid UTF-8 text",
        ) from exc


def _decode_sender_hash(value: bytes) -> str | None:
    """Validate the provider's cross-video CRC-style sender summary without reversing it."""
    if not value:
        return None
    try:
        sender_hash = value.decode("ascii").lower()
    except UnicodeDecodeError as exc:
        raise ServiceError(
            ErrorCode.UPSTREAM_SCHEMA_CHANGED,
            "Bilibili danmaku protobuf contained an invalid sender summary",
        ) from exc
    if _SENDER_HASH_RE.fullmatch(sender_hash) is None:
        raise ServiceError(
            ErrorCode.UPSTREAM_SCHEMA_CHANGED,
            "Bilibili danmaku protobuf contained an invalid sender summary",
        )
    return sender_hash


def _protobuf_fields(payload: bytes) -> Iterator[tuple[int, int, _ProtobufValue]]:
    """Decode protobuf wire fields with strict bounds and no generated schema dependency."""
    offset = 0
    while offset < len(payload):
        key, offset = _read_varint(payload, offset)
        field_number = key >> 3
        wire_type = key & 0x07
        value: _ProtobufValue
        if field_number == 0:
            raise _protobuf_error()
        if wire_type == 0:
            value, offset = _read_varint(payload, offset)
        elif wire_type == 1:
            value, offset = _read_fixed(payload, offset, 8)
        elif wire_type == 2:
            length, offset = _read_varint(payload, offset)
            value, offset = _read_fixed(payload, offset, length)
        elif wire_type == 5:
            value, offset = _read_fixed(payload, offset, 4)
        else:
            raise _protobuf_error()
        yield field_number, wire_type, value


def _read_varint(payload: bytes, offset: int) -> tuple[int, int]:
    """Read one bounded unsigned protobuf varint and return its following offset."""
    value = 0
    for shift in range(0, 70, 7):
        if offset >= len(payload):
            raise _protobuf_error()
        byte = payload[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if byte & 0x80 == 0:
            return value, offset
    raise _protobuf_error()


def _read_fixed(payload: bytes, offset: int, length: int) -> tuple[bytes, int]:
    """Read one fixed or length-delimited protobuf value after validating bounds."""
    end = offset + length
    if length < 0 or end > len(payload):
        raise _protobuf_error()
    return payload[offset:end], end


def _protobuf_error() -> ServiceError:
    return ServiceError(
        ErrorCode.UPSTREAM_SCHEMA_CHANGED,
        "Bilibili danmaku protobuf could not be parsed",
    )
