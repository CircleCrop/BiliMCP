"""Pure normalization helpers for text, durations, summaries, and HTML fragments."""

from __future__ import annotations

import html
import re
from html.parser import HTMLParser

from .normalization import JsonObject, optional_object, optional_str

_SPACE_RE = re.compile(r"\s+")


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self.parts.append(data)


def plain_text(value: str) -> str:
    """Remove markup and collapse whitespace from an upstream text fragment."""
    parser = _TextExtractor()
    parser.feed(value)
    parser.close()
    return normalize_space(html.unescape("".join(parser.parts)))


def normalize_space(value: str) -> str:
    return _SPACE_RE.sub(" ", value.replace("\u200b", " ")).strip()


def blocked_notice_fields(blocked: JsonObject) -> tuple[str, str, str]:
    """Read one provider notice consistently for activity lists, details, and Opus cards."""
    button = optional_object(blocked, "button")
    return (
        normalize_space(optional_str(blocked, "title")),
        normalize_space(optional_str(blocked, "hint_message") or optional_str(blocked, "hint")),
        normalize_space(optional_str(button, "text")) if button is not None else "",
    )


def truncate_summary(value: str, limit: int = 160) -> tuple[str, bool]:
    """Truncate by Python Unicode characters and expose truncation separately."""
    normalized = normalize_space(value)
    if len(normalized) <= limit:
        return normalized, False
    return normalized[: limit - 1].rstrip() + "…", True


def parse_duration(value: str | int) -> int:
    """Convert Bilibili seconds or colon-separated durations to integer seconds."""
    if isinstance(value, int):
        return max(value, 0)
    parts = value.strip().split(":")
    if not parts or any(not part.isdecimal() for part in parts) or len(parts) > 3:
        return 0
    total = 0
    for part in parts:
        total = total * 60 + int(part)
    return total
