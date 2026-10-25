"""Structured HTTP debug logging with Bilibili credential and risk-field redaction."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import urllib.parse
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from typing import Final

from .logging_context import current_tool_call

logger = logging.getLogger("fxbilibili_mcp")

REDACTED: Final = "[redacted]"
MAX_DEBUG_BODY_BYTES: Final = 64 * 1024
SENSITIVE_HEADER_MARKERS: Final = (
    "x-bili-gaia-vvoucher",
    "authorization",
    "authentication",
    "cookie",
    "csrf",
    "buvid",
    "token",
    "secret",
    "signature",
    "wbi",
    "bili-device",
)
SENSITIVE_QUERY_MARKERS: Final = (
    "password",
    "sessdata",
    "ac_time_value",
    "authorization",
    "cookie",
    "access_key",
    "auth_code",
    "sign",
    "v_voucher",
    "grisk_id",
    "w_rid",
    "csrf",
    "bili_jct",
    "buvid",
    "token",
    "secret",
    "signature",
    "dm_img",
    "dm_cover",
    "x-bili",
)
SENSITIVE_JSON_MARKERS: Final = (
    "password",
    "ac_time_value",
    "access_key",
    "auth_code",
    "v_voucher",
    "grisk_id",
    "bili_ticket",
    "authorization",
    "cookie",
    "csrf",
    "bili_jct",
    "sessdata",
    "buvid",
    "token",
    "secret",
    "signature",
    "w_rid",
    "wbi_img",
    "subtitle_url",
    "base_url",
    "baseurl",
    "backup_url",
    "backupurl",
    "durl",
    "dm_img",
    "dm_cover",
    "x-bili",
)


def encoded_body(body: bytes, *, max_bytes: int = MAX_DEBUG_BODY_BYTES) -> dict[str, str]:
    """Encode every body safely, redacting sensitive keys from JSON documents first."""
    # Large public bodies otherwise replace useful audit history with repeated content.
    if len(body) > max_bytes:
        return {
            "encoding": "omitted",
            "bytes": str(len(body)),
            "sha256": hashlib.sha256(body).hexdigest(),
        }
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        return {
            "encoding": "omitted",
            "bytes": str(len(body)),
        }
    try:
        payload = json.loads(text)
    except (TypeError, ValueError):
        return {"encoding": "utf-8", "value": text}
    redacted = redact_json(payload)
    return {
        "encoding": "utf-8",
        "value": json.dumps(redacted, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
    }


def redacted_headers(headers: Iterable[tuple[str, str]]) -> list[dict[str, str]]:
    """Preserve header order and duplicates while removing reusable credentials."""
    return [
        {
            "name": name,
            "value": REDACTED if _contains_marker(name, SENSITIVE_HEADER_MARKERS) else value,
        }
        for name, value in headers
    ]


def redacted_url(url: str, *, temporary_subtitle: bool = False) -> str:
    """Hide subtitle locations and sensitive query values without changing request behavior."""
    parsed = urllib.parse.urlsplit(url)
    if temporary_subtitle:
        return urllib.parse.urlunsplit(
            (parsed.scheme, parsed.netloc, "/[redacted-subtitle-url]", "", "")
        )
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    safe_query = [
        (key, REDACTED if _contains_marker(key, SENSITIVE_QUERY_MARKERS) else value)
        for key, value in query
    ]
    return urllib.parse.urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            urllib.parse.urlencode(safe_query),
            "",
        )
    )


def emit_http_debug(payload: Mapping[str, object]) -> None:
    """Write one correlated structured HTTP event after caller-side redaction."""
    boundary = payload.get("boundary")
    event_name = f"{boundary}_http" if isinstance(boundary, str) else "debug_http"
    event: dict[str, object] = {
        "time": datetime.now(UTC).isoformat(),
        "event": event_name,
        **payload,
    }
    context = current_tool_call()
    if context is not None:
        event["request_id"] = context.request_id
        event["tool"] = context.tool
    logger.info(json.dumps(event, ensure_ascii=False, separators=(",", ":"), sort_keys=True))


def redact_json(value: object) -> object:
    """Recursively redact sensitive JSON fields while retaining surrounding schema evidence."""
    if isinstance(value, dict):
        if value.get("type") == "image" and isinstance(value.get("data"), str):
            data = value["data"]
            padding = 2 if data.endswith("==") else 1 if data.endswith("=") else 0
            return {
                "type": "image",
                "mimeType": value.get("mimeType"),
                "bytes": len(data) * 3 // 4 - padding,
            }
        return {
            str(key): REDACTED
            if _contains_marker(str(key), SENSITIVE_JSON_MARKERS)
            else redact_json(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_json(item) for item in value]
    if isinstance(value, str):
        return value
    return value


def _contains_marker(name: str, markers: tuple[str, ...]) -> bool:
    """Match separators consistently across HTTP and provider naming conventions."""
    lowered = name.casefold()
    normalized = "".join(character for character in lowered if character.isalnum())
    tokens = re.split(r"[^a-z0-9]+", lowered)
    return "auth" in tokens or any(
        marker.casefold() in lowered
        or "".join(character for character in marker.casefold() if character.isalnum())
        in normalized
        for marker in markers
    )
