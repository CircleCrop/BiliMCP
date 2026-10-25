"""Typed upstream JSON access and cross-domain value normalization."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Literal

from .models import ErrorCode, ServiceError, Verification

type JsonScalar = str | int | float | bool | None
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]
type JsonObject = dict[str, JsonValue]

TAIPEI = timezone(timedelta(hours=8))
ARTICLE_OPUS_MAPPING_VERSION = "v2"
_BVID_RE = re.compile(r"^BV[0-9A-Za-z]{10}$")
_DECIMAL_ID_RE = re.compile(r"^[1-9][0-9]*$")


def validate_bvid(value: str) -> str:
    if not _BVID_RE.fullmatch(value):
        raise ServiceError(ErrorCode.INVALID_ARGUMENT, "bvid must be a valid BV identifier")
    return value


def is_valid_bvid(value: object) -> bool:
    return isinstance(value, str) and _BVID_RE.fullmatch(value) is not None


def validate_decimal_id(value: str, field: str) -> str:
    if not _DECIMAL_ID_RE.fullmatch(value):
        raise ServiceError(ErrorCode.INVALID_ARGUMENT, f"{field} must be a positive decimal string")
    return value


def article_opus_id(source: JsonObject) -> str | None:
    """Read verified note/article mappings without treating arbitrary dynamic IDs as Opus."""
    article_type = source.get("type")
    opus_id = source.get("dyn_id_str")
    # Public notes (2), including image-only notes, have the same readable Opus body as type 4.
    if type(article_type) is not int or article_type not in (2, 4):
        return None
    if isinstance(opus_id, str) and _DECIMAL_ID_RE.fullmatch(opus_id):
        return opus_id
    return None


def as_object(value: object, context: str) -> JsonObject:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise schema_error(context)
    return value


def as_list(value: JsonValue | object, context: str) -> list[JsonValue]:
    if not isinstance(value, list):
        raise schema_error(context)
    return value


def get_object(source: JsonObject, key: str, context: str) -> JsonObject:
    return as_object(source.get(key), f"{context}.{key}")


def optional_object(source: JsonObject, key: str) -> JsonObject | None:
    value = source.get(key)
    if value is None:
        return None
    return as_object(value, key)


def get_list(source: JsonObject, key: str, context: str) -> list[JsonValue]:
    return as_list(source.get(key), f"{context}.{key}")


def optional_list(source: JsonObject, key: str) -> list[JsonValue]:
    value = source.get(key)
    if value is None:
        return []
    return as_list(value, key)


def get_str(source: JsonObject, key: str, context: str) -> str:
    value = source.get(key)
    if not isinstance(value, str):
        raise schema_error(f"{context}.{key}")
    return value


def optional_str(source: JsonObject, key: str, default: str = "") -> str:
    value = source.get(key)
    if value is None:
        return default
    if isinstance(value, str):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    raise schema_error(key)


def get_int(source: JsonObject, key: str, context: str) -> int:
    value = source.get(key)
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str) and value.isdecimal():
        return int(value)
    raise schema_error(f"{context}.{key}")


def optional_int(source: JsonObject, key: str, default: int = 0) -> int:
    """Read an optional provider count while accepting its documented string forms."""
    value = source.get(key)
    if value is None:
        return default
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "--":
            return 0
        normalized = stripped.replace(",", "")
        if normalized.isascii() and normalized.isdecimal():
            return int(normalized)
    raise schema_error(key)


def optional_count(source: JsonObject, key: str) -> int | None:
    """Omit unknown independent counters while preserving known zero and count formats."""
    if source.get(key) is None:
        return None
    value = optional_int(source, key)
    if value < 0:
        raise schema_error(key)
    return value


def optional_bool(source: JsonObject, key: str, default: bool = False) -> bool:
    value = source.get(key)
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    raise schema_error(key)


def format_timestamp(value: int | str) -> str:
    """Return a seconds timestamp as RFC 3339 in the product's fixed +08:00 zone."""
    if isinstance(value, bool):
        raise schema_error("timestamp")
    try:
        timestamp = int(value)
        return datetime.fromtimestamp(timestamp, tz=TAIPEI).isoformat(timespec="seconds")
    except (TypeError, ValueError, OverflowError, OSError) as exc:
        raise schema_error("timestamp") from exc


def normalize_language(language: str) -> str:
    """Remove Bilibili's AI marker while retaining a useful BCP-47-style label."""
    value = language.removeprefix("ai-")
    if value == "zh":
        return "zh-CN"
    return value


def verification_from_profile(profile: JsonObject) -> Verification | None:
    """Use the current space attestation text and official account type verbatim."""
    attestation = optional_object(profile, "attestation")
    official = optional_object(profile, "official")
    if attestation is None or official is None:
        return None
    common = optional_object(attestation, "common_info")
    if common is None:
        return None
    info = optional_str(common, "prefix_title").strip()
    if not info:
        return None
    # The live space contract uses official.type 1 for institutions and 0 for individuals.
    color: Literal["blue", "yellow"] = (
        "blue" if optional_int(official, "type", -1) == 1 else "yellow"
    )
    return Verification(color=color, info=info)


def schema_error(context: str) -> ServiceError:
    return ServiceError(
        ErrorCode.UPSTREAM_SCHEMA_CHANGED,
        f"Bilibili response schema changed at {context}",
    )


def normalize_ip_location(value: str) -> str | None:
    """Normalize a public IP-region display label without guessing a user's location."""
    label = value.strip()
    for prefix in ("IP属地\uff1a", "IP属地:"):
        if label.startswith(prefix):
            label = label.removeprefix(prefix).strip()
            break
    # Preserve the existing compact region format shared by comment and profile readers.
    if label.startswith("中国"):
        label = label.removeprefix("中国").strip()
    return label or None
