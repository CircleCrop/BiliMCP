"""Versioned JSON codec for public cache values with a fixed domain type registry."""

from __future__ import annotations

import hashlib
import json
import math
from array import array
from dataclasses import fields, is_dataclass
from enum import Enum
from functools import lru_cache
from importlib import import_module
from typing import cast

from pydantic import BaseModel, ValidationError

from .normalization import JsonObject, JsonValue

CODEC_VERSION = "public-json-v1"
# Only these public-content modules can supply constructors. Disk tags never select imports.
_DOMAIN_MODULES = (
    "models",
    "videos",
    "video_stats",
    "video_frames",
    "supporters",
    "comment_search",
    "transcripts",
    "danmaku_proto",
    "opuses",
    "users",
    "search_service",
    "content_search",
    "live",
    "medals",
    "collections",
)

# `sign` remains valid public profile text; protocol signatures have distinct keys.
_PRIVATE_KEYS = frozenset(
    {
        "cookie",
        "set-cookie",
        "sessdata",
        "bili_jct",
        "csrf",
        "csrf_token",
        "access_key",
        "access_token",
        "app_access_token",
        "refresh_token",
        "pending_refresh_token",
        "token",
        "auth_code",
        "authorization",
        "ac_time_value",
        "password",
        "buvid",
        "buvid3",
        "buvid4",
        "buvid_fp",
        "device_id",
        "device_token",
        "wbi_key",
        "wbi_img",
        "wbi_sign",
        "w_rid",
        "wts",
        "subtitle_url",
        "subtitle_url_v2",
    }
)


class CacheCodecError(ValueError):
    """Reject an unsupported value or obsolete disk format without exposing its content."""


@lru_cache(maxsize=1)
def _registry() -> dict[str, type[object]]:
    """Lazily register public models and data records from the fixed domain module list."""
    result: dict[str, type[object]] = {}
    for name in _DOMAIN_MODULES:
        module = import_module(f"fxbilibili_mcp.{name}")
        for value in vars(module).values():
            if not isinstance(value, type) or value.__module__ != module.__name__:
                continue
            if issubclass(value, (BaseModel, Enum)) or is_dataclass(value):
                result[f"{module.__name__}.{value.__qualname__}"] = value
    return result


def _type_name(value: object) -> str:
    cls = type(value)
    name = f"{cls.__module__}.{cls.__qualname__}"
    if _registry().get(name) is not cls:
        raise CacheCodecError("cache value type is not in the public domain registry")
    return name


def encode_public_value(value: object) -> JsonObject:
    """Encode one safe object graph; the codec version is independent of release versions."""
    return {"codec": CODEC_VERSION, "value": _encode(value)}


def decode_public_value(payload: JsonObject) -> object:
    """Restore only allowlisted domain records and supported standard containers."""
    if payload.get("codec") != CODEC_VERSION or "value" not in payload:
        raise CacheCodecError("cache codec version is obsolete")
    try:
        return _decode(payload["value"])
    except (TypeError, ValueError, KeyError, OverflowError, ValidationError) as exc:
        raise CacheCodecError("cache value no longer matches its public schema") from exc


def public_cache_key(key: tuple[object, ...]) -> str:
    """Hash the complete source/version binding without persisting raw key material."""
    encoded = json.dumps(_encode(key), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def _encode(value: object) -> JsonValue:
    """Tag containers so public dictionaries cannot be mistaken for type directives."""
    if isinstance(value, Enum):
        return {"type": "enum", "class": _type_name(value), "value": _encode(value.value)}
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CacheCodecError("non-finite cache number")
        return value
    if isinstance(value, BaseModel):
        return {
            "type": "model",
            "class": _type_name(value),
            "fields": _encode(value.__dict__),
            "fields_set": [_encode(name) for name in sorted(value.model_fields_set)],
        }
    if is_dataclass(value) and not isinstance(value, type):
        return {
            "type": "record",
            "class": _type_name(value),
            "fields": _encode({field.name: getattr(value, field.name) for field in fields(value)}),
        }
    if isinstance(value, dict):
        _check_mapping_keys(value)
        return {
            "type": "dict",
            "items": [[_encode(key), _encode(item)] for key, item in value.items()],
        }
    if isinstance(value, (tuple, list, set, frozenset)):
        return {"type": type(value).__name__, "items": [_encode(item) for item in value]}
    if isinstance(value, array) and value.typecode == "I":
        # The local content-search index uses packed unsigned character positions.
        return {"type": "array", "typecode": "I", "items": list(value)}
    raise CacheCodecError("cache value has an unsupported type")


def _decode(value: JsonValue) -> object:
    """Validate each tag before restoring its matching fixed constructor."""
    if isinstance(value, float) and not math.isfinite(value):
        raise CacheCodecError("non-finite cache number")
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if not isinstance(value, dict):
        raise CacheCodecError("cache node must have a tagged container")
    tag = value.get("type")
    if tag == "dict":
        pairs = _items(value)
        result: dict[object, object] = {}
        for pair in pairs:
            if not isinstance(pair, list) or len(pair) != 2:
                raise CacheCodecError("invalid cache mapping")
            result[_decode(pair[0])] = _decode(pair[1])
        _check_mapping_keys(result)
        return result
    if tag in ("list", "tuple", "set", "frozenset", "array"):
        items = [_decode(item) for item in _items(value)]
        if tag == "tuple":
            return tuple(items)
        if tag == "set":
            return set(items)
        if tag == "frozenset":
            return frozenset(items)
        if tag == "array":
            if value.get("typecode") != "I" or any(type(item) is not int for item in items):
                raise CacheCodecError("invalid cache index array")
            return array("I", cast(list[int], items))
        return items
    name = value.get("class")
    cls = _registry().get(name) if isinstance(name, str) else None
    if cls is None:
        raise CacheCodecError("cache class is outside the public domain registry")
    if tag == "enum" and issubclass(cls, Enum):
        return cls(_decode(value["value"]))
    data = _decode(value.get("fields"))
    if not isinstance(data, dict) or any(not isinstance(key, str) for key in data):
        raise CacheCodecError("invalid cache model fields")
    if tag == "model" and issubclass(cls, BaseModel):
        restored = cls.model_validate(data, strict=True)
        fields_set = value.get("fields_set")
        if not isinstance(fields_set, list) or any(
            not isinstance(key, str) or key not in cls.model_fields for key in fields_set
        ):
            raise CacheCodecError("invalid sparse cache model fields")
        restored.__pydantic_fields_set__ = set(cast(list[str], fields_set))
        return restored
    if tag == "record" and is_dataclass(cls):
        if set(data) != {field.name for field in fields(cls)}:
            raise CacheCodecError("cache record fields changed")
        # The constructor is selected only from the fixed public dataclass registry.
        return cls(**data)
    raise CacheCodecError("cache tag does not match its public class")


def _items(node: JsonObject) -> list[JsonValue]:
    items = node.get("items")
    if not isinstance(items, list):
        raise CacheCodecError("invalid cache container items")
    return items


def _check_mapping_keys(value: dict[object, object]) -> None:
    """Keep protocol credentials and expiring subtitle locators out of public storage."""
    if any(isinstance(key, str) and key.lower() in _PRIVATE_KEYS for key in value):
        raise CacheCodecError("private protocol fields cannot enter the public cache")
