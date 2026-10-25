"""Compatibility boundary for strict schemas on the locked MCP SDK 1.28 API."""

from typing import Any

from mcp.server.fastmcp import FastMCP


def tighten_tool_schemas(mcp: FastMCP[None]) -> None:
    """Forbid unknown arguments and describe JSON schemas or content-only image metadata."""
    for tool in mcp._tool_manager.list_tools():
        tool.fn_metadata.arg_model.model_config["extra"] = "forbid"
        tool.fn_metadata.arg_model.model_rebuild(force=True)
        tool.parameters["additionalProperties"] = False
        compact_output_schema(tool.parameters, keep_defaults=True)
        if tool.output_schema is not None:
            tool.output_schema["type"] = "object"
            compact_output_schema(tool.output_schema)
            require_schema_version(tool.output_schema)
            if tool.name == "list_user_content":
                _tighten_user_content_output(tool.output_schema)
    comments = [tool for tool in mcp._tool_manager.list_tools() if tool.name == "list_comments"]
    if len(comments) != 1:
        raise RuntimeError("list_comments registration changed")
    schema = comments[0].parameters
    schema["oneOf"] = [
        {
            "required": [field],
            "properties": {
                field: {"not": {"type": "null"}},
                **{
                    other: {"type": "null"}
                    for other in ("bvid", "opus_id", "target")
                    if other != field
                },
            },
        }
        for field in ("bvid", "opus_id", "target")
    ]
    user_content = next(
        tool for tool in mcp._tool_manager.list_tools() if tool.name == "list_user_content"
    )
    user_content.parameters["not"] = {
        "required": ["kind", "sort"],
        "properties": {"kind": {"const": "opuses"}, "sort": {"const": "popular"}},
    }
    for live in (
        tool
        for tool in mcp._tool_manager.list_tools()
        if tool.name in {"get_live_room", "list_live_guards"}
    ):
        live.parameters["oneOf"] = [
            {
                "required": ["room_id"],
                "properties": {"room_id": {"type": "string"}, "uid": {"type": "null"}},
            },
            {
                "required": ["uid"],
                "properties": {"room_id": {"type": "null"}, "uid": {"type": "string"}},
            },
        ]
    history = next(
        tool for tool in mcp._tool_manager.list_tools() if tool.name == "search_user_content"
    )
    history.parameters["anyOf"] = [
        {"required": [field], "properties": {field: {"type": "string"}}}
        for field in ("query", "published_after", "published_before")
    ]
    comment_search = next(
        tool for tool in mcp._tool_manager.list_tools() if tool.name == "search_comments"
    )
    comment_search.parameters["anyOf"] = [
        {"required": [field], "properties": {field: {"type": "string"}}}
        for field in ("query", "published_after")
    ]
    search = next(tool for tool in mcp._tool_manager.list_tools() if tool.name == "search")
    search.parameters["allOf"] = [
        {
            "if": {"properties": {"kind": {"const": "live"}}, "required": ["kind"]},
            "then": {
                "properties": {
                    "sort": {"enum": ["relevance", "popularity", "recent_live"]},
                    "filters": {"type": "null"},
                }
            },
            "else": {
                "if": {"properties": {"kind": {"const": "users"}}, "required": ["kind"]},
                "then": {
                    "properties": {
                        "sort": {"enum": ["relevance", "followers"]},
                        "filters": {"type": "null"},
                    }
                },
                "else": {
                    "if": {"properties": {"kind": {"const": "opuses"}}, "required": ["kind"]},
                    "then": {
                        "properties": {
                            "sort": {"enum": ["relevance", "newest"]},
                            "filters": {
                                "anyOf": [
                                    {"type": "null"},
                                    {"properties": {"duration": {"type": "null"}}},
                                ]
                            },
                        }
                    },
                    "else": {"properties": {"sort": {"enum": ["relevance", "newest", "views"]}}},
                },
            },
        }
    ]


def require_schema_version(schema: dict[str, Any]) -> None:
    """Make the fixed version field mandatory in every success/error branch."""
    definitions = schema.get("$defs")
    if not isinstance(definitions, dict):
        return
    for definition in definitions.values():
        if not isinstance(definition, dict):
            continue
        properties = definition.get("properties")
        if not isinstance(properties, dict) or "schema_version" not in properties:
            continue
        required = definition.setdefault("required", [])
        if isinstance(required, list) and "schema_version" not in required:
            required.insert(0, "schema_version")


def _tighten_user_content_output(schema: dict[str, Any]) -> None:
    """Tie each content kind to its item type and video-only popularity rule."""
    definitions = schema.get("$defs")
    if not isinstance(definitions, dict):
        raise RuntimeError("list_user_content output definitions changed")
    output = definitions.get("UserContentOutput")
    if not isinstance(output, dict):
        raise RuntimeError("list_user_content output envelope changed")
    output["oneOf"] = [
        {
            "properties": {
                "kind": {"const": "videos"},
                "items": {"items": {"$ref": "#/$defs/UserVideoSummary"}},
            }
        },
        {
            "properties": {
                "kind": {"const": "opuses"},
                "sort": {"const": "newest"},
                "items": {"items": {"$ref": "#/$defs/OpusListItem"}},
            }
        },
    ]


def compact_output_schema(value: object, *, keep_defaults: bool = False) -> None:
    """Remove presentation-only schema metadata without touching business properties."""
    if not isinstance(value, dict):
        return
    value.pop("title", None)
    if not keep_defaults:
        value.pop("default", None)
    for keyword in ("$defs", "definitions", "properties", "patternProperties"):
        children = value.get(keyword)
        if isinstance(children, dict):
            for child in children.values():
                compact_output_schema(child, keep_defaults=keep_defaults)
    for keyword in ("allOf", "anyOf", "oneOf", "prefixItems"):
        children = value.get(keyword)
        if isinstance(children, list):
            for child in children:
                compact_output_schema(child, keep_defaults=keep_defaults)
    for keyword in ("additionalProperties", "contains", "items", "not"):
        compact_output_schema(value.get(keyword), keep_defaults=keep_defaults)
