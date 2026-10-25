"""Shared dynamic identity, target support, and counters for lists, search, and detail."""

from __future__ import annotations

from typing import Literal

from .content import blocked_notice_fields, normalize_space, parse_duration, plain_text
from .models import (
    ActivityReferenceTarget,
    ActivityStats,
    LiveActivityTarget,
    OpusActivityTarget,
    OpusResourceRef,
    UnavailableActivityTarget,
    UserActivity,
    VideoActivityTarget,
    VideoResourceRef,
)
from .normalization import (
    JsonObject,
    format_timestamp,
    get_int,
    get_object,
    get_str,
    is_valid_bvid,
    optional_count,
    optional_object,
    optional_str,
    schema_error,
)

type ActivityKind = Literal["video", "opus", "live", "repost", "unavailable"]
type ActivityTarget = (
    VideoResourceRef
    | OpusResourceRef
    | LiveActivityTarget
    | ActivityReferenceTarget
    | UnavailableActivityTarget
)
ACTIVITY_NORMALIZATION_VERSION = "v4"
ACTIVITY_KINDS: dict[str, ActivityKind] = {
    "DYNAMIC_TYPE_AV": "video",
    "DYNAMIC_TYPE_DRAW": "opus",
    "DYNAMIC_TYPE_WORD": "opus",
    "DYNAMIC_TYPE_ARTICLE": "opus",
    "DYNAMIC_TYPE_FORWARD": "repost",
    "DYNAMIC_TYPE_LIVE": "live",
}


def activity_kind(data: JsonObject, target: ActivityTarget) -> ActivityKind:
    """Use verified cards for identity and keep known restricted posts in their own category."""
    if isinstance(target, VideoResourceRef):
        return "video"
    if isinstance(target, OpusResourceRef):
        return "opus"
    if isinstance(target, LiveActivityTarget):
        return "live"
    kind = ACTIVITY_KINDS.get(str(data.get("type")), "unavailable")
    return "unavailable" if kind == "live" else kind


def _activity_id(data: JsonObject) -> str:
    value = get_str(data, "id_str", "activity")
    if not value.isascii() or not value.isdecimal() or value.startswith("0"):
        raise schema_error("activity.id_str")
    return value


def _video_id(data: JsonObject) -> str:
    value = get_str(data, "bvid", "activity.archive")
    if not is_valid_bvid(value):
        raise schema_error("activity.archive.bvid")
    return value


def normalize_activity(data: JsonObject) -> UserActivity:
    """Normalize readable targets and provider status cards into one public union."""
    modules = get_object(data, "modules", "activity")
    author = get_object(modules, "module_author", "activity.modules")
    dynamic = get_object(modules, "module_dynamic", "activity.modules")
    stat = get_object(modules, "module_stat", "activity.modules")
    activity_type = get_str(data, "type", "activity")
    own_summary = dynamic_description(dynamic)
    if activity_type == "DYNAMIC_TYPE_FORWARD":
        original = optional_object(data, "orig")
        target = (
            activity_list_target(original)
            if original is not None
            else _unavailable_activity_target(None)
        )
        kind: Literal["video", "opus", "live", "repost", "unavailable"] = "repost"
    else:
        target = activity_list_target(data)
        kind = activity_kind(data, target)
    return UserActivity(
        activity_id=get_str(data, "id_str", "activity"),
        kind=kind,
        published_at=format_timestamp(optional_str(author, "pub_ts")),
        summary=own_summary if kind == "repost" and own_summary else None,
        target=target,
        stats=ActivityStats(
            share_count=activity_stat(stat, "forward"),
            comment_count=activity_stat(stat, "comment"),
            like_count=activity_stat(stat, "like"),
        ),
    )


def activity_target(
    data: JsonObject | None,
) -> ActivityTarget:
    """Extract readable content or retain the provider's unavailable status card."""
    if data is None:
        return _unavailable_activity_target(None)
    modules = get_object(data, "modules", "activity.target")
    dynamic = get_object(modules, "module_dynamic", "activity.target.modules")
    major = optional_object(dynamic, "major")
    if data.get("type") not in ACTIVITY_KINDS:
        return UnavailableActivityTarget(
            summary=_activity_status_summary(major)
            or dynamic_description(dynamic)
            or "该动态当前没有可读取的内容。"
        )
    if major is not None:
        live = activity_live_target(major)
        if live is not None:
            return live
        archive = optional_object(major, "archive")
        if archive is not None:
            return VideoResourceRef(bvid=_video_id(archive))
        if optional_object(major, "opus") is not None:
            return OpusResourceRef(opus_id=_activity_id(data))
        status_summary = _activity_status_summary(major)
        if status_summary is not None:
            return UnavailableActivityTarget(summary=status_summary)
    if data.get("type") == "DYNAMIC_TYPE_FORWARD":
        return ActivityReferenceTarget(activity_id=_activity_id(data))
    summary = dynamic_description(dynamic)
    # Descriptions also accompany live/PGC cards; their activity IDs are not Opus IDs.
    if (
        summary
        and data.get("type") in {"DYNAMIC_TYPE_WORD", "DYNAMIC_TYPE_DRAW"}
        and (major is None or optional_object(major, "draw") is not None)
    ):
        return OpusResourceRef(opus_id=_activity_id(data))
    return (
        UnavailableActivityTarget(summary=summary)
        if summary
        else _unavailable_activity_target(major)
    )


def activity_list_target(
    data: JsonObject,
) -> (
    VideoActivityTarget
    | OpusActivityTarget
    | LiveActivityTarget
    | ActivityReferenceTarget
    | UnavailableActivityTarget
):
    """Add list summaries after the shared classifier establishes a readable identity."""
    target = activity_target(data)
    modules = get_object(data, "modules", "activity")
    dynamic = get_object(modules, "module_dynamic", "activity")
    major = optional_object(dynamic, "major") or {}
    if isinstance(target, VideoResourceRef):
        archive = get_object(major, "archive", "activity")
        return VideoActivityTarget(
            bvid=target.bvid,
            title=plain_text(get_str(archive, "title", "activity.archive")),
            summary=normalize_space(optional_str(archive, "desc")),
            duration_seconds=parse_duration(optional_str(archive, "duration_text")),
        )
    if isinstance(target, OpusResourceRef):
        opus = optional_object(major, "opus") or {}
        summary = optional_object(opus, "summary") or {}
        return OpusActivityTarget(
            opus_id=target.opus_id,
            title=normalize_space(optional_str(opus, "title")) or None,
            summary=normalize_space(optional_str(summary, "text") or dynamic_description(dynamic)),
        )
    return target


def _unavailable_activity_target(major: JsonObject | None) -> UnavailableActivityTarget:
    """Return provider status prose or a neutral fallback for an empty target shell."""
    return UnavailableActivityTarget(
        summary=_activity_status_summary(major) or "该动态当前没有可读取的内容。"
    )


def _activity_status_summary(major: JsonObject | None) -> str | None:
    """Combine the visible title, hint, and action from a provider status card."""
    blocked = optional_object(major, "blocked") if major is not None else None
    none = optional_object(major, "none") if major is not None else None
    if blocked is not None:
        title, message, action_text = blocked_notice_fields(blocked)
        summary = ": ".join(value for value in (title, message) if value)
        if action_text:
            summary = f"{summary or '该动态当前不可读取'} ({action_text})"
        return summary or None
    message = normalize_space(optional_str(none, "tips")) if none is not None else ""
    return message or None


def dynamic_description(dynamic: JsonObject) -> str:
    description = optional_object(dynamic, "desc")
    return normalize_space(optional_str(description, "text")) if description is not None else ""


def activity_stat(stat: JsonObject, key: str) -> int | None:
    """Keep missing counters unknown and use the shared provider count parser."""
    value = optional_object(stat, key)
    return optional_count(value, "count") if value is not None else None


def activity_live_target(major: JsonObject) -> LiveActivityTarget | None:
    """Preserve a verified live-share card identically in activity lists and details."""
    live = optional_object(major, "live")
    if live is None or major.get("type") != "MAJOR_TYPE_LIVE":
        return None
    room_id = get_int(live, "id", "activity.live")
    if room_id <= 0:
        raise schema_error("activity.live.id")
    status: Literal["offline", "live"] | None = None
    if live.get("live_state") is not None:
        # Dynamic-card codes differ from live-room codes; do not infer replay or deleted content.
        code = get_int(live, "live_state", "activity.live")
        if code not in (0, 1):
            raise schema_error("activity.live.live_state")
        status = "live" if code == 1 else "offline"
    badge = optional_object(live, "badge")
    summary = normalize_space(
        " ".join(
            part
            for part in (
                optional_str(live, "desc_first"),
                optional_str(live, "desc_second"),
                optional_str(badge, "text") if badge is not None else "",
            )
            if part
        )
    )
    return LiveActivityTarget(
        room_id=str(room_id),
        title=normalize_space(get_str(live, "title", "activity.live")),
        summary=summary,
        # The activity author may be sharing another broadcaster's room; uname names the host.
        author_name=normalize_space(optional_str(live, "uname")) or None,
        live_status=status,
    )
