"""Strict public models and bounded normalization for live-room metadata."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, cast

from pydantic import Field, ValidationError, model_validator

from .cache import AsyncTTLCache, CachePolicy
from .content import plain_text
from .models import Author, ErrorCode, SearchCoverage, ServiceError, StrictModel
from .normalization import (
    TAIPEI,
    JsonObject,
    as_object,
    format_timestamp,
    get_int,
    get_object,
    get_str,
    optional_object,
    schema_error,
)
from .transport import BilibiliTransport, BusinessBudgetExhausted

type LiveStatus = Literal["offline", "live", "replay"]
_LIVE_STATUSES: dict[int, LiveStatus] = {0: "offline", 1: "live", 2: "replay"}


def normalize_live_status(status: int, context: str) -> LiveStatus:
    """Translate verified upstream state codes into readable public values."""
    normalized = _LIVE_STATUSES.get(status)
    if normalized is None:
        raise schema_error(context)
    return normalized


class GetLiveRoomInput(StrictModel):
    room_id: str | None = Field(default=None, pattern=r"^[1-9][0-9]*$")
    uid: str | None = Field(default=None, pattern=r"^[1-9][0-9]*$")

    @model_validator(mode="after")
    def require_one_identity(self) -> GetLiveRoomInput:
        if (self.room_id is None) == (self.uid is None):
            raise ValueError("provide exactly one of room_id or uid")
        return self


class LiveRoomArea(StrictModel):
    area_id: int = Field(ge=0)
    name: str
    parent_area_id: int = Field(ge=0)
    parent_name: str


class LiveRoomOutput(StrictModel):
    schema_version: Literal[1] = 1
    room_id: str = Field(pattern=r"^[1-9][0-9]*$")
    short_id: str | None = Field(default=None, pattern=r"^[1-9][0-9]*$")
    author: Author
    title: str
    area: LiveRoomArea
    live_status: LiveStatus
    description: str
    started_at: str | None = None
    population: int = Field(ge=0, description="Upstream popularity value, not a viewer count")
    original_url: str
    fetched_at: str


class LiveSearchItem(StrictModel):
    room_id: str = Field(pattern=r"^[1-9][0-9]*$")
    author: Author
    title: str
    live_status: LiveStatus
    population: int = Field(ge=0, description="Source popularity value, not a viewer count")
    watched_count: int | None = Field(
        default=None, ge=0, description="Source cumulative watched count"
    )
    started_at: str | None = None
    area_name: str


class ExtendedLiveSearchOutput(StrictModel):
    schema_version: Literal[1] = 1
    query: str
    kind: Literal["live"] = "live"
    sort: Literal["relevance", "popularity", "recent_live"]
    items: list[LiveSearchItem] = Field(max_length=10)
    next_cursor: str | None = None
    coverage: SearchCoverage
    fetched_at: str


@dataclass(frozen=True, slots=True)
class _RoomProfile:
    author: Author
    title: str
    area: LiveRoomArea
    description: str


@dataclass(frozen=True, slots=True)
class _RoomSnapshot:
    room_id: str
    short_id: str | None
    uid: str
    profile: _RoomProfile
    live_status: LiveStatus
    started_at: str | None
    population: int
    fetched_at: str


class LiveService:
    """Read fixed public live metadata with separate state and profile lifetimes."""

    def __init__(
        self, transport: BilibiliTransport, cache: AsyncTTLCache, policy: CachePolicy
    ) -> None:
        """Bind the audited transport and shared cache with live-specific lifetimes."""
        self._transport = transport
        self._cache = cache
        self._policy = policy

    async def get_live_room(
        self, room_id: str | None = None, uid: str | None = None
    ) -> tuple[LiveRoomOutput, bool]:
        """Resolve one room or owner and bound all actual upstream attempts to six."""
        try:
            selection = GetLiveRoomInput(room_id=room_id, uid=uid)
        except ValidationError as exc:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT, "provide exactly one positive string room_id or uid"
            ) from exc
        async with self._transport.scoped_work(6):
            try:
                mapping_hit = True
                requested_id = cast(str, selection.room_id)
                if selection.uid is not None:
                    mapped_id, mapping_hit = await self.room_for_uid(selection.uid)
                    if mapped_id is None:
                        raise ServiceError(ErrorCode.NOT_FOUND, "this user has no public live room")
                    requested_id = mapped_id
                snapshot, state_hit = await self.room_snapshot(requested_id)
                if selection.uid is not None and snapshot.uid != selection.uid:
                    raise ServiceError(
                        ErrorCode.NOT_FOUND, "this user has no matching public live room"
                    )

                async def profile_from_snapshot() -> _RoomProfile:
                    return snapshot.profile

                profile, profile_hit = await self._cache.get_or_create(
                    ("live_profile", snapshot.room_id, snapshot.uid, "v1"),
                    self._policy.live_profile,
                    profile_from_snapshot,
                )
            except BusinessBudgetExhausted as exc:
                raise ServiceError(
                    ErrorCode.RATE_LIMITED, "live-room lookup exhausted its request budget"
                ) from exc
        return LiveRoomOutput(
            room_id=snapshot.room_id,
            short_id=snapshot.short_id,
            author=profile.author,
            title=profile.title,
            area=profile.area,
            live_status=snapshot.live_status,
            description=profile.description,
            started_at=snapshot.started_at,
            population=snapshot.population,
            original_url=f"https://live.bilibili.com/{snapshot.room_id}",
            fetched_at=snapshot.fetched_at,
        ), mapping_hit and state_hit and profile_hit

    async def room_for_uid(self, uid: str) -> tuple[str | None, bool]:
        """Cache only a verified public UID-to-room mapping from the space API."""

        async def produce() -> str | None:
            """Request the existing signed profile endpoint and discard unrelated fields."""
            raw = await self._transport.request_data(
                "/x/space/wbi/acc/info",
                {"mid": uid},
                wbi=True,
                wbi2=True,
                not_found_codes=frozenset({-400, -404}),
            )
            data = as_object(raw, "live.user")
            if get_int(data, "mid", "live.user") != int(uid):
                raise schema_error("live.user.mid")
            room = optional_object(data, "live_room")
            if room is None:
                return None
            room_status = get_int(room, "roomStatus", "live.user.room")
            if room_status == 0:
                return None
            if room_status != 1:
                raise schema_error("live.user.room.roomStatus")
            room_id = get_int(room, "roomid", "live.user.room")
            if room_id <= 0:
                raise schema_error("live.user.room.roomid")
            return str(room_id)

        return await self._cache.get_or_create(
            ("live_uid_room", uid, "v1"), self._policy.live_profile, produce
        )

    async def room_snapshot(self, requested_id: str) -> tuple[_RoomSnapshot, bool]:
        """Fetch state once and let known short and standard IDs share the same snapshot."""

        async def produce() -> _RoomSnapshot:
            """Read the audited live JSON endpoint without retaining playback or account data."""
            raw = await self._transport.request_live_data(
                "/xlive/web-room/v1/index/getInfoByRoom", {"room_id": requested_id}
            )
            return _normalize_room(as_object(raw, "live.room"), requested_id)

        snapshot, hit = await self._cache.get_or_create(
            ("live_status", requested_id, "v1"), self._policy.live_status, produce
        )

        async def retained_snapshot() -> _RoomSnapshot:
            return snapshot

        # Provider-confirmed aliases share this state rather than fetching the same room again.
        for identity in (snapshot.room_id, snapshot.short_id) if not hit else ():
            if identity is not None and identity != requested_id:
                await self._cache.get_or_create(
                    ("live_status", identity, "v1"), self._policy.live_status, retained_snapshot
                )
        return snapshot, hit


def _normalize_room(data: JsonObject, requested_id: str) -> _RoomSnapshot:
    """Project the verified room and anchor shapes into public metadata and popularity."""
    room = get_object(data, "room_info", "live.room")
    anchor = get_object(data, "anchor_info", "live.room")
    base = get_object(anchor, "base_info", "live.anchor")
    standard_id = get_int(room, "room_id", "live.room_info")
    short_id = get_int(room, "short_id", "live.room_info")
    uid = get_int(room, "uid", "live.room_info")
    status = get_int(room, "live_status", "live.room_info")
    start = get_int(room, "live_start_time", "live.room_info")
    population = get_int(room, "online", "live.room_info")
    area_id = get_int(room, "area_id", "live.room_info")
    parent_area_id = get_int(room, "parent_area_id", "live.room_info")
    if (
        standard_id <= 0
        or short_id < 0
        or uid <= 0
        or start < 0
        or population < 0
        or area_id < 0
        or parent_area_id < 0
        or requested_id not in {str(standard_id), str(short_id)}
    ):
        raise schema_error("live.room_info.identity_or_state")
    return _RoomSnapshot(
        room_id=str(standard_id),
        short_id=str(short_id) if short_id else None,
        uid=str(uid),
        profile=_RoomProfile(
            author=Author(uid=str(uid), name=get_str(base, "uname", "live.anchor")),
            title=plain_text(get_str(room, "title", "live.room_info")),
            area=LiveRoomArea(
                area_id=area_id,
                name=get_str(room, "area_name", "live.room_info"),
                parent_area_id=parent_area_id,
                parent_name=get_str(room, "parent_area_name", "live.room_info"),
            ),
            description=plain_text(get_str(room, "description", "live.room_info")),
        ),
        live_status=normalize_live_status(status, "live.room_info.live_status"),
        started_at=format_timestamp(start) if start else None,
        population=population,
        fetched_at=datetime.now(TAIPEI).isoformat(),
    )
