"""Read only the fan medals made visible by a user's official privacy settings."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import Field, ValidationError

from .cache import AsyncTTLCache
from .models import Author, ErrorCode, ServiceError, StrictModel
from .normalization import (
    TAIPEI,
    as_object,
    get_int,
    get_list,
    get_object,
    get_str,
    schema_error,
    validate_decimal_id,
)
from .ranges import parse_range, resolve_range
from .transport import BilibiliTransport

type GuardTier = Literal["governor", "admiral", "captain"]
_GUARD_TIERS: dict[int, GuardTier] = {1: "governor", 2: "admiral", 3: "captain"}


def _normalize_guard_tier(level: int) -> GuardTier | None:
    """Name the source subscription tier while omitting the no-subscription state."""
    if level == 0:
        return None
    tier = _GUARD_TIERS.get(level)
    if tier is None:
        raise schema_error("medal.guard_level")
    return tier


class FanMedal(StrictModel):
    medal_id: str = Field(pattern=r"^[1-9][0-9]*$")
    name: str
    level: int = Field(ge=0)
    author: Author
    wearing: Literal[True] | None = None
    guard_tier: GuardTier | None = None


class UserMedalsOutput(StrictModel):
    schema_version: Literal[1] = 1
    uid: str
    items: list[FanMedal] = Field(max_length=10)
    visibility: Literal["public", "wearing_only", "public_subset"]
    available_count: int = Field(ge=0)
    returned_range: str | None
    next_range: str | None
    fetched_at: str


class _MedalInventory(StrictModel):
    items: list[FanMedal]
    visibility: Literal["public", "wearing_only", "public_subset"]
    fetched_at: str


class MedalService:
    def __init__(self, transport: BilibiliTransport, cache: AsyncTTLCache) -> None:
        """Use the account's existing authenticated, scheduled Bilibili transport."""
        self._transport = transport
        self._cache = cache

    async def list_user_medals(
        self, uid: str, range: str = "1..10"
    ) -> tuple[UserMedalsOutput, bool]:
        """Cache a visible inventory for one hour and slice only public entries."""
        validate_decimal_id(uid, "uid")
        requested = parse_range(range, max_span=10)

        async def produce() -> _MedalInventory:
            """Check visibility before retaining any user medal details."""
            data = as_object(
                await self._transport.request_live_data(
                    "/xlive/web-ucenter/user/MedalWall", {"target_id": uid}
                ),
                "medal wall",
            )
            if get_int(data, "close_space_medal", "medal wall") != 0:
                raise ServiceError(ErrorCode.NOT_FOUND, "User fan medals are not publicly visible")
            wearing_only = get_int(data, "only_show_wearing", "medal wall") != 0
            items: list[FanMedal] = []
            for value in get_list(data, "list", "medal wall"):
                row = as_object(value, "medal")
                medal = get_object(row, "medal_info", "medal")
                wearing = get_int(medal, "wearing_status", "medal") == 1
                # The response also contains the viewer's UID and temporary playback links.
                # Neither identifies the requested subject or belongs in a public medal projection.
                if wearing_only and not wearing:
                    continue
                guard = _normalize_guard_tier(get_int(medal, "guard_level", "medal"))
                items.append(
                    FanMedal.model_validate(
                        {
                            "medal_id": str(get_int(medal, "medal_id", "medal")),
                            "name": get_str(medal, "medal_name", "medal"),
                            "level": get_int(medal, "level", "medal"),
                            "author": {
                                "uid": str(get_int(medal, "target_id", "medal")),
                                "name": get_str(row, "target_name", "medal"),
                            },
                            "wearing": True if wearing else None,
                            "guard_tier": guard,
                        }
                    )
                )
            return _MedalInventory(
                items=items,
                visibility="wearing_only"
                if wearing_only
                else (
                    "public"
                    if len(items) == get_int(data, "count", "medal wall")
                    else "public_subset"
                ),
                fetched_at=datetime.now(TAIPEI).isoformat(timespec="seconds"),
            )

        try:
            inventory, hit = await self._cache.get_or_create(
                ("medal-wall", uid, "v1"), 3600, produce
            )
        except ValidationError as error:
            raise ServiceError(
                ErrorCode.UPSTREAM_SCHEMA_CHANGED, "Fan medal fields changed"
            ) from error
        total = len(inventory.items)
        if total == 0:
            if requested.start != 1:
                raise ServiceError(ErrorCode.INVALID_RANGE, "range starts beyond visible medals")
            return UserMedalsOutput(
                uid=uid,
                items=[],
                visibility=inventory.visibility,
                available_count=0,
                returned_range=None,
                next_range=None,
                fetched_at=inventory.fetched_at,
            ), hit
        resolved = resolve_range(range, total=total, max_span=10)
        return UserMedalsOutput(
            uid=uid,
            items=inventory.items[resolved.python_slice],
            visibility=inventory.visibility,
            available_count=total,
            returned_range=resolved.resolved,
            next_range=resolved.next_range,
            fetched_at=inventory.fetched_at,
        ), hit
