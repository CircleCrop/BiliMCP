"""Read only the supporters and messages shown by a creator's public showcase."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import Field, ValidationError

from .cache import AsyncTTLCache
from .models import ErrorCode, ServiceError, StrictModel
from .normalization import (
    TAIPEI,
    JsonObject,
    as_object,
    get_int,
    get_list,
    get_str,
    schema_error,
)
from .transport import BilibiliTransport, BusinessBudgetExhausted

_SUPPORTERS_PATH = "/x/ugcpay-rank/elec/month/up"
_CACHE_TTL_SECONDS = 3600
_MAX_VISIBLE_SUPPORTERS = 100


class ListUserSupportersInput(StrictModel):
    uid: str = Field(pattern=r"^[1-9][0-9]*$")


class SupporterMember(StrictModel):
    rank: int = Field(ge=1)
    uid: str | None = Field(default=None, pattern=r"^[1-9][0-9]*$")
    name: str
    message: str | None = None
    masked: Literal[True] | None = None


class UserSupportersOutput(StrictModel):
    schema_version: Literal[1] = 1
    uid: str = Field(pattern=r"^[1-9][0-9]*$", description="Creator UID, not a supporter UID.")
    coverage: Literal["visible_subset"] = "visible_subset"
    items: list[SupporterMember] = Field(max_length=_MAX_VISIBLE_SUPPORTERS)
    fetched_at: str


class _SupporterShowcase(StrictModel):
    items: list[SupporterMember] = Field(max_length=_MAX_VISIBLE_SUPPORTERS)
    fetched_at: str


class SupporterService:
    """Use the existing authenticated transport and shared persistent public cache."""

    def __init__(self, transport: BilibiliTransport, cache: AsyncTTLCache) -> None:
        """Bind shared resources without creating another client or database."""
        self._transport = transport
        self._cache = cache

    async def list_user_supporters(self, uid: str) -> tuple[UserSupportersOutput, bool]:
        """Return the source's one visible showcase without inventing a full inventory or pages."""
        try:
            ListUserSupportersInput(uid=uid)
        except ValidationError as error:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT, "uid must be a positive decimal string"
            ) from error

        async def produce() -> _SupporterShowcase:
            """Cache only publicly displayed identities and explicitly visible messages."""
            async with self._transport.scoped_work(max_attempts=2):
                try:
                    raw = await self._transport.request_data(
                        _SUPPORTERS_PATH,
                        {"up_mid": uid},
                        not_found_codes=frozenset({-404, 88214}),
                    )
                except BusinessBudgetExhausted as error:
                    raise ServiceError(
                        ErrorCode.RATE_LIMITED, "supporter lookup exhausted its request budget"
                    ) from error
            return _normalize_showcase(raw, uid=uid)

        try:
            showcase, hit = await self._cache.get_or_create(
                ("user_supporters", uid, "v1"), _CACHE_TTL_SECONDS, produce
            )
        except ValidationError as error:
            raise schema_error("supporter showcase fields") from error
        return UserSupportersOutput(
            uid=uid, items=showcase.items, fetched_at=showcase.fetched_at
        ), hit


def _normalize_showcase(raw: object, *, uid: str) -> _SupporterShowcase:
    """Preserve the displayed rank order and discard unverified aggregate statistics."""
    data = as_object(raw, "supporter showcase")
    rows = get_list(data, "list", "supporter showcase")
    if len(rows) > _MAX_VISIBLE_SUPPORTERS:
        raise schema_error("supporter showcase size")
    members = [_normalize_supporter(value, uid=uid) for value in rows]
    ranks = [member.rank for member in members]
    if ranks != sorted(set(ranks)):
        raise schema_error("supporter showcase rank order")
    # Source count fields lack a verified public period and people/payment interpretation.
    # They cannot turn this short showcase into a claimed complete supporter inventory.
    return _SupporterShowcase(
        items=members, fetched_at=datetime.now(TAIPEI).isoformat(timespec="seconds")
    )


def _normalize_supporter(raw: object, *, uid: str) -> SupporterMember:
    """Distinguish the creator from the supporter and retain only explicit public messages."""
    row = as_object(raw, "supporter")
    if get_int(row, "mid", "supporter") != int(uid):
        raise schema_error("supporter creator identity")
    supporter_uid = get_int(row, "pay_mid", "supporter")
    masked = supporter_uid <= 0
    # A missing visibility marker never authorizes publishing a message.
    # Both documented spellings must agree when a source supplies both.
    message = _public_message(row)
    return SupporterMember(
        rank=get_int(row, "rank", "supporter"),
        uid=None if masked else str(supporter_uid),
        name=get_str(row, "uname", "supporter"),
        message=message,
        masked=True if masked else None,
    )


def _public_message(row: JsonObject) -> str | None:
    """Return a nonempty message only when all supplied visibility markers equal zero."""
    flags = [row[key] for key in ("message_hidden", "msg_hidden") if key in row]
    if not flags or any(type(flag) is not int or flag != 0 for flag in flags):
        return None
    message = row.get("message")
    if message is None:
        return None
    if not isinstance(message, str):
        raise schema_error("supporter.message")
    return message if message.strip() else None
