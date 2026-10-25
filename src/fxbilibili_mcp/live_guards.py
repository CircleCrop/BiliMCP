"""Read current public live subscriptions with bounded official pagination."""

from __future__ import annotations

from datetime import datetime

from pydantic import ValidationError

from .cache import AsyncTTLCache
from .live import GetLiveRoomInput, LiveService
from .medals import _normalize_guard_tier
from .models import (
    CachedLiveGuardsPage,
    ErrorCode,
    LiveGuardMember,
    LiveGuardsOutput,
    ServiceError,
)
from .normalization import TAIPEI, as_object, get_int, get_list, get_object, get_str, schema_error
from .tokens import CursorTokenStore, LiveGuardsCursorState
from .transport import BilibiliTransport, BusinessBudgetExhausted

_GUARDS_PATH = "/xlive/app-room/v2/guardTab/topListNew"
_PAGE_SIZE = 10
_CACHE_TTL = 3600


class LiveGuardsService:
    """Use one authenticated account's scheduler and persistent public cache."""

    def __init__(
        self,
        transport: BilibiliTransport,
        cache: AsyncTTLCache,
        token_store: CursorTokenStore,
        *,
        live_service: LiveService,
    ) -> None:
        """Bind shared resources without creating an additional client or database."""
        self._transport = transport
        self._cache = cache
        self._tokens = token_store
        self._live = live_service

    async def list_live_guards(
        self,
        room_id: str | None = None,
        uid: str | None = None,
        cursor: str | None = None,
    ) -> tuple[LiveGuardsOutput, bool]:
        """Read one source page, keeping repeated top-three entries only on the first page."""
        try:
            GetLiveRoomInput(room_id=room_id, uid=uid)
        except ValidationError as error:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT, "provide exactly one positive string room_id or uid"
            ) from error
        mapping_hit = True
        async with self._transport.scoped_work(max_attempts=6):
            try:
                if cursor is not None:
                    state = self._tokens.resolve_live_guards(
                        cursor, requested_room_id=room_id, requested_uid=uid
                    )
                    standard_room, anchor_uid, page = state.room_id, state.uid, state.next_page
                elif uid is not None:
                    mapped_room, mapping_hit = await self._live.room_for_uid(uid)
                    if mapped_room is None:
                        raise ServiceError(ErrorCode.NOT_FOUND, "this user has no public live room")
                    standard_room, anchor_uid, page = mapped_room, uid, 1
                else:
                    assert room_id is not None
                    snapshot, mapping_hit = await self._live.room_snapshot(room_id)
                    standard_room, anchor_uid, page = snapshot.room_id, snapshot.uid, 1

                async def produce() -> CachedLiveGuardsPage:
                    """Retain only public member identities, tiers and source pagination."""
                    raw = await self._transport.request_live_data(
                        _GUARDS_PATH,
                        {
                            "roomid": standard_room,
                            "ruid": anchor_uid,
                            "page": page,
                            "page_size": _PAGE_SIZE,
                        },
                    )
                    return _normalize_page(raw, uid=anchor_uid, page=page)

                projection, page_hit = await self._cache.get_or_create(
                    ("live_guards", standard_room, anchor_uid, page, _PAGE_SIZE, "v1"),
                    _CACHE_TTL,
                    produce,
                )
            except BusinessBudgetExhausted as error:
                raise ServiceError(
                    ErrorCode.RATE_LIMITED, "live-member lookup exhausted its request budget"
                ) from error
            except ValidationError as error:
                raise schema_error("live_guards member fields") from error
        next_cursor = (
            self._tokens.issue(
                LiveGuardsCursorState(
                    requested_room_id=room_id,
                    requested_uid=uid,
                    room_id=standard_room,
                    uid=anchor_uid,
                    next_page=page + 1,
                )
            )
            if projection.has_next
            else None
        )
        return LiveGuardsOutput(
            room_id=standard_room,
            uid=anchor_uid,
            total_count=projection.total_count,
            items=projection.items,
            next_cursor=next_cursor,
            fetched_at=projection.fetched_at,
        ), mapping_hit and page_hit


def _normalize_page(raw: object, *, uid: str, page: int) -> CachedLiveGuardsPage:
    """Validate official page identity and exclude viewer-specific subscription data."""
    data = as_object(raw, "live_guards")
    info = get_object(data, "info", "live_guards")
    total = get_int(info, "num", "live_guards.info")
    page_count = get_int(info, "page", "live_guards.info")
    now = get_int(info, "now", "live_guards.info")
    if total < 0 or page_count < 1 or now != page:
        raise schema_error("live_guards.info")
    top = get_list(data, "top3", "live_guards")
    rows = get_list(data, "list", "live_guards")
    if len(top) > 3 or len(rows) > _PAGE_SIZE:
        raise schema_error("live_guards page size")
    # The endpoint repeats top3 even on an empty out-of-range page.
    selected = [*top, *rows] if page == 1 else rows
    members = [_normalize_member(value, uid=uid) for value in selected]
    ranks = [member.rank for member in members]
    if ranks != sorted(set(ranks)):
        raise schema_error("live_guards member ranks")
    if total == 0 and members:
        raise schema_error("live_guards empty inventory")
    return CachedLiveGuardsPage(
        total_count=total,
        items=members,
        has_next=page < page_count and bool(rows),
        fetched_at=datetime.now(TAIPEI).isoformat(timespec="seconds"),
    )


def _normalize_member(raw: object, *, uid: str) -> LiveGuardMember:
    """Keep displayed names and suppress identifiers when upstream masks the identity."""
    row = as_object(raw, "live_guards.member")
    if get_int(row, "ruid", "live_guards.member") != int(uid):
        raise schema_error("live_guards broadcaster identity")
    user = get_object(row, "uinfo", "live_guards.member")
    base = get_object(user, "base", "live_guards.member")
    guard = get_object(user, "guard", "live_guards.member")
    tier = _normalize_guard_tier(get_int(guard, "level", "live_guards.member"))
    if tier is None:
        raise schema_error("live_guards active tier")
    mystery = base.get("is_mystery", False)
    if not isinstance(mystery, bool):
        raise schema_error("live_guards masked identity")
    # origin_info can reveal the name hidden by a displayed mystery identity.
    # Public projections must use base.name and omit the provided UID when masked.
    member_uid = 0 if mystery else get_int(user, "uid", "live_guards.member")
    masked = mystery or member_uid <= 0
    return LiveGuardMember(
        rank=get_int(row, "rank", "live_guards.member"),
        uid=None if masked else str(member_uid),
        name=get_str(base, "name", "live_guards.member"),
        guard_tier=tier,
        masked=True if masked else None,
    )
