"""Public video collection projections and account-bound incremental versions."""

from __future__ import annotations

import hashlib
from datetime import datetime
from functools import partial
from typing import Annotated, Literal

from pydantic import Field, TypeAdapter

from .cache import AsyncTTLCache, KeyedLocks
from .content import plain_text
from .models import (
    ArticleImage,
    ErrorCode,
    ServiceError,
    StrictModel,
)
from .models import (
    CollectionTarget as CollectionTarget,
)
from .models import (
    PublicFavoritesTarget as PublicFavoritesTarget,
)
from .models import (
    VideoSeasonTarget as VideoSeasonTarget,
)
from .models import (
    VideoSeriesTarget as VideoSeriesTarget,
)
from .normalization import (
    TAIPEI,
    JsonObject,
    JsonValue,
    as_object,
    format_timestamp,
    get_int,
    get_list,
    get_object,
    get_str,
    is_valid_bvid,
    schema_error,
    validate_decimal_id,
)
from .persistent_cache import HistoryItem, HistorySnapshot, PersistentHistoryCache
from .ranges import ITEM_MAX_SPAN, parse_range, resolve_range
from .transport import BilibiliTransport, BusinessBudgetExhausted

_ID = r"^[1-9][0-9]*$"
_VERSION = "collection-v2"
_PAGE_SIZE = 20


class CollectionOwner(StrictModel):
    uid: str = Field(pattern=_ID)
    name: str | None = None


class CollectionMetadata(StrictModel):
    target: CollectionTarget
    title: str
    owner: CollectionOwner
    available_count: int = Field(ge=0)


class CollectionDirectoryEntry(StrictModel):
    target: CollectionTarget
    title: str
    available_count: int = Field(ge=0)


class CollectionDirectoryOutput(StrictModel):
    schema_version: Literal[1] = 1
    uid: str = Field(pattern=_ID)
    kind: Literal["public_favorites", "video_collections"]
    items: list[CollectionDirectoryEntry] = Field(max_length=10)
    available_count: int = Field(ge=0)
    returned_range: str | None
    next_range: str | None
    fetched_at: str


class _DirectoryPage(StrictModel):
    items: list[CollectionDirectoryEntry]
    total: int = Field(ge=0)
    fetched_at: str


class CollectionVideo(StrictModel):
    kind: Literal["video"] = "video"
    bvid: str = Field(pattern=r"^BV[0-9A-Za-z]{10}$")
    title: str
    duration_seconds: int | None = Field(default=None, ge=0)
    published_at: str | None = None
    author: CollectionOwner | None = None


class UnavailableCollectionMember(StrictModel):
    kind: Literal["unavailable"] = "unavailable"
    title: str | None = None
    reason: Literal["deleted_or_unavailable", "unsupported_resource"]


type CollectionMember = Annotated[
    CollectionVideo | UnavailableCollectionMember, Field(discriminator="kind")
]


class CollectionConsistency(StrictModel):
    mode: Literal["incremental_listing"] = "incremental_listing"
    fetched_from: str
    fetched_through: str


class CollectionOutput(StrictModel):
    schema_version: Literal[1] = 1
    collection: CollectionMetadata
    items: list[CollectionMember] = Field(max_length=10)
    collection_version: str
    returned_range: str | None
    next_range: str | None
    fetched_at: str
    consistency: CollectionConsistency


class _CollectionFrontier(StrictModel):
    metadata: CollectionMetadata
    metadata_signature: str
    last_page: int = Field(ge=1)
    last_page_signature: str
    fetched_from: str
    fetched_through: str
    invalidated: bool = False


class _CollectionPage(StrictModel):
    metadata: CollectionMetadata
    metadata_signature: str
    members: tuple[CollectionMember, ...]
    exhausted: bool


_MEMBER_ADAPTER: TypeAdapter[CollectionMember] = TypeAdapter(CollectionMember)


def _fingerprint(target: CollectionTarget) -> str:
    return hashlib.sha256(target.model_dump_json().encode()).hexdigest()


def _page_signature(members: tuple[CollectionMember, ...]) -> str:
    # Compare positions, including repeated and deleted entries; deduplication would shift ranges.
    value = "\n".join(member.model_dump_json() for member in members)
    return hashlib.sha256(value.encode()).hexdigest()


def _now() -> str:
    return datetime.now(TAIPEI).isoformat(timespec="seconds")


def _frontier(snapshot: HistorySnapshot) -> _CollectionFrontier:
    """Validate persisted progress before using its private page position."""
    if snapshot.frontier is None:
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT, "collection version is incomplete; restart at 1"
        )
    return _CollectionFrontier.model_validate_json(snapshot.frontier)


def _output(snapshot: HistorySnapshot, range_value: str) -> CollectionOutput:
    """Slice committed members without inventing unobserved positions or completion."""
    state = _frontier(snapshot)
    total = state.metadata.available_count
    if total == 0:
        if parse_range(range_value, max_span=ITEM_MAX_SPAN).start != 1:
            raise ServiceError(ErrorCode.INVALID_RANGE, "range starts beyond the empty collection")
        resolved = None
        next_range = None
        members: list[CollectionMember] = []
    else:
        window = resolve_range(range_value, total=total, max_span=ITEM_MAX_SPAN)
        if window.end > snapshot.item_count:
            raise ServiceError(
                ErrorCode.UPSTREAM_SCHEMA_CHANGED, "collection omitted requested members"
            )
        members = [
            _MEMBER_ADAPTER.validate_python(item.payload)
            for item in snapshot.items[
                window.start - 1 - snapshot.item_offset : window.end - snapshot.item_offset
            ]
        ]
        resolved = window.resolved
        next_range = window.next_range
    return CollectionOutput(
        collection=state.metadata,
        items=members,
        collection_version=snapshot.generation,
        returned_range=resolved,
        next_range=next_range,
        fetched_at=state.fetched_through,
        consistency=CollectionConsistency(
            fetched_from=state.fetched_from, fetched_through=state.fetched_through
        ),
    )


class CollectionService:
    """Persist bounded public listings and verify changes before extending their order."""

    def __init__(
        self,
        transport: BilibiliTransport,
        history: PersistentHistoryCache,
        *,
        ttl_seconds: int = 3600,
    ) -> None:
        """Share the runtime's transport and history lifecycle without opening resources."""
        self._transport = transport
        self._history = history
        self._ttl_seconds = ttl_seconds
        self._write_locks = KeyedLocks()

    async def get_cover(
        self, target: CollectionTarget, cache: AsyncTTLCache
    ) -> tuple[tuple[ArticleImage, str], bool]:
        """Resolve one collection cover without traversing its member list."""
        # Visibility must be checked even when public image bytes are already cached.
        favorite = (
            await self._favorite_info(target) if isinstance(target, PublicFavoritesTarget) else None
        )

        async def produce() -> tuple[ArticleImage, str]:
            """Validate ownership and normalize only the requested cover metadata."""
            if isinstance(target, PublicFavoritesTarget):
                assert favorite is not None
                metadata = _favorite_metadata(favorite, target)
                cover = favorite.get("cover")
                original_url = (
                    f"https://space.bilibili.com/{metadata.owner.uid}/favlist"
                    f"?fid={target.favorite_list_id}"
                )
            elif isinstance(target, VideoSeriesTarget):
                info = as_object(
                    await self._transport.request_data(
                        "/x/series/series", {"series_id": target.series_id}
                    ),
                    "series info",
                )
                _series_metadata(info, target)
                cover = get_object(info, "meta", "series").get("cover")
                if not cover:
                    # Series detail omits the cover shown in the space directory;
                    # that displayed cover is the newest member's thumbnail.
                    data = as_object(
                        await self._transport.request_data(
                            "/x/series/archives",
                            {
                                "mid": target.owner_uid,
                                "series_id": target.series_id,
                                "pn": 1,
                                "ps": 1,
                                "sort": "desc",
                            },
                            wbi=True,
                        ),
                        "series cover",
                    )
                    archives = get_list(data, "archives", "series cover")
                    cover = (
                        as_object(archives[0], "series cover archive").get("pic")
                        if archives
                        else None
                    )
                original_url = f"https://space.bilibili.com/{target.owner_uid}/lists/{target.series_id}?type=series"
            else:
                data = as_object(
                    await self._transport.request_data(
                        "/x/polymer/web-space/seasons_archives_list",
                        {
                            "mid": target.owner_uid,
                            "season_id": target.season_id,
                            "sort_reverse": "false",
                            "page_num": 1,
                            "page_size": 1,
                        },
                        wbi=True,
                    ),
                    "season cover",
                )
                _season_metadata(data, target)
                cover = get_object(data, "meta", "season").get("cover")
                original_url = f"https://space.bilibili.com/{target.owner_uid}/lists/{target.season_id}?type=season"
            if cover is None or cover == "":
                raise ServiceError(ErrorCode.NOT_FOUND, "collection has no cover")
            if not isinstance(cover, str):
                raise schema_error("collection cover")
            return ArticleImage(index=1, url=cover, width=0, height=0), original_url

        if favorite is not None:
            return await produce(), False
        result, hit = await cache.get_or_create(
            ("collection-cover", _fingerprint(target), "v1"), self._ttl_seconds, produce
        )
        return result, hit

    async def list_user_collections(
        self,
        uid: str,
        kind: Literal["public_favorites", "video_collections"],
        range_value: str,
        cache: AsyncTTLCache,
    ) -> tuple[CollectionDirectoryOutput, bool]:
        """Discover visible targets with cached directory pages and no private folder metadata."""
        validate_decimal_id(uid, "uid")
        parsed = parse_range(range_value, max_span=ITEM_MAX_SPAN)
        if kind not in {"public_favorites", "video_collections"}:
            raise ServiceError(ErrorCode.INVALID_ARGUMENT, "unsupported collection directory kind")
        first, hit = await cache.get_or_create(
            ("collection_directory", uid, kind, 1),
            self._ttl_seconds,
            lambda: self._directory_page(uid, kind, 1),
        )
        if first.total == 0:
            if parsed.start != 1:
                raise ServiceError(ErrorCode.INVALID_RANGE, "range starts beyond empty directory")
            return CollectionDirectoryOutput(
                uid=uid,
                kind=kind,
                items=[],
                available_count=0,
                returned_range=None,
                next_range=None,
                fetched_at=first.fetched_at,
            ), hit
        window = resolve_range(range_value, total=first.total, max_span=ITEM_MAX_SPAN)
        if kind == "public_favorites":
            items = first.items[window.python_slice]
        else:
            items = []
            for page in range(
                (window.start - 1) // _PAGE_SIZE + 1, (window.end - 1) // _PAGE_SIZE + 2
            ):
                directory, page_hit = (
                    (first, hit)
                    if page == 1
                    else await cache.get_or_create(
                        ("collection_directory", uid, kind, page),
                        self._ttl_seconds,
                        partial(self._directory_page, uid, kind, page),
                    )
                )
                hit = hit and page_hit
                if directory.total != first.total:
                    raise ServiceError(
                        ErrorCode.UPSTREAM_SCHEMA_CHANGED,
                        "collection directory changed; restart its first range",
                    )
                offset = (page - 1) * _PAGE_SIZE
                items.extend(
                    directory.items[max(0, window.start - 1 - offset) : window.end - offset]
                )
        if len(items) != window.end - window.start + 1:
            raise schema_error("collection directory omitted positions")
        return CollectionDirectoryOutput(
            uid=uid,
            kind=kind,
            items=items,
            available_count=first.total,
            returned_range=window.resolved,
            next_range=window.next_range,
            fetched_at=first.fetched_at,
        ), hit

    async def _directory_page(
        self,
        uid: str,
        kind: Literal["public_favorites", "video_collections"],
        page: int,
    ) -> _DirectoryPage:
        """Validate directory JSON and strip previews and private fields."""
        if kind == "public_favorites":
            raw = await self._transport.request_data(
                "/x/v3/fav/folder/created/list-all",
                {"up_mid": int(uid)},
            )
            if raw is None:
                raise ServiceError(ErrorCode.NOT_FOUND, "user's favorites directory is hidden")
            data = as_object(raw, "favorites directory")
            values = (
                [] if data.get("list") is None else get_list(data, "list", "favorites directory")
            )
            entries = []
            for value in values:
                item = as_object(value, "favorite directory entry")
                # An authenticated owner can see private folders; never expose those entries.
                if get_int(item, "attr", "favorite directory entry") & 1:
                    continue
                if get_int(item, "mid", "favorite directory entry") != int(uid):
                    raise schema_error("favorite directory owner")
                entries.append(
                    CollectionDirectoryEntry(
                        target=PublicFavoritesTarget(
                            kind="public_favorites",
                            favorite_list_id=str(get_int(item, "id", "favorite directory entry")),
                        ),
                        title=plain_text(get_str(item, "title", "favorite directory entry")),
                        available_count=get_int(item, "media_count", "favorite directory entry"),
                    )
                )
            return _DirectoryPage(items=entries, total=len(entries), fetched_at=_now())
        raw = await self._transport.request_data(
            "/x/polymer/web-space/seasons_series_list",
            {"mid": int(uid), "page_num": page, "page_size": _PAGE_SIZE, "web_location": "333.999"},
            wbi=True,
        )
        lists = get_object(as_object(raw, "video directory"), "items_lists", "video directory")
        pagination = get_object(lists, "page", "video directory")
        if get_int(pagination, "page_num", "video directory") != page:
            raise schema_error("video directory page")
        entries = []
        # The upstream groups each page into seasons and series; preserve that page grouping.
        for field, target_kind in (
            ("seasons_list", "video_season"),
            ("series_list", "video_series"),
        ):
            for value in get_list(lists, field, "video directory"):
                meta = get_object(
                    as_object(value, "video directory entry"), "meta", "video directory"
                )
                if get_int(meta, "mid", "video directory entry") != int(uid):
                    raise schema_error("video directory owner")
                target = (
                    VideoSeasonTarget(
                        kind="video_season",
                        owner_uid=uid,
                        season_id=str(get_int(meta, "season_id", "video directory entry")),
                    )
                    if target_kind == "video_season"
                    else VideoSeriesTarget(
                        kind="video_series",
                        owner_uid=uid,
                        series_id=str(get_int(meta, "series_id", "video directory entry")),
                    )
                )
                entries.append(
                    CollectionDirectoryEntry(
                        target=target,
                        title=plain_text(get_str(meta, "name", "video directory entry")),
                        available_count=get_int(meta, "total", "video directory entry"),
                    )
                )
        total = get_int(pagination, "total", "video directory")
        if len(entries) != min(_PAGE_SIZE, max(0, total - (page - 1) * _PAGE_SIZE)):
            raise schema_error("video directory page size")
        return _DirectoryPage(items=entries, total=total, fetched_at=_now())

    async def list_collection(
        self,
        target: CollectionTarget,
        *,
        range_value: str = "1..10",
        collection_version: str | None = None,
    ) -> tuple[CollectionOutput, bool]:
        """Read one public range, extending a bound generation within eight actual attempts."""
        parsed = parse_range(range_value, max_span=ITEM_MAX_SPAN)
        if collection_version is None and parsed.start != 1:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT,
                "first collection range must start at 1; use collection_version",
            )
        account = self._transport.account_uid
        if not account:
            raise ServiceError(
                ErrorCode.CREDENTIALS_INVALID, "verified account identity is unavailable"
            )
        fingerprint = _fingerprint(target)
        async with (
            self._write_locks.hold((account, fingerprint)),
            self._transport.scoped_work(max_attempts=8),
        ):
            snapshot = await self._read_snapshot(account, fingerprint, collection_version)
            if snapshot is not None:
                state = _frontier(snapshot)
                if state.invalidated:
                    if collection_version is not None:
                        raise _version_changed()
                    snapshot = None
                elif collection_version is None:
                    age = (
                        datetime.now(TAIPEI) - datetime.fromisoformat(state.fetched_from)
                    ).total_seconds()
                    if age >= self._ttl_seconds:
                        snapshot = None
            needs_initial = snapshot is None
            if snapshot is not None:
                state = _frontier(snapshot)
                required = min(parsed.end, state.metadata.available_count)
                if parsed.start > snapshot.item_count + 1 and not snapshot.source_exhausted:
                    raise ServiceError(
                        ErrorCode.INVALID_RANGE, "continue using the returned next_range"
                    )
                if required <= snapshot.item_count:
                    # Owners can withdraw public access while a content generation remains cached.
                    if isinstance(target, PublicFavoritesTarget):
                        await self._favorite_info(target)
                    return await self._output(snapshot, range_value, account, fingerprint), True
            try:
                async with self._transport.scoped_work(max_attempts=8):
                    if needs_initial:
                        snapshot = await self._create_snapshot(target, account, fingerprint)
                    assert snapshot is not None
                    snapshot = await self._extend_snapshot(
                        snapshot, target, account, fingerprint, parsed.end
                    )
            except BusinessBudgetExhausted as exc:
                raise ServiceError(
                    ErrorCode.RATE_LIMITED,
                    "collection request budget exhausted; retry the same range",
                ) from exc
            return await self._output(snapshot, range_value, account, fingerprint), False

    async def _output(
        self, snapshot: HistorySnapshot, range_value: str, account: str, fingerprint: str
    ) -> CollectionOutput:
        """Load only the requested output positions after frontier validation and extension."""
        parsed = parse_range(range_value, max_span=ITEM_MAX_SPAN)
        window = await self._history.read_generation(
            snapshot.generation,
            scope=account,
            kind="collection",
            query_fingerprint=fingerprint,
            normalization_version=_VERSION,
            item_offset=parsed.start - 1,
            item_limit=parsed.end - parsed.start + 1,
        )
        if window is None:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT, "collection version expired; restart at 1"
            )
        return _output(window, range_value)

    async def _read_snapshot(
        self, account: str, fingerprint: str, version: str | None
    ) -> HistorySnapshot | None:
        """Reject missing or cross-account continuations rather than rebuilding them."""
        if version is None:
            snapshot = await self._history.latest_generation(
                account, "collection", fingerprint, _VERSION, item_limit=0
            )
            # Cancellation before the first commit leaves no readable index.
            return snapshot if snapshot is not None and snapshot.frontier is not None else None
        snapshot = await self._history.read_generation(
            version,
            scope=account,
            kind="collection",
            query_fingerprint=fingerprint,
            normalization_version=_VERSION,
            item_limit=0,
        )
        if snapshot is None:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT, "collection version expired; restart at 1"
            )
        return snapshot

    async def _create_snapshot(
        self, target: CollectionTarget, account: str, fingerprint: str
    ) -> HistorySnapshot:
        """Commit the first public page and its ordering as a new generation."""
        page = await self._fetch_page(target, 1)
        fetched = _now()
        generation = await self._history.create_generation(
            account, "collection", fingerprint, _VERSION
        )
        state = _CollectionFrontier(
            metadata=page.metadata,
            metadata_signature=page.metadata_signature,
            last_page=1,
            last_page_signature=_page_signature(page.members),
            fetched_from=fetched,
            fetched_through=fetched,
        )
        await self._history.commit_batch(
            generation, _history_items(page.members, 0), state.model_dump_json(), page.exhausted
        )
        snapshot = await self._read_snapshot(account, fingerprint, generation)
        assert snapshot is not None
        return snapshot

    async def _fetch_page(self, target: CollectionTarget, page: int) -> _CollectionPage:
        """Read one fixed collection endpoint and immediately normalize its public projection."""
        if isinstance(target, VideoSeriesTarget):
            meta = as_object(
                await self._transport.request_data(
                    "/x/series/series", {"series_id": target.series_id}
                ),
                "series info",
            )
            _series_metadata(meta, target)
            data = as_object(
                await self._transport.request_data(
                    "/x/series/archives",
                    {
                        "mid": target.owner_uid,
                        "series_id": target.series_id,
                        "pn": page,
                        "ps": _PAGE_SIZE,
                        "sort": "desc",
                    },
                    wbi=True,
                ),
                "series members",
            )
            return _series_page(meta, data, target, page)
        if isinstance(target, PublicFavoritesTarget):
            await self._favorite_info(target)
            identities = await self._transport.request_data(
                "/x/v3/fav/resource/ids",
                {"media_id": target.favorite_list_id, "platform": "web"},
            )
            data = as_object(
                await self._transport.request_data(
                    "/x/v3/fav/resource/list",
                    {
                        "media_id": target.favorite_list_id,
                        "pn": page,
                        "ps": _PAGE_SIZE,
                        "order": "mtime",
                        "type": 0,
                        "tid": 0,
                        "platform": "web",
                        "web_location": "333.1387",
                    },
                ),
                "favorite members",
            )
            return _favorite_page(data, identities, target, page)
        data = as_object(
            await self._transport.request_data(
                "/x/polymer/web-space/seasons_archives_list",
                {
                    "mid": target.owner_uid,
                    "season_id": target.season_id,
                    "sort_reverse": "false",
                    "page_num": page,
                    "page_size": _PAGE_SIZE,
                },
                wbi=True,
            ),
            "season members",
        )
        return _season_page(data, target, page)

    async def _favorite_info(self, target: PublicFavoritesTarget) -> JsonObject:
        """Check the owner's current public setting before reading folder members or cached data."""
        data = as_object(
            await self._transport.request_data(
                "/x/v3/fav/folder/info",
                {"media_id": target.favorite_list_id},
                not_found_codes=frozenset({-404, 62002, 10003}),
            ),
            "favorite info",
        )
        _favorite_metadata(data, target)
        return data

    async def _extend_snapshot(
        self,
        snapshot: HistorySnapshot,
        target: CollectionTarget,
        account: str,
        fingerprint: str,
        requested_end: int,
    ) -> HistorySnapshot:
        """Recheck the last page before appending to detect observed reorderings."""
        state = _frontier(snapshot)
        required = min(requested_end, state.metadata.available_count)
        if required <= snapshot.item_count or snapshot.source_exhausted:
            return snapshot
        previous = await self._fetch_page(target, state.last_page)
        if (
            previous.metadata_signature != state.metadata_signature
            or _page_signature(previous.members) != state.last_page_signature
        ):
            state.invalidated = True
            await self._history.commit_batch(
                snapshot.generation, (), state.model_dump_json(), snapshot.source_exhausted
            )
            raise _version_changed()
        while snapshot.item_count < required and not snapshot.source_exhausted:
            page_number = state.last_page + 1
            page = await self._fetch_page(target, page_number)
            if page.metadata_signature != state.metadata_signature:
                state.invalidated = True
                await self._history.commit_batch(
                    snapshot.generation, (), state.model_dump_json(), False
                )
                raise _version_changed()
            state.last_page = page_number
            state.last_page_signature = _page_signature(page.members)
            state.fetched_through = _now()
            await self._history.commit_batch(
                snapshot.generation,
                _history_items(page.members, snapshot.item_count),
                state.model_dump_json(),
                page.exhausted,
            )
            updated = await self._read_snapshot(account, fingerprint, snapshot.generation)
            assert updated is not None
            snapshot = updated
        return snapshot


def _history_items(members: tuple[CollectionMember, ...], offset: int) -> tuple[HistoryItem, ...]:
    """Persist ordinal identities to preserve duplicates and unavailable positions."""
    return tuple(
        HistoryItem(
            key=f"position:{offset + index + 1:012d}",
            published_at=member.published_at if isinstance(member, CollectionVideo) else None,
            payload=member.model_dump(mode="json", exclude_none=True),
        )
        for index, member in enumerate(members)
    )


def _season_page(data: JsonObject, target: VideoSeasonTarget, page_number: int) -> _CollectionPage:
    """Use the provider's ordered aids, preserving omitted archives as unavailable positions."""
    metadata = _season_metadata(data, target)
    meta = get_object(data, "meta", "season")
    page = get_object(data, "page", "season")
    _check_page(
        page, page_number, metadata.available_count, number_key="page_num", size_key="page_size"
    )
    members = _ordered_archives(data)
    exhausted = _check_page_size(len(members), page_number, metadata.available_count)
    return _CollectionPage(
        metadata=metadata,
        metadata_signature=_metadata_signature(metadata, get_int(meta, "ptime", "season.meta")),
        members=members,
        exhausted=exhausted,
    )


def _season_metadata(data: JsonObject, target: VideoSeasonTarget) -> CollectionMetadata:
    """Validate the same season identity for listings and on-demand cover reads."""
    meta = get_object(data, "meta", "season")
    if str(get_int(meta, "season_id", "season.meta")) != target.season_id:
        raise schema_error("season identity")
    if str(get_int(meta, "mid", "season.meta")) != target.owner_uid:
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT, "owner_uid does not own the requested season"
        )
    return CollectionMetadata(
        target=target,
        title=plain_text(get_str(meta, "title", "season.meta")),
        owner=CollectionOwner(uid=target.owner_uid),
        available_count=_nonnegative(meta, "total", "season.meta"),
    )


def _ordered_archives(data: JsonObject) -> tuple[CollectionMember, ...]:
    """Keep provider positions even when an archive is missing or appears repeatedly."""
    aids = get_list(data, "aids", "collection")
    ordered_aids: list[str] = []
    for aid in aids:
        if not isinstance(aid, int) or isinstance(aid, bool) or aid <= 0:
            raise schema_error("collection ordered aids")
        ordered_aids.append(str(aid))
    archives: dict[str, JsonObject] = {}
    for value in get_list(data, "archives", "collection"):
        archive = as_object(value, "collection archive")
        aid = str(get_int(archive, "aid", "collection archive"))
        if aid not in ordered_aids or aid in archives:
            raise schema_error("collection archive identity")
        archives[aid] = archive
    return tuple(
        _archive_member(archives[aid])
        if aid in archives
        else UnavailableCollectionMember(reason="deleted_or_unavailable")
        for aid in ordered_aids
    )


def _archive_member(archive: JsonObject) -> CollectionMember:
    """Retain an unavailable slot for non-public archive states instead of filtering it out."""
    title = plain_text(get_str(archive, "title", "collection archive"))
    if get_int(archive, "state", "collection archive") != 0:
        return UnavailableCollectionMember(title=title, reason="deleted_or_unavailable")
    bvid = get_str(archive, "bvid", "collection archive")
    if not is_valid_bvid(bvid):
        raise schema_error("collection archive bvid")
    return CollectionVideo(
        bvid=bvid,
        title=title,
        duration_seconds=_nonnegative(archive, "duration", "collection archive"),
        published_at=format_timestamp(get_int(archive, "pubdate", "collection archive")),
    )


def _check_page(
    page: JsonObject, page_number: int, total: int, *, number_key: str, size_key: str
) -> None:
    """Reject ignored pagination and inconsistent totals before binding range positions."""
    if (
        get_int(page, number_key, "collection page") != page_number
        or get_int(page, size_key, "collection page") != _PAGE_SIZE
        or get_int(page, "total", "collection page") != total
    ):
        raise schema_error("collection pagination")


def _check_page_size(count: int, page_number: int, total: int) -> bool:
    """Do not declare exhaustion from a short page while metadata still promises more positions."""
    expected = max(0, min(_PAGE_SIZE, total - (page_number - 1) * _PAGE_SIZE))
    if count != expected:
        raise schema_error("collection member count")
    return page_number * _PAGE_SIZE >= total


def _metadata_signature(metadata: CollectionMetadata, modified: int | None) -> str:
    value = metadata.model_dump_json() + ":" + str(modified)
    return hashlib.sha256(value.encode()).hexdigest()


def _nonnegative(source: JsonObject, key: str, context: str) -> int:
    value = get_int(source, key, context)
    if value < 0:
        raise schema_error(f"{context}.{key}")
    return value


def _version_changed() -> ServiceError:
    return ServiceError(
        ErrorCode.INVALID_ARGUMENT,
        "collection changed during pagination; restart from range 1 with no version",
    )


def _series_page(
    info: JsonObject, data: JsonObject, target: VideoSeriesTarget, page_number: int
) -> _CollectionPage:
    """Bind series owner and ordered archive positions to the metadata fetched for this page."""
    metadata = _series_metadata(info, target)
    meta = get_object(info, "meta", "series")
    _check_page(
        get_object(data, "page", "series"),
        page_number,
        metadata.available_count,
        number_key="num",
        size_key="size",
    )
    members = _ordered_archives(data)
    return _CollectionPage(
        metadata=metadata,
        metadata_signature=_metadata_signature(metadata, get_int(meta, "mtime", "series.meta")),
        members=members,
        exhausted=_check_page_size(len(members), page_number, metadata.available_count),
    )


def _series_metadata(info: JsonObject, target: VideoSeriesTarget) -> CollectionMetadata:
    """Validate ownership before fetching the requested series member list."""
    meta = get_object(info, "meta", "series")
    if str(get_int(meta, "series_id", "series.meta")) != target.series_id:
        raise schema_error("series identity")
    if str(get_int(meta, "mid", "series.meta")) != target.owner_uid:
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT, "owner_uid does not own the requested series"
        )
    return CollectionMetadata(
        target=target,
        title=plain_text(get_str(meta, "name", "series.meta")),
        owner=CollectionOwner(uid=target.owner_uid),
        available_count=_nonnegative(meta, "total", "series.meta"),
    )


def _favorite_metadata(info: JsonObject, target: PublicFavoritesTarget) -> CollectionMetadata:
    """Refuse private folders even when the service account is their owner."""
    if str(get_int(info, "id", "favorite info")) != target.favorite_list_id:
        raise schema_error("favorite identity")
    if _nonnegative(info, "attr", "favorite info") & 1:
        raise ServiceError(
            ErrorCode.INVALID_ARGUMENT,
            "favorite folder is private; only public folders are supported",
        )
    upper = get_object(info, "upper", "favorite info")
    return CollectionMetadata(
        target=target,
        title=plain_text(get_str(info, "title", "favorite info")),
        owner=CollectionOwner(
            uid=str(get_int(upper, "mid", "favorite owner")),
            name=plain_text(get_str(upper, "name", "favorite owner")),
        ),
        available_count=_nonnegative(info, "media_count", "favorite info"),
    )


def _favorite_page(
    data: JsonObject, identities: JsonValue, target: PublicFavoritesTarget, page_number: int
) -> _CollectionPage:
    """Keep favorite ordering and deleted or unsupported resources as explicit positions."""
    info = get_object(data, "info", "favorite members")
    metadata = _favorite_metadata(info, target)
    values = data.get("medias")
    if values is None and metadata.available_count == 0:
        values = []
    if not isinstance(values, list):
        raise schema_error("favorite members.medias")
    if not isinstance(identities, list):
        raise schema_error("favorite ordered identities")
    keys = [_favorite_key(as_object(value, "favorite identity")) for value in identities]
    if len(keys) != metadata.available_count:
        raise schema_error("favorite identity count")
    expected = keys[(page_number - 1) * _PAGE_SIZE : page_number * _PAGE_SIZE]
    media: dict[tuple[int, str], JsonObject] = {}
    for value in values:
        item = as_object(value, "favorite member")
        key = _favorite_key(item)
        if key not in expected or key in media:
            raise schema_error("favorite member identity")
        media[key] = item
    # The resource page can omit deleted videos; the full identity list preserves their positions.
    members = tuple(
        _favorite_member(media[key])
        if key in media
        else UnavailableCollectionMember(reason="deleted_or_unavailable")
        for key in expected
    )
    exhausted = _check_page_size(len(members), page_number, metadata.available_count)
    has_more = data.get("has_more")
    if type(has_more) is not bool or has_more == exhausted:
        raise schema_error("favorite pagination")
    return _CollectionPage(
        metadata=metadata,
        metadata_signature=_metadata_signature(metadata, get_int(info, "mtime", "favorite info"))
        + hashlib.sha256(repr(keys).encode()).hexdigest(),
        members=members,
        exhausted=exhausted,
    )


def _favorite_key(item: JsonObject) -> tuple[int, str]:
    resource_id = get_int(item, "id", "favorite resource")
    resource_type = get_int(item, "type", "favorite resource")
    if resource_id <= 0 or resource_type <= 0:
        raise schema_error("favorite resource identity")
    return resource_type, str(resource_id)


def _favorite_member(item: JsonObject) -> CollectionMember:
    """Project only public video metadata and omit details for deleted and unsupported items."""
    title = plain_text(get_str(item, "title", "favorite member"))
    if get_int(item, "type", "favorite member") != 2:
        return UnavailableCollectionMember(title=title, reason="unsupported_resource")
    if _nonnegative(item, "attr", "favorite member") & 1:
        return UnavailableCollectionMember(title=title, reason="deleted_or_unavailable")
    bvid = get_str(item, "bvid", "favorite member")
    if not is_valid_bvid(bvid):
        raise schema_error("favorite member bvid")
    upper = get_object(item, "upper", "favorite member")
    return CollectionVideo(
        bvid=bvid,
        title=title,
        duration_seconds=_nonnegative(item, "duration", "favorite member"),
        published_at=format_timestamp(get_int(item, "pubtime", "favorite member")),
        author=CollectionOwner(
            uid=str(get_int(upper, "mid", "favorite member author")),
            name=plain_text(get_str(upper, "name", "favorite member author")),
        ),
    )
