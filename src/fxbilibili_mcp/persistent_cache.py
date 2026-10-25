"""Bounded SQLite storage for public history batches and account-scoped progress."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import sqlite3
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import TypedDict, TypeVar, cast
from uuid import uuid4

from pydantic import Field, TypeAdapter, ValidationError

from .models import ErrorCode, ServiceError, StrictModel
from .normalization import TAIPEI, JsonObject

_T = TypeVar("_T")
_PAGE_SIZE = 4096
_WAL_FRAME_OVERHEAD = 24
_SHM_BYTES = 32768
_WAL_HEADER_BYTES = 32


class HistoryBinding(TypedDict):
    scope: str
    kind: str
    query_fingerprint: str
    normalization_version: str


class HistoryItem(StrictModel):
    """Persist only a caller-validated public projection under its explicit identity."""

    key: str = Field(min_length=1)
    published_at: str | None = None
    payload: JsonObject


class HistorySnapshot(StrictModel):
    """Return committed ordering and its frontier without changing expiry on reads."""

    generation: str
    items: tuple[HistoryItem, ...]
    item_count: int
    item_offset: int = 0
    frontier: str | None
    source_exhausted: bool
    fetched_at: str
    expires_at: str


class PersistentHistoryCache:
    """Serialize SQLite I/O off-loop and commit content with its scan progress."""

    def __init__(
        self,
        path: Path | None,
        max_bytes: int = 4 * 1024**3,
        ttl_seconds: int = 14 * 86400,
        *,
        source_exclusion_max_items: int = 100_000,
        source_exclusion_max_bytes: int = 8 * 1024**2,
    ) -> None:
        """Own one connection; constructing a disabled runtime creates no files."""
        if min(max_bytes, ttl_seconds, source_exclusion_max_items, source_exclusion_max_bytes) <= 0:
            raise ValueError("persistent history capacity and TTL must be positive")
        self.path = path
        self.max_bytes = max_bytes
        self.ttl_seconds = ttl_seconds
        self.source_exclusion_max_items = source_exclusion_max_items
        self.source_exclusion_max_bytes = source_exclusion_max_bytes
        self._lock = asyncio.Lock()
        self._connection: sqlite3.Connection | None = None
        self._page_limit = 0
        self._blobs_initialized = False

    async def start(self) -> None:
        """Open or initialize the versioned database on a worker thread."""
        await self._run(self._start)

    async def close(self) -> None:
        """Finish pending operations, truncate WAL, and close the owned connection."""
        await self._run(self._close)

    async def create_generation(
        self,
        scope: str,
        kind: str,
        query_fingerprint: str,
        normalization_version: str,
    ) -> str:
        """Create an independent version without replacing existing continuation data."""
        _validate_binding(scope, kind, query_fingerprint, normalization_version)
        return await self._run(
            lambda: self._create_generation(scope, kind, query_fingerprint, normalization_version)
        )

    async def commit_batch(
        self,
        generation: str,
        items: tuple[HistoryItem, ...],
        frontier: str | None,
        source_exhausted: bool,
    ) -> None:
        """Commit one ordered batch and frontier together, settling writes on cancellation."""
        await self._run(lambda: self._commit_batch(generation, items, frontier, source_exhausted))

    async def read_generation(
        self,
        generation: str,
        *,
        scope: str,
        kind: str,
        query_fingerprint: str,
        normalization_version: str,
        item_offset: int = 0,
        item_limit: int = 128,
    ) -> HistorySnapshot | None:
        """Read a bounded window from a live generation with exact ownership bindings."""
        _validate_window(item_offset, item_limit)
        _validate_binding(scope, kind, query_fingerprint, normalization_version)
        return await self._run(
            lambda: self._read_generation(
                generation,
                scope,
                kind,
                query_fingerprint,
                normalization_version,
                item_offset,
                item_limit,
            )
        )

    async def latest_generation(
        self,
        scope: str,
        kind: str,
        query_fingerprint: str,
        normalization_version: str,
        item_offset: int = 0,
        item_limit: int = 128,
    ) -> HistorySnapshot | None:
        """Find the latest live matching version without extending its retention."""
        _validate_window(item_offset, item_limit)
        _validate_binding(scope, kind, query_fingerprint, normalization_version)
        return await self._run(
            lambda: self._latest_generation(
                scope, kind, query_fingerprint, normalization_version, item_offset, item_limit
            )
        )

    async def source_is_unlisted(self, provider: str, uid: str) -> bool:
        """Check a durable source exclusion independently of ordinary history expiry."""
        await self.start()
        return await self._run(
            lambda: self._require_connection()
            .execute(
                "SELECT 1 FROM source_unlisted WHERE provider = ? AND uid = ?",
                (provider, uid),
            )
            .fetchone()
            is not None
        )

    async def mark_source_unlisted(self, provider: str, uid: str) -> None:
        """Record one source's missing UID within the existing database capacity limit."""
        await self.start()
        await self._run(lambda: self._mark_source_unlisted(provider, uid))

    async def require_source_exclusion_capacity(self, provider: str, uid: str) -> None:
        """Stop an unknown-UID cold lookup if its possible permanent refusal cannot fit."""
        await self.start()
        await self._run(lambda: self._require_source_exclusion_capacity(provider, uid))

    def _require_source_exclusion_capacity(self, provider: str, uid: str) -> None:
        """Give the permanent set a separate logical budget without expiring known exclusions."""
        conn = self._require_connection()
        if conn.execute(
            "SELECT 1 FROM source_unlisted WHERE provider=? AND uid=?", (provider, uid)
        ).fetchone():
            return
        count, size = conn.execute(
            "SELECT COUNT(*),COALESCE(SUM(length(CAST(provider AS BLOB)) "
            "+ length(CAST(uid AS BLOB)) + 32),0) FROM source_unlisted"
        ).fetchone()
        # Account for keys and a fixed row allowance; the enclosing DB still has its hard page cap.
        proposed_bytes = len(provider.encode()) + len(uid.encode()) + 32
        if (
            count >= self.source_exclusion_max_items
            or size + proposed_bytes > self.source_exclusion_max_bytes
        ):
            logging.getLogger("fxbilibili_mcp").warning(
                "source_exclusion_capacity_exceeded count=%d logical_bytes=%d", count, size
            )
            raise ServiceError(
                ErrorCode.RESOURCE_LIMIT_EXCEEDED,
                "Permanent source exclusion capacity is full; new UID scans are paused. "
                "Do not retry until the administrator increases capacity.",
            )

    async def get_projection(
        self, provider: str, key: str, normalization_version: str
    ) -> tuple[JsonObject, float] | None:
        """Return an unexpired JSON projection and its original epoch expiry."""
        await self.start()
        return await self._run(lambda: self._get_projection(provider, key, normalization_version))

    async def set_projection(
        self,
        provider: str,
        key: str,
        normalization_version: str,
        payload: JsonObject,
        expires_at: float,
    ) -> None:
        """Store a public projection without extending its caller-supplied expiration."""
        if not math.isfinite(expires_at):
            raise ServiceError(ErrorCode.INVALID_ARGUMENT, "projection expiry must be finite")
        try:
            # Full-session public snapshots can contain tens of thousands of events.
            encoded = await asyncio.to_thread(
                json.dumps, payload, ensure_ascii=False, allow_nan=False, separators=(",", ":")
            )
        except ValueError as exc:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT, "projection must contain finite JSON values"
            ) from exc
        await self.start()
        await self._run(
            lambda: self._set_projection(provider, key, normalization_version, encoded, expires_at)
        )

    async def delete_projection(self, provider: str, key: str, normalization_version: str) -> None:
        """Delete one normalized cache key without affecting other source namespaces."""
        await self.start()
        await self._run(
            lambda: self._delete_projections(provider, key=key, version=normalization_version)
        )

    async def get_blob(self, provider: str, key: str) -> tuple[bytes, JsonObject, float] | None:
        """Read public image bytes and metadata with their original expiration."""
        await self.start()

        def read() -> tuple[bytes, JsonObject, float] | None:
            """Read one live binary entry without serializing its body as JSON."""
            self._ensure_blobs()
            row = (
                self._require_connection()
                .execute(
                    "SELECT body, metadata_json, expires_at FROM public_blobs "
                    "WHERE provider = ? AND cache_key = ? AND expires_at > ?",
                    (provider, key, time.time()),
                )
                .fetchone()
            )
            if row is None:
                return None
            return (
                bytes(row[0]),
                TypeAdapter(JsonObject).validate_python(json.loads(row[1])),
                float(row[2]),
            )

        return await self._run(read)

    async def set_blob(
        self, provider: str, key: str, body: bytes, metadata: JsonObject, expires_at: float
    ) -> None:
        """Atomically persist binary bytes and public metadata under the shared byte cap."""
        if not math.isfinite(expires_at):
            raise ServiceError(ErrorCode.INVALID_ARGUMENT, "image expiry must be finite")
        encoded = json.dumps(metadata, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        await self.start()

        def write() -> None:
            """Evict old entries when bounded SQLite cannot admit the complete blob."""
            self._ensure_blobs()
            self._prune_expired()
            while True:
                try:
                    self._require_connection().execute(
                        "INSERT OR REPLACE INTO public_blobs VALUES (?, ?, ?, ?, ?, ?)",
                        (provider, key, body, encoded, time.time(), expires_at),
                    )
                    self._checkpoint()
                    self._enforce_capacity()
                    return
                except sqlite3.OperationalError as exc:
                    if exc.sqlite_errorcode != sqlite3.SQLITE_FULL or not self._evict_oldest():
                        raise

        await self._run(write)

    def _ensure_blobs(self) -> None:
        """Allocate media tables only for binary stores, preserving small JSON cache budgets."""
        if self._blobs_initialized:
            return
        self._require_connection().executescript(
            """
                CREATE TABLE IF NOT EXISTS public_blobs (
                    provider TEXT NOT NULL,
                    cache_key TEXT NOT NULL,
                    body BLOB NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    PRIMARY KEY (provider, cache_key)
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS blob_expiry ON public_blobs (expires_at);
            """
        )
        self._blobs_initialized = True
        self._checkpoint()

    async def clear_projections(self, provider: str) -> None:
        """Clear a positive-cache namespace while keeping source exclusions and other sources."""
        await self.start()
        await self._run(lambda: self._delete_projections(provider))

    async def _run(self, operation: Callable[[], _T]) -> _T:
        """Keep the lock until a worker finishes even when its awaiting caller is cancelled."""
        try:
            async with self._lock:
                task = asyncio.create_task(asyncio.to_thread(operation))
                try:
                    return await asyncio.shield(task)
                except asyncio.CancelledError:
                    # Cancelling to_thread does not stop SQLite; unlock only after settlement.
                    completion = asyncio.gather(task, return_exceptions=True)
                    while not completion.done():
                        try:
                            await asyncio.shield(completion)
                        except asyncio.CancelledError:
                            continue
                    completion.result()
                    raise
        except (sqlite3.Error, OSError, ValidationError, json.JSONDecodeError) as exc:
            raise ServiceError(
                ErrorCode.UPSTREAM_UNAVAILABLE, "persistent history storage is unavailable"
            ) from exc

    def _start(self) -> None:
        """Initialize a bounded WAL database without transient filesystem copies."""
        if self._connection is not None:
            return
        page_limit = (self.max_bytes - _SHM_BYTES - _WAL_HEADER_BYTES) // (
            2 * _PAGE_SIZE + _WAL_FRAME_OVERHEAD
        )
        if page_limit < 8:
            raise ValueError("persistent history capacity cannot accommodate SQLite overhead")
        self._page_limit = page_limit
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(
            self.path if self.path is not None else ":memory:",
            check_same_thread=False,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        initialized = False
        try:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version not in {0, 1}:
                raise ServiceError(
                    ErrorCode.UPSTREAM_UNAVAILABLE,
                    "persistent history schema version is unsupported",
                )
            connection.execute("PRAGMA page_size = 4096")
            connection.execute("PRAGMA auto_vacuum = FULL")
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.execute("PRAGMA journal_size_limit = 0")
            # Dirty pages must reach WAL once per commit so page limits also bound transient WAL.
            connection.execute("PRAGMA cache_spill = OFF")
            # Reserving one database's worth of WAL frames caps transient writes as well as DB size.
            connection.execute(f"PRAGMA max_page_count = {page_limit}")
            connection.executescript(
                """
                BEGIN IMMEDIATE;
                CREATE TABLE IF NOT EXISTS generations (
                    generation TEXT PRIMARY KEY,
                    scope TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    query_fingerprint TEXT NOT NULL,
                    normalization_version TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    frontier TEXT,
                    source_exhausted INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS generation_binding ON generations (
                    scope, kind, query_fingerprint, normalization_version, created_at DESC
                );
                CREATE TABLE IF NOT EXISTS history_items (
                    generation TEXT NOT NULL REFERENCES generations(generation) ON DELETE CASCADE,
                    item_key TEXT NOT NULL,
                    position INTEGER NOT NULL,
                    item_json TEXT NOT NULL,
                    PRIMARY KEY (generation, item_key),
                    UNIQUE (generation, position)
                );
                CREATE TABLE IF NOT EXISTS source_unlisted (
                    provider TEXT NOT NULL,
                    uid TEXT NOT NULL,
                    recorded_at REAL NOT NULL,
                    PRIMARY KEY (provider, uid)
                ) WITHOUT ROWID;
                CREATE TABLE IF NOT EXISTS public_projections (
                    provider TEXT NOT NULL,
                    cache_key TEXT NOT NULL,
                    normalization_version TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    PRIMARY KEY (provider, cache_key, normalization_version)
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS projection_expiry ON public_projections (expires_at);
                PRAGMA user_version = 1;
                COMMIT;
                """
            )
            self._connection = connection
            self._blobs_initialized = (
                connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE name = 'public_blobs'"
                ).fetchone()
                is not None
            )
            self._prune_expired()
            self._checkpoint()
            self._enforce_capacity()
            initialized = True
        finally:
            if not initialized:
                self._connection = None
                connection.close()

    def _close(self) -> None:
        """Close only after the serialized worker operation has completed."""
        if self._connection is None:
            return
        connection = self._connection
        try:
            self._checkpoint()
        finally:
            self._connection = None
            connection.close()

    def _require_connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise RuntimeError("persistent history cache has not been started")
        return self._connection

    def _mark_source_unlisted(self, provider: str, uid: str) -> None:
        """Evict ordinary history if needed, while keeping source exclusions durable."""
        connection = self._require_connection()
        self._require_source_exclusion_capacity(provider, uid)
        while True:
            try:
                connection.execute(
                    "INSERT OR IGNORE INTO source_unlisted VALUES (?, ?, ?)",
                    (provider, uid, time.time()),
                )
                self._checkpoint()
                self._enforce_capacity()
                return
            except sqlite3.OperationalError as exc:
                if exc.sqlite_errorcode != sqlite3.SQLITE_FULL or not self._evict_oldest():
                    raise

    def _get_projection(
        self, provider: str, key: str, normalization_version: str
    ) -> tuple[JsonObject, float] | None:
        """Validate stored JSON only for the exact provider, key and normalization version."""
        row = (
            self._require_connection()
            .execute(
                "SELECT payload_json, expires_at FROM public_projections WHERE provider = ? "
                "AND cache_key = ? AND normalization_version = ? AND expires_at > ?",
                (provider, key, normalization_version, time.time()),
            )
            .fetchone()
        )
        if row is None:
            return None
        # Parsing recursive JSON unions directly reparses large snapshots for each branch.
        payload: JsonObject = TypeAdapter(JsonObject).validate_python(json.loads(row[0]))
        return payload, float(row[1])

    def _set_projection(
        self, provider: str, key: str, version: str, encoded: str, expires_at: float
    ) -> None:
        """Persist JSON under the common SQLite byte cap, evicting positive cache first."""
        connection = self._require_connection()
        self._prune_expired()
        while True:
            try:
                connection.execute(
                    "INSERT OR REPLACE INTO public_projections VALUES (?, ?, ?, ?, ?, ?)",
                    (provider, key, version, encoded, time.time(), expires_at),
                )
                self._checkpoint()
                self._enforce_capacity()
                return
            except sqlite3.OperationalError as exc:
                if exc.sqlite_errorcode != sqlite3.SQLITE_FULL or not self._evict_oldest():
                    raise

    def _delete_projections(
        self, provider: str, *, key: str | None = None, version: str | None = None
    ) -> None:
        """Remove only selected positive projections and reclaim their WAL writes."""
        connection = self._require_connection()
        if key is None:
            connection.execute("DELETE FROM public_projections WHERE provider = ?", (provider,))
            if self._blobs_initialized:
                connection.execute("DELETE FROM public_blobs WHERE provider = ?", (provider,))
        else:
            connection.execute(
                "DELETE FROM public_projections WHERE provider = ? AND cache_key = ? "
                "AND normalization_version = ?",
                (provider, key, version),
            )
        self._checkpoint()

    def _create_generation(
        self,
        scope: str,
        kind: str,
        query_fingerprint: str,
        normalization_version: str,
    ) -> str:
        """Insert a new account-bound version while making room from older cached versions."""
        connection = self._require_connection()
        self._prune_expired()
        generation = uuid4().hex
        now = time.time()
        while True:
            try:
                connection.execute(
                    "INSERT INTO generations VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, 0)",
                    (
                        generation,
                        scope,
                        kind,
                        query_fingerprint,
                        normalization_version,
                        now,
                        now,
                        now + self.ttl_seconds,
                    ),
                )
                self._checkpoint()
                self._enforce_capacity()
                return generation
            except sqlite3.OperationalError as exc:
                if exc.sqlite_errorcode != sqlite3.SQLITE_FULL or not self._evict_oldest():
                    raise

    def _commit_batch(
        self,
        generation: str,
        items: tuple[HistoryItem, ...],
        frontier: str | None,
        source_exhausted: bool,
    ) -> None:
        """Retry full-database writes after evicting older generations, never partial batches."""
        connection = self._require_connection()
        self._prune_expired()
        row = connection.execute(
            "SELECT source_exhausted FROM generations WHERE generation = ?", (generation,)
        ).fetchone()
        if row is None:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT, "history generation expired or was evicted"
            )
        if row[0]:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT, "history generation is already exhausted"
            )
        try:
            encoded = tuple(
                (
                    item.key,
                    json.dumps(
                        item.model_dump(mode="python"),
                        ensure_ascii=False,
                        allow_nan=False,
                        separators=(",", ":"),
                    ),
                )
                for item in items
            )
        except ValueError as exc:
            raise ServiceError(
                ErrorCode.INVALID_ARGUMENT, "history projection must contain finite JSON values"
            ) from exc
        while True:
            try:
                self._write_batch(generation, encoded, frontier, source_exhausted)
                self._checkpoint()
                self._enforce_capacity()
                return
            except sqlite3.OperationalError as exc:
                if exc.sqlite_errorcode != sqlite3.SQLITE_FULL:
                    raise
                if not self._evict_oldest(exclude=generation):
                    connection.execute(
                        "DELETE FROM generations WHERE generation = ?", (generation,)
                    )
                    self._checkpoint()
                    raise ServiceError(
                        ErrorCode.INVALID_ARGUMENT, "history generation exceeds storage capacity"
                    ) from exc

    def _write_batch(
        self,
        generation: str,
        encoded: tuple[tuple[str, str], ...],
        frontier: str | None,
        exhausted: bool,
    ) -> None:
        """Keep first-seen positions while atomically replacing duplicate payloads and frontier."""
        connection = self._require_connection()
        now = time.time()
        connection.execute("BEGIN IMMEDIATE")
        try:
            position = int(
                connection.execute(
                    "SELECT COALESCE(MAX(position) + 1, 0) FROM history_items WHERE generation = ?",
                    (generation,),
                ).fetchone()[0]
            )
            for key, item_json in encoded:
                existing = connection.execute(
                    "SELECT position FROM history_items WHERE generation = ? AND item_key = ?",
                    (generation, key),
                ).fetchone()
                if existing is None:
                    connection.execute(
                        "INSERT INTO history_items VALUES (?, ?, ?, ?)",
                        (generation, key, position, item_json),
                    )
                    position += 1
                else:
                    connection.execute(
                        "UPDATE history_items SET item_json = ? "
                        "WHERE generation = ? AND item_key = ?",
                        (item_json, generation, key),
                    )
            connection.execute(
                "UPDATE generations SET frontier = ?, source_exhausted = ?, "
                "updated_at = ?, expires_at = ? WHERE generation = ?",
                (frontier, int(exhausted), now, now + self.ttl_seconds, generation),
            )
            connection.execute("COMMIT")
        finally:
            if connection.in_transaction:
                connection.execute("ROLLBACK")

    def _read_generation(
        self,
        generation: str,
        scope: str,
        kind: str,
        query_fingerprint: str,
        normalization_version: str,
        item_offset: int = 0,
        item_limit: int = 128,
    ) -> HistorySnapshot | None:
        """Require exact ownership and normalization bindings on every persisted read."""
        row = (
            self._require_connection()
            .execute(
                "SELECT * FROM generations WHERE generation = ? AND scope = ? "
                "AND kind = ? AND query_fingerprint = ? "
                "AND normalization_version = ? AND expires_at > ?",
                (generation, scope, kind, query_fingerprint, normalization_version, time.time()),
            )
            .fetchone()
        )
        return self._snapshot(row, item_offset, item_limit) if row is not None else None

    def _latest_generation(
        self,
        scope: str,
        kind: str,
        query_fingerprint: str,
        normalization_version: str,
        item_offset: int = 0,
        item_limit: int = 128,
    ) -> HistorySnapshot | None:
        """Select by creation time so older cursor writes cannot replace the newest version."""
        row = (
            self._require_connection()
            .execute(
                "SELECT * FROM generations WHERE scope = ? AND kind = ? "
                "AND query_fingerprint = ? AND normalization_version = ? "
                "AND expires_at > ? ORDER BY created_at DESC, rowid DESC LIMIT 1",
                (scope, kind, query_fingerprint, normalization_version, time.time()),
            )
            .fetchone()
        )
        return self._snapshot(row, item_offset, item_limit) if row is not None else None

    def _snapshot(self, row: sqlite3.Row, item_offset: int, item_limit: int) -> HistorySnapshot:
        """Read indexed positions without decoding the whole generation."""
        generation = cast(str, row["generation"])
        items = tuple(
            HistoryItem.model_validate_json(item_row[0])
            for item_row in self._require_connection().execute(
                "SELECT item_json FROM history_items WHERE generation = ? "
                "AND position >= ? ORDER BY position LIMIT ?",
                (generation, item_offset, item_limit),
            )
        )
        return HistorySnapshot(
            generation=generation,
            items=items,
            item_offset=item_offset,
            item_count=int(
                self._require_connection()
                .execute(
                    "SELECT COALESCE(MAX(position) + 1, 0) FROM history_items WHERE generation = ?",
                    (generation,),
                )
                .fetchone()[0]
            ),
            frontier=cast(str | None, row["frontier"]),
            source_exhausted=bool(row["source_exhausted"]),
            fetched_at=_timestamp(float(row["updated_at"])),
            expires_at=_timestamp(float(row["expires_at"])),
        )

    def _prune_expired(self) -> None:
        """Delete expired generations with their items before allocating new disk space."""
        self._require_connection().execute(
            "DELETE FROM generations WHERE expires_at <= ?", (time.time(),)
        )
        self._require_connection().execute(
            "DELETE FROM public_projections WHERE expires_at <= ?", (time.time(),)
        )
        if self._blobs_initialized:
            self._require_connection().execute(
                "DELETE FROM public_blobs WHERE expires_at <= ?", (time.time(),)
            )
        self._checkpoint()

    def _evict_oldest(self, *, exclude: str | None = None) -> bool:
        """Evict an entire oldest version so remaining frontiers never outlive their batches."""
        connection = self._require_connection()
        blob = (
            connection.execute(
                "SELECT provider, cache_key FROM public_blobs ORDER BY created_at LIMIT 1"
            ).fetchone()
            if self._blobs_initialized
            else None
        )
        if blob is not None:
            connection.execute(
                "DELETE FROM public_blobs WHERE provider = ? AND cache_key = ?", tuple(blob)
            )
            self._checkpoint()
            return True
        projection = connection.execute(
            "SELECT provider, cache_key, normalization_version FROM public_projections "
            "ORDER BY created_at, provider, cache_key, normalization_version LIMIT 1"
        ).fetchone()
        if projection is not None:
            connection.execute(
                "DELETE FROM public_projections WHERE provider = ? AND cache_key = ? "
                "AND normalization_version = ?",
                tuple(projection),
            )
            self._checkpoint()
            return True
        row = connection.execute(
            "SELECT generation FROM generations WHERE generation != ? "
            "ORDER BY created_at, rowid LIMIT 1",
            (exclude or "",),
        ).fetchone()
        if row is None:
            return False
        connection.execute("DELETE FROM generations WHERE generation = ?", (row[0],))
        self._checkpoint()
        return True

    def _enforce_capacity(self) -> None:
        """Count database, WAL, and shared memory while reclaiming whole cached versions."""
        connection = self._require_connection()
        while (
            self._storage_bytes() > self.max_bytes
            or connection.execute("PRAGMA page_count").fetchone()[0] > self._page_limit
        ):
            if not self._evict_oldest():
                raise ServiceError(
                    ErrorCode.INVALID_ARGUMENT, "history storage exceeds configured capacity"
                )
        connection.execute(f"PRAGMA max_page_count = {self._page_limit}")

    def _storage_bytes(self) -> int:
        """Count every SQLite-owned file, including transient WAL and shared memory."""
        if self.path is None:
            return (
                int(self._require_connection().execute("PRAGMA page_count").fetchone()[0])
                * _PAGE_SIZE
            )
        return sum(
            path.stat().st_size
            for path in (self.path, Path(str(self.path) + "-wal"), Path(str(self.path) + "-shm"))
            if path.exists()
        )

    def _checkpoint(self) -> None:
        """Reclaim WAL after every serialized write; auto-vacuum avoids temporary DB copies."""
        result = self._require_connection().execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if result[0] != 0:
            raise ServiceError(
                ErrorCode.UPSTREAM_UNAVAILABLE, "history storage is locked by another reader"
            )


def _validate_binding(
    scope: str, kind: str, query_fingerprint: str, normalization_version: str
) -> None:
    if not all((scope, kind, query_fingerprint, normalization_version)):
        raise ServiceError(ErrorCode.INVALID_ARGUMENT, "history binding fields must be nonempty")


def _timestamp(value: float) -> str:
    return datetime.fromtimestamp(value, TAIPEI).isoformat(timespec="microseconds")


def _validate_window(offset: int, limit: int) -> None:
    if offset < 0 or not 0 <= limit <= 128:
        raise ValueError("history reads require a nonnegative position and at most 128 items")
