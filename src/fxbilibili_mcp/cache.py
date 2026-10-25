"""Bounded asynchronous TTL-LRU cache with request coalescing."""

from __future__ import annotations

import asyncio
import sys
import time
from collections import OrderedDict
from collections.abc import AsyncIterator, Awaitable, Callable, Hashable, Iterable, Iterator
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, fields, is_dataclass
from typing import TypeVar, cast

from pydantic import BaseModel

from .cache_codec import (
    CODEC_VERSION,
    CacheCodecError,
    decode_public_value,
    encode_public_value,
    public_cache_key,
)
from .logging_context import current_tool_call
from .models import ErrorCode, ServiceError
from .normalization import JsonObject
from .persistent_cache import PersistentHistoryCache

T = TypeVar("T")
CacheKey = tuple[object, ...]
Factory = Callable[[], Awaitable[T]]


@dataclass(frozen=True, slots=True)
class BinaryCacheValue:
    """Carry image bytes separately from their public JSON metadata."""

    data: bytes
    metadata: JsonObject
    expires_at: float | None = None


@dataclass(frozen=True, slots=True)
class CachePolicy:
    """Centralize bounded capacities and the product-approved resource TTLs."""

    max_items: int = 1_000_000
    max_bytes: int = 256 * 1024 * 1024
    search: int = 1800
    detail: int = 3600
    profile: int = 300
    relation: int = 7200
    comments: int = 1800
    comments_time: int = 900
    activities: int = 900
    user_content: int = 3600
    danmaku: int = 3600
    subtitle_inventory: int = 3600
    subtitle_body: int = 86400
    transcript: int = 604800
    live_status: int = 30
    live_profile: int = 300

    def ttl_seconds(self) -> dict[str, int]:
        values = asdict(self)
        values.pop("max_items")
        values.pop("max_bytes")
        return values


@dataclass(frozen=True, slots=True)
class _Produced[T]:
    value: T
    persisted_hit: bool


@dataclass(slots=True)
class _CacheEntry[T]:
    value: T
    expires_at: float
    estimated_bytes: int


# Charge each entry for a standalone mapping allocation, conservatively covering LRU nodes.
_ENTRY_OVERHEAD = (
    sys.getsizeof(_CacheEntry(None, 0.0, 0))
    + sys.getsizeof(0.0)
    + sys.getsizeof(0)
    + sys.getsizeof(OrderedDict((((), None),)))
    - sys.getsizeof(OrderedDict())
)


def _object_children(value: object) -> Iterable[object]:
    """Visit owned containers and model storage without following class-level metadata."""
    if isinstance(value, dict):
        for key, item in value.items():
            yield key
            yield item
    elif isinstance(value, (list, tuple, set, frozenset)):
        yield from value
    elif isinstance(value, BaseModel):
        yield value.__dict__
        yield value.__pydantic_fields_set__
        yield value.__pydantic_extra__
        yield value.__pydantic_private__
    elif is_dataclass(value) and not isinstance(value, type):
        storage = getattr(value, "__dict__", None)
        if storage is not None:
            yield storage
        for field in fields(value):
            yield getattr(value, field.name)


def _estimate_entry_bytes(key: CacheKey, value: object, limit: int) -> int:
    """Estimate one object graph off-loop, stopping once it cannot fit in the cache."""
    total = _ENTRY_OVERHEAD
    seen: set[int] = set()
    pending: list[Iterator[object]] = [iter((key, value))]
    while pending:
        try:
            item = next(pending[-1])
        except StopIteration:
            pending.pop()
            continue
        identity = id(item)
        if identity in seen:
            continue
        seen.add(identity)
        total += sys.getsizeof(item)
        if total > limit:
            return total
        # Iterators avoid duplicating a large collection in the estimator's work queue.
        pending.append(iter(_object_children(item)))
    return total


class AsyncTTLCache:
    """Keep fresh values and one shielded producer task per cache key."""

    def __init__(
        self,
        maxsize: int = 1_000_000,
        *,
        max_bytes: int = 256 * 1024 * 1024,
        producer_timeout_seconds: float = 90.0,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        """Own bounded entries, active producers, waiter counts, and their lock."""
        if maxsize <= 0:
            raise ValueError("cache maxsize must be positive")
        if max_bytes <= 0:
            raise ValueError("cache max_bytes must be positive")
        if producer_timeout_seconds <= 0:
            raise ValueError("cache producer timeout must be positive")
        self.maxsize = maxsize
        self.max_bytes = max_bytes
        self._producer_timeout = producer_timeout_seconds
        self._clock = clock
        self._wall_clock = wall_clock
        self._persistent_store: PersistentHistoryCache | None = None
        self._persistent_namespace = "bilibili-cache"
        self._persistent_binary = False
        self._persistent_hits = 0
        self._persistent_invalidations = 0
        self._persistent_unsupported = 0
        self._entries: OrderedDict[CacheKey, _CacheEntry[object]] = OrderedDict()
        self._inflight: dict[CacheKey, asyncio.Task[object]] = {}
        self._waiters: dict[CacheKey, int] = {}
        self._lock = asyncio.Lock()
        self._hits = 0
        self._misses = 0
        self._coalesced = 0
        self._evictions = 0
        self._failures = 0
        self._estimated_bytes = 0
        self._peak_estimated_bytes = 0
        self._oversized_skips = 0

    def set_persistent_store(
        self,
        store: PersistentHistoryCache,
        namespace: str = "bilibili-cache",
        *,
        binary: bool = False,
    ) -> None:
        """Bind a pool-owned store before queries; its lifecycle remains with the runtime."""
        if self._entries or self._inflight:
            raise RuntimeError("persistent cache must be bound before queries")
        self._persistent_store = store
        self._persistent_namespace = namespace
        self._persistent_binary = binary

    async def get_or_create(self, key: CacheKey, ttl: float, factory: Factory[T]) -> tuple[T, bool]:
        """Return a fresh entry or join an existing producer without coupling cancellation."""
        context = current_tool_call()
        work = context.upstream_work if context is not None else None
        if work is not None:
            work.cache_lookups += 1
        async with self._lock:
            now = self._clock()
            entry = self._entries.get(key)
            if entry is not None and entry.expires_at > now:
                self._entries.move_to_end(key)
                self._hits += 1
                if work is not None:
                    work.cache_hits += 1
                return cast(T, entry.value), True
            if entry is not None:
                del self._entries[key]
                self._estimated_bytes -= entry.estimated_bytes
            existing = self._inflight.get(key)
            if existing is not None:
                task = existing
                reused = True
                self._coalesced += 1
                if work is not None:
                    work.coalesced_cache_reads += 1
            else:
                task = asyncio.create_task(self._produce(key, ttl, factory))
                task.add_done_callback(_consume_task_exception)
                self._inflight[key] = task
                reused = False
            self._waiters[key] = self._waiters.get(key, 0) + 1
        cancelled = False
        try:
            produced = cast(_Produced[T], await asyncio.shield(task))
            if work is not None and not reused:
                if produced.persisted_hit:
                    work.cache_hits += 1
                    work.persistent_cache_hits += 1
                else:
                    work.cache_misses += 1
            return produced.value, reused or produced.persisted_hit
        except Exception:
            if work is not None and not reused:
                work.cache_misses += 1
            raise
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            async with self._lock:
                remaining = self._waiters[key] - 1
                if remaining:
                    self._waiters[key] = remaining
                else:
                    del self._waiters[key]
                    if cancelled and not task.done() and self._inflight.get(key) is task:
                        del self._inflight[key]
                        task.cancel()

    async def _produce(self, key: CacheKey, ttl: float, factory: Factory[T]) -> _Produced[T]:
        """Reuse unexpired disk content or populate both cache levels with one producer."""
        try:
            # Disk reads share the producer slot, so concurrent restart misses still coalesce.
            async with asyncio.timeout(self._producer_timeout):
                stored = await self._read_persistent(key)
                if stored is not None:
                    value, expires_at = cast(tuple[T, float], stored)
                    ttl = max(0.0, expires_at - self._wall_clock())
                    self._persistent_hits += 1
                    self._hits += 1
                    persisted_hit = True
                else:
                    self._misses += 1
                    value = await factory()
                    expires_at = self._wall_clock() + ttl
                    if isinstance(value, BinaryCacheValue) and value.expires_at is not None:
                        expires_at = min(expires_at, value.expires_at)
                        ttl = max(0.0, expires_at - self._wall_clock())
                    await self._write_persistent(key, value, expires_at)
                    persisted_hit = False
                estimated_bytes = await asyncio.to_thread(
                    _estimate_entry_bytes, key, value, self.max_bytes
                )
            async with self._lock:
                if self._inflight.get(key) is asyncio.current_task():
                    if estimated_bytes > self.max_bytes:
                        self._oversized_skips += 1
                        return _Produced(value, persisted_hit)
                    while self._entries and (
                        len(self._entries) >= self.maxsize
                        or self._estimated_bytes + estimated_bytes > self.max_bytes
                    ):
                        _, removed = self._entries.popitem(last=False)
                        self._estimated_bytes -= removed.estimated_bytes
                        self._evictions += 1
                    self._entries[key] = _CacheEntry(
                        value=value,
                        expires_at=self._clock()
                        + (
                            max(0.0, expires_at - self._wall_clock())
                            if self._persistent_store is not None
                            else ttl
                        ),
                        estimated_bytes=estimated_bytes,
                    )
                    self._estimated_bytes += estimated_bytes
                    self._peak_estimated_bytes = max(
                        self._peak_estimated_bytes, self._estimated_bytes
                    )
            return _Produced(value, persisted_hit)
        except Exception:
            self._failures += 1
            raise
        finally:
            async with self._lock:
                current = asyncio.current_task()
                if self._inflight.get(key) is current:
                    del self._inflight[key]

    async def invalidate_projection(self, key: CacheKey) -> None:
        """Discard an observed bad page from memory and persistent storage."""
        async with self._lock:
            entry = self._entries.pop(key, None)
            if entry is not None:
                self._estimated_bytes -= entry.estimated_bytes
            if self._persistent_store is not None:
                disk_key = await asyncio.to_thread(public_cache_key, key)
                await self._persistent_store.delete_projection(
                    self._persistent_namespace, disk_key, CODEC_VERSION
                )

    async def _read_persistent(self, key: CacheKey) -> tuple[object, float] | None:
        """Restore one public value without extending the original absolute expiry."""
        store = self._persistent_store
        if store is None:
            return None
        disk_key = await asyncio.to_thread(public_cache_key, key)
        if self._persistent_binary:
            blob = await store.get_blob(self._persistent_namespace, disk_key)
            if blob is None:
                return None
            body, metadata, expires_at = blob
            return BinaryCacheValue(body, metadata, expires_at), expires_at
        found = await store.get_projection(self._persistent_namespace, disk_key, CODEC_VERSION)
        if found is None:
            return None
        payload, expires_at = found
        try:
            restored = await asyncio.to_thread(decode_public_value, payload)
        except CacheCodecError:
            self._persistent_invalidations += 1
            await store.delete_projection(self._persistent_namespace, disk_key, CODEC_VERSION)
            return None
        if expires_at <= self._wall_clock():
            return None
        return restored, expires_at

    async def _write_persistent(self, key: CacheKey, value: object, expires_at: float) -> None:
        """Commit successful public values before returning; unsupported types are observable."""
        store = self._persistent_store
        if store is None:
            return
        if self._persistent_binary:
            if not isinstance(value, BinaryCacheValue):
                raise TypeError("binary cache accepts only BinaryCacheValue")
            disk_key = await asyncio.to_thread(public_cache_key, key)
            await store.set_blob(
                self._persistent_namespace, disk_key, value.data, value.metadata, expires_at
            )
            return
        try:
            payload = await asyncio.to_thread(encode_public_value, value)
            disk_key = await asyncio.to_thread(public_cache_key, key)
        except CacheCodecError:
            self._persistent_unsupported += 1
            return
        await store.set_projection(
            self._persistent_namespace, disk_key, CODEC_VERSION, payload, expires_at
        )

    async def clear(self) -> None:
        """Clear this pool's positive cache namespace while preserving durable source records."""
        async with self._lock:
            self._entries.clear()
            self._estimated_bytes = 0
            if self._persistent_store is not None:
                await self._persistent_store.clear_projections(self._persistent_namespace)

    async def close(self) -> None:
        """Cancel and await active producers before their transport is closed."""
        async with self._lock:
            tasks = tuple(self._inflight.values())
            self._entries.clear()
            self._estimated_bytes = 0
            for task in tasks:
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def size(self) -> int:
        """Count completed entries after pruning expired values."""
        async with self._lock:
            now = self._clock()
            expired = [key for key, entry in self._entries.items() if entry.expires_at <= now]
            for key in expired:
                self._estimated_bytes -= self._entries.pop(key).estimated_bytes
            return len(self._entries)

    def metrics(self) -> dict[str, int]:
        """Report completed-cache hits separately from callers waiting on shared producers."""
        return {
            "hits": self._hits,
            "misses": self._misses,
            "coalesced": self._coalesced,
            "evictions": self._evictions,
            "producer_failures": self._failures,
            "producers": len(self._inflight),
            "max_items": self.maxsize,
            "max_bytes": self.max_bytes,
            "estimated_bytes": self._estimated_bytes,
            "peak_estimated_bytes": self._peak_estimated_bytes,
            "oversized_skips": self._oversized_skips,
            "persistent_hits": self._persistent_hits,
            "persistent_invalidations": self._persistent_invalidations,
            "persistent_unsupported": self._persistent_unsupported,
        }


def _consume_task_exception(task: asyncio.Task[object]) -> None:
    """Retrieve a detached producer exception without logging sensitive context."""
    if not task.cancelled():
        task.exception()


class KeyedLocks:
    """Serialize one index while allowing unrelated resources to advance independently."""

    def __init__(self) -> None:
        self._entries: dict[Hashable, tuple[asyncio.Lock, int]] = {}

    @asynccontextmanager
    async def hold(self, key: Hashable, *, wait_seconds: float = 20.0) -> AsyncIterator[None]:
        """Bound lock waiting and remove entries after the last owner or waiter leaves."""
        lock, users = self._entries.get(key, (asyncio.Lock(), 0))
        self._entries[key] = (lock, users + 1)
        acquired = False
        try:
            try:
                async with asyncio.timeout(wait_seconds):
                    await lock.acquire()
                acquired = True
            except TimeoutError as exc:
                raise ServiceError(
                    ErrorCode.RATE_LIMITED, "This content index is busy; retry later"
                ) from exc
            yield
        finally:
            if acquired:
                lock.release()
            remaining = self._entries[key][1] - 1
            if remaining:
                self._entries[key] = (lock, remaining)
            else:
                del self._entries[key]
