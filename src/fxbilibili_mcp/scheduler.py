"""Bound account-local HTTP work with cancellable, fair admission and pacing."""

from __future__ import annotations

import asyncio
import math
import random
import time
from collections import OrderedDict, deque
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass

from .logging_context import current_tool_call
from .models import ErrorCode, ServiceError


@dataclass(slots=True)
class _Bucket:
    rate: float
    capacity: int
    tokens: float
    updated: float

    def delay(self, now: float, cost: int) -> float:
        if not self.rate:
            return 0.0
        self.tokens = min(self.capacity, self.tokens + max(0.0, now - self.updated) * self.rate)
        self.updated = now
        return max(0.0, (cost - self.tokens) / self.rate)

    def spend(self, cost: int) -> None:
        if self.rate:
            self.tokens -= cost


@dataclass(eq=False, slots=True)
class _Waiter:
    owner: str
    maintenance: bool
    endpoint: str


class SendBudget:
    """Share one send interval and request rate across API and media clients."""

    def __init__(
        self,
        requests_per_minute: int,
        burst: int,
        spacing_seconds: float,
        endpoint_limits: Mapping[str, int] | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
        jitter: Callable[[], float] = random.random,
    ) -> None:
        """Create only configured endpoint buckets; zero disables their minute limits."""
        limits = dict(endpoint_limits or {})
        if (
            requests_per_minute < 0
            or burst < 1
            or not math.isfinite(spacing_seconds)
            or spacing_seconds < 0
        ):
            raise ValueError("invalid send budget")
        if any(
            type(rate) is not int
            or rate < 0
            or (not key.startswith("/") and key not in {"subtitle"})
            or "?" in key
            for key, rate in limits.items()
        ):
            raise ValueError("invalid endpoint limits")
        self.clock = clock
        self.jitter = jitter
        self.spacing = spacing_seconds
        now = clock()
        self.next_send = now
        self.requests = _Bucket(requests_per_minute / 60, burst, float(burst), now)
        # A one-request burst prevents a configured endpoint cap from allowing a search spike.
        self.endpoints = {
            key: _Bucket(rate / 60, 1, 1.0, now) for key, rate in limits.items() if rate
        }
        self._business_requests = 0
        self._maintenance_requests = 0
        self._endpoint_requests: dict[str, int] = {}

    def metrics(self) -> dict[str, int | dict[str, int]]:
        """Report actual API, media and credential-maintenance sends."""
        return {
            "business_requests": self._business_requests,
            "maintenance_requests": self._maintenance_requests,
            "endpoint_requests": dict(self._endpoint_requests),
        }

    def delay(self, waiter: _Waiter, now: float, *, include_endpoint: bool = True) -> float:
        """Apply total and endpoint limits at admission, before any actual HTTP send."""
        delay = max(0.0, self.next_send - now)
        if not waiter.maintenance:
            delay = max(delay, self.requests.delay(now, 1))
            if include_endpoint and (endpoint := self.endpoints.get(waiter.endpoint)):
                delay = max(delay, endpoint.delay(now, 1))
        return delay

    def spend(self, waiter: _Waiter) -> None:
        """Consume request permits atomically on this process event loop."""
        if not waiter.maintenance:
            self.requests.spend(1)
            self._business_requests += 1
            if endpoint := self.endpoints.get(waiter.endpoint):
                endpoint.spend(1)
        else:
            self._maintenance_requests += 1
        self._endpoint_requests[waiter.endpoint] = (
            self._endpoint_requests.get(waiter.endpoint, 0) + 1
        )
        if self.spacing:
            self.next_send = self.clock() + self.spacing + 0.1 * self.jitter()


class UpstreamScheduler:
    """Rotate calls within one account without background tasks or unbounded waiters."""

    def __init__(
        self,
        *,
        concurrency: int,
        requests_per_minute: int,
        burst: int,
        spacing_seconds: float = 0.0,
        queue_timeout_seconds: float = 15.0,
        queue_capacity: int = 64,
        endpoint_limits: Mapping[str, int] | None = None,
        send_budget: SendBudget | None = None,
        clock: Callable[[], float] = time.monotonic,
        jitter: Callable[[], float] = random.random,
    ) -> None:
        """Own independent business and maintenance buckets within shared sending limits."""
        if (
            concurrency < 1
            or requests_per_minute < 0
            or burst < 1
            or not math.isfinite(spacing_seconds)
            or spacing_seconds < 0
            or not math.isfinite(queue_timeout_seconds)
            or queue_timeout_seconds <= 0
            or queue_capacity < 1
        ):
            raise ValueError("invalid upstream scheduler limits")
        self._concurrency = concurrency
        self._spacing = spacing_seconds
        self._queue_timeout = queue_timeout_seconds
        self._queue_capacity = queue_capacity
        self._clock = clock
        self._jitter = jitter
        now = clock()
        rate = requests_per_minute / 60
        self._requests = _Bucket(rate, burst, float(burst), now)
        # Cookie rotation must make progress even when user work has exhausted its bucket.
        self._maintenance = _Bucket(2.0 if spacing_seconds else 0.0, 8, 8.0, now)
        self._condition = asyncio.Condition()
        self._queues: OrderedDict[str, deque[_Waiter]] = OrderedDict()
        self._waiting = 0
        self._active = 0
        self._send_budget = send_budget or SendBudget(
            requests_per_minute, burst, spacing_seconds, endpoint_limits, clock=clock, jitter=jitter
        )
        self._closed = False
        self._business_requests = 0
        self._maintenance_requests = 0

    def metrics(self) -> dict[str, int]:
        """Report actual sends and queue occupancy for this account."""
        return {
            "business_requests": self._business_requests,
            "maintenance_requests": self._maintenance_requests,
            "waiting": self._waiting,
            "active": self._active,
        }

    async def close(self) -> None:
        """Reject queued sends during runtime shutdown and wake every waiting caller."""
        async with self._condition:
            self._closed = True
            self._condition.notify_all()

    @asynccontextmanager
    async def slot(
        self,
        *,
        owner: str,
        maintenance: bool = False,
        endpoint: str = "upstream",
        on_admit: Callable[[], None],
    ) -> AsyncIterator[None]:
        """Wait within a bounded queue, then account immediately before the actual send."""
        waiter = _Waiter(owner, maintenance, endpoint)
        started = self._clock()
        admitted = False
        async with self._condition:
            if self._waiting >= self._queue_capacity:
                raise ServiceError(
                    ErrorCode.RATE_LIMITED,
                    "upstream waiting queue is full",
                    http_status=429,
                    retry_after_seconds=1,
                )
            self._queues.setdefault(owner, deque()).append(waiter)
            self._waiting += 1
            self._condition.notify_all()
            try:
                async with asyncio.timeout(self._queue_timeout):
                    while True:
                        delay = self._admission_delay(waiter)
                        if delay == 0:
                            # Validation can reject a call without spending tokens or a slot.
                            on_admit()
                            self._spend(waiter)
                            self._remove(waiter, rotate=True)
                            self._active += 1
                            admitted = True
                            self._condition.notify_all()
                            break
                        if delay is None:
                            await self._condition.wait()
                        else:
                            try:
                                async with asyncio.timeout(delay):
                                    await self._condition.wait()
                            except TimeoutError:
                                pass
            except TimeoutError as exc:
                raise ServiceError(
                    ErrorCode.RATE_LIMITED,
                    "upstream queue wait timed out",
                    http_status=429,
                    retry_after_seconds=1,
                ) from exc
            finally:
                context = current_tool_call()
                if context is not None and not maintenance:
                    # Sum send waits; fan-out waits can overlap and are not call wall time.
                    context.upstream_work.queue_wait_ms += max(
                        0.0, (self._clock() - started) * 1000
                    )
                if not admitted:
                    self._remove(waiter, rotate=False)
                    self._condition.notify_all()
        try:
            yield
        finally:
            async with self._condition:
                self._active -= 1
                self._condition.notify_all()

    def _admission_delay(self, waiter: _Waiter) -> float | None:
        """Let the next call progress without letting an exhausted business bucket stop upkeep."""
        if self._closed:
            raise ServiceError(ErrorCode.UPSTREAM_UNAVAILABLE, "upstream scheduler is closed")
        if self._active >= self._concurrency:
            return None
        now = self._clock()
        selected: _Waiter | None = None
        shortest: float | None = None
        seen_classes: set[tuple[bool, str]] = set()
        business_blocked = False
        for queue in self._queues.values():
            head = queue[0]
            if business_blocked and not head.maintenance:
                continue
            queue_class = (head.maintenance, head.endpoint)
            if queue_class in seen_classes:
                continue
            seen_classes.add(queue_class)
            token_delay = self._token_delay(head, now)
            if token_delay == 0:
                selected = head
                break
            shortest = token_delay if shortest is None else min(shortest, token_delay)
            # Only an endpoint-specific wait may be bypassed by another business endpoint.
            # Reserve refilling global tokens for the older call while allowing maintenance.
            if not head.maintenance and self._token_delay(head, now, include_endpoint=False) > 0:
                business_blocked = True
        if selected is waiter:
            return 0.0
        if selected is not None:
            return None
        if not self._spacing and not self._send_budget.spacing and not self._send_budget.endpoints:
            # Preserve fail-fast rate rejection when pacing has not been enabled.
            raise ServiceError(ErrorCode.RATE_LIMITED, "shared Bilibili request budget is busy")
        return shortest

    def _token_delay(self, waiter: _Waiter, now: float, *, include_endpoint: bool = True) -> float:
        if waiter.maintenance:
            return max(self._maintenance.delay(now, 1), self._send_budget.delay(waiter, now))
        return max(
            self._requests.delay(now, 1),
            self._send_budget.delay(waiter, now, include_endpoint=include_endpoint),
        )

    def _spend(self, waiter: _Waiter) -> None:
        """Consume request permits only after admission succeeds."""
        if waiter.maintenance:
            self._maintenance.spend(1)
            self._maintenance_requests += 1
        else:
            self._requests.spend(1)
            self._business_requests += 1
        self._send_budget.spend(waiter)

    def _remove(self, waiter: _Waiter, *, rotate: bool) -> None:
        queue = self._queues[waiter.owner]
        queue.remove(waiter)
        self._waiting -= 1
        if not queue:
            del self._queues[waiter.owner]
        elif rotate:
            self._queues.move_to_end(waiter.owner)
