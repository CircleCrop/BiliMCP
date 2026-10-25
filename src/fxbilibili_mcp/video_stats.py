"""Read compact counter batches from shared persistent public video snapshots."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Annotated, Literal

from pydantic import Field, model_validator

from .cache import AsyncTTLCache
from .models import ErrorCode, ServiceError, StrictModel, VideoStats
from .normalization import format_timestamp, validate_bvid
from .transport import BilibiliTransport
from .videos import get_video_view

VIDEO_STATS_TTL_SECONDS = 3600
type Bvid = Annotated[str, Field(pattern=r"^BV[0-9A-Za-z]{10}$")]


class VideoStatsInput(StrictModel):
    bvids: list[Bvid] = Field(min_length=1, max_length=5)

    @model_validator(mode="after")
    def require_distinct_videos(self) -> VideoStatsInput:
        """Reject duplicate results before any video request is admitted."""
        if len(set(self.bvids)) != len(self.bvids):
            raise ValueError("bvids must contain distinct BV identifiers")
        return self


class VideoStatsSnapshot(StrictModel):
    bvid: Bvid
    stats: VideoStats
    fetched_at: str


class VideoStatsSuccess(VideoStatsSnapshot):
    status: Literal["success"] = "success"


class VideoStatsFailure(StrictModel):
    bvid: Bvid
    status: Literal["error"] = "error"
    code: ErrorCode
    message: str


type VideoStatsItem = Annotated[
    VideoStatsSuccess | VideoStatsFailure, Field(discriminator="status")
]


class VideoStatsBatchOutput(StrictModel):
    schema_version: Literal[1] = 1
    items: list[VideoStatsItem] = Field(min_length=1, max_length=5)


class VideoStatsService:
    """Read exactly one view response per uncached video without fetching related resources."""

    def __init__(
        self,
        transport: BilibiliTransport,
        cache: AsyncTTLCache,
        *,
        ttl_seconds: int = VIDEO_STATS_TTL_SECONDS,
        clock: Callable[[], float] = time.time,
    ) -> None:
        """Bind the pool-owned transport and cache without creating additional clients."""
        if ttl_seconds <= 0:
            raise ValueError("video statistics TTL must be positive")
        self._transport = transport
        self._cache = cache
        self._ttl = ttl_seconds
        self._clock = clock

    async def get_video_stats(self, bvids: list[str]) -> tuple[VideoStatsBatchOutput, bool]:
        """Preserve input order and localize individual upstream failures to their video."""
        if not 1 <= len(bvids) <= 5:
            raise ServiceError(ErrorCode.INVALID_ARGUMENT, "bvids must contain one to five videos")
        for bvid in bvids:
            validate_bvid(bvid)
        if len(set(bvids)) != len(bvids):
            raise ServiceError(ErrorCode.INVALID_ARGUMENT, "bvids must contain distinct videos")
        items: list[VideoStatsItem] = []
        all_hit = True
        # At most five sequential reads avoid manufacturing an additional upstream burst.
        for bvid in bvids:
            try:
                snapshot, hit = await self._fetch(bvid)
            except ServiceError as exc:
                # Capacity and account failures apply to the call, so preserve HTTP 429 and stop.
                if exc.code in {ErrorCode.RATE_LIMITED, ErrorCode.CREDENTIALS_INVALID}:
                    raise
                items.append(VideoStatsFailure(bvid=bvid, code=exc.code, message=exc.message))
                all_hit = False
                continue
            items.append(VideoStatsSuccess(**snapshot.model_dump()))
            all_hit = all_hit and hit
        return VideoStatsBatchOutput(items=items), all_hit

    async def _fetch(self, bvid: str) -> tuple[VideoStatsSnapshot, bool]:
        """Project counters without requiring pages, subtitle inventory, or owner data."""
        view, hit = await get_video_view(
            self._transport, self._cache, bvid, self._ttl, clock=self._clock
        )
        return (
            VideoStatsSnapshot(
                bvid=bvid,
                stats=view.stats,
                fetched_at=format_timestamp(int(view.fetched_at)),
            ),
            hit,
        )
