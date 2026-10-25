"""Shared Bilibili HTTP transport, Cookie boundary, WBI/WBI2, and error mapping."""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import logging
import math
import secrets
import socket
import ssl
import time
import urllib.parse
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from functools import partial
from pathlib import Path
from typing import Final

import httpx

from .credentials import (
    CredentialState,
    CredentialStore,
    cookie_values,
    credential_state_from_qr_login,
    generate_correspond_path,
    merge_cookie,
    parse_refresh_csrf,
    remove_qr_png,
    render_terminal_qr,
    save_qr_png,
    write_console_block,
)
from .http_debug import emit_http_debug, encoded_body, redacted_headers, redacted_url
from .models import ErrorCode, ServiceError
from .normalization import JsonObject, JsonValue, as_object
from .scheduler import SendBudget, UpstreamScheduler

BASE_URL: Final = "https://api.bilibili.com"
MAX_UPSTREAM_RESPONSE_BYTES: Final = 16 * 1024 * 1024
UPSTREAM_RESPONSE_TIMEOUT_SECONDS: Final = 30.0


class BusinessBudgetExhausted(Exception):
    """Stop a bounded scan without disguising an upstream refusal as partial results."""


@dataclass(slots=True)
class BusinessBudget:
    max_attempts: int
    attempts: int = 0


@dataclass(frozen=True, slots=True)
class ShortLinkResponse:
    status_code: int
    location: str | None


_BUSINESS_BUDGETS: ContextVar[tuple[BusinessBudget, ...]] = ContextVar(
    "bilibili_business_budgets", default=()
)
COOKIE_INFO_URL: Final = "https://passport.bilibili.com/x/passport-login/web/cookie/info"
COOKIE_REFRESH_URL: Final = "https://passport.bilibili.com/x/passport-login/web/cookie/refresh"
COOKIE_CONFIRM_URL: Final = "https://passport.bilibili.com/x/passport-login/web/confirm/refresh"
CORRESPOND_BASE_URL: Final = "https://www.bilibili.com/correspond/1/"
QR_GENERATE_URL: Final = "https://passport.bilibili.com/x/passport-login/web/qrcode/generate"
QR_POLL_URL: Final = "https://passport.bilibili.com/x/passport-login/web/qrcode/poll"
FINGER_SPI_URL: Final = "https://api.bilibili.com/x/frontend/finger/spi"
QR_LOGIN_TIMEOUT_SECONDS: Final = 60.0
QR_LOGIN_POLL_INTERVAL_SECONDS: Final = 0.5
USER_AGENT: Final = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/154.0.0.0 Safari/537.36"
)
_MIXIN_TABLE = (
    46,
    47,
    18,
    2,
    53,
    8,
    23,
    32,
    15,
    50,
    10,
    31,
    58,
    3,
    45,
    35,
    27,
    43,
    5,
    49,
    33,
    9,
    42,
    19,
    29,
    28,
    14,
    39,
    12,
    38,
    41,
    13,
    37,
    48,
    7,
    16,
    24,
    55,
    40,
    61,
    26,
    17,
    0,
    1,
    60,
    51,
    30,
    4,
    22,
    25,
    54,
    21,
    56,
    59,
    6,
    63,
    57,
    62,
    11,
    36,
    20,
    34,
    44,
    52,
)
_WBI2_ALPHABET = "ABCDEFGHIJK"
_logger = logging.getLogger("fxbilibili_mcp")
SEARCH_PATH: Final = "/x/web-interface/wbi/search/type"


@dataclass(slots=True)
class _RiskGate:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    blocked_until: float = 0.0
    failures: int = 0
    last_sent: float = -math.inf


class BilibiliTransport:
    """Own one async client and keep every credential/signature detail private."""

    def __init__(
        self,
        cookie: str | None,
        *,
        ac_time_value: str | None = None,
        credential_state_path: Path | None = None,
        qr_code_path: Path | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Callable[[], float] = time.time,
        retry_delay: float = 0.08,
        refresh_check_interval: float = 3_600,
        qr_login_timeout: float = QR_LOGIN_TIMEOUT_SECONDS,
        qr_poll_interval: float = QR_LOGIN_POLL_INTERVAL_SECONDS,
        qr_output: Callable[[str], None] = write_console_block,
        debug_log_http: bool = False,
        upstream_requests_per_minute: int = 0,
        upstream_burst: int = 60,
        upstream_concurrency: int = 8,
        upstream_requests_per_call: int = 0,
        upstream_spacing_seconds: float = 0.0,
        upstream_send_budget: SendBudget | None = None,
        upstream_queue_timeout_seconds: float = 15.0,
        upstream_queue_capacity: int = 64,
        monotonic_clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Create the HTTP client and private bootstrap/persistent credential boundary."""
        if credential_state_path is not None and credential_state_path == qr_code_path:
            raise ValueError("credential state and QR code paths must be different")
        self._bootstrap_state = CredentialState.create(cookie, ac_time_value)
        self._state = self._bootstrap_state
        self._store = CredentialStore(credential_state_path)
        self._qr_code_path = qr_code_path
        headers = {
            "User-Agent": USER_AGENT,
            "Referer": "https://www.bilibili.com/",
            "Origin": "https://www.bilibili.com",
            "Accept": "application/json, text/plain, */*",
        }
        if self._state.cookie:
            headers["Cookie"] = self._state.cookie
        self._client = httpx.AsyncClient(
            headers=headers,
            # Use the maintained system CA store for upstream TLS verification.
            verify=ssl.create_default_context(),
            timeout=httpx.Timeout(20.0, connect=8.0, read=20.0, write=10.0, pool=5.0),
            follow_redirects=False,
            transport=transport,
        )
        self._short_link_client = httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT},
            timeout=httpx.Timeout(8.0),
            follow_redirects=False,
            transport=transport,
            trust_env=False,
        )
        self._clock = clock
        self._retry_delay = retry_delay
        self._refresh_check_interval = refresh_check_interval
        self._qr_login_timeout = qr_login_timeout
        self._qr_poll_interval = qr_poll_interval
        self._qr_output = qr_output
        self._debug_log_http = debug_log_http
        if (
            upstream_requests_per_minute < 0
            or upstream_burst < 1
            or upstream_concurrency < 1
            or upstream_requests_per_call < 0
        ):
            raise ValueError("upstream request budget settings must be positive")
        self._upstream_rate = upstream_requests_per_minute / 60
        self._scheduler = UpstreamScheduler(
            concurrency=upstream_concurrency,
            requests_per_minute=upstream_requests_per_minute,
            burst=upstream_burst,
            spacing_seconds=upstream_spacing_seconds,
            send_budget=upstream_send_budget,
            queue_timeout_seconds=upstream_queue_timeout_seconds,
            queue_capacity=upstream_queue_capacity,
        )
        self._upstream_cooldown = 0.0
        self._maintenance_cooldown = 0.0
        self._upstream_requests_per_call = upstream_requests_per_call
        self._risk_clock = monotonic_clock
        self._risk_gates: dict[str, _RiskGate] = {}
        self._wbi_key: str | None = None
        self._verified_account_uid: str | None = None
        self._wbi_expires_at = 0.0
        self._wbi_lock = asyncio.Lock()
        self._refresh_lock = asyncio.Lock()
        self._refresh_checked_at = 0.0
        self._credential_state_loaded = False
        self._closed = False

    async def start(self) -> None:
        """Load persisted credentials, fall back to bootstrap, refresh, and preload WBI."""
        candidates = await self._credential_candidates()
        last_error: ServiceError | None = None
        for state, persisted in candidates:
            self._apply_credentials(state)
            self._credential_state_loaded = persisted
            if not state.has_runtime_cookie:
                continue
            try:
                await self._maybe_refresh_credentials(force=True)
                await self._get_wbi_key()
                if self._refresh_configured() and not self._credential_state_loaded:
                    await self._store.save(self._state)
                    self._credential_state_loaded = True
                return
            except ServiceError as exc:
                last_error = exc
                if exc.code is not ErrorCode.CREDENTIALS_INVALID:
                    raise
                await self._invalidate_wbi_key()
        if self._store.configured:
            self._apply_credentials(CredentialState.create(None, None))
            await self._login_with_qr()
            return
        if last_error is not None:
            raise last_error

    async def close(self) -> None:
        """Close the scheduler and shared HTTP clients."""
        if not self._closed:
            self._closed = True
            await self._scheduler.close()
            await self._client.aclose()
            await self._short_link_client.aclose()

    async def request_short_link(self, url: str) -> ShortLinkResponse:
        """Resolve one approved short-link hop without credentials, pinning its public peer."""
        parsed = urllib.parse.urlsplit(url)
        if (
            parsed.scheme != "https"
            or parsed.netloc != "b23.tv"
            or parsed.query
            or parsed.fragment
            or not parsed.path.strip("/").isalnum()
            or not parsed.path.strip("/").isascii()
            or len(parsed.path.strip("/")) > 32
            or "/" in parsed.path.strip("/")
        ):
            raise ServiceError(ErrorCode.INVALID_ARGUMENT, "short link is not supported")
        try:
            async with asyncio.timeout(8.0):
                peers = await asyncio.get_running_loop().getaddrinfo(
                    "b23.tv", 443, type=socket.SOCK_STREAM
                )
            addresses = [ipaddress.ip_address(peer[4][0]) for peer in peers]
            if not addresses or any(not address.is_global for address in addresses):
                raise ServiceError(ErrorCode.INVALID_ARGUMENT, "short link peer is not public")
            # DNS validation must constrain the connection too; Host and SNI preserve TLS identity.
            target = httpx.URL(url).copy_with(host=str(addresses[0]))
            request = self._short_link_client.build_request(
                "GET", target, headers={"Host": "b23.tv"}, extensions={"sni_hostname": "b23.tv"}
            )
            async with self.upstream_request_slot(endpoint="shortlink") as record_status:
                response = await self._short_link_client.send(request, stream=True)
                try:
                    record_status(response.status_code)
                    status_code = response.status_code
                    location = response.headers.get("location")
                    if self._debug_log_http:
                        emit_http_debug(
                            {
                                "boundary": "shortlink",
                                "exchange_id": uuid.uuid4().hex,
                                "url": redacted_url(url),
                                "status": status_code,
                            }
                        )
                finally:
                    # Only redirect metadata is needed; a final page body is never downloaded.
                    await response.aclose()
            if response.status_code in {403, 429}:
                raise ServiceError(
                    ErrorCode.RATE_LIMITED, "short link is temporarily unavailable", http_status=429
                )
            if response.status_code >= 500:
                raise ServiceError(
                    ErrorCode.UPSTREAM_UNAVAILABLE, "short link service is unavailable"
                )
            return ShortLinkResponse(status_code, location)
        except (OSError, httpx.HTTPError, TimeoutError) as exc:
            raise ServiceError(
                ErrorCode.UPSTREAM_UNAVAILABLE, "short link could not be resolved"
            ) from exc

    @property
    def account_uid(self) -> str:
        """Return only the account UID verified by the existing upstream nav exchange."""
        if self._verified_account_uid and self._verified_account_uid != self._state.dedeuserid:
            raise ServiceError(
                ErrorCode.CREDENTIALS_INVALID,
                "Bilibili account identity does not match credential state",
            )
        return self._verified_account_uid or ""

    def scheduling_metrics(self) -> dict[str, int]:
        """Expose non-secret counts without changing per-tool business attempt accounting."""
        return self._scheduler.metrics()

    def readiness(self) -> dict[str, bool]:
        return {
            "cookie": self._state.has_runtime_cookie,
            "wbi_key": self._wbi_key is not None and self._wbi_expires_at > self._clock(),
            "provider": not self._closed,
            "refresh_configured": self._refresh_configured(),
            "credential_state": self._credential_state_loaded,
        }

    async def request_data(
        self,
        path: str,
        params: dict[str, str | int | bool] | None = None,
        *,
        wbi: bool = False,
        wbi2: bool = False,
        not_found_codes: frozenset[int] = frozenset({-404, 62002}),
    ) -> JsonValue:
        """Send one authenticated JSON GET and return only its validated data field."""
        self._require_credentials()
        self._check_risk_gate(path)
        await self._maybe_refresh_credentials()
        original = dict(params or {})
        for attempt in range(2):
            resolved = await self._signed_params(original, wbi2=wbi2) if wbi else original
            payload = await self._request_payload(BASE_URL + path, resolved)
            code = _api_code(payload)
            if code == -403 and wbi and attempt == 0:
                await self._invalidate_wbi_key()
                continue
            if code != 0:
                raise _api_error(code, payload, not_found_codes=not_found_codes)
            if "data" not in payload:
                raise ServiceError(
                    ErrorCode.UPSTREAM_SCHEMA_CHANGED,
                    "Bilibili response omitted its data field",
                )
            return payload["data"]
        raise ServiceError(ErrorCode.RATE_LIMITED, "Bilibili WBI verification was rejected")

    async def request_live_data(
        self,
        path: str,
        params: dict[str, str | int | bool] | None = None,
    ) -> JsonValue:
        """Read audited live-room JSON endpoints through the same account budget."""
        if path not in {
            "/xlive/web-room/v1/index/getInfoByRoom",
            "/xlive/web-room/v1/index/getRoomPlayInfo",
            "/xlive/web-ucenter/user/MedalWall",
            "/xlive/app-room/v2/guardTab/topListNew",
        }:
            raise ServiceError(ErrorCode.INVALID_ARGUMENT, "unsupported live-room endpoint")
        self._require_credentials()
        self._check_risk_gate(path)
        await self._maybe_refresh_credentials()
        response = await self._request_http(
            "https://api.live.bilibili.com" + path,
            dict(params or {}),
            headers={
                "Referer": "https://live.bilibili.com/",
                "Origin": "https://live.bilibili.com",
            },
        )
        try:
            payload = as_object(response.json(), "live response")
        except ValueError as exc:
            raise ServiceError(
                ErrorCode.UPSTREAM_SCHEMA_CHANGED, "live response was not JSON"
            ) from exc
        code = _api_code(payload)
        if code != 0:
            raise _api_error(code, payload, not_found_codes=frozenset({-404, 19002003}))
        if "data" not in payload:
            raise ServiceError(ErrorCode.UPSTREAM_SCHEMA_CHANGED, "live response omitted data")
        return payload["data"]

    async def download_json(self, url: str) -> JsonObject:
        """Download one temporary subtitle JSON document without exposing its URL."""
        self._require_credentials()
        await self._maybe_refresh_credentials()
        normalized = "https:" + url if url.startswith("//") else url
        # Use the sender's URL parser so malformed upstream URLs become stable tool errors.
        try:
            parsed = httpx.URL(normalized)
            host = parsed.host
        except (httpx.InvalidURL, ValueError) as error:
            raise ServiceError(
                ErrorCode.UPSTREAM_SCHEMA_CHANGED, "Bilibili returned an invalid subtitle location"
            ) from error
        if parsed.scheme != "https" or not (
            host == "bilibili.com"
            or host.endswith(".bilibili.com")
            or host == "hdslb.com"
            or host.endswith(".hdslb.com")
        ):
            raise ServiceError(
                ErrorCode.UPSTREAM_SCHEMA_CHANGED,
                "Bilibili returned an invalid subtitle location",
            )
        response = await self._request_http(normalized, None, temporary_subtitle=True)
        try:
            return as_object(response.json(), "subtitle")
        except ValueError as exc:
            raise ServiceError(
                ErrorCode.UPSTREAM_SCHEMA_CHANGED,
                "Bilibili subtitle response was not JSON",
            ) from exc

    async def request_bytes(
        self,
        path: str,
        params: dict[str, str | int | bool] | None = None,
        *,
        wbi: bool = False,
    ) -> bytes:
        """Send one authenticated binary GET with optional first-request WBI signing."""
        self._require_credentials()
        self._check_risk_gate(path)
        await self._maybe_refresh_credentials()
        original = dict(params or {})
        for attempt in range(2):
            resolved = await self._signed_params(original, wbi2=False) if wbi else original
            response = await self._request_http(BASE_URL + path, resolved, expects_json=False)
            try:
                provider_error = as_object(response.json(), "binary_response")
            except ValueError:
                return response.content
            code = _api_code(provider_error)
            if code == -403 and wbi and attempt == 0:
                await self._invalidate_wbi_key()
                continue
            if code != 0:
                raise _api_error(code, provider_error, not_found_codes=frozenset({-404, 62002}))
            raise ServiceError(
                ErrorCode.UPSTREAM_SCHEMA_CHANGED,
                "Bilibili binary endpoint returned an unexpected JSON envelope",
            )
        raise ServiceError(ErrorCode.RATE_LIMITED, "Bilibili WBI verification was rejected")

    async def _request_payload(
        self,
        url: str,
        params: dict[str, str | int | bool],
    ) -> JsonObject:
        """Validate a top-level API object after bounded transport retry."""
        response = await self._request_http(url, params)
        try:
            return as_object(response.json(), "response")
        except ValueError as exc:
            raise ServiceError(
                ErrorCode.UPSTREAM_SCHEMA_CHANGED,
                "Bilibili response was not JSON",
            ) from exc

    async def _request_http(
        self,
        url: str,
        params: dict[str, str | int | bool] | None,
        *,
        temporary_subtitle: bool = False,
        expects_json: bool = True,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """Retry one network or upstream server failure while preserving cancellation."""
        exchange_id = uuid.uuid4().hex
        for attempt in range(2):
            attempt_started = time.monotonic()
            try:
                request = self._client.build_request(
                    "GET",
                    url,
                    params=params,
                    headers=headers,
                    extensions={"fxbilibili.expects_json": expects_json},
                )
                host = request.url.host
            except (httpx.InvalidURL, ValueError) as error:
                raise ServiceError(
                    ErrorCode.UPSTREAM_SCHEMA_CHANGED,
                    "Bilibili returned an invalid request location",
                ) from error
            # Signed CDN URLs authorize downloads; login cookies must stay on Bilibili's domain.
            if host != "bilibili.com" and not host.endswith(".bilibili.com"):
                request.headers.pop("cookie", None)
            if self._debug_log_http:
                emit_http_debug(
                    {
                        "boundary": "upstream",
                        "phase": "request",
                        "exchange_id": exchange_id,
                        "attempt": attempt + 1,
                        "request": {
                            "method": request.method,
                            "url": redacted_url(
                                str(request.url), temporary_subtitle=temporary_subtitle
                            ),
                            "headers": redacted_headers(request.headers.multi_items()),
                            "body": encoded_body(request.content),
                        },
                    }
                )
            try:
                response = await self._send_business_request(request)
            except asyncio.CancelledError as exc:
                if self._debug_log_http:
                    self._log_request_failure(
                        exchange_id,
                        attempt + 1,
                        attempt_started,
                        exc,
                    )
                raise
            except ServiceError as exc:
                if self._debug_log_http:
                    self._log_request_failure(exchange_id, attempt + 1, attempt_started, exc)
                raise
            except httpx.RequestError as exc:
                if self._debug_log_http:
                    self._log_request_failure(
                        exchange_id,
                        attempt + 1,
                        attempt_started,
                        exc,
                    )
                if attempt == 0:
                    await asyncio.sleep(self._retry_delay)
                    continue
                raise ServiceError(
                    ErrorCode.UPSTREAM_UNAVAILABLE,
                    "Bilibili request could not be completed",
                ) from exc
            if self._debug_log_http:
                emit_http_debug(
                    {
                        "boundary": "upstream",
                        "phase": "response",
                        "exchange_id": exchange_id,
                        "attempt": attempt + 1,
                        "duration_ms": round((time.monotonic() - attempt_started) * 1000),
                        "response": {
                            "status": response.status_code,
                            "headers": redacted_headers(response.headers.multi_items()),
                            "body": encoded_body(response.content),
                        },
                    }
                )
            if response.status_code in {502, 503, 504} and attempt == 0:
                await asyncio.sleep(self._retry_delay)
                continue
            if response.status_code == 429:
                raise ServiceError(
                    ErrorCode.RATE_LIMITED,
                    "Bilibili capacity is busy; retry later",
                    http_status=429,
                    retry_after_seconds=_retry_after_seconds(response),
                )
            _raise_http_status(response.status_code)
            if _risk_reason(response) is not None:
                self._check_risk_gate(_risk_key(request))
            return response
        raise ServiceError(ErrorCode.UPSTREAM_UNAVAILABLE, "Bilibili request failed")

    async def _send_business_request(self, request: httpx.Request) -> httpx.Response:
        """Pause challenged endpoints; serialize search and allow one recovery probe at a time."""
        key = _risk_key(request)
        gate = self._risk_gates.setdefault(key, _RiskGate())
        self._check_risk_gate(key)
        # A challenged endpoint recovers with one probe; unaffected readers retain concurrency.
        if key == SEARCH_PATH or gate.failures:
            async with gate.lock:
                self._check_risk_gate(key)
                if key == SEARCH_PATH and self._upstream_rate:
                    delay = gate.last_sent + 0.5 - self._risk_clock()
                    if delay > 0:
                        await asyncio.sleep(delay)
                return await self._send_risk_checked(request, key, gate)
        return await self._send_risk_checked(request, key, gate)

    async def _send_risk_checked(
        self, request: httpx.Request, key: str, gate: _RiskGate
    ) -> httpx.Response:
        """Send once and pause the affected endpoint when Bilibili rejects it."""
        gate.last_sent = self._risk_clock()
        response = await self._send_counted_request(request)
        # Respect HTTP 429 before interpreting any API challenge envelope.
        if response.status_code == 429:
            return response
        reason = _risk_reason(response)
        if reason is None:
            if response.is_success:
                gate.failures = 0
                gate.blocked_until = 0.0
            return response
        gate.failures = min(gate.failures + 1, 5)
        seconds = min(30 * 2 ** (gate.failures - 1), 300)
        gate.blocked_until = self._risk_clock() + seconds
        # A challenge may mean stale WBI keys; refresh them after the cooldown.
        if reason == "VoC":
            self._wbi_expires_at = 0.0
        from .logging_context import current_tool_call

        context = current_tool_call()
        _logger.warning(
            json.dumps(
                {
                    "event": "upstream_risk_detected",
                    "reason": reason,
                    "endpoint": key,
                    "retry_after_seconds": seconds,
                    "request_id": context.request_id if context else None,
                    "tool": context.tool if context else None,
                },
                separators=(",", ":"),
            )
        )
        raise ServiceError(
            ErrorCode.RATE_LIMITED,
            f"Bilibili risk control paused this endpoint; retry after {seconds} seconds",
            http_status=429,
            retry_after_seconds=seconds,
        )

    def _check_risk_gate(self, key: str) -> None:
        """Reject before refreshing WBI or counting work while the endpoint is paused."""
        gate = self._risk_gates.get(key)
        if gate is not None and gate.blocked_until > self._risk_clock():
            seconds = max(1, math.ceil(gate.blocked_until - self._risk_clock()))
            raise ServiceError(
                ErrorCode.RATE_LIMITED,
                f"Bilibili risk control paused this endpoint; retry after {seconds} seconds",
                http_status=429,
                retry_after_seconds=seconds,
            )

    async def _send_counted_request(self, request: httpx.Request) -> httpx.Response:
        """Budget actual sends; bounded tool fan-out waits for upstream concurrency slots."""
        async with self.upstream_request_slot(endpoint=_risk_key(request)) as record_status:
            response = await self._send_bounded_request(request)
            record_status(response.status_code)
            if response.status_code == 429:
                self._upstream_cooldown = time.monotonic() + _retry_after_seconds(response)
            return response

    async def _send_bounded_request(self, request: httpx.Request) -> httpx.Response:
        """Bound decoded response bytes and wall-clock time before retaining any complete body."""
        try:
            async with asyncio.timeout(UPSTREAM_RESPONSE_TIMEOUT_SECONDS):
                response = await self._client.send(request, stream=True)
                try:
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        if len(body) + len(chunk) > MAX_UPSTREAM_RESPONSE_BYTES:
                            raise ServiceError(
                                ErrorCode.UPSTREAM_UNAVAILABLE,
                                "Bilibili response exceeds byte limit",
                            )
                        body.extend(chunk)
                    # Attach wire headers afterwards because aiter_bytes already decompresses.
                    buffered = httpx.Response(
                        response.status_code,
                        content=bytes(body),
                        request=request,
                        extensions=response.extensions,
                    )
                    buffered.headers = response.headers
                    return buffered
                finally:
                    await response.aclose()
        except TimeoutError as error:
            raise httpx.ReadTimeout(
                "Bilibili response deadline exceeded", request=request
            ) from error

    @asynccontextmanager
    async def upstream_request_slot(
        self, maintenance: bool = False, endpoint: str = "upstream"
    ) -> AsyncIterator[Callable[[int], None]]:
        """Admit HTTP sends through shared rate, concurrency and request limits."""
        from .logging_context import current_tool_call

        context = current_tool_call()
        owner = "maintenance" if maintenance else context.request_id if context else "background"
        queued_at = time.monotonic()

        def before_send() -> None:
            """Reject cooldown and exhausted call work before spending a scheduler permit."""
            cooldown = self._maintenance_cooldown if maintenance else self._upstream_cooldown
            if time.monotonic() < cooldown:
                raise ServiceError(
                    ErrorCode.RATE_LIMITED,
                    "shared Bilibili request budget is busy",
                    http_status=429,
                    retry_after_seconds=max(1, math.ceil(cooldown - time.monotonic())),
                )
            if not maintenance:
                self._count_business_attempt()

        async with self._scheduler.slot(
            owner=owner, maintenance=maintenance, endpoint=endpoint, on_admit=before_send
        ):
            if self._debug_log_http:
                # Request construction logs precede pacing; admission marks the actual send start.
                emit_http_debug(
                    {
                        "boundary": "upstream",
                        "phase": "admitted",
                        "maintenance": maintenance,
                        "endpoint": endpoint,
                        "queue_wait_ms": round((time.monotonic() - queued_at) * 1000, 3),
                    }
                )
            yield self._record_maintenance_status if maintenance else self._record_upstream_status

    def _record_maintenance_status(self, status: int) -> None:
        """Keep credential-maintenance cooldown separate from content-request cooldown."""
        if status in {403, 429}:
            self._maintenance_cooldown = time.monotonic() + 30

    def _record_upstream_status(self, status: int) -> None:
        """Pause every admitted sender after an upstream HTTP refusal."""
        if status in {403, 429}:
            self._upstream_cooldown = time.monotonic() + 30

    def _count_business_attempt(self) -> None:
        """Check the shared child-task counter immediately before sending a business request."""
        from .logging_context import current_tool_call

        context = current_tool_call()
        budgets = _BUSINESS_BUDGETS.get()
        if any(budget.attempts >= budget.max_attempts for budget in budgets):
            raise BusinessBudgetExhausted()
        if context is not None:
            if (
                self._upstream_requests_per_call
                and context.upstream_work.attempts >= self._upstream_requests_per_call
            ):
                raise ServiceError(ErrorCode.RATE_LIMITED, "tool upstream work budget is exhausted")
            context.upstream_work.send()
        for budget in budgets:
            budget.attempts += 1

    @asynccontextmanager
    async def scoped_work(self, max_attempts: int) -> AsyncIterator[BusinessBudget]:
        """Count actual sends, including retries and child tasks, within a bounded operation."""
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        budget = BusinessBudget(max_attempts)
        token = _BUSINESS_BUDGETS.set((*_BUSINESS_BUDGETS.get(), budget))
        try:
            yield budget
        finally:
            _BUSINESS_BUDGETS.reset(token)

    @staticmethod
    def _log_request_failure(
        exchange_id: str,
        attempt: int,
        started: float,
        error: BaseException,
    ) -> None:
        """Record only a failed attempt's type after the request was already redacted."""
        emit_http_debug(
            {
                "boundary": "upstream",
                "phase": "response",
                "exchange_id": exchange_id,
                "attempt": attempt,
                "duration_ms": round((time.monotonic() - started) * 1000),
                "response": {
                    "status": None,
                    "headers": [],
                    "body": encoded_body(b""),
                    "error_type": type(error).__name__,
                },
            }
        )

    async def _signed_params(
        self,
        params: dict[str, str | int | bool],
        *,
        wbi2: bool,
    ) -> dict[str, str | int | bool]:
        """Apply WBI2 risk fields before calculating the WBI digest."""
        signed = dict(params)
        signed.pop("w_rid", None)
        if wbi2:
            signed.update(
                {
                    "dm_img_list": "[]",
                    "dm_img_str": _random_pair(),
                    "dm_cover_img_str": _random_pair(),
                    "dm_img_inter": '{"ds":[],"wh":[0,0,0],"of":[0,0,0]}',
                }
            )
        signed.setdefault("web_location", 1550101)
        signed["wts"] = int(self._clock())
        encoded = urllib.parse.urlencode(sorted(signed.items()))
        signed["w_rid"] = hashlib.md5(
            (encoded + await self._get_wbi_key()).encode(),
            usedforsecurity=False,
        ).hexdigest()
        return signed

    async def _get_wbi_key(self) -> str:
        """Cache the nav-derived mixin key for one hour and coalesce refreshes."""
        if self._wbi_key is not None and self._wbi_expires_at > self._clock():
            return self._wbi_key
        async with self._wbi_lock:
            if self._wbi_key is not None and self._wbi_expires_at > self._clock():
                return self._wbi_key
            payload = await self._request_payload(BASE_URL + "/x/web-interface/nav", {})
            if _api_code(payload) != 0:
                raise _api_error(_api_code(payload), payload, not_found_codes=frozenset())
            data = as_object(payload.get("data"), "nav.data")
            mid = data.get("mid")
            if type(mid) is int and mid > 0:
                self._verified_account_uid = str(mid)
            wbi_img = as_object(data.get("wbi_img"), "nav.data.wbi_img")
            img_url = wbi_img.get("img_url")
            sub_url = wbi_img.get("sub_url")
            if not isinstance(img_url, str) or not isinstance(sub_url, str):
                raise ServiceError(
                    ErrorCode.UPSTREAM_SCHEMA_CHANGED,
                    "Bilibili nav response omitted WBI image keys",
                )
            raw = _filename_stem(img_url) + _filename_stem(sub_url)
            key = "".join(raw[index] for index in _MIXIN_TABLE if index < len(raw))[:32]
            if len(key) != 32:
                raise ServiceError(
                    ErrorCode.UPSTREAM_SCHEMA_CHANGED,
                    "Bilibili nav returned invalid WBI keys",
                )
            self._wbi_key = key
            self._wbi_expires_at = self._clock() + 3_600
            return key

    async def _invalidate_wbi_key(self) -> None:
        async with self._wbi_lock:
            self._wbi_key = None
            self._wbi_expires_at = 0.0

    async def _credential_candidates(self) -> list[tuple[CredentialState, bool]]:
        """Prefer private persisted state while retaining env bootstrap recovery."""
        try:
            persisted = await self._store.load()
        except ValueError as exc:
            _logger.warning(
                json.dumps(
                    {
                        "event": "credential_state_rejected",
                        "error_type": type(exc).__name__,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                )
            )
            persisted = None
        candidates: list[tuple[CredentialState, bool]] = []
        account_changed = (
            persisted is not None
            and bool(persisted.dedeuserid)
            and bool(self._bootstrap_state.dedeuserid)
            and persisted.dedeuserid != self._bootstrap_state.dedeuserid
        )
        if persisted is not None and not account_changed:
            candidates.append((persisted, True))
        if not candidates or self._bootstrap_state.cookie != candidates[0][0].cookie:
            candidates.append((self._bootstrap_state, False))
        elif not candidates[0][0].ac_time_value and self._bootstrap_state.ac_time_value:
            candidates[0] = (self._bootstrap_state, False)
        return candidates

    def _apply_credentials(self, state: CredentialState) -> None:
        """Replace only in-memory headers and invalidate credential-bound WBI state."""
        self._state = state
        if state.cookie:
            self._client.headers["Cookie"] = state.cookie
        else:
            self._client.headers.pop("Cookie", None)
        self._client.cookies.clear()
        self._verified_account_uid = None
        self._wbi_key = None
        self._wbi_expires_at = 0.0
        self._refresh_checked_at = 0.0

    def _refresh_configured(self) -> bool:
        return self._store.configured and self._state.can_refresh

    async def _maybe_refresh_credentials(self, *, force: bool = False) -> None:
        """Check Bilibili's refresh flag at most hourly and rotate credentials when requested."""
        if not self._refresh_configured():
            return
        now = self._clock()
        if (
            not force
            and self._state.pending_refresh_token is None
            and now - self._refresh_checked_at < self._refresh_check_interval
        ):
            return
        async with self._refresh_lock:
            if self._state.pending_refresh_token is not None:
                await self._confirm_refresh()
                self._refresh_checked_at = self._clock()
                await self._get_wbi_key()
                return
            now = self._clock()
            if not force and now - self._refresh_checked_at < self._refresh_check_interval:
                return
            payload = await self._credential_json_request("GET", COOKIE_INFO_URL, self._state)
            code = _api_code(payload)
            if code != 0:
                raise _api_error(code, payload, not_found_codes=frozenset())
            data = as_object(payload.get("data"), "cookie.info.data")
            refresh = data.get("refresh")
            if not isinstance(refresh, bool):
                raise ServiceError(
                    ErrorCode.UPSTREAM_SCHEMA_CHANGED,
                    "Bilibili cookie info omitted its refresh flag",
                )
            if refresh:
                await self._refresh_credentials()
            self._refresh_checked_at = self._clock()

    async def _login_with_qr(self) -> None:
        """Publish one QR, poll for 60 seconds, persist login, then remove the PNG copy."""
        anonymous = await self._qr_device_state()
        payload = await self._credential_json_request(
            "GET",
            QR_GENERATE_URL,
            anonymous,
            params={"source": "main-fe-header"},
        )
        login_url, qrcode_key = _qr_login_fields(payload)
        terminal_qr = await asyncio.to_thread(render_terminal_qr, login_url)
        png_notice = f"\nPNG copy: {self._qr_code_path}" if self._qr_code_path is not None else ""
        try:
            if self._qr_code_path is not None:
                await _to_thread_complete(partial(save_qr_png, login_url, self._qr_code_path))
            _logger.info(
                json.dumps(
                    {"event": "qr_login_started", "timeout_seconds": 60},
                    separators=(",", ":"),
                    sort_keys=True,
                )
            )
            self._qr_output(
                "\nBilibili login required. Scan this QR code within 60 seconds:\n\n"
                + terminal_qr
                + png_notice
                + "\n\nWaiting for scan confirmation..."
            )
            await self._poll_qr_login(anonymous, qrcode_key)
        finally:
            if self._qr_code_path is not None:
                await _to_thread_complete(partial(remove_qr_png, self._qr_code_path))

    async def _poll_qr_login(self, anonymous: CredentialState, qrcode_key: str) -> None:
        """Poll one generated QR key until login succeeds, expires, or reaches 60 seconds."""
        deadline = self._clock() + self._qr_login_timeout
        scanned = False
        while self._clock() < deadline:
            response = await self._credential_request(
                "GET",
                QR_POLL_URL,
                anonymous,
                params={"qrcode_key": qrcode_key, "source": "main-fe-header"},
            )
            poll_data, state_code = _qr_poll_state(response)
            if state_code == 0:
                await self._complete_qr_login(poll_data, response, anonymous)
                return
            if state_code == 86090:
                if not scanned:
                    _logger.info('{"event":"qr_login_scanned"}')
                    scanned = True
            elif state_code == 86038:
                break
            elif state_code != 86101:
                raise ServiceError(
                    ErrorCode.CREDENTIALS_INVALID,
                    f"Bilibili QR login failed with code {state_code}",
                )
            await asyncio.sleep(self._qr_poll_interval)
        _logger.warning('{"event":"qr_login_timeout","timeout_seconds":60}')
        raise ServiceError(
            ErrorCode.CREDENTIALS_INVALID,
            "Bilibili QR login timed out after 60 seconds; restart the container to retry",
        )

    async def _qr_device_state(self) -> CredentialState:
        """Obtain provider-issued Buvid values before creating the QR login session."""
        empty = CredentialState.create(None, None)
        payload = await self._credential_json_request("GET", FINGER_SPI_URL, empty)
        if _api_code(payload) != 0:
            raise _api_error(_api_code(payload), payload, not_found_codes=frozenset())
        data = as_object(payload.get("data"), "finger.spi.data")
        buvid3 = data.get("b_3")
        buvid4 = data.get("b_4")
        if not isinstance(buvid3, str) or not isinstance(buvid4, str):
            raise ServiceError(
                ErrorCode.UPSTREAM_SCHEMA_CHANGED,
                "Bilibili device response omitted Buvid values",
            )
        return CredentialState.create(f"buvid3={buvid3}; buvid4={buvid4}", None)

    async def _complete_qr_login(
        self,
        poll_data: JsonObject,
        response: httpx.Response,
        device_state: CredentialState,
    ) -> None:
        """Validate, persist, and activate credentials returned by a confirmed QR login."""
        redirect_url = poll_data.get("url")
        refresh_token = poll_data.get("refresh_token")
        if not isinstance(redirect_url, str) or not isinstance(refresh_token, str):
            raise ServiceError(
                ErrorCode.UPSTREAM_SCHEMA_CHANGED,
                "Bilibili QR login response omitted credentials",
            )
        response_cookies = cookie_values(device_state.cookie) | {
            cookie.name: cookie.value for cookie in response.cookies.jar if cookie.value is not None
        }
        try:
            state = credential_state_from_qr_login(
                redirect_url,
                refresh_token,
                response_cookies,
            )
        except ValueError as exc:
            raise ServiceError(
                ErrorCode.UPSTREAM_SCHEMA_CHANGED,
                "Bilibili QR login returned incomplete credentials",
            ) from exc
        await self._persist_credentials(state)
        await self._get_wbi_key()
        _logger.info('{"event":"qr_login_succeeded"}')

    async def _refresh_credentials(self) -> None:
        """Rotate and persist Cookie/refresh token before confirming the old token."""
        old_state = self._state
        values = cookie_values(old_state.cookie)
        correspond_path = generate_correspond_path(round(self._clock() * 1000))
        request_state = CredentialState.create(
            merge_cookie(old_state.cookie, {"buvid3": str(uuid.uuid4())}),
            old_state.ac_time_value,
        )
        document = await self._credential_text_request(
            "GET",
            CORRESPOND_BASE_URL + correspond_path,
            request_state,
        )
        try:
            refresh_csrf = parse_refresh_csrf(document)
        except ValueError as exc:
            raise ServiceError(
                ErrorCode.UPSTREAM_SCHEMA_CHANGED,
                "Bilibili refresh document changed",
            ) from exc
        response = await self._credential_request(
            "POST",
            COOKIE_REFRESH_URL,
            request_state,
            data={
                "csrf": values["bili_jct"],
                "refresh_csrf": refresh_csrf,
                "refresh_token": old_state.ac_time_value,
                "source": "main_web",
            },
        )
        payload = _response_json(response, "cookie.refresh")
        if _api_code(payload) != 0:
            raise _api_error(_api_code(payload), payload, not_found_codes=frozenset())
        data = as_object(payload.get("data"), "cookie.refresh.data")
        refresh_token = data.get("refresh_token")
        if not isinstance(refresh_token, str) or not refresh_token:
            raise ServiceError(
                ErrorCode.UPSTREAM_SCHEMA_CHANGED,
                "Bilibili refresh response omitted its refresh token",
            )
        updates = {
            cookie.name: cookie.value for cookie in response.cookies.jar if cookie.value is not None
        }
        if not {"SESSDATA", "bili_jct", "DedeUserID"}.issubset(updates):
            raise ServiceError(
                ErrorCode.UPSTREAM_SCHEMA_CHANGED,
                "Bilibili refresh response omitted required Cookies",
            )
        new_state = CredentialState.create(
            merge_cookie(old_state.cookie, updates),
            refresh_token,
            pending_refresh_token=old_state.ac_time_value,
        )
        await self._persist_credentials(new_state)
        await self._confirm_refresh()
        self._refresh_checked_at = self._clock()
        await self._get_wbi_key()

    async def _persist_credentials(self, state: CredentialState) -> None:
        """Commit disk and runtime state together, even if the caller cancels the write."""
        saving = asyncio.create_task(self._store.save(state))
        try:
            await asyncio.shield(saving)
        except asyncio.CancelledError:
            # Atomic replacement runs in a worker; cancellation cannot undo its completed write.
            await saving
            self._apply_credentials(state)
            self._credential_state_loaded = True
            raise
        self._apply_credentials(state)
        self._credential_state_loaded = True

    async def _confirm_refresh(self) -> None:
        """Resume persisted confirmation without issuing another credential rotation."""
        state = self._state
        assert state.pending_refresh_token is not None
        csrf = cookie_values(state.cookie).get("bili_jct", "")
        payload = await self._credential_json_request(
            "POST",
            COOKIE_CONFIRM_URL,
            state,
            data={"csrf": csrf, "refresh_token": state.pending_refresh_token},
        )
        if _api_code(payload) != 0:
            raise _api_error(_api_code(payload), payload, not_found_codes=frozenset())
        await self._persist_credentials(replace(state, pending_refresh_token=None))

    async def _credential_json_request(
        self,
        method: str,
        url: str,
        state: CredentialState,
        *,
        data: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
    ) -> JsonObject:
        """Send a credential request and parse JSON without entering HTTP debug logs."""
        response = await self._credential_request(method, url, state, data=data, params=params)
        return _response_json(response, "credential")

    async def _credential_text_request(
        self,
        method: str,
        url: str,
        state: CredentialState,
    ) -> str:
        """Send a credential request and return its non-secret HTML document."""
        response = await self._credential_request(method, url, state)
        return response.text

    async def _credential_request(
        self,
        method: str,
        url: str,
        state: CredentialState,
        *,
        data: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
    ) -> httpx.Response:
        """Retry credential network failures once while never logging auth exchanges."""
        request_cookie = (
            merge_cookie(state.cookie, {"ac_time_value": state.ac_time_value})
            if state.ac_time_value
            else state.cookie
        )
        for attempt in range(2):
            request = self._client.build_request(
                method,
                url,
                headers={"Cookie": request_cookie} if request_cookie else None,
                data=data,
                params=params,
            )
            try:
                async with self.upstream_request_slot(maintenance=True) as record_status:
                    response = await self._send_bounded_request(request)
                    record_status(response.status_code)
            except asyncio.CancelledError:
                raise
            except httpx.RequestError as exc:
                if attempt == 0:
                    await asyncio.sleep(self._retry_delay)
                    continue
                raise ServiceError(
                    ErrorCode.UPSTREAM_UNAVAILABLE,
                    "Bilibili credential service could not be reached",
                ) from exc
            if response.status_code in {502, 503, 504} and attempt == 0:
                await asyncio.sleep(self._retry_delay)
                continue
            _raise_http_status(response.status_code)
            return response
        raise ServiceError(ErrorCode.UPSTREAM_UNAVAILABLE, "Bilibili credential request failed")

    def _require_credentials(self) -> None:
        if not self._state.has_runtime_cookie:
            raise ServiceError(
                ErrorCode.CREDENTIALS_INVALID,
                "a valid server-side Bilibili Cookie is required",
            )


def _api_code(payload: JsonObject) -> int:
    code = payload.get("code")
    if not isinstance(code, int) or isinstance(code, bool):
        raise ServiceError(
            ErrorCode.UPSTREAM_SCHEMA_CHANGED,
            "Bilibili response omitted its numeric code",
        )
    return code


def _risk_key(request: httpx.Request) -> str:
    # Temporary subtitle paths contain credentials and must never become logged endpoint names.
    return (
        request.url.path
        if request.url.host in {"api.bilibili.com", "api.live.bilibili.com", "app.bilibili.com"}
        else "subtitle"
    )


def _response_voc(response: httpx.Response) -> str | None:
    """Read the private VoC only at the upstream wire boundary."""
    header: str | None = response.headers.get("x-bili-gaia-vvoucher")
    if header:
        return header
    try:
        payload: object = response.json()
    except ValueError:
        return None
    data = payload.get("data") if isinstance(payload, dict) else None
    value: object = data.get("v_voucher") if isinstance(data, dict) else None
    return value if isinstance(value, str) and value else None


def _risk_reason(response: httpx.Response) -> str | None:
    """Recognize observed challenges, including successful HTTP/code envelopes and headers."""
    if response.status_code in {403, 429}:
        return f"http_{response.status_code}"
    if _response_voc(response) is not None:
        return "VoC"
    try:
        payload: object = response.json()
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    code = payload.get("code")
    if type(code) is int and code in {-352, -412, -509}:
        return f"api_{code}"
    return None


def _qr_login_fields(payload: JsonObject) -> tuple[str, str]:
    """Validate a generated login URL and its private poll key."""
    if _api_code(payload) != 0:
        raise _api_error(_api_code(payload), payload, not_found_codes=frozenset())
    data = as_object(payload.get("data"), "qrcode.generate.data")
    login_url = data.get("url")
    qrcode_key = data.get("qrcode_key")
    if not isinstance(login_url, str) or not isinstance(qrcode_key, str):
        raise ServiceError(
            ErrorCode.UPSTREAM_SCHEMA_CHANGED,
            "Bilibili QR generate response omitted login fields",
        )
    return login_url, qrcode_key


def _qr_poll_state(response: httpx.Response) -> tuple[JsonObject, int]:
    """Validate one QR poll envelope and return its provider state code."""
    payload = _response_json(response, "qrcode.poll")
    if _api_code(payload) != 0:
        raise _api_error(_api_code(payload), payload, not_found_codes=frozenset())
    data = as_object(payload.get("data"), "qrcode.poll.data")
    state_code = data.get("code")
    if not isinstance(state_code, int) or isinstance(state_code, bool):
        raise ServiceError(
            ErrorCode.UPSTREAM_SCHEMA_CHANGED,
            "Bilibili QR poll response omitted its state code",
        )
    return data, state_code


async def _to_thread_complete[T](operation: Callable[[], T]) -> T:
    """Let a side-effecting thread finish before propagating task cancellation."""
    task = asyncio.create_task(asyncio.to_thread(operation))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


def _response_json(response: httpx.Response, context: str) -> JsonObject:
    """Parse a credential response without including its body in public errors."""
    try:
        return as_object(response.json(), context)
    except ValueError as exc:
        raise ServiceError(
            ErrorCode.UPSTREAM_SCHEMA_CHANGED,
            "Bilibili credential response was not JSON",
        ) from exc


def _api_error(
    code: int,
    payload: JsonObject,
    *,
    not_found_codes: frozenset[int],
) -> ServiceError:
    """Map known provider codes to the stable public error vocabulary."""
    if code in not_found_codes:
        return ServiceError(ErrorCode.NOT_FOUND, "the requested Bilibili resource was not found")
    if code == 62012:
        # Uploader-only videos are inaccessible resources, not transient API failures.
        return ServiceError(ErrorCode.NOT_FOUND, "this video is only visible to its uploader")
    if code == -101:
        return ServiceError(
            ErrorCode.CREDENTIALS_INVALID, "the server-side Bilibili Cookie is invalid"
        )
    if code == -111:
        # Search can reject selected keywords with -111 while the same account stays usable.
        return ServiceError(
            ErrorCode.UPSTREAM_UNAVAILABLE,
            "Bilibili rejected this request with code -111 (CSRF validation failed)",
        )
    if code in {-352, -412, -403, -509}:
        return ServiceError(ErrorCode.RATE_LIMITED, "Bilibili risk control rejected the request")
    if code == 22115:
        # A public count does not grant access to a list hidden by its owner.
        return ServiceError(
            ErrorCode.NOT_FOUND,
            "this public resource is unavailable because of user privacy settings",
        )
    message = payload.get("message", payload.get("msg"))
    if code == -400 and isinstance(message, str) and "不存在" in message:
        return ServiceError(ErrorCode.NOT_FOUND, "the requested Bilibili resource was not found")
    return ServiceError(ErrorCode.UPSTREAM_UNAVAILABLE, f"Bilibili API returned code {code}")


def _raise_http_status(status: int) -> None:
    """Map HTTP failures without retrying credential, not-found, or rate errors."""
    if status < 400:
        return
    if status == 401:
        raise ServiceError(
            ErrorCode.CREDENTIALS_INVALID, "the server-side Bilibili Cookie is invalid"
        )
    if status == 429:
        raise ServiceError(
            ErrorCode.RATE_LIMITED,
            "Bilibili capacity is busy; retry later",
            http_status=429,
            retry_after_seconds=30,
        )
    if status == 403:
        raise ServiceError(ErrorCode.RATE_LIMITED, "Bilibili rate or risk limit was reached")
    if status == 404:
        raise ServiceError(ErrorCode.NOT_FOUND, "the requested Bilibili resource was not found")
    raise ServiceError(ErrorCode.UPSTREAM_UNAVAILABLE, "Bilibili HTTP service is unavailable")


def _filename_stem(url: str) -> str:
    return url.rsplit("/", 1)[-1].split(".", 1)[0]


def _random_pair() -> str:
    first = secrets.choice(_WBI2_ALPHABET)
    second = secrets.choice(_WBI2_ALPHABET.replace(first, ""))
    return first + second


def _retry_after_seconds(response: httpx.Response) -> int:
    """Honor numeric upstream backpressure, bounded to one hour, with a thirty-second default."""
    value = response.headers.get("retry-after", "")
    return max(1, min(3600, int(value))) if value.isascii() and value.isdigit() else 30
