"""Configuration, strict HTTP routes, runtime factory, lifecycle, and process entrypoint."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import urllib.parse
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import uvicorn
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import TypeAdapter
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from .cache import CachePolicy
from .client import BilibiliClient
from .http_debug import emit_http_debug, encoded_body, redacted_headers, redacted_url
from .mcp_tools import (
    SERVER_INSTRUCTIONS,
    BilibiliFastMCP,
    logger,
    register_tools,
    tighten_tool_schemas,
)
from .scheduler import SendBudget

DEFAULT_ALLOWED_HOSTS = ("bilimcp:*", "127.0.0.1:*", "localhost:*", "[::1]:*")
MAX_DEBUG_REQUEST_BODY_BYTES = 128 * 1024
# Retain tools/list definitions, which exceed the smaller upstream-body audit allowance.
MAX_DEBUG_MCP_RESPONSE_BODY_BYTES = 256 * 1024
DEBUG_REQUEST_TIMEOUT_SECONDS = 10.0


@dataclass(frozen=True, slots=True)
class Config:
    cookie: str | None = field(repr=False)
    host: str
    port: int
    allowed_hosts: tuple[str, ...]
    ac_time_value: str | None = field(default=None, repr=False)
    credential_state_path: Path | None = None
    qr_code_path: Path | None = None
    debug_allow_external: bool = False
    debug_log_http: bool = False
    upstream_requests_per_minute: int = 0
    upstream_burst: int = 60
    upstream_concurrency: int = 8
    upstream_requests_per_call: int = 0
    upstream_spacing_seconds: float = 0.0
    upstream_endpoint_limits: dict[str, int] = field(default_factory=dict)
    upstream_queue_timeout_seconds: float = 15.0
    upstream_queue_capacity: int = 64
    cache_max_items: int = 1_000_000
    cache_max_bytes: int = 256 * 1024 * 1024
    history_path: Path | None = None
    history_max_bytes: int = 4 * 1024**3
    history_ttl_seconds: int = 14 * 86400
    video_cache_path: Path | None = None
    video_cache_max_bytes: int = 20 * 1024**3
    video_cache_ttl_seconds: int = 7 * 86400

    def __post_init__(self) -> None:
        """Reject invalid cache capacities and overlapping private state files."""
        if self.history_max_bytes < 100000 or not 1 <= self.history_ttl_seconds <= 30 * 86400:
            raise ValueError(
                "history capacity must be at least 100000 bytes and TTL at most 30 days"
            )
        if (
            self.video_cache_max_bytes < 8 * 1024**2
            or not 1 <= self.video_cache_ttl_seconds <= 30 * 86400
        ):
            raise ValueError("video cache capacity must be at least 8 MiB and TTL at most 30 days")
        if self.cache_max_items < 1 or self.cache_max_bytes < 1:
            raise ValueError("cache capacity must be positive")
        if (
            self.credential_state_path is not None
            and self.qr_code_path is not None
            and self.credential_state_path.resolve() == self.qr_code_path.resolve()
        ):
            raise ValueError("credential state and QR paths must be distinct")

    @classmethod
    def from_env(cls) -> Config:
        """Read process configuration without loading dotenv files or changing HOME."""
        port = int(os.getenv("FXBILIBILI_PORT", "8000"))
        if not 1 <= port <= 65535:
            raise ValueError("FXBILIBILI_PORT must be between 1 and 65535")
        return cls(
            cookie=None,
            host=os.getenv("FXBILIBILI_HOST", "127.0.0.1"),
            port=port,
            allowed_hosts=_allowed_hosts(),
            ac_time_value=None,
            credential_state_path=_optional_path_env("FXBILIBILI_CREDENTIALS_FILE")
            or (Path.cwd() / "data" / "credentials.json"),
            qr_code_path=_optional_path_env("FXBILIBILI_QR_CODE_FILE")
            or (Path.cwd() / "data" / "login-qr.png"),
            debug_allow_external=_bool_env("FXBILIBILI_DEBUG_ALLOW_EXTERNAL", False),
            debug_log_http=_bool_env("FXBILIBILI_DEBUG_LOG_HTTP", False),
            upstream_requests_per_minute=int(
                os.getenv("FXBILIBILI_UPSTREAM_REQUESTS_PER_MINUTE", "0")
            ),
            upstream_burst=int(os.getenv("FXBILIBILI_UPSTREAM_BURST", "60")),
            upstream_concurrency=int(os.getenv("FXBILIBILI_UPSTREAM_CONCURRENCY", "3")),
            upstream_requests_per_call=int(
                os.getenv("FXBILIBILI_UPSTREAM_REQUESTS_PER_CALL", "64")
            ),
            upstream_spacing_seconds=float(os.getenv("FXBILIBILI_UPSTREAM_SPACING_SECONDS", "0.1")),
            upstream_endpoint_limits=TypeAdapter(dict[str, int]).validate_json(
                os.getenv("FXBILIBILI_UPSTREAM_ENDPOINT_LIMITS") or "{}", strict=True
            ),
            upstream_queue_timeout_seconds=float(
                os.getenv("FXBILIBILI_UPSTREAM_QUEUE_TIMEOUT_SECONDS", "15")
            ),
            upstream_queue_capacity=int(os.getenv("FXBILIBILI_UPSTREAM_QUEUE_CAPACITY", "64")),
            cache_max_items=int(os.getenv("FXBILIBILI_CACHE_MAX_ITEMS", "1000000")),
            cache_max_bytes=int(os.getenv("FXBILIBILI_CACHE_MAX_BYTES", "268435456")),
            history_path=_optional_path_env("FXBILIBILI_HISTORY_FILE"),
            history_max_bytes=int(os.getenv("FXBILIBILI_HISTORY_MAX_BYTES", "4294967296")),
            history_ttl_seconds=int(os.getenv("FXBILIBILI_HISTORY_TTL_SECONDS", "1209600")),
            video_cache_path=_optional_path_env("FXBILIBILI_VIDEO_CACHE_FILE"),
            video_cache_max_bytes=int(os.getenv("FXBILIBILI_VIDEO_CACHE_MAX_BYTES", "21474836480")),
            video_cache_ttl_seconds=int(os.getenv("FXBILIBILI_VIDEO_CACHE_TTL_SECONDS", "604800")),
        )


@dataclass(frozen=True, slots=True)
class Runtime:
    config: Config
    cache_policy: CachePolicy
    client: BilibiliClient
    mcp: BilibiliFastMCP
    app: StrictHTTPApp


class StrictHTTPApp:
    """Expose only POST /mcp plus GET health and readiness JSON."""

    def __init__(
        self,
        mcp_app: ASGIApp,
        *,
        readiness: Callable[[], dict[str, bool]],
        cache_policy: CachePolicy,
        cursor_settings: tuple[int, int],
        on_startup: Callable[[], Awaitable[None]],
        on_shutdown: Callable[[], Awaitable[None]],
        debug_settings: tuple[bool, bool] = (False, False),
        cache_metrics: Callable[[], dict[str, object]] | None = None,
        upstream_metrics: Callable[[], dict[str, object]] | None = None,
    ) -> None:
        """Own routing metadata and lifecycle callbacks without global runtime state."""
        self._mcp_app = mcp_app
        self._readiness = readiness
        self._cache_policy = cache_policy
        self._cursor_capacity, self._cursor_ttl = cursor_settings
        self._on_startup = on_startup
        self._on_shutdown = on_shutdown
        self._debug_allow_external, self._debug_log_http = debug_settings
        self._cache_metrics = cache_metrics
        self._upstream_metrics = upstream_metrics
        self._debug_catalog_digest: str | None = None

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Dispatch documented routes and return machine-readable errors everywhere else."""
        if scope["type"] == "lifespan":
            await self._mcp_app(scope, self._lifespan_receive(receive), self._lifespan_send(send))
            return
        if scope["type"] != "http":
            await _json_error(404, "not_found", "route not found", scope, receive, send)
            return
        if self._debug_log_http:
            await self._call_http_with_debug(scope, receive, send)
            return
        await self._dispatch_http(scope, receive, send)

    async def _dispatch_http(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Route one HTTP request without buffering its body in the normal path."""
        path = scope["path"]
        method = scope["method"]
        if path == "/mcp" and method == "POST":
            await self._mcp_app(scope, receive, send)
            return
        if path == "/healthz" and method == "GET":
            await JSONResponse({"status": "ok"})(scope, receive, send)
            return
        if path == "/readyz" and method == "GET":
            state = self._readiness()
            ready = state["cookie"] and state["wbi_key"] and state["provider"]
            payload = {
                "status": "ready" if ready else "not_ready",
                "credentials": {
                    "cookie": state["cookie"],
                    "refresh_configured": state.get("refresh_configured", False),
                    "persistent_state": state.get("credential_state", False),
                },
                "wbi": {"key_loaded": state["wbi_key"]},
                "providers": {
                    "bilibili_json_api": state["provider"],
                },
                "authentication": {"mode": "none"},
                "cache": {
                    "max_items": self._cache_policy.max_items,
                    "ttl_seconds": self._cache_policy.ttl_seconds(),
                    "metrics": self._cache_metrics() if self._cache_metrics is not None else {},
                },
                "upstream": self._upstream_metrics() if self._upstream_metrics is not None else {},
                "cursor_kv": {
                    "token_length": 6,
                    "max_items": self._cursor_capacity,
                    "ttl_seconds": self._cursor_ttl,
                },
                "debug": {
                    "allow_external": self._debug_allow_external,
                    "log_http": self._debug_log_http,
                },
            }
            await JSONResponse(payload, status_code=200 if ready else 503)(scope, receive, send)
            return
        if path in {"/mcp", "/healthz", "/readyz"}:
            await _json_error(
                405,
                "invalid_argument",
                "method not allowed",
                scope,
                receive,
                send,
                allow="POST" if path == "/mcp" else "GET",
            )
            return
        await _json_error(404, "not_found", "route not found", scope, receive, send)

    async def _call_http_with_debug(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> None:
        """Capture one complete inbound exchange while preserving downstream ASGI behavior."""
        incoming: list[Message] = []
        request_body = bytearray()
        try:
            async with asyncio.timeout(DEBUG_REQUEST_TIMEOUT_SECONDS):
                while True:
                    message = await receive()
                    if message["type"] == "http.disconnect":
                        return
                    if message["type"] == "http.request":
                        body = message.get("body", b"")
                        if len(request_body) + len(body) > MAX_DEBUG_REQUEST_BODY_BYTES:
                            await _json_error(
                                413,
                                "invalid_argument",
                                "request body exceeds 128 KiB",
                                scope,
                                receive,
                                send,
                            )
                            return
                        request_body.extend(body)
                    incoming.append(message)
                    if not message.get("more_body", False):
                        break
        except TimeoutError:
            await _json_error(
                408, "invalid_argument", "request body deadline exceeded", scope, receive, send
            )
            return

        messages = iter(incoming)

        async def replay_receive() -> Message:
            """Replay buffered debug input before resuming the live receive channel."""
            buffered = next(messages, None)
            return buffered if buffered is not None else await receive()

        response_status: int | None = None
        response_headers: list[tuple[str, str]] = []
        response_body = bytearray()
        response_bytes = 0
        try:
            rpc_request = json.loads(request_body)
            catalog_response = (
                isinstance(rpc_request, dict) and rpc_request.get("method") == "tools/list"
            )
            image_response = (
                isinstance(rpc_request, dict)
                and isinstance(rpc_request.get("params"), dict)
                and rpc_request["params"].get("name") in {"get_image", "get_video_frame"}
            )
        except (ValueError, TypeError):
            catalog_response = False
            image_response = False

        async def capture_send(message: Message) -> None:
            """Capture response metadata while forwarding every ASGI message unchanged."""
            nonlocal response_status, response_headers, response_bytes
            if message["type"] == "http.response.start":
                response_status = message["status"]
                response_headers = _asgi_headers(message.get("headers", []))
            elif message["type"] == "http.response.body":
                body = message.get("body", b"")
                response_bytes += len(body)
                if not image_response:
                    response_body.extend(body)
            await send(message)

        error_type: str | None = None
        # Record only the type, then re-raise so cancellation and framework control flow survive.
        try:
            await self._dispatch_http(scope, replay_receive, capture_send)
        except BaseException as exc:
            error_type = type(exc).__name__
            raise
        finally:
            response: dict[str, object] = {
                "status": response_status,
                "headers": redacted_headers(response_headers),
                "body": {"encoding": "omitted", "bytes": str(response_bytes)}
                if image_response
                else self._debug_response_body(bytes(response_body), catalog=catalog_response),
            }
            if error_type is not None:
                response["error_type"] = error_type
            emit_http_debug(
                {
                    "boundary": "server",
                    "request": {
                        "method": scope["method"],
                        "scheme": scope.get("scheme", "http"),
                        "path": scope["path"],
                        "query_string": urllib.parse.urlsplit(
                            redacted_url(
                                "http://localhost/?"
                                + scope.get("query_string", b"").decode("latin-1")
                            )
                        ).query,
                        "client": list(scope["client"]) if scope.get("client") else None,
                        "headers": redacted_headers(_asgi_headers(scope.get("headers", []))),
                        "body": encoded_body(bytes(request_body)),
                    },
                    "response": response,
                }
            )

    def _debug_response_body(self, body: bytes, *, catalog: bool) -> dict[str, str]:
        """Retain changed catalogs once, then identify repeated definitions by digest."""
        encoded = encoded_body(body, max_bytes=MAX_DEBUG_MCP_RESPONSE_BODY_BYTES)
        if not catalog or encoded.get("encoding") != "utf-8":
            return encoded
        try:
            payload = json.loads(encoded["value"])
        except ValueError:
            return encoded
        result = payload.get("result") if isinstance(payload, dict) else None
        if not isinstance(result, dict) or not isinstance(result.get("tools"), list):
            return encoded
        # JSON-RPC IDs change on every discovery; only definition changes need a new snapshot.
        digest = hashlib.sha256(json.dumps(result, sort_keys=True).encode()).hexdigest()
        if digest == self._debug_catalog_digest:
            return {"encoding": "omitted", "bytes": str(len(body)), "catalog_sha256": digest}
        self._debug_catalog_digest = digest
        return {**encoded, "catalog_sha256": digest}

    def _lifespan_receive(self, receive: Receive) -> Receive:
        """Wrap lifespan startup to warm signed provider state once."""

        async def managed_receive() -> Message:
            """Warm provider state before MCP startup and propagate any startup failure."""
            message = await receive()
            if message["type"] == "lifespan.startup":
                await self._on_startup()
            return message

        return managed_receive

    def _lifespan_send(self, send: Send) -> Send:
        """Wrap lifespan shutdown to release runtime-owned resources once."""

        async def managed_send(message: Message) -> None:
            """Close runtime resources exactly once after downstream shutdown."""
            if message["type"] == "lifespan.shutdown.complete":
                await self._on_shutdown()
            await send(message)

        return managed_send


def create_runtime(
    config: Config | None = None,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
    cache_policy: CachePolicy | None = None,
) -> Runtime:
    """Create one account, one MCP server and their bounded lifecycle."""
    configure_logging()
    resolved_config = config or Config.from_env()
    resolved_policy = cache_policy or CachePolicy(
        max_items=resolved_config.cache_max_items, max_bytes=resolved_config.cache_max_bytes
    )
    send_budget = SendBudget(
        resolved_config.upstream_requests_per_minute,
        resolved_config.upstream_burst,
        resolved_config.upstream_spacing_seconds,
        resolved_config.upstream_endpoint_limits,
    )
    client = BilibiliClient(
        resolved_config.cookie,
        ac_time_value=resolved_config.ac_time_value,
        credential_state_path=resolved_config.credential_state_path,
        qr_code_path=resolved_config.qr_code_path,
        transport=transport,
        cache_policy=resolved_policy,
        debug_log_http=resolved_config.debug_log_http,
        upstream_requests_per_minute=resolved_config.upstream_requests_per_minute,
        upstream_burst=resolved_config.upstream_burst,
        upstream_concurrency=resolved_config.upstream_concurrency,
        upstream_requests_per_call=resolved_config.upstream_requests_per_call,
        upstream_spacing_seconds=resolved_config.upstream_spacing_seconds,
        upstream_send_budget=send_budget,
        upstream_queue_timeout_seconds=resolved_config.upstream_queue_timeout_seconds,
        upstream_queue_capacity=resolved_config.upstream_queue_capacity,
        history_path=resolved_config.history_path,
        history_max_bytes=resolved_config.history_max_bytes,
        history_ttl_seconds=resolved_config.history_ttl_seconds,
        video_cache_path=resolved_config.video_cache_path,
        video_cache_max_bytes=resolved_config.video_cache_max_bytes,
        video_cache_ttl_seconds=resolved_config.video_cache_ttl_seconds,
    )

    async def start_client() -> None:
        """Load the account and release resources if startup fails."""
        try:
            await client.start()
        except BaseException:
            await client.close()
            raise

    mcp = BilibiliFastMCP(
        "Bilibili 互联",
        instructions=SERVER_INSTRUCTIONS,
        host=resolved_config.host,
        port=resolved_config.port,
        transport_security=TransportSecuritySettings(
            enable_dns_rebinding_protection=not resolved_config.debug_allow_external,
            allowed_hosts=list(resolved_config.allowed_hosts),
            allowed_origins=[],
        ),
        streamable_http_path="/mcp",
        json_response=True,
        stateless_http=True,
    )
    register_tools(mcp, client)
    tighten_tool_schemas(mcp)
    app = StrictHTTPApp(
        mcp.streamable_http_app(),
        readiness=client.readiness,
        cache_policy=resolved_policy,
        cursor_settings=(client.tokens.max_items, int(client.tokens.ttl_seconds)),
        on_startup=start_client,
        on_shutdown=client.close,
        cache_metrics=lambda: dict(client.cache.metrics()),
        upstream_metrics=lambda: dict(client.transport.scheduling_metrics()),
        debug_settings=(resolved_config.debug_allow_external, resolved_config.debug_log_http),
    )
    return Runtime(
        config=resolved_config, cache_policy=resolved_policy, client=client, mcp=mcp, app=app
    )


def create_app(
    config: Config | None = None,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
    cache_policy: CachePolicy | None = None,
) -> StrictHTTPApp:
    """Create a production-compatible ASGI app without module-level clients."""
    return create_runtime(config, transport=transport, cache_policy=cache_policy).app


def configure_logging() -> None:
    """Use message-only JSON logs and suppress request lines containing identifiers."""
    for noisy in ("httpx", "httpcore", "mcp", "uvicorn.access"):
        logging.getLogger(noisy).setLevel(logging.CRITICAL)
    if logger.handlers:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


def _bool_env(name: str, default: bool) -> bool:
    """Parse explicit boolean environment values and reject silent configuration typos."""
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean")


def _optional_path_env(name: str) -> Path | None:
    """Read an explicit filesystem path without deriving it from HOME."""
    value = os.getenv(name, "").strip()
    if not value:
        return None
    path = Path(value)
    if not path.is_absolute():
        raise ValueError(f"{name} must be an absolute path")
    return path


def _allowed_hosts() -> tuple[str, ...]:
    """Add exact proxy Host values while retaining local and Compose defaults."""
    configured = [
        value.strip()
        for value in os.getenv("FXBILIBILI_ALLOWED_HOSTS", "").split(",")
        if value.strip()
    ]
    if any(any(character in value for character in "*/\\\r\n") for value in configured):
        raise ValueError("FXBILIBILI_ALLOWED_HOSTS contains an invalid Host value")
    return tuple(dict.fromkeys((*DEFAULT_ALLOWED_HOSTS, *configured)))


def _asgi_headers(headers: Iterable[tuple[bytes, bytes]]) -> list[tuple[str, str]]:
    """Decode ASGI header bytes losslessly for structured debug output."""
    return [(name.decode("latin-1"), value.decode("latin-1")) for name, value in headers]


async def _json_error(
    status: int,
    code: str,
    message: str,
    scope: Scope,
    receive: Receive,
    send: Send,
    *,
    allow: str | None = None,
) -> None:
    """Return one stable HTTP error without redirects, HTML, or internal context."""
    headers = {"Allow": allow} if allow is not None else None
    await JSONResponse(
        {"error": {"code": code, "message": message}},
        status_code=status,
        headers=headers,
    )(scope, receive, send)


def main() -> None:
    """Run the production Streamable HTTP server from environment configuration."""
    configure_logging()
    config = Config.from_env()
    runtime = create_runtime(config)
    _warn_debug_settings(config)
    uvicorn.run(
        runtime.app,
        host=config.host,
        port=config.port,
        access_log=False,
        proxy_headers=False,
        log_level="info",
        lifespan="on",
    )


def _warn_debug_settings(config: Config) -> None:
    """Make risky debug behavior visible once without echoing environment values."""
    enabled = [
        name
        for name, active in (
            ("allow_external", config.debug_allow_external),
            ("log_http", config.debug_log_http),
        )
        if active
    ]
    if enabled:
        logger.warning(
            json.dumps(
                {"event": "debug_settings_enabled", "settings": enabled},
                separators=(",", ":"),
                sort_keys=True,
            )
        )


if __name__ == "__main__":
    main()
