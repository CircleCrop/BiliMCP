"""Credential normalization, refresh helpers, and private persistent state."""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import os
import re
import sys
import tempfile
import threading
import urllib.parse
from collections.abc import Mapping
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path
from typing import Final

import qrcode
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from qrcode.image.pil import PilImage

_COOKIE_NAME_RE = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")
_CONSOLE_OUTPUT_LOCK = threading.Lock()
_REFRESH_PUBLIC_KEY: Final = b"""-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDLgd2OAkcGVtoE3ThUREbio0Eg
Uc/prcajMKXvkCKFCWhJYJcLkcM2DKKcSeFpD/j6Boy538YXnR6VhcuUJOhH2x71
nzPjfdTcqMz7djHum0qSZA0AyCBDABUqCrfNgCiJ00Ra7GmRj+YCK1NJEuewlb40
JNrRuoEUXpabUzGB8QIDAQAB
-----END PUBLIC KEY-----
"""


@dataclass(frozen=True, slots=True)
class CredentialState:
    cookie: str = field(repr=False)
    ac_time_value: str = field(repr=False)
    pending_refresh_token: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        """Validate pending Web credential-refresh confirmation."""
        if self.pending_refresh_token is not None and (
            not isinstance(self.pending_refresh_token, str)
            or not self.pending_refresh_token
            or any(character.isspace() for character in self.pending_refresh_token)
        ):
            raise ValueError("Pending refresh confirmation requires a nonempty token")

    @classmethod
    def create(
        cls,
        cookie: str | None,
        ac_time_value: str | None,
        *,
        pending_refresh_token: str | None = None,
    ) -> CredentialState:
        """Validate bootstrap or persisted secrets without retaining malformed values."""
        normalized_cookie = normalize_cookie(cookie or "")
        refresh_token = (ac_time_value or "").strip()
        if "\r" in refresh_token or "\n" in refresh_token:
            raise ValueError("FXBILIBILI_AC_TIME_VALUE must be one value")
        return cls(
            cookie=normalized_cookie,
            ac_time_value=refresh_token,
            pending_refresh_token=pending_refresh_token,
        )

    @property
    def cookie_names(self) -> frozenset[str]:
        return frozenset(cookie_values(self.cookie))

    @property
    def has_runtime_cookie(self) -> bool:
        return {"SESSDATA", "DedeUserID"}.issubset(self.cookie_names)

    @property
    def can_refresh(self) -> bool:
        return (
            self.has_runtime_cookie and "bili_jct" in self.cookie_names and bool(self.ac_time_value)
        )

    @property
    def dedeuserid(self) -> str:
        return cookie_values(self.cookie).get("DedeUserID", "")


class CredentialStore:
    """Load and atomically replace a private credential state file."""

    def __init__(self, path: Path | None) -> None:
        self.path = path

    @property
    def configured(self) -> bool:
        return self.path is not None

    async def load(self) -> CredentialState | None:
        """Read persisted secrets off the event loop and validate the fixed JSON shape."""
        if self.path is None:
            return None
        return await asyncio.to_thread(self._load_sync)

    async def save(self, state: CredentialState) -> None:
        """Atomically persist rotated secrets with owner-only file permissions."""
        if self.path is None:
            raise RuntimeError("credential persistence is not configured")
        await asyncio.to_thread(self._save_sync, state)

    def _load_sync(self) -> CredentialState | None:
        """Read one credential document without exposing its values in exceptions."""
        assert self.path is not None
        try:
            raw = self.path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        try:
            payload = json.loads(raw)
        except ValueError as exc:
            raise ValueError("credential state file is not valid JSON") from exc
        required_fields = {"version", "cookie", "ac_time_value"}
        if not isinstance(payload, dict) or set(payload) not in (
            required_fields,
            required_fields | {"pending_refresh_token"},
        ):
            raise ValueError("credential state file has an invalid shape")
        if (
            type(payload["version"]) is not int
            or payload["version"] != 1
            or not isinstance(payload["cookie"], str)
            or not isinstance(payload["ac_time_value"], str)
        ):
            raise ValueError("credential state file has invalid field types")
        if "pending_refresh_token" in payload and not isinstance(
            payload["pending_refresh_token"], str
        ):
            raise ValueError("credential state file has invalid field types")
        return CredentialState.create(
            payload["cookie"],
            payload["ac_time_value"],
            pending_refresh_token=payload.get("pending_refresh_token"),
        )

    def _save_sync(self, state: CredentialState) -> None:
        """Write and fsync a temporary file before replacing the active credential state."""
        assert self.path is not None
        parent_existed = self.path.parent.exists()
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if not parent_existed:
            _set_owner_only_mode(self.path.parent, 0o700)
        fields: dict[str, str | int] = {
            "version": 1,
            "cookie": state.cookie,
            "ac_time_value": state.ac_time_value,
        }
        if state.pending_refresh_token is not None:
            fields["pending_refresh_token"] = state.pending_refresh_token
        payload = json.dumps(
            fields,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self.path.parent,
                prefix=".credentials-",
                delete=False,
            ) as temporary:
                temporary_path = Path(temporary.name)
                # Prefer fchmod before writing so the temp file is never world-readable
                # on Unix; Windows has no fchmod, so chmod the path after close instead.
                _set_owner_only_mode(temporary.fileno(), 0o600)
                temporary.write(payload)
                temporary.flush()
                os.fsync(temporary.fileno())
            if temporary_path is not None and not hasattr(os, "fchmod"):
                _set_owner_only_mode(temporary_path, 0o600)
            os.replace(temporary_path, self.path)
            _set_owner_only_mode(self.path, 0o600)
        finally:
            if temporary_path is not None and temporary_path.exists():
                temporary_path.unlink()


def _set_owner_only_mode(target: Path | int, mode: int) -> None:
    """Apply Unix owner-only permissions when the platform supports them."""
    if isinstance(target, int):
        fchmod = getattr(os, "fchmod", None)
        if fchmod is not None:
            fchmod(target, mode)
        return
    try:
        os.chmod(target, mode)
    except OSError:
        # Windows and some remotes accept chmod for a subset of bits or ignore them.
        if os.name != "nt":
            raise


class RefreshCsrfParser(HTMLParser):
    """Extract Bilibili's refresh CSRF value from the correspondPath document."""

    def __init__(self) -> None:
        super().__init__()
        self.value = ""
        self._capture = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self._capture = tag == "div" and dict(attrs).get("id") == "1-name"

    def handle_endtag(self, tag: str) -> None:
        if tag == "div":
            self._capture = False

    def handle_data(self, data: str) -> None:
        if self._capture:
            self.value += data


def normalize_cookie(raw: str) -> str:
    """Normalize a raw Cookie request header and reject injection or malformed pairs."""
    if not raw.strip():
        return ""
    if "\r" in raw or "\n" in raw:
        raise ValueError("FXBILIBILI_COOKIE must be one Cookie header value")
    pairs: list[tuple[str, str]] = []
    positions: dict[str, int] = {}
    for fragment in raw.split(";"):
        part = fragment.strip()
        if not part:
            continue
        name, separator, value = part.partition("=")
        name = name.strip()
        value = value.strip()
        if separator != "=" or not name or not _COOKIE_NAME_RE.fullmatch(name):
            raise ValueError("FXBILIBILI_COOKIE contains a malformed cookie pair")
        if name in positions:
            pairs[positions[name]] = (name, value)
        else:
            positions[name] = len(pairs)
            pairs.append((name, value))
    return "; ".join(f"{name}={value}" for name, value in pairs)


def cookie_values(raw: str) -> dict[str, str]:
    """Return normalized Cookie pairs keyed by their exact provider names."""
    normalized = normalize_cookie(raw)
    return {
        name: value
        for fragment in normalized.split("; ")
        if fragment
        for name, _, value in (fragment.partition("="),)
    }


def merge_cookie(raw: str, updates: dict[str, str]) -> str:
    """Overlay rotated Set-Cookie values while retaining unrelated device cookies."""
    values = cookie_values(raw)
    values.update(updates)
    return normalize_cookie("; ".join(f"{name}={value}" for name, value in values.items()))


def _build_qr(login_url: str) -> qrcode.QRCode[PilImage]:
    """Build one QR matrix shared by terminal and PNG renderers."""
    qr = qrcode.QRCode[PilImage](
        border=4,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        image_factory=PilImage,
    )
    qr.add_data(login_url)
    qr.make(fit=True)
    return qr


def render_terminal_qr(login_url: str) -> str:
    """Render a compact inverted QR image that remains scannable in dark Docker logs."""
    output = io.StringIO()
    _build_qr(login_url).print_ascii(out=output, tty=False, invert=True)
    return output.getvalue().rstrip("\n")


def write_console_block(message: str) -> None:
    """Write and flush one complete console block so concurrent logs cannot split the QR."""
    payload = message.rstrip("\n") + "\n"
    with _CONSOLE_OUTPUT_LOCK:
        sys.stdout.write(payload)
        sys.stdout.flush()


def save_qr_png(login_url: str, path: Path) -> None:
    """Atomically save a private PNG copy of the active one-time login QR code."""
    parent_existed = path.parent.exists()
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not parent_existed:
        _set_owner_only_mode(path.parent, 0o700)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w+b",
            dir=path.parent,
            prefix=".login-qr-",
            suffix=".png",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            _set_owner_only_mode(temporary.fileno(), 0o600)
            _build_qr(login_url).make_image(
                fill_color="black",
                back_color="white",
            ).save(temporary, format="PNG")
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_path, path)
        _set_owner_only_mode(path, 0o600)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


def remove_qr_png(path: Path) -> None:
    """Remove the one-time QR image after login succeeds, expires, or fails."""
    with contextlib.suppress(FileNotFoundError):
        path.unlink()


def credential_state_from_qr_login(
    redirect_url: str,
    refresh_token: str,
    response_cookies: Mapping[str, str],
) -> CredentialState:
    """Build one refreshable state from a successful QR poll without decoding Cookie values."""
    parsed = urllib.parse.urlsplit(redirect_url)
    if parsed.scheme != "https" or not parsed.query:
        raise ValueError("Bilibili QR login returned an invalid redirect URL")
    values: dict[str, str] = {}
    for fragment in parsed.query.split("&"):
        name, separator, value = fragment.partition("=")
        decoded_name = urllib.parse.unquote_plus(name)
        if separator and decoded_name in {"SESSDATA", "bili_jct", "DedeUserID"}:
            # Keep the encoded value exactly as Bilibili supplied it for the Cookie header.
            values[decoded_name] = value
    values.update(response_cookies)
    state = CredentialState.create(
        "; ".join(f"{name}={value}" for name, value in values.items()),
        refresh_token,
    )
    if not state.can_refresh:
        raise ValueError("Bilibili QR login omitted required credentials")
    return state


def generate_correspond_path(timestamp_ms: int) -> str:
    """Encrypt the refresh timestamp with Bilibili's documented RSA-OAEP public key."""
    key = serialization.load_pem_public_key(_REFRESH_PUBLIC_KEY)
    if not isinstance(key, rsa.RSAPublicKey):
        raise RuntimeError("Bilibili refresh key is not RSA")
    encrypted = key.encrypt(
        f"refresh_{timestamp_ms}".encode(),
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None,
        ),
    )
    return encrypted.hex()


def parse_refresh_csrf(document: str) -> str:
    """Parse the correspondPath HTML and reject a missing refresh token."""
    parser = RefreshCsrfParser()
    parser.feed(document)
    value = parser.value.strip()
    if not value:
        raise ValueError("Bilibili refresh document omitted its CSRF value")
    return value
