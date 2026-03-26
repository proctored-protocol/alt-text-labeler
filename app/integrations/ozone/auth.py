from __future__ import annotations

import base64
import json
import time
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from app.config import get_settings


class OzoneAuthError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        body_text: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body_text = body_text


@dataclass
class _CachedToken:
    access_jwt: str
    expires_at_epoch: float | None


_access_jwt_cache: _CachedToken | None = None
_test_viewer_access_jwt_cache: _CachedToken | None = None


def _decode_http_error(exc: HTTPError) -> tuple[str, Any | None]:
    raw = b""
    try:
        raw = exc.read()
    except Exception:
        pass

    body_text = raw.decode("utf-8", errors="replace") if raw else ""
    body_json = None
    if body_text:
        try:
            body_json = json.loads(body_text)
        except Exception:
            body_json = None

    return body_text, body_json


def _create_session(identifier: str, password: str, base_url: str) -> dict[str, Any]:
    payload = {
        "identifier": identifier,
        "password": password,
    }

    body = json.dumps(payload).encode("utf-8")
    req = Request(
        f"{base_url.rstrip('/')}/xrpc/com.atproto.server.createSession",
        data=body,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        method="POST",
    )

    try:
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        body_text, body_json = _decode_http_error(exc)

        details = [f"createSession failed with HTTP {exc.code}"]
        if isinstance(body_json, dict):
            err = body_json.get("error")
            msg = body_json.get("message")
            if err:
                details.append(f"error={err}")
            if msg:
                details.append(f"message={msg}")
            details.append(f"body={json.dumps(body_json, ensure_ascii=False)}")
        elif body_text:
            details.append(f"body={body_text}")

        raise OzoneAuthError(
            " | ".join(details),
            status_code=exc.code,
            body_text=body_text,
        ) from exc


def _decode_jwt_exp(access_jwt: str) -> float | None:
    try:
        parts = access_jwt.split(".")
        if len(parts) != 3:
            return None

        payload_part = parts[1]
        padding = "=" * (-len(payload_part) % 4)
        decoded = base64.urlsafe_b64decode(payload_part + padding)
        payload = json.loads(decoded.decode("utf-8"))

        exp = payload.get("exp")
        if exp is None:
            return None

        return float(exp)
    except Exception:
        return None


def _is_token_fresh(cached: _CachedToken | None, *, skew_seconds: int = 60) -> bool:
    if cached is None:
        return False
    if cached.expires_at_epoch is None:
        return True
    return time.time() < (cached.expires_at_epoch - skew_seconds)


def get_access_jwt(*, force_refresh: bool = False) -> str:
    global _access_jwt_cache

    if not force_refresh and _is_token_fresh(_access_jwt_cache):
        return _access_jwt_cache.access_jwt

    settings = get_settings()

    if not settings.ozone_handle or not settings.ozone_app_password:
        raise RuntimeError("OZONE_HANDLE and OZONE_APP_PASSWORD must be set in .env")

    data = _create_session(
        identifier=settings.ozone_handle,
        password=settings.ozone_app_password,
        base_url=settings.bsky_pds_url,
    )

    access_jwt = data.get("accessJwt")
    if not access_jwt:
        raise RuntimeError("createSession did not return accessJwt")

    _access_jwt_cache = _CachedToken(
        access_jwt=access_jwt,
        expires_at_epoch=_decode_jwt_exp(access_jwt),
    )
    return access_jwt


def clear_access_jwt_cache() -> None:
    global _access_jwt_cache
    _access_jwt_cache = None


def get_test_viewer_access_jwt(*, force_refresh: bool = False) -> str | None:
    global _test_viewer_access_jwt_cache

    if not force_refresh and _is_token_fresh(_test_viewer_access_jwt_cache):
        return _test_viewer_access_jwt_cache.access_jwt

    settings = get_settings()

    if not settings.test_viewer_handle or not settings.test_viewer_app_password:
        return None

    data = _create_session(
        identifier=settings.test_viewer_handle,
        password=settings.test_viewer_app_password,
        base_url=settings.bsky_pds_url,
    )

    access_jwt = data.get("accessJwt")
    if not access_jwt:
        return None

    _test_viewer_access_jwt_cache = _CachedToken(
        access_jwt=access_jwt,
        expires_at_epoch=_decode_jwt_exp(access_jwt),
    )
    return access_jwt


def clear_test_viewer_access_jwt_cache() -> None:
    global _test_viewer_access_jwt_cache
    _test_viewer_access_jwt_cache = None