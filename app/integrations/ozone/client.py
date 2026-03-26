from __future__ import annotations

import json
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from app.config import get_settings
from app.integrations.ozone.auth import (
    OzoneAuthError,
    clear_access_jwt_cache,
    get_access_jwt,
)


class OzoneAPIError(RuntimeError):
    pass


def _get_headers(*, force_refresh: bool = False) -> dict[str, str]:
    settings = get_settings()

    if not settings.ozone_proxy_did:
        raise RuntimeError("OZONE_PROXY_DID must be set in .env")

    return {
        "Authorization": f"Bearer {get_access_jwt(force_refresh=force_refresh)}",
        "atproto-proxy": settings.ozone_proxy_did,
        "Accept": "application/json",
    }


def _build_url(nsid: str) -> str:
    settings = get_settings()
    if not settings.ozone_base_url:
        raise RuntimeError("OZONE_BASE_URL must be set in .env")
    return f"{settings.ozone_base_url.rstrip('/')}/xrpc/{nsid}"


def _extract_http_error_details(exc: HTTPError) -> tuple[str, dict | None]:
    raw_body = b""
    try:
        raw_body = exc.read()
    except Exception:
        pass

    body_text = raw_body.decode("utf-8", errors="replace") if raw_body else ""
    body_json = None
    if body_text:
        try:
            body_json = json.loads(body_text)
        except Exception:
            body_json = None

    return body_text, body_json


def _format_http_error(exc: HTTPError, nsid: str, payload: dict | None = None) -> OzoneAPIError:
    body_text, body_json = _extract_http_error_details(exc)

    parts = [f"Ozone {nsid} failed with HTTP {exc.code}"]

    if body_json and isinstance(body_json, dict):
        err = body_json.get("error")
        msg = body_json.get("message")
        if err:
            parts.append(f"error={err}")
        if msg:
            parts.append(f"message={msg}")
        parts.append(f"body={json.dumps(body_json, ensure_ascii=False)}")
    elif body_text:
        parts.append(f"body={body_text}")

    if payload is not None:
        parts.append(f"payload={json.dumps(payload, ensure_ascii=False, sort_keys=True)}")

    return OzoneAPIError(" | ".join(parts))


def _is_expired_token(exc: HTTPError) -> bool:
    if exc.code == 401:
        return True

    _, body_json = _extract_http_error_details(exc)
    return isinstance(body_json, dict) and body_json.get("error") == "ExpiredToken"


def ozone_get(nsid: str) -> dict:
    url = _build_url(nsid)

    try:
        req = Request(
            url,
            headers=_get_headers(force_refresh=False),
            method="GET",
        )

        with urlopen(req, timeout=30) as resp:
            raw = resp.read()
            return json.loads(raw.decode("utf-8")) if raw else {}

    except HTTPError as exc:
        if _is_expired_token(exc):
            clear_access_jwt_cache()

            retry_req = Request(
                url,
                headers=_get_headers(force_refresh=True),
                method="GET",
            )
            try:
                with urlopen(retry_req, timeout=30) as resp:
                    raw = resp.read()
                    return json.loads(raw.decode("utf-8")) if raw else {}
            except HTTPError as retry_exc:
                raise _format_http_error(retry_exc, nsid) from retry_exc

        raise _format_http_error(exc, nsid) from exc
    except OzoneAuthError:
        raise


def ozone_post(nsid: str, payload: dict) -> dict:
    url = _build_url(nsid)
    body = json.dumps(payload).encode("utf-8")

    try:
        headers = _get_headers(force_refresh=False)
        headers["Content-Type"] = "application/json"

        req = Request(
            url,
            data=body,
            headers=headers,
            method="POST",
        )

        with urlopen(req, timeout=30) as resp:
            raw = resp.read()
            return json.loads(raw.decode("utf-8")) if raw else {}

    except HTTPError as exc:
        if _is_expired_token(exc):
            clear_access_jwt_cache()

            retry_headers = _get_headers(force_refresh=True)
            retry_headers["Content-Type"] = "application/json"

            retry_req = Request(
                url,
                data=body,
                headers=retry_headers,
                method="POST",
            )

            try:
                with urlopen(retry_req, timeout=30) as resp:
                    raw = resp.read()
                    return json.loads(raw.decode("utf-8")) if raw else {}
            except HTTPError as retry_exc:
                raise _format_http_error(retry_exc, nsid, payload=payload) from retry_exc

        raise _format_http_error(exc, nsid, payload=payload) from exc
    except OzoneAuthError:
        raise


def get_server_config() -> dict:
    return ozone_get("tools.ozone.server.getConfig")