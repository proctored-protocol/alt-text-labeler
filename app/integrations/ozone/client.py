import json
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from app.config import get_settings
from app.integrations.ozone.auth import get_access_jwt


class OzoneAPIError(RuntimeError):
    pass


def _get_headers() -> dict[str, str]:
    settings = get_settings()

    if not settings.ozone_proxy_did:
        raise RuntimeError("OZONE_PROXY_DID must be set in .env")

    return {
        "Authorization": f"Bearer {get_access_jwt()}",
        "atproto-proxy": settings.ozone_proxy_did,
        "Accept": "application/json",
    }


def _build_url(nsid: str) -> str:
    settings = get_settings()
    if not settings.ozone_base_url:
        raise RuntimeError("OZONE_BASE_URL must be set in .env")
    return f"{settings.ozone_base_url.rstrip('/')}/xrpc/{nsid}"


def _format_http_error(exc: HTTPError, nsid: str, payload: dict | None = None) -> OzoneAPIError:
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


def ozone_get(nsid: str) -> dict:
    req = Request(
        _build_url(nsid),
        headers=_get_headers(),
        method="GET",
    )
    try:
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        raise _format_http_error(exc, nsid) from exc


def ozone_post(nsid: str, payload: dict) -> dict:
    body = json.dumps(payload).encode("utf-8")
    headers = _get_headers()
    headers["Content-Type"] = "application/json"

    req = Request(
        _build_url(nsid),
        data=body,
        headers=headers,
        method="POST",
    )

    try:
        with urlopen(req, timeout=30) as resp:
            raw = resp.read()
            return json.loads(raw.decode("utf-8")) if raw else {}
    except HTTPError as exc:
        raise _format_http_error(exc, nsid, payload=payload) from exc


def get_server_config() -> dict:
    return ozone_get("tools.ozone.server.getConfig")