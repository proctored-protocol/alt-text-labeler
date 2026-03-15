import json
from urllib.request import Request, urlopen

from app.config import get_settings
from app.integrations.ozone.auth import get_access_jwt


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


def ozone_get(nsid: str) -> dict:
    req = Request(
        _build_url(nsid),
        headers=_get_headers(),
        method="GET",
    )
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


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
    with urlopen(req, timeout=30) as resp:
        raw = resp.read()
        return json.loads(raw.decode("utf-8")) if raw else {}


def get_server_config() -> dict:
    return ozone_get("tools.ozone.server.getConfig")