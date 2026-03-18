import json
from functools import lru_cache
from urllib.request import Request, urlopen

from app.config import get_settings


def _create_session(identifier: str, password: str, base_url: str) -> dict:
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

    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


@lru_cache(maxsize=1)
def get_access_jwt() -> str:
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

    return access_jwt


def clear_access_jwt_cache() -> None:
    get_access_jwt.cache_clear()


@lru_cache(maxsize=1)
def get_test_viewer_access_jwt() -> str | None:
    settings = get_settings()

    if not settings.test_viewer_handle or not settings.test_viewer_app_password:
        return None

    data = _create_session(
        identifier=settings.test_viewer_handle,
        password=settings.test_viewer_app_password,
        base_url=settings.bsky_pds_url,
    )

    return data.get("accessJwt")


def clear_test_viewer_access_jwt_cache() -> None:
    get_test_viewer_access_jwt.cache_clear()