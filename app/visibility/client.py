from __future__ import annotations

import base64
import json
import time
from dataclasses import dataclass
from typing import Any
from urllib import error, parse, request


@dataclass(frozen=True, slots=True)
class _CachedToken:
    access_jwt: str
    expires_at_epoch: float | None


@dataclass(frozen=True, slots=True)
class ForcedHydrationResult:
    ok: bool
    status_code: int
    found_label: bool
    response_headers: dict[str, str]
    payload: dict[str, Any] | None
    error_text: str | None = None


class VisibilityClientError(Exception):
    def __init__(
        self,
        *,
        http_status: int | None,
        error_code: str,
        error_text: str,
        response_json: dict[str, Any] | None,
        retryable: bool,
    ) -> None:
        super().__init__(error_text)
        self.http_status = http_status
        self.error_code = error_code
        self.error_text = error_text
        self.response_json = response_json
        self.retryable = retryable


class VisibilityClient:
    def __init__(
        self,
        *,
        pds_url: str,
        appview_url: str,
        viewer_identifier: str,
        viewer_password: str,
        labeler_did: str,
        timeout_seconds: int = 30,
        user_agent: str = "alt-text-labeler-visibility/2",
    ) -> None:
        self.pds_url = pds_url.rstrip("/")
        self.appview_url = appview_url.rstrip("/")
        self.viewer_identifier = viewer_identifier
        self.viewer_password = viewer_password
        self.labeler_did = labeler_did
        self.timeout_seconds = timeout_seconds
        self.user_agent = user_agent

        self._token_cache: _CachedToken | None = None

    def _decode_jwt_exp(self, access_jwt: str) -> float | None:
        try:
            parts = access_jwt.split(".")
            if len(parts) != 3:
                return None
            payload_part = parts[1]
            padding = "=" * (-len(payload_part) % 4)
            decoded = base64.urlsafe_b64decode(payload_part + padding)
            payload = json.loads(decoded.decode("utf-8"))
            exp = payload.get("exp")
            return None if exp is None else float(exp)
        except Exception:
            return None

    def _token_fresh(self, skew_seconds: int = 60) -> bool:
        if self._token_cache is None:
            return False
        if self._token_cache.expires_at_epoch is None:
            return True
        return time.time() < (self._token_cache.expires_at_epoch - skew_seconds)

    def _login(self, *, force_refresh: bool) -> str:
        if not force_refresh and self._token_fresh():
            assert self._token_cache is not None
            return self._token_cache.access_jwt

        payload = {
            "identifier": self.viewer_identifier,
            "password": self.viewer_password,
        }

        req = request.Request(
            f"{self.pds_url}/xrpc/com.atproto.server.createSession",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": self.user_agent,
            },
            method="POST",
        )

        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                raw = resp.read()
                body = json.loads(raw.decode("utf-8")) if raw else {}
        except error.HTTPError as exc:
            raw = exc.read()
            body = None
            try:
                body = json.loads(raw.decode("utf-8")) if raw else None
            except Exception:
                body = None

            error_code = "create_session_failed"
            error_text = f"HTTP {exc.code}"
            if isinstance(body, dict):
                error_code = str(body.get("error") or error_code)
                error_text = str(body.get("message") or error_text)

            raise VisibilityClientError(
                http_status=exc.code,
                error_code=error_code,
                error_text=error_text,
                response_json=body if isinstance(body, dict) else None,
                retryable=False,
            ) from exc

        access_jwt = body.get("accessJwt")
        if not access_jwt:
            raise VisibilityClientError(
                http_status=200,
                error_code="invalid_session_response",
                error_text="createSession did not return accessJwt",
                response_json=body if isinstance(body, dict) else None,
                retryable=False,
            )

        self._token_cache = _CachedToken(
            access_jwt=str(access_jwt),
            expires_at_epoch=self._decode_jwt_exp(str(access_jwt)),
        )
        return self._token_cache.access_jwt

    def _request_json(
        self,
        *,
        url: str,
        retry_on_401: bool = True,
    ) -> tuple[int, dict[str, str], dict[str, Any] | None]:
        token = self._login(force_refresh=False)

        req = request.Request(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "User-Agent": self.user_agent,
                "atproto-accept-labelers": self.labeler_did,
            },
            method="GET",
        )

        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                raw = resp.read()
                body = json.loads(raw.decode("utf-8")) if raw else None
                return resp.status, dict(resp.headers.items()), body

        except error.HTTPError as exc:
            raw = exc.read()
            body = None
            try:
                body = json.loads(raw.decode("utf-8")) if raw else None
            except Exception:
                body = None

            is_expired_token = exc.code == 401 or (
                isinstance(body, dict) and body.get("error") == "ExpiredToken"
            )

            if is_expired_token and retry_on_401:
                self._token_cache = None
                return self._request_json(url=url, retry_on_401=False)

            error_code = "http_error"
            error_text = f"HTTP {exc.code}"
            if isinstance(body, dict):
                error_code = str(body.get("error") or error_code)
                error_text = str(body.get("message") or error_text)

            raise VisibilityClientError(
                http_status=exc.code,
                error_code=error_code,
                error_text=error_text,
                response_json=body if isinstance(body, dict) else None,
                retryable=exc.code == 429 or 500 <= exc.code < 600,
            ) from exc

        except Exception as exc:
            raise VisibilityClientError(
                http_status=None,
                error_code=exc.__class__.__name__,
                error_text=str(exc),
                response_json=None,
                retryable=True,
            ) from exc

    def check_forced_hydration(
        self,
        *,
        uri: str,
        label_value: str,
    ) -> ForcedHydrationResult:
        params = parse.urlencode(
            [
                ("uri", uri),
                ("depth", "0"),
                ("parentHeight", "0"),
            ]
        )
        url = f"{self.appview_url}/xrpc/app.bsky.feed.getPostThread?{params}"

        status, headers, payload = self._request_json(url=url)

        thread = (payload or {}).get("thread") or {}
        post = thread.get("post") or {}
        labels = post.get("labels") or []

        found = any(
            lbl.get("src") == self.labeler_did
            and lbl.get("uri") == uri
            and lbl.get("val") == label_value
            for lbl in labels
        )

        return ForcedHydrationResult(
            ok=True,
            status_code=status,
            found_label=found,
            response_headers=headers,
            payload=payload,
            error_text=None,
        )