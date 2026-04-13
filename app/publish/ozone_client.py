from __future__ import annotations

import base64
import json
import time
from dataclasses import dataclass
from typing import Any
from urllib import error, request


@dataclass(frozen=True, slots=True)
class _CachedToken:
    access_jwt: str
    expires_at_epoch: float | None


@dataclass(frozen=True, slots=True)
class OzoneResponse:
    http_status: int
    json_body: dict[str, Any] | None


class OzonePublishError(Exception):
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


class OzoneClient:
    def __init__(
        self,
        *,
        base_url: str,
        pds_url: str,
        identifier: str,
        password: str,
        proxy_did: str,
        timeout_seconds: float = 30.0,
        user_agent: str = "alt-text-labeler-publisher/2",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.pds_url = pds_url.rstrip("/")
        self.identifier = identifier
        self.password = password
        self.proxy_did = proxy_did
        self.timeout_seconds = timeout_seconds
        self.user_agent = user_agent

        self._token_cache: _CachedToken | None = None
        self._session_did: str | None = None

    @property
    def created_by_did(self) -> str:
        return self.proxy_did.split("#", 1)[0]

    @property
    def session_did(self) -> str:
        if self._session_did is None:
            self._login(force_refresh=True)
        assert self._session_did is not None
        return self._session_did

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
            if exp is None:
                return None
            return float(exp)
        except Exception:
            return None

    def _is_token_fresh(self, skew_seconds: int = 60) -> bool:
        if self._token_cache is None:
            return False
        if self._token_cache.expires_at_epoch is None:
            return True
        return time.time() < (self._token_cache.expires_at_epoch - skew_seconds)

    def _login(self, *, force_refresh: bool) -> str:
        if not force_refresh and self._is_token_fresh():
            assert self._token_cache is not None
            return self._token_cache.access_jwt

        payload = {
            "identifier": self.identifier,
            "password": self.password,
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

            raise OzonePublishError(
                http_status=exc.code,
                error_code=error_code,
                error_text=error_text,
                response_json=body if isinstance(body, dict) else None,
                retryable=False,
            ) from exc

        access_jwt = body.get("accessJwt")
        did = body.get("did")

        if not access_jwt or not did:
            raise OzonePublishError(
                http_status=200,
                error_code="invalid_session_response",
                error_text="createSession did not return accessJwt and did",
                response_json=body if isinstance(body, dict) else None,
                retryable=False,
            )

        self._token_cache = _CachedToken(
            access_jwt=str(access_jwt),
            expires_at_epoch=self._decode_jwt_exp(str(access_jwt)),
        )
        self._session_did = str(did)
        return self._token_cache.access_jwt

    def _build_headers(self, *, force_refresh: bool, include_json: bool) -> dict[str, str]:
        access_jwt = self._login(force_refresh=force_refresh)

        headers = {
            "Authorization": f"Bearer {access_jwt}",
            "atproto-proxy": self.proxy_did,
            "Accept": "application/json",
            "User-Agent": self.user_agent,
        }
        if include_json:
            headers["Content-Type"] = "application/json"
        return headers

    def _http_json(
        self,
        *,
        method: str,
        nsid: str,
        payload: dict[str, Any] | None,
        retry_on_401: bool = True,
    ) -> OzoneResponse:
        url = f"{self.base_url}/xrpc/{nsid}"
        data = None if payload is None else json.dumps(payload).encode("utf-8")

        req = request.Request(
            url=url,
            data=data,
            method=method,
            headers=self._build_headers(
                force_refresh=False,
                include_json=payload is not None,
            ),
        )

        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                raw = resp.read()
                body = json.loads(raw.decode("utf-8")) if raw else None
                return OzoneResponse(http_status=resp.status, json_body=body)

        except error.HTTPError as exc:
            raw = exc.read()
            body = None
            try:
                body = json.loads(raw.decode("utf-8")) if raw else None
            except Exception:
                body = None

            if exc.code == 401 and retry_on_401:
                self._token_cache = None
                retry_req = request.Request(
                    url=url,
                    data=data,
                    method=method,
                    headers=self._build_headers(
                        force_refresh=True,
                        include_json=payload is not None,
                    ),
                )
                try:
                    with request.urlopen(retry_req, timeout=self.timeout_seconds) as resp:
                        raw = resp.read()
                        body = json.loads(raw.decode("utf-8")) if raw else None
                        return OzoneResponse(http_status=resp.status, json_body=body)
                except error.HTTPError as retry_exc:
                    raw = retry_exc.read()
                    retry_body = None
                    try:
                        retry_body = json.loads(raw.decode("utf-8")) if raw else None
                    except Exception:
                        retry_body = None

                    error_code = "http_error"
                    error_text = f"HTTP {retry_exc.code}"
                    if isinstance(retry_body, dict):
                        error_code = str(retry_body.get("error") or error_code)
                        error_text = str(retry_body.get("message") or error_text)

                    raise OzonePublishError(
                        http_status=retry_exc.code,
                        error_code=error_code,
                        error_text=error_text,
                        response_json=retry_body if isinstance(retry_body, dict) else None,
                        retryable=retry_exc.code == 429 or 500 <= retry_exc.code < 600,
                    ) from retry_exc

            error_code = "http_error"
            error_text = f"HTTP {exc.code}"
            if isinstance(body, dict):
                error_code = str(body.get("error") or error_code)
                error_text = str(body.get("message") or error_text)

            raise OzonePublishError(
                http_status=exc.code,
                error_code=error_code,
                error_text=error_text,
                response_json=body if isinstance(body, dict) else None,
                retryable=exc.code == 429 or 500 <= exc.code < 600,
            ) from exc

        except OzonePublishError:
            raise

        except Exception as exc:
            raise OzonePublishError(
                http_status=None,
                error_code=exc.__class__.__name__,
                error_text=str(exc),
                response_json=None,
                retryable=True,
            ) from exc

    def get_server_config(self) -> dict[str, Any]:
        resp = self._http_json(
            method="GET",
            nsid="tools.ozone.server.getConfig",
            payload=None,
        )
        return resp.json_body or {}

    def emit_label_event(
        self,
        *,
        uri: str,
        cid: str,
        create_label_vals: list[str],
        negate_label_vals: list[str],
        comment: str | None = None,
        duration_in_hours: int | None = None,
    ) -> dict[str, Any]:
        event: dict[str, Any] = {
            "$type": "tools.ozone.moderation.defs#modEventLabel",
            "createLabelVals": create_label_vals,
            "negateLabelVals": negate_label_vals,
        }

        if comment:
            event["comment"] = comment
        if duration_in_hours is not None:
            event["durationInHours"] = int(duration_in_hours)

        payload = {
            "event": event,
            "subject": {
                "$type": "com.atproto.repo.strongRef",
                "uri": uri,
                "cid": cid,
            },
            "createdBy": self.created_by_did,
        }

        resp = self._http_json(
            method="POST",
            nsid="tools.ozone.moderation.emitEvent",
            payload=payload,
        )
        return resp.json_body or {}

    def emit_label(
        self,
        *,
        uri: str,
        cid: str,
        label_value: str,
        comment: str | None = None,
        duration_in_hours: int | None = None,
    ) -> dict[str, Any]:
        return self.emit_label_event(
            uri=uri,
            cid=cid,
            create_label_vals=[label_value],
            negate_label_vals=[],
            comment=comment,
            duration_in_hours=duration_in_hours,
        )

    def negate_label(
        self,
        *,
        uri: str,
        cid: str,
        label_value: str,
        comment: str | None = None,
        duration_in_hours: int | None = None,
    ) -> dict[str, Any]:
        return self.emit_label_event(
            uri=uri,
            cid=cid,
            create_label_vals=[],
            negate_label_vals=[label_value],
            comment=comment,
            duration_in_hours=duration_in_hours,
        )