from __future__ import annotations

import base64
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
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
        retry_after_seconds: int | None = None,
        response_headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(error_text)
        self.http_status = http_status
        self.error_code = error_code
        self.error_text = error_text
        self.response_json = response_json
        self.retryable = retryable
        self.retry_after_seconds = retry_after_seconds
        self.response_headers = response_headers or {}


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

    def _headers_to_dict(self, headers: Any) -> dict[str, str]:
        out: dict[str, str] = {}
        try:
            for key, value in headers.items():
                out[str(key).lower()] = str(value)
        except Exception:
            pass
        return out

    def _parse_retry_after_seconds(self, headers: dict[str, str]) -> int | None:
        value = headers.get("retry-after")
        if not value:
            return None

        value = value.strip()
        try:
            return max(1, int(float(value)))
        except Exception:
            pass

        try:
            dt = parsedate_to_datetime(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return max(1, int((dt - datetime.now(timezone.utc)).total_seconds()))
        except Exception:
            return None

    def _publish_error_from_http_error(
        self,
        exc: error.HTTPError,
        *,
        body: dict[str, Any] | None,
    ) -> OzonePublishError:
        headers = self._headers_to_dict(exc.headers)
        retry_after_seconds = self._parse_retry_after_seconds(headers)

        if exc.code == 429:
            error_code = "rate_limited"
        else:
            error_code = "http_error"

        error_text = f"HTTP {exc.code}"
        if isinstance(body, dict):
            error_code = str(body.get("error") or error_code)
            error_text = str(body.get("message") or error_text)
            if exc.code == 429 and error_code == "http_error":
                error_code = "rate_limited"

        return OzonePublishError(
            http_status=exc.code,
            error_code=error_code,
            error_text=error_text,
            response_json=body if isinstance(body, dict) else None,
            retryable=exc.code == 429 or 500 <= exc.code < 600,
            retry_after_seconds=retry_after_seconds,
            response_headers=headers,
        )

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

            publish_error = self._publish_error_from_http_error(exc, body=body)
            raise OzonePublishError(
                http_status=publish_error.http_status,
                error_code="create_session_failed" if publish_error.error_code == "http_error" else publish_error.error_code,
                error_text=publish_error.error_text,
                response_json=publish_error.response_json,
                retryable=False,
                retry_after_seconds=publish_error.retry_after_seconds,
                response_headers=publish_error.response_headers,
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
                    raise self._publish_error_from_http_error(retry_exc, body=retry_body) from retry_exc

            raise self._publish_error_from_http_error(exc, body=body) from exc

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
