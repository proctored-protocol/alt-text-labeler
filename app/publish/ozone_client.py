from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib import error, request


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
        identifier: str,
        password: str,
        timeout_seconds: float = 30.0,
        user_agent: str = "alt-text-labeler-publisher/2",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.identifier = identifier
        self.password = password
        self.timeout_seconds = timeout_seconds
        self.user_agent = user_agent

        self._access_jwt: str | None = None
        self._did: str | None = None

    @property
    def did(self) -> str:
        if not self._did:
            self._login()
        assert self._did is not None
        return self._did

    def _build_headers(self, *, authorized: bool) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": self.user_agent,
        }
        if authorized:
            if not self._access_jwt:
                self._login()
            headers["Authorization"] = f"Bearer {self._access_jwt}"
        return headers

    def _http_json(
        self,
        *,
        method: str,
        path: str,
        payload: dict[str, Any] | None,
        authorized: bool,
        retry_on_401: bool = True,
    ) -> OzoneResponse:
        url = f"{self.base_url}{path}"
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        req = request.Request(
            url=url,
            data=data,
            method=method,
            headers=self._build_headers(authorized=authorized),
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

            if authorized and exc.code == 401 and retry_on_401:
                self._access_jwt = None
                self._did = None
                self._login()
                return self._http_json(
                    method=method,
                    path=path,
                    payload=payload,
                    authorized=authorized,
                    retry_on_401=False,
                )

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

    def _login(self) -> None:
        resp = self._http_json(
            method="POST",
            path="/xrpc/com.atproto.server.createSession",
            payload={
                "identifier": self.identifier,
                "password": self.password,
            },
            authorized=False,
            retry_on_401=False,
        )

        body = resp.json_body or {}
        access_jwt = body.get("accessJwt")
        did = body.get("did")

        if not access_jwt or not did:
            raise OzonePublishError(
                http_status=resp.http_status,
                error_code="invalid_session_response",
                error_text="Ozone session response did not include accessJwt and did.",
                response_json=body,
                retryable=False,
            )

        self._access_jwt = str(access_jwt)
        self._did = str(did)

    def emit_label(
        self,
        *,
        uri: str,
        cid: str,
        label_value: str,
        comment: str | None = None,
        duration_in_hours: int | None = None,
    ) -> dict[str, Any]:
        event: dict[str, Any] = {
            "$type": "tools.ozone.moderation.defs#modEventLabel",
            "createLabelVals": [label_value],
            "negateLabelVals": [],
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
            "createdBy": self.did,
        }

        resp = self._http_json(
            method="POST",
            path="/xrpc/tools.ozone.moderation.emitEvent",
            payload=payload,
            authorized=True,
        )

        return resp.json_body or {}