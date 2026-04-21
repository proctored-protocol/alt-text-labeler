from __future__ import annotations

import base64
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen


RECORD_COLLECTION = "app.bsky.labeler.service"
RECORD_RKEY = "self"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return now_utc().isoformat().replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class CachedSession:
    access_jwt: str
    refresh_jwt: str | None
    access_expires_at_epoch: float | None
    refresh_expires_at_epoch: float | None
    did: str | None
    handle: str | None


class LabelerRefreshError(Exception):
    def __init__(
        self,
        *,
        http_status: int | None,
        error_code: str,
        error_text: str,
        response_json: dict[str, Any] | None = None,
        raw_text: str | None = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(error_text)
        self.http_status = http_status
        self.error_code = error_code
        self.error_text = error_text
        self.response_json = response_json
        self.raw_text = raw_text
        self.retryable = retryable


class LabelerRecordRefreshClient:
    def __init__(
        self,
        *,
        handle: str,
        app_password: str,
        backup_json_path: str | Path,
        cache_path: str | Path,
        login_host: str = "https://bsky.social",
        timeout_seconds: int = 30,
        access_refresh_margin_seconds: int = 60,
        refresh_refresh_margin_seconds: int = 300,
        user_agent: str = "alt-text-labeler-record-refresh/5",
    ) -> None:
        self.handle = handle.strip()
        self.app_password = app_password.strip()
        self.backup_json_path = Path(backup_json_path)
        self.cache_path = Path(cache_path)
        self.login_host = login_host.rstrip("/")
        self.timeout_seconds = int(timeout_seconds)
        self.access_refresh_margin_seconds = int(access_refresh_margin_seconds)
        self.refresh_refresh_margin_seconds = int(refresh_refresh_margin_seconds)
        self.user_agent = user_agent

        self.repo_did: str | None = None
        self.resolved_pds_url: str | None = None
        self._session: CachedSession | None = None
        self.last_refresh_at_iso: str | None = None
        self.last_refresh_cid: str | None = None

        self._load_cache()

    # ------------------------------------------------------------------
    # cache helpers
    # ------------------------------------------------------------------

    def _load_cache(self) -> None:
        if not self.cache_path.exists():
            return

        payload = json.loads(self.cache_path.read_text(encoding="utf-8"))
        self.repo_did = payload.get("repo_did")
        self.resolved_pds_url = payload.get("resolved_pds_url")
        self.last_refresh_at_iso = payload.get("last_refresh_at_iso")
        self.last_refresh_cid = payload.get("last_refresh_cid")

        access_jwt = payload.get("access_jwt")
        refresh_jwt = payload.get("refresh_jwt")
        if access_jwt:
            self._session = CachedSession(
                access_jwt=str(access_jwt),
                refresh_jwt=(str(refresh_jwt) if refresh_jwt else None),
                access_expires_at_epoch=(
                    float(payload["access_expires_at_epoch"])
                    if payload.get("access_expires_at_epoch") is not None
                    else None
                ),
                refresh_expires_at_epoch=(
                    float(payload["refresh_expires_at_epoch"])
                    if payload.get("refresh_expires_at_epoch") is not None
                    else None
                ),
                did=payload.get("session_did"),
                handle=payload.get("session_handle"),
            )

    def _save_cache(self) -> None:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)

        payload = {
            "repo_did": self.repo_did,
            "resolved_pds_url": self.resolved_pds_url,
            "last_refresh_at_iso": self.last_refresh_at_iso,
            "last_refresh_cid": self.last_refresh_cid,
            "session_did": None if self._session is None else self._session.did,
            "session_handle": None if self._session is None else self._session.handle,
            "access_jwt": None if self._session is None else self._session.access_jwt,
            "refresh_jwt": None if self._session is None else self._session.refresh_jwt,
            "access_expires_at_epoch": None if self._session is None else self._session.access_expires_at_epoch,
            "refresh_expires_at_epoch": None if self._session is None else self._session.refresh_expires_at_epoch,
            "saved_at_iso": now_iso(),
        }

        tmp_path = self.cache_path.with_suffix(self.cache_path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.chmod(tmp_path, 0o600)
        os.replace(tmp_path, self.cache_path)

    # ------------------------------------------------------------------
    # low-level http
    # ------------------------------------------------------------------

    def _http_json(
        self,
        url: str,
        *,
        method: str = "GET",
        headers: dict[str, str] | None = None,
        body: dict[str, Any] | None = None,
        timeout: int | None = None,
    ) -> tuple[int, dict[str, str], dict[str, Any] | None]:
        req_headers = {
            "Accept": "application/json",
            "User-Agent": self.user_agent,
        }
        if headers:
            req_headers.update(headers)

        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            req_headers["Content-Type"] = "application/json"

        req = Request(url, data=data, headers=req_headers, method=method)

        try:
            with urlopen(req, timeout=timeout or self.timeout_seconds) as resp:
                raw = resp.read()
                payload = json.loads(raw.decode("utf-8")) if raw else None
                return resp.status, dict(resp.headers.items()), payload
        except HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            payload = None
            try:
                payload = json.loads(raw) if raw else None
            except Exception:
                payload = None

            error_code = "http_error"
            error_text = f"HTTP {exc.code}"
            if isinstance(payload, dict):
                error_code = str(payload.get("error") or error_code)
                error_text = str(payload.get("message") or error_text)

            raise LabelerRefreshError(
                http_status=exc.code,
                error_code=error_code,
                error_text=error_text,
                response_json=payload if isinstance(payload, dict) else None,
                raw_text=raw,
                retryable=(exc.code == 429 or 500 <= exc.code < 600),
            ) from exc
        except Exception as exc:
            raise LabelerRefreshError(
                http_status=None,
                error_code=exc.__class__.__name__,
                error_text=str(exc),
                response_json=None,
                raw_text=None,
                retryable=True,
            ) from exc

    # ------------------------------------------------------------------
    # jwt/session helpers
    # ------------------------------------------------------------------

    def _decode_jwt_exp(self, token: str | None) -> float | None:
        if not token:
            return None
        try:
            parts = token.split(".")
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

    def _access_token_fresh(self) -> bool:
        if self._session is None:
            return False
        if self._session.access_expires_at_epoch is None:
            return True
        return time.time() < (self._session.access_expires_at_epoch - self.access_refresh_margin_seconds)

    def _refresh_token_fresh(self) -> bool:
        if self._session is None or not self._session.refresh_jwt:
            return False
        if self._session.refresh_expires_at_epoch is None:
            return True
        return time.time() < (self._session.refresh_expires_at_epoch - self.refresh_refresh_margin_seconds)

    def _create_session(self, host_url: str) -> CachedSession:
        _, _, payload = self._http_json(
            f"{host_url.rstrip('/')}/xrpc/com.atproto.server.createSession",
            method="POST",
            body={
                "identifier": self.handle,
                "password": self.app_password,
            },
        )
        if not isinstance(payload, dict) or not payload.get("accessJwt"):
            raise LabelerRefreshError(
                http_status=200,
                error_code="invalid_session_response",
                error_text="createSession did not return accessJwt",
                response_json=payload if isinstance(payload, dict) else None,
                retryable=False,
            )

        session = CachedSession(
            access_jwt=str(payload["accessJwt"]),
            refresh_jwt=(str(payload["refreshJwt"]) if payload.get("refreshJwt") else None),
            access_expires_at_epoch=self._decode_jwt_exp(payload.get("accessJwt")),
            refresh_expires_at_epoch=self._decode_jwt_exp(payload.get("refreshJwt")),
            did=(str(payload["did"]) if payload.get("did") else None),
            handle=(str(payload["handle"]) if payload.get("handle") else None),
        )
        self._session = session
        if session.did and not self.repo_did:
            self.repo_did = session.did
        self._save_cache()
        return session

    def _refresh_session(self) -> CachedSession:
        if self._session is None or not self._session.refresh_jwt:
            raise LabelerRefreshError(
                http_status=None,
                error_code="missing_refresh_jwt",
                error_text="No refreshJwt available",
                retryable=False,
            )
        if not self.resolved_pds_url:
            raise LabelerRefreshError(
                http_status=None,
                error_code="missing_resolved_pds_url",
                error_text="No resolved PDS URL available",
                retryable=False,
            )

        _, _, payload = self._http_json(
            f"{self.resolved_pds_url.rstrip('/')}/xrpc/com.atproto.server.refreshSession",
            method="POST",
            headers={"Authorization": f"Bearer {self._session.refresh_jwt}"},
        )
        if not isinstance(payload, dict) or not payload.get("accessJwt"):
            raise LabelerRefreshError(
                http_status=200,
                error_code="invalid_refresh_response",
                error_text="refreshSession did not return accessJwt",
                response_json=payload if isinstance(payload, dict) else None,
                retryable=False,
            )

        session = CachedSession(
            access_jwt=str(payload["accessJwt"]),
            refresh_jwt=(str(payload["refreshJwt"]) if payload.get("refreshJwt") else self._session.refresh_jwt),
            access_expires_at_epoch=self._decode_jwt_exp(payload.get("accessJwt")),
            refresh_expires_at_epoch=self._decode_jwt_exp(payload.get("refreshJwt") or self._session.refresh_jwt),
            did=(str(payload["did"]) if payload.get("did") else self._session.did),
            handle=(str(payload["handle"]) if payload.get("handle") else self._session.handle),
        )
        self._session = session
        if session.did and not self.repo_did:
            self.repo_did = session.did
        self._save_cache()
        return session

    # ------------------------------------------------------------------
    # DID/PDS resolution
    # ------------------------------------------------------------------

    def _resolve_did_document(self, did: str) -> dict[str, Any]:
        if did.startswith("did:plc:"):
            _, _, payload = self._http_json(f"https://plc.directory/{quote(did, safe=':')}")
        elif did.startswith("did:web:"):
            host = did[len("did:web:"):].replace("%3A", ":").replace("%3a", ":")
            _, _, payload = self._http_json(f"https://{host}/.well-known/did.json")
        else:
            raise LabelerRefreshError(
                http_status=None,
                error_code="unsupported_did_method",
                error_text=f"Unsupported DID method: {did}",
                retryable=False,
            )

        if not isinstance(payload, dict):
            raise LabelerRefreshError(
                http_status=200,
                error_code="invalid_did_document",
                error_text="Resolved DID document is not a JSON object",
                response_json=payload if isinstance(payload, dict) else None,
                retryable=False,
            )
        return payload

    def _extract_pds_url(self, did_doc: dict[str, Any]) -> str:
        services = did_doc.get("service")
        if not isinstance(services, list):
            raise LabelerRefreshError(
                http_status=None,
                error_code="missing_service_array",
                error_text="DID document missing service array",
                retryable=False,
            )

        for svc in services:
            if not isinstance(svc, dict):
                continue
            svc_id = str(svc.get("id") or "")
            svc_type = str(svc.get("type") or "")
            endpoint = str(svc.get("serviceEndpoint") or "").strip()
            if svc_id.endswith("#atproto_pds") and svc_type == "AtprotoPersonalDataServer" and endpoint:
                return endpoint.rstrip("/")

        raise LabelerRefreshError(
            http_status=None,
            error_code="missing_atproto_pds_service",
            error_text="Could not find #atproto_pds service endpoint in DID document",
            retryable=False,
        )

    def ensure_resolution(self) -> None:
        if self.repo_did and self.resolved_pds_url:
            return

        bootstrap_session = self._create_session(self.login_host)
        if not bootstrap_session.did:
            raise LabelerRefreshError(
                http_status=200,
                error_code="bootstrap_session_missing_did",
                error_text="Bootstrap session did not return did",
                retryable=False,
            )

        self.repo_did = bootstrap_session.did
        did_doc = self._resolve_did_document(self.repo_did)
        self.resolved_pds_url = self._extract_pds_url(did_doc)
        self._save_cache()

        if self.resolved_pds_url == self.login_host:
            return

        # If the bootstrap host is not the real PDS, throw away the bootstrap session.
        self._session = None
        self._save_cache()

    # ------------------------------------------------------------------
    # public session management
    # ------------------------------------------------------------------

    def ensure_session(self) -> str:
        self.ensure_resolution()

        if self._access_token_fresh():
            assert self._session is not None
            return self._session.access_jwt

        if self._refresh_token_fresh():
            try:
                session = self._refresh_session()
                return session.access_jwt
            except LabelerRefreshError as exc:
                if exc.http_status != 401:
                    raise

        assert self.resolved_pds_url is not None
        session = self._create_session(self.resolved_pds_url)
        return session.access_jwt

    def state_dict(self) -> dict[str, Any]:
        return {
            "repo_did": self.repo_did,
            "resolved_pds_url": self.resolved_pds_url,
            "login_host": self.login_host,
            "cache_path": str(self.cache_path),
            "cache_exists": self.cache_path.exists(),
            "session_handle": None if self._session is None else self._session.handle,
            "access_token_expires_at_epoch": None if self._session is None else self._session.access_expires_at_epoch,
            "refresh_token_expires_at_epoch": None if self._session is None else self._session.refresh_expires_at_epoch,
            "last_refresh_at_iso": self.last_refresh_at_iso,
            "last_refresh_cid": self.last_refresh_cid,
        }

    # ------------------------------------------------------------------
    # record helpers
    # ------------------------------------------------------------------

    def load_backup_record(self) -> dict[str, Any]:
        if not self.backup_json_path.exists():
            raise LabelerRefreshError(
                http_status=None,
                error_code="backup_json_missing",
                error_text=f"Backup labeler service JSON not found: {self.backup_json_path}",
                retryable=False,
            )

        payload = json.loads(self.backup_json_path.read_text(encoding="utf-8"))
        value = payload.get("value")
        if not isinstance(value, dict):
            raise LabelerRefreshError(
                http_status=None,
                error_code="backup_json_invalid",
                error_text="Backup JSON does not contain a usable value object",
                retryable=False,
            )

        record = json.loads(json.dumps(value))
        record["createdAt"] = now_iso()
        return record

    def put_record(self, record: dict[str, Any]) -> dict[str, Any]:
        access_jwt = self.ensure_session()
        if not self.resolved_pds_url or not self.repo_did:
            raise LabelerRefreshError(
                http_status=None,
                error_code="missing_resolution_state",
                error_text="Resolved PDS URL or repo DID missing",
                retryable=False,
            )

        def _do_put(token: str) -> dict[str, Any]:
            _, _, payload = self._http_json(
                f"{self.resolved_pds_url.rstrip('/')}/xrpc/com.atproto.repo.putRecord",
                method="POST",
                headers={"Authorization": f"Bearer {token}"},
                body={
                    "repo": self.repo_did,
                    "collection": RECORD_COLLECTION,
                    "rkey": RECORD_RKEY,
                    "record": record,
                    "validate": True,
                },
            )
            if not isinstance(payload, dict):
                raise LabelerRefreshError(
                    http_status=200,
                    error_code="invalid_put_record_response",
                    error_text="putRecord did not return a JSON object",
                    response_json=payload if isinstance(payload, dict) else None,
                    retryable=False,
                )
            return payload

        try:
            return _do_put(access_jwt)
        except LabelerRefreshError as exc:
            if exc.http_status != 401:
                raise
            access_jwt = self.ensure_session()
            return _do_put(access_jwt)

    def refresh_from_backup(self) -> dict[str, Any]:
        record = self.load_backup_record()
        payload = self.put_record(record)
        self.last_refresh_at_iso = now_iso()
        self.last_refresh_cid = str(payload.get("cid") or "") or None
        self._save_cache()
        return payload