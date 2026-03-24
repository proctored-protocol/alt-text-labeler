#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

RECORD_COLLECTION = "app.bsky.labeler.service"
RECORD_RKEY = "self"


class HTTPJSONError(RuntimeError):
    def __init__(self, status_code: int, raw_text: str, payload: Any | None = None) -> None:
        super().__init__(f"HTTP {status_code}: {raw_text}")
        self.status_code = status_code
        self.raw_text = raw_text
        self.payload = payload


@dataclass
class Session:
    did: str
    handle: str
    access_jwt: str


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fail(msg: str, *, details: dict[str, Any] | None = None) -> None:
    payload = {
        "event": "labeler_record_reset_failed",
        "checked_at": now_iso(),
        "message": msg,
    }
    if details:
        payload["details"] = details
    print(json.dumps(payload, ensure_ascii=False))
    raise SystemExit(1)


def http_json(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    body: dict[str, Any] | None = None,
    timeout: int = 30,
) -> tuple[int, dict[str, str], Any]:
    req_headers = {"Accept": "application/json"}
    if headers:
        req_headers.update(headers)

    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        req_headers["Content-Type"] = "application/json"

    req = Request(url, data=data, headers=req_headers, method=method)

    try:
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            parsed = json.loads(raw) if raw else {}
            return resp.status, dict(resp.headers.items()), parsed
    except HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            parsed = None
        raise HTTPJSONError(exc.code, raw, payload=parsed) from exc


def env_required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        fail(f"Missing required environment variable: {name}")
    return value


def create_session(pds_url: str, handle: str, app_password: str) -> Session:
    url = f"{pds_url.rstrip('/')}/xrpc/com.atproto.server.createSession"
    _status, _headers, payload = http_json(
        url,
        method="POST",
        body={"identifier": handle, "password": app_password},
    )
    return Session(
        did=payload["did"],
        handle=payload["handle"],
        access_jwt=payload["accessJwt"],
    )


def get_record(pds_url: str, access_jwt: str, repo: str) -> dict[str, Any]:
    params = urlencode(
        {
            "repo": repo,
            "collection": RECORD_COLLECTION,
            "rkey": RECORD_RKEY,
        }
    )
    url = f"{pds_url.rstrip('/')}/xrpc/com.atproto.repo.getRecord?{params}"
    _status, _headers, payload = http_json(
        url,
        headers={"Authorization": f"Bearer {access_jwt}"},
    )
    return payload


def delete_record(pds_url: str, access_jwt: str, repo: str) -> dict[str, Any]:
    url = f"{pds_url.rstrip('/')}/xrpc/com.atproto.repo.deleteRecord"
    _status, _headers, payload = http_json(
        url,
        method="POST",
        headers={"Authorization": f"Bearer {access_jwt}"},
        body={
            "repo": repo,
            "collection": RECORD_COLLECTION,
            "rkey": RECORD_RKEY,
        },
    )
    return payload


def put_record(pds_url: str, access_jwt: str, repo: str, record: dict[str, Any]) -> dict[str, Any]:
    url = f"{pds_url.rstrip('/')}/xrpc/com.atproto.repo.putRecord"
    _status, _headers, payload = http_json(
        url,
        method="POST",
        headers={"Authorization": f"Bearer {access_jwt}"},
        body={
            "repo": repo,
            "collection": RECORD_COLLECTION,
            "rkey": RECORD_RKEY,
            "record": record,
            "validate": True,
        },
    )
    return payload


def main() -> None:
    pds_url = os.getenv("BSKY_PDS_URL", "https://bsky.social").strip()
    handle = env_required("BSKY_HANDLE")
    app_password = env_required("BSKY_APP_PASSWORD")

    try:
        session = create_session(pds_url, handle, app_password)
    except HTTPJSONError as exc:
        fail(
            "Could not create session for labeler account",
            details={"status_code": exc.status_code, "response": exc.raw_text},
        )

    repo = session.did
    auth_header = {"Authorization": f"Bearer {session.access_jwt}"}

    try:
        current = get_record(pds_url, session.access_jwt, repo)
    except HTTPJSONError as exc:
        if exc.status_code == 400:
            fail(
                "Fetching the current labeler service record returned HTTP 400. "
                "This often means the labeler account has not fully completed the one-time "
                "labeler registration / guidelines acceptance flow in Ozone.",
                details={"status_code": exc.status_code, "response": exc.raw_text},
            )
        fail(
            "Could not fetch current labeler service record",
            details={"status_code": exc.status_code, "response": exc.raw_text},
        )

    current_cid = current.get("cid")
    current_value = current.get("value")
    if not isinstance(current_value, dict):
        fail("Current labeler service record did not contain a usable value", details={"current": current})

    delete_payload: dict[str, Any] | None = None
    try:
        delete_payload = delete_record(pds_url, session.access_jwt, repo)
    except HTTPJSONError as exc:
        fail(
            "Could not delete current labeler service record",
            details={"status_code": exc.status_code, "response": exc.raw_text},
        )

    try:
        put_payload = put_record(pds_url, session.access_jwt, repo, current_value)
    except HTTPJSONError as exc:
        fail(
            "Could not recreate labeler service record after delete",
            details={
                "status_code": exc.status_code,
                "response": exc.raw_text,
                "delete_payload": delete_payload,
                "current_cid": current_cid,
            },
        )

    print(
        json.dumps(
            {
                "event": "labeler_record_reset_complete",
                "checked_at": now_iso(),
                "repo": repo,
                "handle": session.handle,
                "old_cid": current_cid,
                "delete_response": delete_payload,
                "put_response": put_payload,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()