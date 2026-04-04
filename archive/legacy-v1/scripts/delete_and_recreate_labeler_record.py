from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ENV_PATH = Path("/srv/alt-text-labeler/.env")
BACKUP_JSON_PATH = Path("/tmp/labeler-service-backup.json")

RECORD_COLLECTION = "app.bsky.labeler.service"
RECORD_RKEY = "self"


class HTTPJSONError(RuntimeError):
    def __init__(self, status_code: int, raw_text: str, payload: Any | None = None) -> None:
        super().__init__(f"HTTP {status_code}: {raw_text}")
        self.status_code = status_code
        self.raw_text = raw_text
        self.payload = payload


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_env(path: Path) -> dict[str, str]:
    data: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        data[k.strip()] = v.strip()
    return data


def fail(msg: str, *, details: dict[str, Any] | None = None) -> None:
    payload: dict[str, Any] = {
        "event": "labeler_record_reset_failed",
        "checked_at": now_iso(),
        "message": msg,
    }
    if details:
        payload["details"] = details
    print(json.dumps(payload, ensure_ascii=False), flush=True)
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


def create_session(pds_url: str, handle: str, app_password: str) -> dict[str, Any]:
    url = f"{pds_url.rstrip('/')}/xrpc/com.atproto.server.createSession"
    _status, _headers, payload = http_json(
        url,
        method="POST",
        body={"identifier": handle, "password": app_password},
    )
    return payload


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


def load_backup_record() -> dict[str, Any]:
    if not BACKUP_JSON_PATH.exists():
        fail("Backup labeler service JSON not found", details={"path": str(BACKUP_JSON_PATH)})

    payload = json.loads(BACKUP_JSON_PATH.read_text(encoding="utf-8"))
    value = payload.get("value")
    if not isinstance(value, dict):
        fail("Backup JSON does not contain a usable value object", details={"path": str(BACKUP_JSON_PATH)})

    value = json.loads(json.dumps(value))
    value["createdAt"] = now_iso()
    return value


def main() -> None:
    env = load_env(ENV_PATH)

    handle = (env.get("BSKY_HANDLE") or "").strip()
    app_password = (env.get("BSKY_APP_PASSWORD") or "").strip()
    pds_url = (env.get("BSKY_PDS_URL") or "https://bsky.social").strip()

    if not handle:
        fail("Missing required environment variable: BSKY_HANDLE")
    if not app_password:
        fail("Missing required environment variable: BSKY_APP_PASSWORD")

    try:
        session = create_session(pds_url, handle, app_password)
    except HTTPJSONError as exc:
        fail(
            "Could not create session for labeler account",
            details={"status_code": exc.status_code, "response": exc.raw_text},
        )

    repo = session["did"]
    access_jwt = session["accessJwt"]

    old_cid = None
    old_record = None
    try:
        current = get_record(pds_url, access_jwt, repo)
        old_cid = current.get("cid")
        old_record = current.get("value")
    except HTTPJSONError as exc:
        if exc.status_code != 400:
            fail(
                "Could not fetch current labeler service record",
                details={"status_code": exc.status_code, "response": exc.raw_text},
            )

    delete_payload = None
    if old_cid is not None:
        try:
            delete_payload = delete_record(pds_url, access_jwt, repo)
        except HTTPJSONError as exc:
            fail(
                "Could not delete current labeler service record",
                details={"status_code": exc.status_code, "response": exc.raw_text},
            )

    record = load_backup_record()

    try:
        put_payload = put_record(pds_url, access_jwt, repo, record)
    except HTTPJSONError as exc:
        fail(
            "Could not recreate labeler service record from backup payload",
            details={
                "status_code": exc.status_code,
                "response": exc.raw_text,
                "old_cid": old_cid,
                "delete_payload": delete_payload,
            },
        )

    print(
        json.dumps(
            {
                "event": "labeler_record_reset_complete",
                "checked_at": now_iso(),
                "repo": repo,
                "handle": session.get("handle"),
                "old_cid": old_cid,
                "new_cid": put_payload.get("cid"),
                "uri": put_payload.get("uri"),
                "validationStatus": put_payload.get("validationStatus"),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        raise