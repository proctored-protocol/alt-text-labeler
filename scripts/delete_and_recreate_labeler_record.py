from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

DEFAULT_ENV_PATH = Path("/srv/alt-text-labeler/.env")
DEFAULT_BACKUP_JSON_PATH = Path("/tmp/labeler-service-backup.json")

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
    if not path.exists():
        return data

    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key.strip()] = value.strip()
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


def create_session(pds_url: str, handle: str, app_password: str, timeout: int) -> dict[str, Any]:
    url = f"{pds_url.rstrip('/')}/xrpc/com.atproto.server.createSession"
    _status, _headers, payload = http_json(
        url,
        method="POST",
        body={"identifier": handle, "password": app_password},
        timeout=timeout,
    )
    return payload


def put_record(
    pds_url: str,
    access_jwt: str,
    repo: str,
    record: dict[str, Any],
    timeout: int,
) -> dict[str, Any]:
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
        timeout=timeout,
    )
    return payload


def load_backup_record(path: Path) -> dict[str, Any]:
    if not path.exists():
        fail("Backup labeler service JSON not found", details={"path": str(path)})

    payload = json.loads(path.read_text(encoding="utf-8"))
    value = payload.get("value")
    if not isinstance(value, dict):
        fail("Backup JSON does not contain a usable value object", details={"path": str(path)})

    record = json.loads(json.dumps(value))
    record["createdAt"] = now_iso()
    return record


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reset labeler service record by authenticated putRecord overwrite."
    )
    parser.add_argument("--env-path", default=str(DEFAULT_ENV_PATH))
    parser.add_argument("--backup-json-path", default=str(DEFAULT_BACKUP_JSON_PATH))
    parser.add_argument("--handle", default=None)
    parser.add_argument("--app-password", default=None)
    parser.add_argument("--pds-url", default=None)
    parser.add_argument("--timeout-seconds", type=int, default=30)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    env_path = Path(args.env_path)
    backup_json_path = Path(args.backup_json_path)

    env = load_env(env_path)

    handle = (args.handle or env.get("BSKY_HANDLE") or "").strip()
    app_password = (args.app_password or env.get("BSKY_APP_PASSWORD") or "").strip()
    pds_url = (args.pds_url or env.get("BSKY_PDS_URL") or "https://bsky.social").strip()
    timeout_seconds = int(args.timeout_seconds)

    if not handle:
        fail("Missing required handle", details={"source": "CLI or BSKY_HANDLE"})
    if not app_password:
        fail("Missing required app password", details={"source": "CLI or BSKY_APP_PASSWORD"})

    try:
        session = create_session(pds_url, handle, app_password, timeout_seconds)
    except HTTPJSONError as exc:
        fail(
            "Could not create session for labeler account",
            details={"status_code": exc.status_code, "response": exc.raw_text},
        )

    repo = session.get("did")
    access_jwt = session.get("accessJwt")

    if not repo or not access_jwt:
        fail(
            "Session response missing did or accessJwt",
            details={"session_keys": sorted(session.keys()) if isinstance(session, dict) else None},
        )

    record = load_backup_record(backup_json_path)

    try:
        put_payload = put_record(
            pds_url=pds_url,
            access_jwt=str(access_jwt),
            repo=str(repo),
            record=record,
            timeout=timeout_seconds,
        )
    except HTTPJSONError as exc:
        fail(
            "Could not overwrite labeler service record via putRecord",
            details={
                "status_code": exc.status_code,
                "response": exc.raw_text,
                "repo": repo,
                "pds_url": pds_url,
            },
        )

    print(
        json.dumps(
            {
                "event": "labeler_record_reset_complete",
                "checked_at": now_iso(),
                "mode": "put_only",
                "repo": repo,
                "handle": session.get("handle"),
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