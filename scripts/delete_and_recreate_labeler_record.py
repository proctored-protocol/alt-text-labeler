from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from app.labeler_record_refresh.client import LabelerRecordRefreshClient, LabelerRefreshError


DEFAULT_ENV_PATH = Path("/srv/alt-text-labeler/.env")
DEFAULT_BACKUP_JSON_PATH = Path("/tmp/labeler-service-backup.json")


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh labeler service record once, using resolved PDS and cached-style client logic."
    )
    parser.add_argument("--env-path", default=str(DEFAULT_ENV_PATH))
    parser.add_argument("--backup-json-path", default=str(DEFAULT_BACKUP_JSON_PATH))
    parser.add_argument("--handle", default=None)
    parser.add_argument("--app-password", default=None)
    parser.add_argument("--login-host", default=None)
    parser.add_argument("--timeout-seconds", type=int, default=30)
    parser.add_argument("--session-refresh-margin-seconds", type=int, default=60)
    return parser.parse_args()


def fail(msg: str, *, details: dict | None = None) -> None:
    payload = {
        "event": "labeler_record_reset_failed",
        "message": msg,
        "details": details or {},
    }
    print(json.dumps(payload, ensure_ascii=False), flush=True)
    raise SystemExit(1)


def main() -> None:
    args = parse_args()
    env = load_env(Path(args.env_path))

    handle = (args.handle or env.get("BSKY_HANDLE") or "").strip()
    app_password = (args.app_password or env.get("BSKY_APP_PASSWORD") or "").strip()
    login_host = (args.login_host or env.get("BSKY_PDS_URL") or "https://bsky.social").strip()

    if not handle:
        fail("Missing required handle", details={"source": "CLI or BSKY_HANDLE"})
    if not app_password:
        fail("Missing required app password", details={"source": "CLI or BSKY_APP_PASSWORD"})

    client = LabelerRecordRefreshClient(
        handle=handle,
        app_password=app_password,
        backup_json_path=args.backup_json_path,
        login_host=login_host,
        timeout_seconds=args.timeout_seconds,
        session_refresh_margin_seconds=args.session_refresh_margin_seconds,
    )

    try:
        put_payload = client.refresh_from_backup()
    except LabelerRefreshError as exc:
        fail(
            "Could not refresh labeler service record",
            details={
                "http_status": exc.http_status,
                "error_code": exc.error_code,
                "error_text": exc.error_text,
                "response_json": exc.response_json,
                "raw_text": exc.raw_text,
                **client.state_dict(),
            },
        )

    state = client.state_dict()

    print(
        json.dumps(
            {
                "event": "labeler_record_reset_complete",
                "mode": "put_only_via_resolved_pds_cached_client",
                "repo": state["repo_did"],
                "handle": state["session_handle"] or handle,
                "login_host": state["login_host"],
                "resolved_pds_url": state["resolved_pds_url"],
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