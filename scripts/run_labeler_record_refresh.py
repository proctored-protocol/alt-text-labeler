from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import text

from app.db import get_engine
from app.labeler_record_refresh.client import LabelerRecordRefreshClient, LabelerRefreshError


DEFAULT_ENV_PATH = Path("/srv/alt-text-labeler/.env")
DEFAULT_BACKUP_JSON_PATH = Path("/tmp/labeler-service-backup.json")
DEFAULT_CACHE_PATH = Path("/srv/alt-text-labeler/metrics/labeler_record_refresh_session.json")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return now_utc().isoformat().replace("+00:00", "Z")


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


def default_log_path(metrics_dir: Path) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return metrics_dir / f"labeler_record_refresh_{stamp}.jsonl"


def append_jsonl(path: Path, payload: dict) -> None:
    line = json.dumps(payload, ensure_ascii=False, default=str)
    print(line, flush=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def get_emit_counts_since(since_utc: datetime) -> dict[str, int]:
    with get_engine().connect() as conn:
        row = conn.execute(
            text("""
                SELECT
                    COALESCE((
                        SELECT COUNT(*)
                        FROM publish_attempt
                        WHERE result_status = 'published'
                          AND finished_at > :since_utc
                    ), 0) AS publish_emits,
                    COALESCE((
                        SELECT COUNT(*)
                        FROM visibility_remediation
                        WHERE first_relabel_event_id IS NOT NULL
                          AND first_attempt_at > :since_utc
                    ), 0) AS remediation_first_relabels,
                    COALESCE((
                        SELECT COUNT(*)
                        FROM visibility_remediation
                        WHERE second_relabel_event_id IS NOT NULL
                          AND second_attempt_at > :since_utc
                    ), 0) AS remediation_second_relabels
            """),
            {"since_utc": since_utc},
        ).mappings().one()

    publish_emits = int(row["publish_emits"] or 0)
    remediation_first = int(row["remediation_first_relabels"] or 0)
    remediation_second = int(row["remediation_second_relabels"] or 0)

    return {
        "publish_emits": publish_emits,
        "remediation_first_relabels": remediation_first,
        "remediation_second_relabels": remediation_second,
        "total_emits": publish_emits + remediation_first + remediation_second,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Long-lived labeler record refresh daemon with persistent session cache and emit/time triggers."
    )
    parser.add_argument("--env-path", default=str(DEFAULT_ENV_PATH))
    parser.add_argument("--backup-json-path", default=str(DEFAULT_BACKUP_JSON_PATH))
    parser.add_argument("--cache-path", default=str(DEFAULT_CACHE_PATH))
    parser.add_argument("--handle", default=None)
    parser.add_argument("--app-password", default=None)
    parser.add_argument("--login-host", default=None)

    parser.add_argument(
        "--max-interval-seconds",
        type=int,
        default=int(os.environ.get("REFRESH_MAX_INTERVAL_SECONDS", "300")),
    )
    parser.add_argument(
        "--min-interval-seconds",
        type=int,
        default=int(os.environ.get("REFRESH_MIN_INTERVAL_SECONDS", "60")),
    )
    parser.add_argument(
        "--emit-threshold",
        type=int,
        default=int(os.environ.get("REFRESH_EMIT_THRESHOLD", "250")),
    )
    parser.add_argument(
        "--check-poll-seconds",
        type=int,
        default=int(os.environ.get("REFRESH_CHECK_POLL_SECONDS", "5")),
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=int(os.environ.get("RESET_TIMEOUT_SECONDS", "30")),
    )
    parser.add_argument(
        "--access-refresh-margin-seconds",
        type=int,
        default=int(os.environ.get("REFRESH_ACCESS_MARGIN_SECONDS", "60")),
    )
    parser.add_argument(
        "--refresh-refresh-margin-seconds",
        type=int,
        default=int(os.environ.get("REFRESH_REFRESH_MARGIN_SECONDS", "300")),
    )
    parser.add_argument(
        "--metrics-dir",
        default=os.environ.get("METRICS_DIR", "metrics"),
    )
    parser.add_argument(
        "--startup-refresh",
        action="store_true",
        help="Perform one refresh immediately at startup.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Perform one refresh attempt and exit.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    env = load_env(Path(args.env_path))

    handle = (args.handle or env.get("BSKY_HANDLE") or "").strip()
    app_password = (args.app_password or env.get("BSKY_APP_PASSWORD") or "").strip()
    login_host = (args.login_host or env.get("BSKY_PDS_URL") or "https://bsky.social").strip()

    if not handle:
        raise SystemExit("Missing handle (CLI or BSKY_HANDLE)")
    if not app_password:
        raise SystemExit("Missing app password (CLI or BSKY_APP_PASSWORD)")

    metrics_dir = Path(args.metrics_dir)
    metrics_dir.mkdir(parents=True, exist_ok=True)
    log_path = default_log_path(metrics_dir)

    client = LabelerRecordRefreshClient(
        handle=handle,
        app_password=app_password,
        backup_json_path=args.backup_json_path,
        cache_path=args.cache_path,
        login_host=login_host,
        timeout_seconds=args.timeout_seconds,
        access_refresh_margin_seconds=args.access_refresh_margin_seconds,
        refresh_refresh_margin_seconds=args.refresh_refresh_margin_seconds,
    )

    if client.last_refresh_at_iso:
        try:
            last_success_at = datetime.fromisoformat(client.last_refresh_at_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            last_success_at = now_utc()
    else:
        last_success_at = now_utc()

    last_attempt_at: datetime | None = None

    startup = {
        "event": "labeler_record_refresh_daemon_started",
        "started_at": now_iso(),
        "handle": handle,
        "login_host": login_host,
        "cache_path": str(args.cache_path),
        "cache_exists": Path(args.cache_path).exists(),
        "max_interval_seconds": args.max_interval_seconds,
        "min_interval_seconds": args.min_interval_seconds,
        "emit_threshold": args.emit_threshold,
        "check_poll_seconds": args.check_poll_seconds,
        "timeout_seconds": args.timeout_seconds,
        "access_refresh_margin_seconds": args.access_refresh_margin_seconds,
        "refresh_refresh_margin_seconds": args.refresh_refresh_margin_seconds,
        "startup_refresh": bool(args.startup_refresh),
        "once": bool(args.once),
        "log_path": str(log_path),
        **client.state_dict(),
    }
    append_jsonl(log_path, startup)

    def do_refresh(reason: str, emit_counts: dict[str, int]) -> None:
        nonlocal last_success_at, last_attempt_at

        started = now_utc()
        try:
            put_payload = client.refresh_from_backup()
            finished = now_utc()

            payload = {
                "event": "labeler_record_refresh_attempt",
                "started_at": started.isoformat().replace("+00:00", "Z"),
                "finished_at": finished.isoformat().replace("+00:00", "Z"),
                "reason": reason,
                "emit_counts_since_last_success": emit_counts,
                "success": True,
                "new_cid": put_payload.get("cid"),
                "uri": put_payload.get("uri"),
                "validationStatus": put_payload.get("validationStatus"),
                **client.state_dict(),
            }
            append_jsonl(log_path, payload)
            last_success_at = finished
            last_attempt_at = finished
        except LabelerRefreshError as exc:
            finished = now_utc()
            payload = {
                "event": "labeler_record_refresh_attempt",
                "started_at": started.isoformat().replace("+00:00", "Z"),
                "finished_at": finished.isoformat().replace("+00:00", "Z"),
                "reason": reason,
                "emit_counts_since_last_success": emit_counts,
                "success": False,
                "http_status": exc.http_status,
                "error_code": exc.error_code,
                "error_text": exc.error_text,
                "response_json": exc.response_json,
                "raw_text": exc.raw_text,
                **client.state_dict(),
            }
            append_jsonl(log_path, payload)
            last_attempt_at = finished

    if args.startup_refresh:
        do_refresh(
            "startup_refresh",
            {"publish_emits": 0, "remediation_first_relabels": 0, "remediation_second_relabels": 0, "total_emits": 0},
        )
        if args.once:
            return

    if args.once:
        emit_counts = get_emit_counts_since(last_success_at)
        do_refresh("once", emit_counts)
        return

    while True:
        current = now_utc()
        emit_counts = get_emit_counts_since(last_success_at)

        seconds_since_success = (current - last_success_at).total_seconds()
        seconds_since_attempt = None if last_attempt_at is None else (current - last_attempt_at).total_seconds()

        eligible_by_threshold = emit_counts["total_emits"] >= args.emit_threshold
        eligible_by_time = seconds_since_success >= args.max_interval_seconds
        blocked_by_floor = seconds_since_attempt is not None and seconds_since_attempt < args.min_interval_seconds

        heartbeat = {
            "event": "labeler_record_refresh_tick",
            "checked_at": current.isoformat().replace("+00:00", "Z"),
            "seconds_since_last_success": round(seconds_since_success, 3),
            "seconds_since_last_attempt": None if seconds_since_attempt is None else round(seconds_since_attempt, 3),
            "emit_counts_since_last_success": emit_counts,
            "eligible_by_threshold": eligible_by_threshold,
            "eligible_by_time": eligible_by_time,
            "blocked_by_min_interval": blocked_by_floor,
            "next_poll_in_seconds": args.check_poll_seconds,
            **client.state_dict(),
        }
        append_jsonl(log_path, heartbeat)

        if (eligible_by_threshold or eligible_by_time) and not blocked_by_floor:
            reason = "emit_threshold" if eligible_by_threshold else "max_interval"
            do_refresh(reason, emit_counts)

        time.sleep(max(args.check_poll_seconds, 1))


if __name__ == "__main__":
    main()