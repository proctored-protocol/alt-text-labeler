from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import text

from app.db import engine


UTC = timezone.utc


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat()


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def seconds_since(value: datetime | None) -> float | None:
    if value is None:
        return None
    return round((utc_now() - value).total_seconds(), 3)


@dataclass
class Snapshot:
    checked_at_utc: str
    live_monitor_last_seq: int | None
    live_monitor_last_message_at: str | None
    intake_cursor_last_seq: int | None
    intake_cursor_updated_at: str | None
    post_eval_max_last_seen_seq: int | None
    post_eval_max_evaluated_at: str | None
    evaluated_rows_2m: int
    evaluated_rows_10m: int
    labeled_rows_10m: int
    seq_gap_live_minus_intake: int | None
    seq_gap_live_minus_post_eval: int | None
    seq_gap_post_eval_minus_cursor: int | None
    cursor_updated_age_seconds: float | None
    post_eval_updated_age_seconds: float | None


def get_live_sqlite_conn(db_path: str) -> sqlite3.Connection:
    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(f"Live firehose SQLite DB not found: {db_path}")

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def get_live_monitor_state(conn: sqlite3.Connection) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT
            last_seq,
            last_message_at
        FROM firehose_monitor_state
        WHERE singleton_id = 1
        """
    ).fetchone()

    if row is None:
        return {}

    data = dict(row)
    last_message_at = data.get("last_message_at")
    if last_message_at is not None:
        data["last_message_at_iso"] = datetime.fromtimestamp(
            int(last_message_at), tz=UTC
        ).isoformat()
    else:
        data["last_message_at_iso"] = None
    return data


def collect_postgres_state() -> dict[str, Any]:
    with engine.connect() as conn:
        cursor_row = conn.execute(
            text(
                """
                SELECT
                    last_seq,
                    updated_at
                FROM firehose_cursor
                WHERE stream_name = 'subscribe_repos'
                """
            )
        ).mappings().first()

        post_eval_row = conn.execute(
            text(
                """
                SELECT
                    COUNT(*) FILTER (
                        WHERE evaluated_at >= NOW() - INTERVAL '2 minutes'
                    ) AS evaluated_rows_2m,
                    COUNT(*) FILTER (
                        WHERE evaluated_at >= NOW() - INTERVAL '10 minutes'
                    ) AS evaluated_rows_10m,
                    COUNT(*) FILTER (
                        WHERE evaluated_at >= NOW() - INTERVAL '10 minutes'
                          AND derived_label IN ('missing-alt-text', 'partial-alt-text')
                    ) AS labeled_rows_10m,
                    MAX(last_seen_seq) AS max_last_seen_seq,
                    MAX(evaluated_at) AS max_evaluated_at
                FROM post_evaluation
                """
            )
        ).mappings().one()

    return {
        "cursor": dict(cursor_row) if cursor_row is not None else {},
        "post_evaluation": dict(post_eval_row),
    }


def collect_snapshot(live_db_path: str) -> Snapshot:
    conn = get_live_sqlite_conn(live_db_path)
    try:
        live_state = get_live_monitor_state(conn)
    finally:
        conn.close()

    pg_state = collect_postgres_state()
    cursor_row = pg_state["cursor"]
    post_eval_row = pg_state["post_evaluation"]

    live_seq = live_state.get("last_seq")
    intake_seq = cursor_row.get("last_seq")
    post_eval_seq = post_eval_row.get("max_last_seen_seq")

    gap_live_intake = None
    if live_seq is not None and intake_seq is not None:
        gap_live_intake = int(live_seq) - int(intake_seq)

    gap_live_post_eval = None
    if live_seq is not None and post_eval_seq is not None:
        gap_live_post_eval = int(live_seq) - int(post_eval_seq)

    gap_post_eval_cursor = None
    if post_eval_seq is not None and intake_seq is not None:
        gap_post_eval_cursor = int(post_eval_seq) - int(intake_seq)

    cursor_updated_at = cursor_row.get("updated_at")
    post_eval_updated_at = post_eval_row.get("max_evaluated_at")

    return Snapshot(
        checked_at_utc=iso(utc_now()),
        live_monitor_last_seq=live_seq,
        live_monitor_last_message_at=live_state.get("last_message_at_iso"),
        intake_cursor_last_seq=intake_seq,
        intake_cursor_updated_at=iso(cursor_updated_at) if cursor_updated_at else None,
        post_eval_max_last_seen_seq=post_eval_seq,
        post_eval_max_evaluated_at=iso(post_eval_updated_at) if post_eval_updated_at else None,
        evaluated_rows_2m=int(post_eval_row.get("evaluated_rows_2m") or 0),
        evaluated_rows_10m=int(post_eval_row.get("evaluated_rows_10m") or 0),
        labeled_rows_10m=int(post_eval_row.get("labeled_rows_10m") or 0),
        seq_gap_live_minus_intake=gap_live_intake,
        seq_gap_live_minus_post_eval=gap_live_post_eval,
        seq_gap_post_eval_minus_cursor=gap_post_eval_cursor,
        cursor_updated_age_seconds=seconds_since(cursor_updated_at),
        post_eval_updated_age_seconds=seconds_since(post_eval_updated_at),
    )


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "bad_streak": 0,
            "last_restart_at": None,
            "last_snapshot": None,
        }
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def compute_delta(current: int | None, previous: int | None) -> int | None:
    if current is None or previous is None:
        return None
    return int(current) - int(previous)


def restart_service(service_name: str, timeout_seconds: int) -> dict[str, Any]:
    completed = subprocess.run(
        ["systemctl", "restart", service_name],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )
    return {
        "returncode": completed.returncode,
        "stdout": completed.stdout.strip(),
        "stderr": completed.stderr.strip(),
        "ok": completed.returncode == 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Watch intake forward progress and restart the service if it stalls."
    )
    parser.add_argument(
        "--live-db-path",
        default="data/firehose_live.sqlite3",
    )
    parser.add_argument(
        "--state-file",
        default="data/intake_watchdog_state.json",
    )
    parser.add_argument(
        "--service-name",
        default="alt-text-labeler-intake.service",
    )
    parser.add_argument(
        "--min-live-seq-advance",
        type=int,
        default=5000,
        help="Minimum live head movement between checks to consider the firehose active.",
    )
    parser.add_argument(
        "--min-intake-seq-advance",
        type=int,
        default=1000,
        help="Minimum intake cursor movement between checks to count as progress.",
    )
    parser.add_argument(
        "--min-post-eval-seq-advance",
        type=int,
        default=1000,
        help="Minimum post_evaluation max_last_seen_seq movement between checks to count as progress.",
    )
    parser.add_argument(
        "--restart-bad-streak",
        type=int,
        default=3,
        help="Restart after this many consecutive stalled checks.",
    )
    parser.add_argument(
        "--restart-cooldown-seconds",
        type=int,
        default=900,
        help="Minimum time between watchdog-triggered restarts.",
    )
    parser.add_argument(
        "--systemctl-timeout-seconds",
        type=int,
        default=30,
    )
    args = parser.parse_args()

    state_path = Path(args.state_file)
    state = load_state(state_path)

    current = collect_snapshot(args.live_db_path)
    previous_dict = state.get("last_snapshot")
    previous = Snapshot(**previous_dict) if previous_dict else None

    live_delta = compute_delta(
        current.live_monitor_last_seq,
        previous.live_monitor_last_seq if previous else None,
    )
    intake_delta = compute_delta(
        current.intake_cursor_last_seq,
        previous.intake_cursor_last_seq if previous else None,
    )
    post_eval_delta = compute_delta(
        current.post_eval_max_last_seen_seq,
        previous.post_eval_max_last_seen_seq if previous else None,
    )
    gap_delta = compute_delta(
        current.seq_gap_live_minus_intake,
        previous.seq_gap_live_minus_intake if previous else None,
    )

    live_moving = live_delta is not None and live_delta >= args.min_live_seq_advance
    intake_moving = intake_delta is not None and intake_delta >= args.min_intake_seq_advance
    post_eval_moving = (
        post_eval_delta is not None and post_eval_delta >= args.min_post_eval_seq_advance
    )

    status = "healthy"
    bad_streak = int(state.get("bad_streak") or 0)

    if previous is None:
        status = "primed"
        bad_streak = 0
    elif not live_moving:
        status = "live_idle_or_unobservable"
        bad_streak = 0
    elif intake_moving or post_eval_moving:
        status = "healthy"
        bad_streak = 0
    else:
        status = "stalled"
        bad_streak += 1

    now = utc_now()
    last_restart_at = parse_iso(state.get("last_restart_at"))
    cooldown_elapsed = (
        last_restart_at is None
        or (now - last_restart_at).total_seconds() >= args.restart_cooldown_seconds
    )

    action = {
        "restart_attempted": False,
        "restart_performed": False,
        "restart_result": None,
    }

    if status == "stalled" and bad_streak >= args.restart_bad_streak and cooldown_elapsed:
        restart_result = restart_service(
            service_name=args.service_name,
            timeout_seconds=args.systemctl_timeout_seconds,
        )
        action = {
            "restart_attempted": True,
            "restart_performed": bool(restart_result["ok"]),
            "restart_result": restart_result,
        }
        if restart_result["ok"]:
            bad_streak = 0
            last_restart_at = now

    new_state = {
        "bad_streak": bad_streak,
        "last_restart_at": iso(last_restart_at) if last_restart_at else None,
        "last_snapshot": asdict(current),
    }
    save_state(state_path, new_state)

    payload = {
        "event": "intake_watchdog_check",
        "checked_at_utc": iso(now),
        "service_name": args.service_name,
        "status": status,
        "bad_streak": bad_streak,
        "thresholds": {
            "min_live_seq_advance": args.min_live_seq_advance,
            "min_intake_seq_advance": args.min_intake_seq_advance,
            "min_post_eval_seq_advance": args.min_post_eval_seq_advance,
            "restart_bad_streak": args.restart_bad_streak,
            "restart_cooldown_seconds": args.restart_cooldown_seconds,
        },
        "current": asdict(current),
        "deltas_since_previous_check": {
            "live_seq_delta": live_delta,
            "intake_cursor_delta": intake_delta,
            "post_eval_seq_delta": post_eval_delta,
            "gap_delta": gap_delta,
        },
        "action": action,
        "last_restart_at": iso(last_restart_at) if last_restart_at else None,
    }
    print(json.dumps(payload, ensure_ascii=False, default=str), flush=True)


if __name__ == "__main__":
    main()