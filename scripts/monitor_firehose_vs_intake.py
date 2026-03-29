from __future__ import annotations

import argparse
import json
import math
import sqlite3
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import text

from app.db import engine


UTC = timezone.utc


def utc_now() -> datetime:
    return datetime.now(UTC)


def floor_minute(dt: datetime) -> datetime:
    return dt.astimezone(UTC).replace(second=0, microsecond=0)


def minute_epoch(dt: datetime) -> int:
    return int(dt.timestamp())


def dt_from_epoch(epoch: int | None) -> datetime | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=UTC)


def iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.isoformat()


def to_number(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return float(value)


def ratio(numerator: Any, denominator: Any) -> float | None:
    num = to_number(numerator)
    den = to_number(denominator)
    if den <= 0:
        return None
    return round((num / den) * 100.0, 2)


@dataclass
class CursorSnapshot:
    sampled_at_utc: str
    live_monitor_last_seq: int | None
    live_monitor_last_message_at: str | None
    intake_cursor_last_seq: int | None
    intake_cursor_updated_at: str | None
    seq_gap_live_minus_intake: int | None


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
            started_at,
            updated_at,
            last_message_at,
            last_seq,
            reconnect_count,
            error_count,
            current_status,
            last_error
        FROM firehose_monitor_state
        WHERE singleton_id = 1
        """
    ).fetchone()

    if row is None:
        return {}

    data = dict(row)
    for key in ("started_at", "updated_at", "last_message_at"):
        if data.get(key) is not None:
            data[f"{key}_iso"] = iso(dt_from_epoch(int(data[key])))
    return data


def get_live_minute_counts(conn: sqlite3.Connection, bucket_start: datetime) -> dict[str, int]:
    start_epoch = minute_epoch(bucket_start)
    end_epoch = start_epoch + 60

    row = conn.execute(
        """
        SELECT
            COALESCE(SUM(commit_count), 0) AS commit_count,
            COALESCE(SUM(post_create_count), 0) AS post_create_count,
            COALESCE(SUM(image_post_count), 0) AS image_post_count,
            COALESCE(SUM(missing_alt_post_count), 0) AS missing_alt_post_count,
            COALESCE(SUM(partial_alt_post_count), 0) AS partial_alt_post_count,
            COALESCE(SUM(gif_post_count), 0) AS gif_post_count,
            COALESCE(SUM(video_post_count), 0) AS video_post_count
        FROM firehose_metrics_second
        WHERE ts_epoch >= ? AND ts_epoch < ?
        """,
        (start_epoch, end_epoch),
    ).fetchone()

    return dict(row) if row is not None else {
        "commit_count": 0,
        "post_create_count": 0,
        "image_post_count": 0,
        "missing_alt_post_count": 0,
        "partial_alt_post_count": 0,
        "gif_post_count": 0,
        "video_post_count": 0,
    }


def get_live_window_counts(conn: sqlite3.Connection, start_minute: datetime, end_minute_exclusive: datetime) -> dict[str, int]:
    start_epoch = minute_epoch(start_minute)
    end_epoch = minute_epoch(end_minute_exclusive)

    row = conn.execute(
        """
        SELECT
            COALESCE(SUM(commit_count), 0) AS commit_count,
            COALESCE(SUM(post_create_count), 0) AS post_create_count,
            COALESCE(SUM(image_post_count), 0) AS image_post_count,
            COALESCE(SUM(missing_alt_post_count), 0) AS missing_alt_post_count,
            COALESCE(SUM(partial_alt_post_count), 0) AS partial_alt_post_count,
            COALESCE(SUM(gif_post_count), 0) AS gif_post_count,
            COALESCE(SUM(video_post_count), 0) AS video_post_count
        FROM firehose_metrics_second
        WHERE ts_epoch >= ? AND ts_epoch < ?
        """,
        (start_epoch, end_epoch),
    ).fetchone()

    return dict(row) if row is not None else {
        "commit_count": 0,
        "post_create_count": 0,
        "image_post_count": 0,
        "missing_alt_post_count": 0,
        "partial_alt_post_count": 0,
        "gif_post_count": 0,
        "video_post_count": 0,
    }


def get_intake_minute_counts(bucket_start: datetime) -> dict[str, int]:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT
                    commit_count,
                    post_create_count,
                    image_post_count,
                    image_eval_count,
                    missing_label_count,
                    partial_label_count
                FROM firehose_minute_stats
                WHERE bucket = :bucket
                """
            ),
            {"bucket": bucket_start},
        ).mappings().first()

    if row is None:
        return {
            "commit_count": 0,
            "post_create_count": 0,
            "image_post_count": 0,
            "image_eval_count": 0,
            "missing_label_count": 0,
            "partial_label_count": 0,
        }

    return dict(row)


def get_intake_window_counts(start_minute: datetime, end_minute_exclusive: datetime) -> dict[str, int]:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT
                    COALESCE(SUM(commit_count), 0) AS commit_count,
                    COALESCE(SUM(post_create_count), 0) AS post_create_count,
                    COALESCE(SUM(image_post_count), 0) AS image_post_count,
                    COALESCE(SUM(image_eval_count), 0) AS image_eval_count,
                    COALESCE(SUM(missing_label_count), 0) AS missing_label_count,
                    COALESCE(SUM(partial_label_count), 0) AS partial_label_count
                FROM firehose_minute_stats
                WHERE bucket >= :start_bucket
                  AND bucket < :end_bucket
                """
            ),
            {
                "start_bucket": start_minute,
                "end_bucket": end_minute_exclusive,
            },
        ).mappings().one()

    return dict(row)


def get_intake_cursor() -> dict[str, Any]:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT
                    stream_name,
                    last_seq,
                    updated_at
                FROM firehose_cursor
                WHERE stream_name = 'subscribe_repos'
                """
            )
        ).mappings().first()

    return dict(row) if row is not None else {}


def get_cursor_snapshot(conn: sqlite3.Connection) -> CursorSnapshot:
    live_state = get_live_monitor_state(conn)
    intake_cursor = get_intake_cursor()

    live_seq = live_state.get("last_seq")
    intake_seq = intake_cursor.get("last_seq")

    gap = None
    if live_seq is not None and intake_seq is not None:
        gap = int(live_seq) - int(intake_seq)

    return CursorSnapshot(
        sampled_at_utc=iso(utc_now()),
        live_monitor_last_seq=live_seq,
        live_monitor_last_message_at=live_state.get("last_message_at_iso"),
        intake_cursor_last_seq=intake_seq,
        intake_cursor_updated_at=iso(intake_cursor.get("updated_at")) if intake_cursor.get("updated_at") else None,
        seq_gap_live_minus_intake=gap,
    )


def build_update_payload(
    *,
    conn: sqlite3.Connection,
    monitor_start_minute: datetime,
    latest_completed_minute: datetime,
    sample_no: int,
) -> dict[str, Any]:
    live_minute = get_live_minute_counts(conn, latest_completed_minute)
    intake_minute = get_intake_minute_counts(latest_completed_minute)

    window_end = latest_completed_minute + timedelta(minutes=1)
    live_window = get_live_window_counts(conn, monitor_start_minute, window_end)
    intake_window = get_intake_window_counts(monitor_start_minute, window_end)

    cursor_snapshot = get_cursor_snapshot(conn)

    return {
        "event": "firehose_vs_intake_update",
        "sample_no": sample_no,
        "generated_at_utc": iso(utc_now()),
        "monitor_window": {
            "start_minute_utc": iso(monitor_start_minute),
            "latest_completed_minute_utc": iso(latest_completed_minute),
            "completed_minutes_included": int((window_end - monitor_start_minute).total_seconds() // 60),
        },
        "latest_completed_minute": {
            "bucket_utc": iso(latest_completed_minute),
            "live": live_minute,
            "intake": intake_minute,
            "ratios_pct": {
                "post_create": ratio(intake_minute["post_create_count"], live_minute["post_create_count"]),
                "image_post": ratio(intake_minute["image_post_count"], live_minute["image_post_count"]),
                "missing_alt": ratio(intake_minute["missing_label_count"], live_minute["missing_alt_post_count"]),
            },
        },
        "cumulative_completed_window": {
            "live": live_window,
            "intake": intake_window,
            "ratios_pct": {
                "post_create": ratio(intake_window["post_create_count"], live_window["post_create_count"]),
                "image_post": ratio(intake_window["image_post_count"], live_window["image_post_count"]),
                "missing_alt": ratio(intake_window["missing_label_count"], live_window["missing_alt_post_count"]),
            },
        },
        "cursor": asdict(cursor_snapshot),
    }


def sleep_until_next_boundary(update_seconds: int) -> None:
    now = time.time()
    next_boundary = math.floor(now / update_seconds) * update_seconds + update_seconds
    sleep_for = max(0.0, next_boundary - now)
    time.sleep(sleep_for)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare live firehose monitor counts against intake-worker counts over a fixed interval."
    )
    parser.add_argument(
        "--live-db-path",
        default="data/firehose_live.sqlite3",
        help="SQLite DB used by the live firehose dashboard collector",
    )
    parser.add_argument(
        "--duration-minutes",
        type=int,
        default=10,
        help="Total monitoring duration",
    )
    parser.add_argument(
        "--update-seconds",
        type=int,
        default=60,
        help="How often to print an update",
    )
    args = parser.parse_args()

    if args.duration_minutes <= 0:
        raise SystemExit("--duration-minutes must be > 0")
    if args.update_seconds <= 0:
        raise SystemExit("--update-seconds must be > 0")

    conn = get_live_sqlite_conn(args.live_db_path)

    started_at = utc_now()
    monitor_start_minute = floor_minute(started_at)

    print(
        json.dumps(
            {
                "event": "firehose_vs_intake_monitor_started",
                "generated_at_utc": iso(started_at),
                "live_db_path": args.live_db_path,
                "duration_minutes": args.duration_minutes,
                "update_seconds": args.update_seconds,
                "monitor_start_minute_utc": iso(monitor_start_minute),
                "note": "Uses completed UTC minute buckets only. In-progress current minute is excluded.",
            },
            ensure_ascii=False,
        ),
        flush=True,
    )

    deadline = started_at + timedelta(minutes=args.duration_minutes)
    sample_no = 0
    gaps: list[int] = []

    while utc_now() < deadline:
        sleep_until_next_boundary(args.update_seconds)

        latest_completed_minute = floor_minute(utc_now()) - timedelta(minutes=1)
        sample_no += 1

        payload = build_update_payload(
            conn=conn,
            monitor_start_minute=monitor_start_minute,
            latest_completed_minute=latest_completed_minute,
            sample_no=sample_no,
        )

        gap = payload["cursor"]["seq_gap_live_minus_intake"]
        if gap is not None:
            gaps.append(int(gap))

        print(json.dumps(payload, ensure_ascii=False, default=str), flush=True)

    final_latest_completed_minute = floor_minute(utc_now()) - timedelta(minutes=1)
    final_payload = build_update_payload(
        conn=conn,
        monitor_start_minute=monitor_start_minute,
        latest_completed_minute=final_latest_completed_minute,
        sample_no=sample_no,
    )

    final_payload["event"] = "firehose_vs_intake_monitor_finished"
    final_payload["monitor_runtime"] = {
        "started_at_utc": iso(started_at),
        "finished_at_utc": iso(utc_now()),
        "duration_minutes_requested": args.duration_minutes,
        "samples_emitted": sample_no,
    }
    final_payload["cursor_gap_summary"] = {
        "min_gap": min(gaps) if gaps else None,
        "max_gap": max(gaps) if gaps else None,
        "last_gap": gaps[-1] if gaps else None,
    }

    print(json.dumps(final_payload, ensure_ascii=False, default=str), flush=True)
    conn.close()


if __name__ == "__main__":
    main()