from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timedelta, timezone
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


def floor_minute(dt: datetime) -> datetime:
    return dt.astimezone(UTC).replace(second=0, microsecond=0)


def minute_epoch(dt: datetime) -> int:
    return int(dt.timestamp())


def seconds_since(value: datetime | None) -> float | None:
    if value is None:
        return None
    return round((utc_now() - value).total_seconds(), 3)


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


def get_live_window_counts(conn: sqlite3.Connection, minutes: int) -> dict[str, int]:
    end_minute = floor_minute(utc_now())
    start_minute = end_minute - timedelta(minutes=minutes)

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
        (minute_epoch(start_minute), minute_epoch(end_minute)),
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collect one intake-vs-head snapshot for the dashboard."
    )
    parser.add_argument(
        "--live-db-path",
        default="data/firehose_live.sqlite3",
    )
    parser.add_argument(
        "--output-path",
        default="metrics/intake_head_timeseries.jsonl",
    )
    args = parser.parse_args()

    conn = get_live_sqlite_conn(args.live_db_path)
    try:
        live_state = get_live_monitor_state(conn)
        latest_completed_minute = floor_minute(utc_now()) - timedelta(minutes=1)
        live_latest_minute = get_live_minute_counts(conn, latest_completed_minute)
        live_window_10m = get_live_window_counts(conn, minutes=10)
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

    payload = {
        "ts": iso(utc_now()),
        "latest_completed_minute_utc": iso(latest_completed_minute),
        "live_monitor_last_seq": live_seq,
        "live_monitor_last_message_at": live_state.get("last_message_at_iso"),
        "intake_cursor_last_seq": intake_seq,
        "intake_cursor_updated_at": iso(cursor_updated_at) if cursor_updated_at else None,
        "post_eval_max_last_seen_seq": post_eval_seq,
        "post_eval_max_evaluated_at": iso(post_eval_updated_at) if post_eval_updated_at else None,
        "seq_gap_live_minus_intake": gap_live_intake,
        "seq_gap_live_minus_post_eval": gap_live_post_eval,
        "seq_gap_post_eval_minus_cursor": gap_post_eval_cursor,
        "cursor_updated_age_seconds": seconds_since(cursor_updated_at),
        "post_eval_updated_age_seconds": seconds_since(post_eval_updated_at),
        "evaluated_rows_2m": int(post_eval_row.get("evaluated_rows_2m") or 0),
        "evaluated_rows_10m": int(post_eval_row.get("evaluated_rows_10m") or 0),
        "labeled_rows_10m": int(post_eval_row.get("labeled_rows_10m") or 0),
        "live_latest_completed_minute": live_latest_minute,
        "live_window_10m": live_window_10m,
    }

    output_path = Path(args.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False))
        fh.write("\n")

    print(json.dumps(payload, ensure_ascii=False, default=str), flush=True)


if __name__ == "__main__":
    main()