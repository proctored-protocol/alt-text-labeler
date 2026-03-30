from __future__ import annotations

import argparse
import json
import math
import sqlite3
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
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


def seconds_between(newer: datetime | None, older: datetime | None) -> float | None:
    if newer is None or older is None:
        return None
    return round((newer - older).total_seconds(), 3)


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


def get_post_evaluation_state() -> dict[str, Any]:
    with engine.connect() as conn:
        row = conn.execute(
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

    return dict(row)


def get_latest_intake_stats_bucket() -> dict[str, Any]:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT
                    bucket,
                    commit_count,
                    post_create_count,
                    image_post_count,
                    image_eval_count,
                    missing_label_count,
                    partial_label_count
                FROM firehose_minute_stats
                ORDER BY bucket DESC
                LIMIT 1
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
    initial_cursor: CursorSnapshot,
    initial_post_eval_max_seq: int | None,
) -> dict[str, Any]:
    live_minute = get_live_minute_counts(conn, latest_completed_minute)
    live_window = get_live_window_counts(
        conn,
        monitor_start_minute,
        latest_completed_minute + timedelta(minutes=1),
    )

    cursor_snapshot = get_cursor_snapshot(conn)
    post_eval_state = get_post_evaluation_state()
    intake_stats_latest_bucket = get_latest_intake_stats_bucket()

    current_live_seq = cursor_snapshot.live_monitor_last_seq
    current_intake_seq = cursor_snapshot.intake_cursor_last_seq
    current_gap = cursor_snapshot.seq_gap_live_minus_intake
    current_post_eval_seq = post_eval_state.get("max_last_seen_seq")

    live_seq_delta_since_start = None
    if initial_cursor.live_monitor_last_seq is not None and current_live_seq is not None:
        live_seq_delta_since_start = int(current_live_seq) - int(initial_cursor.live_monitor_last_seq)

    intake_cursor_delta_since_start = None
    if initial_cursor.intake_cursor_last_seq is not None and current_intake_seq is not None:
        intake_cursor_delta_since_start = int(current_intake_seq) - int(initial_cursor.intake_cursor_last_seq)

    post_eval_seq_delta_since_start = None
    if initial_post_eval_max_seq is not None and current_post_eval_seq is not None:
        post_eval_seq_delta_since_start = int(current_post_eval_seq) - int(initial_post_eval_max_seq)

    seq_gap_change_since_start = None
    if initial_cursor.seq_gap_live_minus_intake is not None and current_gap is not None:
        seq_gap_change_since_start = int(current_gap) - int(initial_cursor.seq_gap_live_minus_intake)

    seq_gap_live_minus_post_eval = None
    if current_live_seq is not None and current_post_eval_seq is not None:
        seq_gap_live_minus_post_eval = int(current_live_seq) - int(current_post_eval_seq)

    seq_gap_post_eval_minus_cursor = None
    if current_post_eval_seq is not None and current_intake_seq is not None:
        seq_gap_post_eval_minus_cursor = int(current_post_eval_seq) - int(current_intake_seq)

    cursor_updated_at_dt = None
    if cursor_snapshot.intake_cursor_updated_at is not None:
        cursor_updated_at_dt = datetime.fromisoformat(cursor_snapshot.intake_cursor_updated_at)

    max_evaluated_at_dt = None
    if post_eval_state.get("max_evaluated_at") is not None:
        max_evaluated_at_dt = post_eval_state["max_evaluated_at"]

    return {
        "event": "firehose_vs_intake_update",
        "sample_no": sample_no,
        "generated_at_utc": iso(utc_now()),
        "monitor_window": {
            "start_minute_utc": iso(monitor_start_minute),
            "latest_completed_minute_utc": iso(latest_completed_minute),
            "completed_minutes_included": int(
                ((latest_completed_minute + timedelta(minutes=1)) - monitor_start_minute).total_seconds() // 60
            ),
        },
        "live": {
            "latest_completed_minute": {
                "bucket_utc": iso(latest_completed_minute),
                "counts": live_minute,
            },
            "cumulative_completed_window": live_window,
        },
        "intake_cursor": asdict(cursor_snapshot),
        "post_evaluation": {
            "evaluated_rows_2m": post_eval_state.get("evaluated_rows_2m"),
            "evaluated_rows_10m": post_eval_state.get("evaluated_rows_10m"),
            "labeled_rows_10m": post_eval_state.get("labeled_rows_10m"),
            "max_last_seen_seq": current_post_eval_seq,
            "max_evaluated_at": iso(max_evaluated_at_dt) if isinstance(max_evaluated_at_dt, datetime) else None,
            "seq_gap_live_minus_post_eval": seq_gap_live_minus_post_eval,
            "seq_gap_post_eval_minus_cursor": seq_gap_post_eval_minus_cursor,
        },
        "intake_stats_latest_bucket": {
            "note": "Flush-bucketed only; useful for activity, not live apples-to-apples comparison.",
            **({
                "bucket_utc": iso(intake_stats_latest_bucket.get("bucket")) if intake_stats_latest_bucket.get("bucket") else None,
                "commit_count": intake_stats_latest_bucket.get("commit_count"),
                "post_create_count": intake_stats_latest_bucket.get("post_create_count"),
                "image_post_count": intake_stats_latest_bucket.get("image_post_count"),
                "image_eval_count": intake_stats_latest_bucket.get("image_eval_count"),
                "missing_label_count": intake_stats_latest_bucket.get("missing_label_count"),
                "partial_label_count": intake_stats_latest_bucket.get("partial_label_count"),
            } if intake_stats_latest_bucket else {}),
        },
        "diagnostics": {
            "live_seq_delta_since_start": live_seq_delta_since_start,
            "intake_cursor_delta_since_start": intake_cursor_delta_since_start,
            "post_eval_seq_delta_since_start": post_eval_seq_delta_since_start,
            "seq_gap_change_since_start": seq_gap_change_since_start,
            "cursor_updated_age_seconds": seconds_between(utc_now(), cursor_updated_at_dt),
            "post_eval_updated_age_seconds": seconds_between(utc_now(), max_evaluated_at_dt) if isinstance(max_evaluated_at_dt, datetime) else None,
        },
    }


def sleep_until_next_boundary(update_seconds: int) -> None:
    now = time.time()
    next_boundary = math.floor(now / update_seconds) * update_seconds + update_seconds
    sleep_for = max(0.0, next_boundary - now)
    time.sleep(sleep_for)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare live firehose head movement against intake cursor and post_evaluation movement."
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

    initial_cursor = get_cursor_snapshot(conn)
    initial_post_eval_state = get_post_evaluation_state()
    initial_post_eval_max_seq = initial_post_eval_state.get("max_last_seen_seq")

    print(
        json.dumps(
            {
                "event": "firehose_vs_intake_monitor_started",
                "generated_at_utc": iso(started_at),
                "live_db_path": args.live_db_path,
                "duration_minutes": args.duration_minutes,
                "update_seconds": args.update_seconds,
                "monitor_start_minute_utc": iso(monitor_start_minute),
                "note": "Focus on live head, intake cursor, and post_evaluation movement. firehose_minute_stats is flush-bucketed only.",
                "initial_cursor": asdict(initial_cursor),
                "initial_post_evaluation": {
                    "max_last_seen_seq": initial_post_eval_state.get("max_last_seen_seq"),
                    "max_evaluated_at": iso(initial_post_eval_state.get("max_evaluated_at")) if initial_post_eval_state.get("max_evaluated_at") else None,
                    "evaluated_rows_2m": initial_post_eval_state.get("evaluated_rows_2m"),
                    "evaluated_rows_10m": initial_post_eval_state.get("evaluated_rows_10m"),
                    "labeled_rows_10m": initial_post_eval_state.get("labeled_rows_10m"),
                },
            },
            ensure_ascii=False,
            default=str,
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
            initial_cursor=initial_cursor,
            initial_post_eval_max_seq=initial_post_eval_max_seq,
        )

        gap = payload["intake_cursor"]["seq_gap_live_minus_intake"]
        if gap is not None:
            gaps.append(int(gap))

        print(json.dumps(payload, ensure_ascii=False, default=str), flush=True)

    final_latest_completed_minute = floor_minute(utc_now()) - timedelta(minutes=1)
    final_payload = build_update_payload(
        conn=conn,
        monitor_start_minute=monitor_start_minute,
        latest_completed_minute=final_latest_completed_minute,
        sample_no=sample_no,
        initial_cursor=initial_cursor,
        initial_post_eval_max_seq=initial_post_eval_max_seq,
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