from __future__ import annotations

import math
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any


METRIC_COLUMNS = [
    "commit_count",
    "post_create_count",
    "image_post_count",
    "missing_alt_post_count",
    "partial_alt_post_count",
    "gif_post_count",
    "video_post_count",
]


def utc_epoch() -> int:
    return int(time.time())


class LiveMetricsStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS firehose_metrics_second (
                    ts_epoch INTEGER PRIMARY KEY,
                    commit_count INTEGER NOT NULL DEFAULT 0,
                    post_create_count INTEGER NOT NULL DEFAULT 0,
                    image_post_count INTEGER NOT NULL DEFAULT 0,
                    missing_alt_post_count INTEGER NOT NULL DEFAULT 0,
                    partial_alt_post_count INTEGER NOT NULL DEFAULT 0,
                    gif_post_count INTEGER NOT NULL DEFAULT 0,
                    video_post_count INTEGER NOT NULL DEFAULT 0,
                    updated_at INTEGER NOT NULL
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS firehose_monitor_state (
                    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
                    started_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    last_message_at INTEGER,
                    last_seq INTEGER,
                    reconnect_count INTEGER NOT NULL DEFAULT 0,
                    error_count INTEGER NOT NULL DEFAULT 0,
                    current_status TEXT,
                    last_error TEXT
                )
                """
            )

            now = utc_epoch()
            conn.execute(
                """
                INSERT OR IGNORE INTO firehose_monitor_state (
                    singleton_id,
                    started_at,
                    updated_at,
                    last_message_at,
                    last_seq,
                    reconnect_count,
                    error_count,
                    current_status,
                    last_error
                ) VALUES (1, ?, ?, NULL, NULL, 0, 0, 'initialized', NULL)
                """,
                (now, now),
            )
            conn.commit()

    def get_resume_cursor(self) -> int | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT last_seq
                FROM firehose_monitor_state
                WHERE singleton_id = 1
                """
            ).fetchone()
            if row is None:
                return None
            return row["last_seq"]

    def set_status(self, status: str, *, last_error: str | None = None) -> None:
        with self._lock, self._connect() as conn:
            now = utc_epoch()
            conn.execute(
                """
                UPDATE firehose_monitor_state
                SET
                    updated_at = ?,
                    current_status = ?,
                    last_error = ?
                WHERE singleton_id = 1
                """,
                (now, status, last_error),
            )
            conn.commit()

    def mark_reconnect(self, note: str | None = None) -> None:
        with self._lock, self._connect() as conn:
            now = utc_epoch()
            conn.execute(
                """
                UPDATE firehose_monitor_state
                SET
                    updated_at = ?,
                    reconnect_count = reconnect_count + 1,
                    current_status = 'reconnecting',
                    last_error = COALESCE(?, last_error)
                WHERE singleton_id = 1
                """,
                (now, note),
            )
            conn.commit()

    def mark_error(self, error_text: str) -> None:
        with self._lock, self._connect() as conn:
            now = utc_epoch()
            conn.execute(
                """
                UPDATE firehose_monitor_state
                SET
                    updated_at = ?,
                    error_count = error_count + 1,
                    current_status = 'error',
                    last_error = ?
                WHERE singleton_id = 1
                """,
                (now, error_text[:2000]),
            )
            conn.commit()

    def record_counts(
        self,
        *,
        ts_epoch: int,
        counts: dict[str, int],
        last_seq: int | None = None,
    ) -> None:
        payload = {col: int(counts.get(col, 0) or 0) for col in METRIC_COLUMNS}
        now = utc_epoch()

        with self._lock, self._connect() as conn:
            conn.execute(
                f"""
                INSERT INTO firehose_metrics_second (
                    ts_epoch,
                    {", ".join(METRIC_COLUMNS)},
                    updated_at
                ) VALUES (
                    ?,
                    {", ".join("?" for _ in METRIC_COLUMNS)},
                    ?
                )
                ON CONFLICT(ts_epoch) DO UPDATE SET
                    {", ".join(f"{col} = {col} + excluded.{col}" for col in METRIC_COLUMNS)},
                    updated_at = excluded.updated_at
                """,
                [ts_epoch, *[payload[col] for col in METRIC_COLUMNS], now],
            )

            conn.execute(
                """
                UPDATE firehose_monitor_state
                SET
                    updated_at = ?,
                    last_message_at = ?,
                    last_seq = COALESCE(?, last_seq),
                    current_status = 'running',
                    last_error = NULL
                WHERE singleton_id = 1
                """,
                (now, ts_epoch, last_seq),
            )
            conn.commit()

    def get_state(self) -> dict[str, Any]:
        with self._connect() as conn:
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
            return dict(row) if row is not None else {}

    def get_latest_second(self) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT
                    ts_epoch,
                    {", ".join(METRIC_COLUMNS)}
                FROM firehose_metrics_second
                ORDER BY ts_epoch DESC
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                return {"ts_epoch": None, **{col: 0 for col in METRIC_COLUMNS}}
            return dict(row)

    def get_window_summary(self, window_seconds: int) -> dict[str, Any]:
        cutoff = utc_epoch() - window_seconds + 1
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT
                    COUNT(*) AS observed_seconds,
                    {", ".join(f"COALESCE(SUM({col}), 0) AS {col}" for col in METRIC_COLUMNS)}
                FROM firehose_metrics_second
                WHERE ts_epoch >= ?
                """,
                (cutoff,),
            ).fetchone()

        result = dict(row) if row is not None else {"observed_seconds": 0}
        observed_seconds = int(result.get("observed_seconds", 0) or 0)

        for col in METRIC_COLUMNS:
            total = int(result.get(col, 0) or 0)
            result[f"{col}_avg_per_sec"] = round(total / max(window_seconds, 1), 3)
            result[f"{col}_avg_per_observed_sec"] = round(total / max(observed_seconds, 1), 3)

        result["window_seconds"] = window_seconds
        return result

    def get_total_summary(self) -> dict[str, Any]:
        with self._connect() as conn:
            totals = conn.execute(
                f"""
                SELECT
                    COUNT(*) AS stored_seconds,
                    {", ".join(f"COALESCE(SUM({col}), 0) AS {col}" for col in METRIC_COLUMNS)}
                FROM firehose_metrics_second
                """
            ).fetchone()

            state = conn.execute(
                """
                SELECT started_at
                FROM firehose_monitor_state
                WHERE singleton_id = 1
                """
            ).fetchone()

        result = dict(totals) if totals is not None else {"stored_seconds": 0}
        started_at = int(state["started_at"]) if state is not None else utc_epoch()
        uptime_seconds = max(1, utc_epoch() - started_at)

        result["started_at"] = started_at
        result["uptime_seconds"] = uptime_seconds

        for col in METRIC_COLUMNS:
            total = int(result.get(col, 0) or 0)
            result[f"{col}_avg_per_sec"] = round(total / uptime_seconds, 3)

        return result

    def get_series(self, *, range_seconds: int, max_points: int = 720) -> dict[str, Any]:
        now = utc_epoch()
        start_epoch = now - range_seconds + 1
        bucket_seconds = max(1, math.ceil(range_seconds / max_points))

        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT
                    ((ts_epoch / ?) * ?) AS bucket_epoch,
                    {", ".join(f"COALESCE(SUM({col}), 0) AS {col}" for col in METRIC_COLUMNS)}
                FROM firehose_metrics_second
                WHERE ts_epoch >= ?
                GROUP BY bucket_epoch
                ORDER BY bucket_epoch ASC
                """,
                (bucket_seconds, bucket_seconds, start_epoch),
            ).fetchall()

        by_bucket = {int(row["bucket_epoch"]): dict(row) for row in rows}

        first_bucket = (start_epoch // bucket_seconds) * bucket_seconds
        last_bucket = (now // bucket_seconds) * bucket_seconds

        points: list[dict[str, Any]] = []
        bucket = first_bucket
        while bucket <= last_bucket:
            raw = by_bucket.get(bucket, {"bucket_epoch": bucket})
            point = {
                "bucket_epoch": bucket,
                "bucket_seconds": bucket_seconds,
            }

            for col in METRIC_COLUMNS:
                total = int(raw.get(col, 0) or 0)
                point[col] = total
                point[f"{col}_rate"] = round(total / bucket_seconds, 4)

            points.append(point)
            bucket += bucket_seconds

        return {
            "range_seconds": range_seconds,
            "bucket_seconds": bucket_seconds,
            "points": points,
        }