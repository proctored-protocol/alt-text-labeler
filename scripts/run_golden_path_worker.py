#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

from sqlalchemy import create_engine, text


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return now_utc().isoformat()


def log_event(event: str, **fields: Any) -> None:
    payload = {"event": event, "checked_at": now_iso()}
    payload.update(fields)
    print(json.dumps(payload, ensure_ascii=False), flush=True)


@dataclass
class Candidate:
    uri: str
    cid: str
    author_did: str
    label_value: str
    record_created_at: str | None
    evaluated_at: str | None

    @property
    def post_url(self) -> str:
        rkey = self.uri.rsplit("/", 1)[-1]
        profile_token = self.author_did
        return f"https://bsky.app/profile/{profile_token}/post/{rkey}"


def ensure_sqlite(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        create table if not exists run_meta (
            key text primary key,
            value text not null
        )
        """
    )
    conn.execute(
        """
        create table if not exists processed_posts (
            uri text primary key,
            post_url text not null,
            cid text,
            label_value text,
            record_created_at text,
            evaluated_at text,
            ozone_event_id integer,
            ozone_created_at text,
            first_forced_true_at text,
            first_query_true_at text,
            final_forced_found_label integer,
            final_query_found_label integer,
            final_subscriber_found_label integer,
            processing_error text,
            retry_count integer not null default 0,
            success integer not null default 0,
            candidate_error integer not null default 0,
            processed_at text not null
        )
        """
    )
    conn.execute(
        """
        create table if not exists worker_events (
            id integer primary key autoincrement,
            event_type text not null,
            payload_json text not null,
            created_at text not null
        )
        """
    )
    conn.commit()


def sqlite_seen(conn: sqlite3.Connection, uri: str) -> bool:
    row = conn.execute("select 1 from processed_posts where uri = ?", (uri,)).fetchone()
    return row is not None


def sqlite_mark_result(
    conn: sqlite3.Connection,
    *,
    candidate: Candidate,
    ozone_event_id: int | None,
    ozone_created_at: str | None,
    first_forced_true_at: str | None,
    first_query_true_at: str | None,
    final_forced_found_label: bool,
    final_query_found_label: bool,
    final_subscriber_found_label: bool,
    processing_error: str | None,
    retry_count: int,
    success: bool,
    candidate_error: bool,
) -> None:
    conn.execute(
        """
        insert into processed_posts (
            uri, post_url, cid, label_value, record_created_at, evaluated_at,
            ozone_event_id, ozone_created_at,
            first_forced_true_at, first_query_true_at,
            final_forced_found_label, final_query_found_label, final_subscriber_found_label,
            processing_error, retry_count, success, candidate_error, processed_at
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        on conflict(uri) do update set
            post_url=excluded.post_url,
            cid=excluded.cid,
            label_value=excluded.label_value,
            record_created_at=excluded.record_created_at,
            evaluated_at=excluded.evaluated_at,
            ozone_event_id=excluded.ozone_event_id,
            ozone_created_at=excluded.ozone_created_at,
            first_forced_true_at=excluded.first_forced_true_at,
            first_query_true_at=excluded.first_query_true_at,
            final_forced_found_label=excluded.final_forced_found_label,
            final_query_found_label=excluded.final_query_found_label,
            final_subscriber_found_label=excluded.final_subscriber_found_label,
            processing_error=excluded.processing_error,
            retry_count=excluded.retry_count,
            success=excluded.success,
            candidate_error=excluded.candidate_error,
            processed_at=excluded.processed_at
        """,
        (
            candidate.uri,
            candidate.post_url,
            candidate.cid,
            candidate.label_value,
            candidate.record_created_at,
            candidate.evaluated_at,
            ozone_event_id,
            ozone_created_at,
            first_forced_true_at,
            first_query_true_at,
            int(final_forced_found_label),
            int(final_query_found_label),
            int(final_subscriber_found_label),
            processing_error,
            retry_count,
            int(success),
            int(candidate_error),
            now_iso(),
        ),
    )
    conn.commit()


def sqlite_log_worker_event(conn: sqlite3.Connection, event_type: str, payload: dict[str, Any]) -> None:
    conn.execute(
        "insert into worker_events (event_type, payload_json, created_at) values (?, ?, ?)",
        (event_type, json.dumps(payload, ensure_ascii=False), now_iso()),
    )
    conn.commit()


def parse_json_block_after_header(output: str, header: str) -> Any | None:
    marker = f"{header}:\n"
    idx = output.find(marker)
    if idx == -1:
        return None
    start = idx + len(marker)
    tail = output[start:].lstrip()
    if not tail:
        return None

    opening = tail[0]
    if opening not in "{[":
        return None

    closing = "}" if opening == "{" else "]"
    depth = 0
    in_string = False
    escape = False
    collected = []

    for ch in tail:
        collected.append(ch)
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
        elif ch == opening:
            depth += 1
        elif ch == closing:
            depth -= 1
            if depth == 0:
                break

    try:
        return json.loads("".join(collected))
    except json.JSONDecodeError:
        return None


def classify_error(stderr: str) -> tuple[str, bool]:
    lower = stderr.lower()
    if "unable to resolve handle" in lower:
        return "candidate_invalid_handle", True
    if "getposts returned no posts" in lower:
        return "candidate_post_not_fetchable", True
    if "invalidrequest" in lower and "resolve handle" in lower:
        return "candidate_invalid_handle", True
    if "post not found" in lower:
        return "candidate_post_not_found", True
    if "expiredtoken" in lower:
        return "auth_expired_token", False
    if "authmissing" in lower:
        return "auth_missing", False
    return "subprocess_failure", False


def parse_manual_publish_output(stdout_text: str, stderr_text: str, returncode: int) -> dict[str, Any]:
    if returncode != 0:
        error_class, candidate_error = classify_error(stderr_text)
        return {
            "success": False,
            "candidate_error": candidate_error,
            "error_class": error_class,
            "error_text": stderr_text.strip() or stdout_text.strip() or f"subprocess exited {returncode}",
            "ozone_event_id": None,
            "ozone_created_at": None,
            "first_forced_true_at": None,
            "first_query_true_at": None,
            "final_forced_found_label": False,
            "final_query_found_label": False,
            "final_subscriber_found_label": False,
        }

    ozone_response = parse_json_block_after_header(stdout_text, "ozone_response") or {}
    attempts = parse_json_block_after_header(stdout_text, "verification_attempts") or []

    first_forced_true_at = None
    first_query_true_at = None
    final_forced = False
    final_query = False
    final_subscriber = False

    for attempt in attempts:
        checked_at = attempt.get("checked_at")
        summary = attempt.get("summary", {})
        forced = summary.get("forced_hydration", {})
        query = summary.get("query_labels", {})
        subscriber = summary.get("subscriber_hydration", {})

        forced_found = bool(forced.get("found_label"))
        query_found = bool(query.get("found_label"))
        subscriber_found = bool(subscriber.get("found_label"))

        if forced_found and first_forced_true_at is None:
            first_forced_true_at = checked_at
        if query_found and first_query_true_at is None:
            first_query_true_at = checked_at

        final_forced = forced_found
        final_query = query_found
        final_subscriber = subscriber_found

    success = bool(first_forced_true_at or first_query_true_at)

    return {
        "success": success,
        "candidate_error": False,
        "error_class": None,
        "error_text": None,
        "ozone_event_id": ozone_response.get("id"),
        "ozone_created_at": ozone_response.get("createdAt"),
        "first_forced_true_at": first_forced_true_at,
        "first_query_true_at": first_query_true_at,
        "final_forced_found_label": final_forced,
        "final_query_found_label": final_query,
        "final_subscriber_found_label": final_subscriber,
    }


def start_intake(repo_root: Path, run_dir: Path, python_bin: str) -> subprocess.Popen[str]:
    intake_log_path = run_dir / "intake.log"
    handle = open(intake_log_path, "a", encoding="utf-8")
    proc = subprocess.Popen(
        [python_bin, "scripts/run_intake_worker.py"],
        cwd=repo_root,
        stdout=handle,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return proc


def run_subprocess(repo_root: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=repo_root,
        capture_output=True,
        text=True,
    )


def reset_labeler(repo_root: Path, python_bin: str) -> dict[str, Any]:
    completed = run_subprocess(repo_root, [python_bin, "scripts/delete_and_recreate_labeler_record.py"])
    if completed.returncode != 0:
        raise RuntimeError(
            "delete_and_recreate_labeler_record.py failed\n"
            f"STDOUT:\n{completed.stdout}\n\nSTDERR:\n{completed.stderr}"
        )
    lines = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
    for line in reversed(lines):
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    return {"raw_stdout": completed.stdout}


def make_engine(database_url: str):
    return create_engine(database_url, pool_pre_ping=True, future=True)


def fetch_candidate(engine, conn_sqlite: sqlite3.Connection, label_values: list[str]) -> Candidate | None:
    placeholders = ", ".join(f":label_{i}" for i in range(len(label_values)))
    params = {f"label_{i}": v for i, v in enumerate(label_values)}

    sql = text(
        f"""
        select
            pe.uri,
            pe.cid,
            pe.author_did,
            pe.derived_label,
            cast(pe.record_created_at as text) as record_created_at,
            cast(pe.evaluated_at as text) as evaluated_at
        from post_evaluation pe
        where pe.derived_label in ({placeholders})
        order by pe.evaluated_at asc
        limit 200
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(sql, params).mappings().all()

    for row in rows:
        uri = row["uri"]
        if sqlite_seen(conn_sqlite, uri):
            continue
        return Candidate(
            uri=uri,
            cid=row["cid"],
            author_did=row["author_did"],
            label_value=row["derived_label"],
            record_created_at=row["record_created_at"],
            evaluated_at=row["evaluated_at"],
        )
    return None


def rolling_success_rate(conn: sqlite3.Connection, window_size: int) -> tuple[int, int, float]:
    rows = conn.execute(
        """
        select success, candidate_error
        from processed_posts
        order by processed_at desc
        limit ?
        """,
        (window_size,),
    ).fetchall()

    relevant = [(bool(r[0]), bool(r[1])) for r in rows if not bool(r[1])]
    n = len(relevant)
    if n == 0:
        return 0, 0, 1.0
    ok = sum(1 for success, _candidate_error in relevant if success)
    return ok, n, ok / n


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration-minutes", type=int, default=60)
    parser.add_argument("--candidate-interval-seconds", type=int, default=10)
    parser.add_argument("--manual-verify-timeout-seconds", type=int, default=10)
    parser.add_argument("--manual-verify-interval-seconds", type=int, default=1)
    parser.add_argument("--request-timeout-seconds", type=int, default=20)
    parser.add_argument("--rolling-window-size", type=int, default=50)
    parser.add_argument("--rolling-success-threshold", type=float, default=0.95)
    parser.add_argument("--reset-cooldown-minutes", type=int, default=10)
    parser.add_argument("--max-retries-per-candidate", type=int, default=1)
    parser.add_argument(
        "--label-values",
        nargs="+",
        default=["missing-alt-text", "partial-alt-text"],
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    python_bin = sys.executable
    database_url = os.environ["DATABASE_URL"]

    started_at = now_utc()
    run_dir = repo_root / "golden_path_runs" / f"golden_path_{started_at.strftime('%Y%m%dT%H%M%S.%f%z')}"
    run_dir.mkdir(parents=True, exist_ok=True)
    sqlite_path = run_dir / "run.sqlite3"

    conn_sqlite = sqlite3.connect(sqlite_path)
    ensure_sqlite(conn_sqlite)

    engine = make_engine(database_url)

    intake_proc: subprocess.Popen[str] | None = None
    stop_requested = False
    last_reset_at: datetime | None = None
    intake_restart_count = 0

    def handle_signal(signum: int, _frame: Any) -> None:
        nonlocal stop_requested
        stop_requested = True
        log_event("stop_requested", signal=signum)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    log_event(
        "golden_path_starting",
        run_dir=str(run_dir),
        sqlite_path=str(sqlite_path),
    )

    reset_payload = reset_labeler(repo_root, python_bin)
    last_reset_at = now_utc()
    sqlite_log_worker_event(conn_sqlite, "labeler_reset", reset_payload)
    log_event("labeler_record_reset_complete", reset=reset_payload)

    intake_proc = start_intake(repo_root, run_dir, python_bin)
    log_event("intake_started", pid=intake_proc.pid)

    deadline = time.time() + args.duration_minutes * 60

    try:
        while time.time() < deadline and not stop_requested:
            if intake_proc is not None and intake_proc.poll() is not None:
                log_event("intake_exited", returncode=intake_proc.returncode)
                sqlite_log_worker_event(
                    conn_sqlite,
                    "intake_exited",
                    {"returncode": intake_proc.returncode},
                )
                intake_restart_count += 1
                intake_proc = start_intake(repo_root, run_dir, python_bin)
                log_event("intake_restarted", pid=intake_proc.pid, restart_count=intake_restart_count)

            candidate = fetch_candidate(engine, conn_sqlite, args.label_values)
            if candidate is None:
                time.sleep(2)
                continue

            log_event(
                "candidate_selected",
                post_url=candidate.post_url,
                post_uri=candidate.uri,
                post_cid=candidate.cid,
                label_value=candidate.label_value,
                record_created_at=candidate.record_created_at,
                evaluated_at=candidate.evaluated_at,
            )

            retry_count = 0
            parsed_result: dict[str, Any] | None = None

            while True:
                completed = run_subprocess(
                    repo_root,
                    [
                        python_bin,
                        "scripts/manual_publish_and_verify.py",
                        "--post-url",
                        candidate.post_url,
                        "--label-value",
                        candidate.label_value,
                        "--verify-timeout-seconds",
                        str(args.manual_verify_timeout_seconds),
                        "--verify-interval-seconds",
                        str(args.manual_verify_interval_seconds),
                        "--request-timeout-seconds",
                        str(args.request_timeout_seconds),
                    ],
                )
                parsed_result = parse_manual_publish_output(completed.stdout, completed.stderr, completed.returncode)

                if parsed_result["success"]:
                    break

                if retry_count >= args.max_retries_per_candidate:
                    break

                retryable = not parsed_result["candidate_error"]
                if not retryable:
                    break

                retry_count += 1
                log_event(
                    "candidate_retrying",
                    post_url=candidate.post_url,
                    post_uri=candidate.uri,
                    retry_count=retry_count,
                    error_class=parsed_result["error_class"],
                    error_text=parsed_result["error_text"],
                )
                time.sleep(1)

            assert parsed_result is not None

            sqlite_mark_result(
                conn_sqlite,
                candidate=candidate,
                ozone_event_id=parsed_result["ozone_event_id"],
                ozone_created_at=parsed_result["ozone_created_at"],
                first_forced_true_at=parsed_result["first_forced_true_at"],
                first_query_true_at=parsed_result["first_query_true_at"],
                final_forced_found_label=parsed_result["final_forced_found_label"],
                final_query_found_label=parsed_result["final_query_found_label"],
                final_subscriber_found_label=parsed_result["final_subscriber_found_label"],
                processing_error=parsed_result["error_text"],
                retry_count=retry_count,
                success=parsed_result["success"],
                candidate_error=parsed_result["candidate_error"],
            )

            log_event(
                "candidate_processed",
                post_url=candidate.post_url,
                post_uri=candidate.uri,
                post_cid=candidate.cid,
                label_value=candidate.label_value,
                record_created_at=candidate.record_created_at,
                ozone_event_id=parsed_result["ozone_event_id"],
                ozone_created_at=parsed_result["ozone_created_at"],
                first_forced_true_at=parsed_result["first_forced_true_at"],
                first_query_true_at=parsed_result["first_query_true_at"],
                final_forced_found_label=parsed_result["final_forced_found_label"],
                final_query_found_label=parsed_result["final_query_found_label"],
                final_subscriber_found_label=parsed_result["final_subscriber_found_label"],
                success=parsed_result["success"],
                candidate_error=parsed_result["candidate_error"],
                retry_count=retry_count,
                error_class=parsed_result["error_class"],
                error_text=parsed_result["error_text"],
            )

            ok, n, rate = rolling_success_rate(conn_sqlite, args.rolling_window_size)
            log_event(
                "rolling_window",
                window_size=args.rolling_window_size,
                ok=ok,
                n=n,
                success_rate=round(rate, 4),
            )

            should_reset = (
                n >= max(10, args.rolling_window_size // 2)
                and rate < args.rolling_success_threshold
                and (
                    last_reset_at is None
                    or (now_utc() - last_reset_at).total_seconds() >= args.reset_cooldown_minutes * 60
                )
            )
            if should_reset:
                log_event(
                    "rolling_window_below_threshold",
                    window_size=args.rolling_window_size,
                    success_rate=round(rate, 4),
                    threshold=args.rolling_success_threshold,
                )
                reset_payload = reset_labeler(repo_root, python_bin)
                last_reset_at = now_utc()
                sqlite_log_worker_event(conn_sqlite, "labeler_reset", reset_payload)
                log_event("labeler_record_reset_complete", reset=reset_payload)

            time.sleep(args.candidate_interval_seconds)

    finally:
        if intake_proc is not None and intake_proc.poll() is None:
            intake_proc.terminate()
            try:
                intake_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                intake_proc.kill()

        log_event(
            "golden_path_finished",
            run_dir=str(run_dir),
            sqlite_path=str(sqlite_path),
        )


if __name__ == "__main__":
    main()