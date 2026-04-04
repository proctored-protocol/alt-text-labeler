from __future__ import annotations

import argparse
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import text

from app.db import engine


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


@dataclass
class Candidate:
    uri: str
    cid: str
    label_value: str
    record_created_at: str | None
    evaluated_at: str | None
    author_did: str | None
    image_count: int | None
    usable_alt_count: int | None


def print_json_line(data: dict[str, Any]) -> None:
    print(json.dumps(data, ensure_ascii=False, default=str), flush=True)


def ensure_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS experiment_posts (
            uri TEXT PRIMARY KEY,
            cid TEXT,
            post_url TEXT,
            label_value TEXT,
            record_created_at TEXT,
            evaluated_at TEXT,
            ozone_event_id INTEGER,
            ozone_created_at TEXT,
            first_forced_true_at TEXT,
            first_query_true_at TEXT,
            final_forced_status_code INTEGER,
            final_query_status_code INTEGER,
            final_subscriber_status_code INTEGER,
            final_forced_found_label INTEGER,
            final_query_found_label INTEGER,
            final_subscriber_found_label INTEGER,
            manual_success INTEGER,
            processing_error TEXT,
            raw_result_json TEXT,
            processed_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS experiment_meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )
    conn.commit()


def meta_set(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        """
        INSERT INTO experiment_meta (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, value),
    )
    conn.commit()


def already_processed(conn: sqlite3.Connection, uri: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM experiment_posts WHERE uri = ?",
        (uri,),
    ).fetchone()
    return row is not None


def save_success_result(
    conn: sqlite3.Connection,
    *,
    candidate: Candidate,
    post_url: str,
    result: dict[str, Any],
) -> None:
    attempts = result.get("verification_attempts") or []

    first_forced_true_at = None
    first_query_true_at = None
    final_forced_status_code = None
    final_query_status_code = None
    final_subscriber_status_code = None
    final_forced_found_label = None
    final_query_found_label = None
    final_subscriber_found_label = None

    for attempt in attempts:
        checked_at = attempt.get("checked_at")
        summary = attempt.get("summary") or {}

        q = summary.get("query_labels") or {}
        f = summary.get("forced_hydration") or {}
        s = summary.get("subscriber_hydration") or {}

        if first_query_true_at is None and q.get("found_label") is True:
            first_query_true_at = checked_at
        if first_forced_true_at is None and f.get("found_label") is True:
            first_forced_true_at = checked_at

    if attempts:
        last = attempts[-1]
        summary = last.get("summary") or {}
        q = summary.get("query_labels") or {}
        f = summary.get("forced_hydration") or {}
        s = summary.get("subscriber_hydration") or {}

        final_query_status_code = q.get("status_code")
        final_forced_status_code = f.get("status_code")
        final_subscriber_status_code = s.get("status_code")

        final_query_found_label = 1 if q.get("found_label") is True else 0
        final_forced_found_label = 1 if f.get("found_label") is True else 0
        final_subscriber_found_label = 1 if s.get("found_label") is True else 0

    ozone_response = result.get("ozone_response") or {}
    ozone_event_id = ozone_response.get("id")
    ozone_created_at = ozone_response.get("createdAt")

    conn.execute(
        """
        INSERT OR REPLACE INTO experiment_posts (
            uri,
            cid,
            post_url,
            label_value,
            record_created_at,
            evaluated_at,
            ozone_event_id,
            ozone_created_at,
            first_forced_true_at,
            first_query_true_at,
            final_forced_status_code,
            final_query_status_code,
            final_subscriber_status_code,
            final_forced_found_label,
            final_query_found_label,
            final_subscriber_found_label,
            manual_success,
            processing_error,
            raw_result_json,
            processed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            candidate.uri,
            candidate.cid,
            post_url,
            candidate.label_value,
            candidate.record_created_at,
            candidate.evaluated_at,
            ozone_event_id,
            ozone_created_at,
            first_forced_true_at,
            first_query_true_at,
            final_forced_status_code,
            final_query_status_code,
            final_subscriber_status_code,
            final_forced_found_label,
            final_query_found_label,
            final_subscriber_found_label,
            1 if result.get("success") is True else 0,
            None,
            json.dumps(result, ensure_ascii=False, default=str),
            iso_now(),
        ),
    )
    conn.commit()


def save_error_result(
    conn: sqlite3.Connection,
    *,
    candidate: Candidate,
    post_url: str,
    error_text: str,
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO experiment_posts (
            uri,
            cid,
            post_url,
            label_value,
            record_created_at,
            evaluated_at,
            ozone_event_id,
            ozone_created_at,
            first_forced_true_at,
            first_query_true_at,
            final_forced_status_code,
            final_query_status_code,
            final_subscriber_status_code,
            final_forced_found_label,
            final_query_found_label,
            final_subscriber_found_label,
            manual_success,
            processing_error,
            raw_result_json,
            processed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            candidate.uri,
            candidate.cid,
            post_url,
            candidate.label_value,
            candidate.record_created_at,
            candidate.evaluated_at,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            0,
            error_text,
            None,
            iso_now(),
        ),
    )
    conn.commit()


def uri_to_post_url(uri: str) -> str:
    prefix = "at://"
    if not uri.startswith(prefix):
        raise ValueError(f"Unexpected AT URI: {uri}")

    rest = uri[len(prefix):]
    parts = rest.split("/")
    if len(parts) != 3:
        raise ValueError(f"Unexpected AT URI shape: {uri}")

    did, collection, rkey = parts
    if collection != "app.bsky.feed.post":
        raise ValueError(f"Unexpected collection in URI: {uri}")

    handle = resolve_did_to_handle(did)
    return f"https://bsky.app/profile/{handle}/post/{rkey}"


def resolve_did_to_handle(did: str) -> str:
    import urllib.parse
    import urllib.request

    url = (
        "https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile?"
        + urllib.parse.urlencode({"actor": did})
    )
    req = urllib.request.Request(url, headers={"Accept": "application/json"}, method="GET")
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    handle = payload.get("handle")
    if not handle:
        raise RuntimeError(f"Could not resolve DID to handle: {did}")
    return handle


def fetch_next_candidate(
    *,
    experiment_started_at: str,
    conn: sqlite3.Connection,
) -> Candidate | None:
    with engine.connect() as db_conn:
        rows = db_conn.execute(
            text(
                """
                SELECT
                    uri,
                    cid,
                    derived_label,
                    record_created_at,
                    evaluated_at,
                    author_did,
                    image_count,
                    usable_alt_count
                FROM post_evaluation
                WHERE evaluated_at >= :experiment_started_at
                  AND derived_label IN ('missing-alt-text', 'partial-alt-text')
                ORDER BY evaluated_at ASC
                LIMIT 500
                """
            ),
            {"experiment_started_at": experiment_started_at},
        ).mappings().all()

    for row in rows:
        uri = row["uri"]
        if already_processed(conn, uri):
            continue
        return Candidate(
            uri=row["uri"],
            cid=row["cid"],
            label_value=row["derived_label"],
            record_created_at=str(row["record_created_at"]) if row["record_created_at"] is not None else None,
            evaluated_at=str(row["evaluated_at"]) if row["evaluated_at"] is not None else None,
            author_did=row.get("author_did"),
            image_count=row.get("image_count"),
            usable_alt_count=row.get("usable_alt_count"),
        )
    return None


def run_subprocess_json(
    cmd: list[str],
    *,
    timeout_seconds: int,
) -> dict[str, Any]:
    completed = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )

    stdout = completed.stdout.strip()
    stderr = completed.stderr.strip()

    if completed.returncode not in (0, 2):
        raise RuntimeError(
            f"Subprocess failed with return code {completed.returncode}\n"
            f"STDOUT:\n{stdout}\n\nSTDERR:\n{stderr}"
        )

    if not stdout:
        raise RuntimeError(
            f"Subprocess produced no stdout\nSTDERR:\n{stderr}"
        )

    try:
        return json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Failed to parse JSON from subprocess stdout\nSTDOUT:\n{stdout}\n\nSTDERR:\n{stderr}"
        ) from exc


def reset_labeler_service_record(*, repo_root: Path, python_bin: str) -> dict[str, Any]:
    cmd = [
        python_bin,
        str(repo_root / "scripts" / "delete_and_recreate_labeler_record.py"),
    ]
    completed = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=120,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "delete_and_recreate_labeler_record.py failed\n"
            f"STDOUT:\n{completed.stdout}\n\nSTDERR:\n{completed.stderr}"
        )
    return {
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def start_intake_worker(*, repo_root: Path, python_bin: str, log_path: Path) -> subprocess.Popen:
    log_file = open(log_path, "a", encoding="utf-8")
    cmd = [
        python_bin,
        str(repo_root / "scripts" / "run_intake_worker.py"),
    ]
    return subprocess.Popen(
        cmd,
        cwd=str(repo_root),
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )


def stop_process(proc: subprocess.Popen | None) -> None:
    if proc is None:
        return
    if proc.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except ProcessLookupError:
        return


def main() -> None:
    parser = argparse.ArgumentParser(
        description="40-minute labeler validity experiment: reset labeler record, run intake only, publish via existing manual script."
    )
    parser.add_argument("--duration-minutes", type=int, default=40)
    parser.add_argument("--candidate-interval-seconds", type=int, default=10)
    parser.add_argument("--manual-verify-timeout-seconds", type=int, default=10)
    parser.add_argument("--manual-verify-interval-seconds", type=int, default=1)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--output-dir", default="experiment_runs")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    python_bin = sys.executable

    output_dir = repo_root / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    run_started_at = iso_now().replace(":", "").replace("-", "")
    run_dir = output_dir / f"labeler_validity_{run_started_at}"
    run_dir.mkdir(parents=True, exist_ok=True)

    sqlite_path = run_dir / "experiment.sqlite3"
    intake_log_path = run_dir / "intake.log"

    conn = sqlite3.connect(sqlite_path)
    ensure_db(conn)

    meta_set(conn, "run_dir", str(run_dir))
    meta_set(conn, "started_at", iso_now())
    meta_set(conn, "duration_minutes", str(args.duration_minutes))
    meta_set(conn, "candidate_interval_seconds", str(args.candidate_interval_seconds))
    meta_set(conn, "manual_verify_timeout_seconds", str(args.manual_verify_timeout_seconds))
    meta_set(conn, "manual_verify_interval_seconds", str(args.manual_verify_interval_seconds))

    print_json_line(
        {
            "event": "experiment_starting",
            "started_at": iso_now(),
            "run_dir": str(run_dir),
            "sqlite_path": str(sqlite_path),
            "intake_log_path": str(intake_log_path),
        }
    )

    reset_result = reset_labeler_service_record(repo_root=repo_root, python_bin=python_bin)
    meta_set(conn, "labeler_reset_stdout", reset_result["stdout"])
    meta_set(conn, "labeler_reset_stderr", reset_result["stderr"])

    print_json_line(
        {
            "event": "labeler_record_reset_complete",
            "checked_at": iso_now(),
        }
    )

    intake_proc = None
    try:
        intake_proc = start_intake_worker(
            repo_root=repo_root,
            python_bin=python_bin,
            log_path=intake_log_path,
        )

        experiment_started_at = iso_now()
        meta_set(conn, "experiment_started_at", experiment_started_at)
        print_json_line(
            {
                "event": "intake_started",
                "checked_at": iso_now(),
                "pid": intake_proc.pid,
            }
        )

        deadline = time.monotonic() + args.duration_minutes * 60
        next_allowed_processing_at = time.monotonic()

        while time.monotonic() < deadline:
            if intake_proc.poll() is not None:
                raise RuntimeError(f"Intake worker exited early with code {intake_proc.returncode}")

            if time.monotonic() < next_allowed_processing_at:
                time.sleep(args.poll_seconds)
                continue

            candidate = fetch_next_candidate(
                experiment_started_at=experiment_started_at,
                conn=conn,
            )
            if candidate is None:
                time.sleep(args.poll_seconds)
                continue

            try:
                post_url = uri_to_post_url(candidate.uri)
            except Exception as exc:
                error_text = f"uri_to_post_url failed: {exc}"
                save_error_result(
                    conn,
                    candidate=candidate,
                    post_url="",
                    error_text=error_text,
                )
                print_json_line(
                    {
                        "event": "candidate_failed",
                        "checked_at": iso_now(),
                        "post_uri": candidate.uri,
                        "post_cid": candidate.cid,
                        "label_value": candidate.label_value,
                        "error": error_text,
                    }
                )
                next_allowed_processing_at = time.monotonic() + args.candidate_interval_seconds
                continue

            print_json_line(
                {
                    "event": "candidate_selected",
                    "checked_at": iso_now(),
                    "post_url": post_url,
                    "post_uri": candidate.uri,
                    "post_cid": candidate.cid,
                    "label_value": candidate.label_value,
                    "record_created_at": candidate.record_created_at,
                    "evaluated_at": candidate.evaluated_at,
                }
            )

            cmd = [
                python_bin,
                str(repo_root / "scripts" / "manual_publish_and_verify.py"),
                "--post-url",
                post_url,
                "--label-value",
                candidate.label_value,
                "--verify-timeout-seconds",
                str(args.manual_verify_timeout_seconds),
                "--verify-interval-seconds",
                str(args.manual_verify_interval_seconds),
                "--json",
            ]

            try:
                result = run_subprocess_json(
                    cmd,
                    timeout_seconds=max(args.manual_verify_timeout_seconds + 30, 60),
                )
                save_success_result(
                    conn,
                    candidate=candidate,
                    post_url=post_url,
                    result=result,
                )

                attempts = result.get("verification_attempts") or []
                final_summary = attempts[-1]["summary"] if attempts else {}

                print_json_line(
                    {
                        "event": "candidate_processed",
                        "checked_at": iso_now(),
                        "post_url": post_url,
                        "post_uri": candidate.uri,
                        "post_cid": candidate.cid,
                        "label_value": candidate.label_value,
                        "record_created_at": candidate.record_created_at,
                        "ozone_event_id": (result.get("ozone_response") or {}).get("id"),
                        "ozone_created_at": (result.get("ozone_response") or {}).get("createdAt"),
                        "manual_success": result.get("success"),
                        "final_summary": final_summary,
                    }
                )
            except Exception as exc:
                error_text = str(exc)
                save_error_result(
                    conn,
                    candidate=candidate,
                    post_url=post_url,
                    error_text=error_text,
                )
                print_json_line(
                    {
                        "event": "candidate_failed",
                        "checked_at": iso_now(),
                        "post_url": post_url,
                        "post_uri": candidate.uri,
                        "post_cid": candidate.cid,
                        "label_value": candidate.label_value,
                        "record_created_at": candidate.record_created_at,
                        "error": error_text,
                    }
                )

            next_allowed_processing_at = time.monotonic() + args.candidate_interval_seconds

        meta_set(conn, "finished_at", iso_now())
        print_json_line(
            {
                "event": "experiment_complete",
                "checked_at": iso_now(),
                "run_dir": str(run_dir),
            }
        )

    finally:
        stop_process(intake_proc)
        conn.close()


if __name__ == "__main__":
    main()