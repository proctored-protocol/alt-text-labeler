from __future__ import annotations

import argparse
import json
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import text

from app.db import SessionLocal


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def print_json_line(data: dict[str, Any]) -> None:
    print(json.dumps(data, ensure_ascii=False, default=str), flush=True)


def truncate_error(value: str, limit: int = 1200) -> str:
    value = value.strip()
    if len(value) <= limit:
        return value
    return value[:limit] + "...[truncated]"


def run_subprocess(
    cmd: list[str],
    *,
    timeout_seconds: int,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )


def parse_result(stdout_text: str) -> dict[str, Any]:
    stdout_text = stdout_text.strip()
    if not stdout_text:
        raise RuntimeError("subprocess produced no stdout")
    try:
        return json.loads(stdout_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"subprocess stdout was not valid JSON: {stdout_text}") from exc


def final_forced_found_label(result: dict[str, Any]) -> bool:
    attempts = result.get("verification_attempts") or []
    if not attempts:
        return False
    summary = attempts[-1].get("summary") or {}
    forced = summary.get("forced_hydration") or {}
    return forced.get("found_label") is True


def final_query_found_label(result: dict[str, Any]) -> bool:
    attempts = result.get("verification_attempts") or []
    if not attempts:
        return False
    summary = attempts[-1].get("summary") or {}
    query = summary.get("query_labels") or {}
    return query.get("found_label") is True


def verification_visible_enough(result: dict[str, Any]) -> bool:
    return final_forced_found_label(result) and final_query_found_label(result)


def lease_label_work_items(
    *,
    worker_id: str,
    batch_size: int,
    lease_seconds: int,
) -> list[dict[str, Any]]:
    with SessionLocal() as session:
        rows = session.execute(
            text(
                """
                WITH candidates AS (
                    SELECT id
                    FROM label_work_item
                    WHERE
                        (
                            state = 'queued'
                            AND (
                                lease_expires_at IS NULL
                                OR lease_expires_at < NOW()
                            )
                        )
                        OR (
                            state = 'leased'
                            AND lease_expires_at IS NOT NULL
                            AND lease_expires_at < NOW()
                        )
                    ORDER BY updated_at ASC, id ASC
                    LIMIT :batch_size
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE label_work_item AS lwi
                SET
                    state = 'leased',
                    leased_by = :worker_id,
                    lease_expires_at = NOW() + (:lease_seconds * INTERVAL '1 second'),
                    updated_at = NOW()
                FROM candidates
                WHERE lwi.id = candidates.id
                RETURNING
                    lwi.id,
                    lwi.post_url,
                    lwi.record_created_at,
                    lwi.label_value,
                    lwi.state,
                    lwi.ozone_event_id,
                    lwi.ozone_created_at,
                    lwi.final_forced_found_label,
                    lwi.final_query_found_label,
                    lwi.manual_success,
                    lwi.last_error,
                    lwi.attempt_count,
                    lwi.leased_by,
                    lwi.lease_expires_at,
                    lwi.updated_at
                """
            ),
            {
                "worker_id": worker_id,
                "batch_size": batch_size,
                "lease_seconds": lease_seconds,
            },
        ).mappings().all()
        session.commit()
        return [dict(row) for row in rows]


def next_backoff_seconds(*, attempt_count_after_failure: int, base_seconds: int) -> int:
    return min(base_seconds * (2 ** max(0, attempt_count_after_failure - 1)), 1800)


def mark_label_work_item_published(
    *,
    job_id: int,
    result: dict[str, Any],
) -> None:
    attempts = result.get("verification_attempts") or []
    last_summary = attempts[-1].get("summary") if attempts else {}

    ozone_response = result.get("ozone_response") or {}
    ozone_event_id = ozone_response.get("id")
    ozone_created_at = ozone_response.get("createdAt")

    query_summary = last_summary.get("query_labels") or {}
    forced_summary = last_summary.get("forced_hydration") or {}

    with SessionLocal() as session:
        session.execute(
            text(
                """
                UPDATE label_work_item
                SET
                    state = 'published',
                    ozone_event_id = :ozone_event_id,
                    ozone_created_at = :ozone_created_at,
                    final_forced_found_label = :final_forced_found_label,
                    final_query_found_label = :final_query_found_label,
                    manual_success = :manual_success,
                    last_error = NULL,
                    leased_by = NULL,
                    lease_expires_at = NULL,
                    updated_at = NOW()
                WHERE id = :job_id
                """
            ),
            {
                "job_id": job_id,
                "ozone_event_id": ozone_event_id,
                "ozone_created_at": ozone_created_at,
                "final_forced_found_label": forced_summary.get("found_label"),
                "final_query_found_label": query_summary.get("found_label"),
                "manual_success": True,
            },
        )
        session.commit()


def mark_label_work_item_retry(
    *,
    job_id: int,
    result: dict[str, Any] | None,
    error_text: str,
    max_attempts: int,
    backoff_base_seconds: int,
) -> tuple[str, int]:
    ozone_event_id = None
    ozone_created_at = None
    final_forced = None
    final_query = None
    manual_success = None

    if result is not None:
        ozone_response = result.get("ozone_response") or {}
        ozone_event_id = ozone_response.get("id")
        ozone_created_at = ozone_response.get("createdAt")

        attempts = result.get("verification_attempts") or []
        if attempts:
            summary = attempts[-1].get("summary") or {}
            forced = summary.get("forced_hydration") or {}
            query = summary.get("query_labels") or {}
            final_forced = forced.get("found_label")
            final_query = query.get("found_label")

        manual_success = result.get("success")

    with SessionLocal() as session:
        current = session.execute(
            text(
                """
                SELECT attempt_count
                FROM label_work_item
                WHERE id = :job_id
                """
            ),
            {"job_id": job_id},
        ).mappings().one()

        attempt_count_after_failure = int(current["attempt_count"] or 0) + 1

        if attempt_count_after_failure >= max_attempts:
            new_state = "dead"
            delay_seconds = 0
            lease_sql = "NULL"
        else:
            new_state = "queued"
            delay_seconds = next_backoff_seconds(
                attempt_count_after_failure=attempt_count_after_failure,
                base_seconds=backoff_base_seconds,
            )
            lease_sql = "NOW() + (:delay_seconds * INTERVAL '1 second')"

        session.execute(
            text(
                f"""
                UPDATE label_work_item
                SET
                    state = :new_state,
                    attempt_count = attempt_count + 1,
                    ozone_event_id = COALESCE(:ozone_event_id, ozone_event_id),
                    ozone_created_at = COALESCE(:ozone_created_at, ozone_created_at),
                    final_forced_found_label = COALESCE(:final_forced_found_label, final_forced_found_label),
                    final_query_found_label = COALESCE(:final_query_found_label, final_query_found_label),
                    manual_success = COALESCE(:manual_success, manual_success),
                    last_error = :last_error,
                    leased_by = NULL,
                    lease_expires_at = {lease_sql},
                    updated_at = NOW()
                WHERE id = :job_id
                """
            ),
            {
                "job_id": job_id,
                "new_state": new_state,
                "ozone_event_id": ozone_event_id,
                "ozone_created_at": ozone_created_at,
                "final_forced_found_label": final_forced,
                "final_query_found_label": final_query,
                "manual_success": manual_success,
                "last_error": truncate_error(error_text),
                "delay_seconds": delay_seconds,
            },
        )
        session.commit()

    return new_state, attempt_count_after_failure


def main() -> None:
    parser = argparse.ArgumentParser(description="Lease label work items and publish labels.")
    parser.add_argument("--worker-id", default=f"apply-{socket.gethostname()}")
    parser.add_argument("--batch-size", type=int, default=3)
    parser.add_argument("--lease-seconds", type=int, default=180)
    parser.add_argument("--verify-timeout-seconds", type=int, default=10)
    parser.add_argument("--verify-interval-seconds", type=int, default=1)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--backoff-base-seconds", type=int, default=15)
    parser.add_argument("--idle-sleep-seconds", type=float, default=1.0)
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    python_bin = sys.executable

    print_json_line(
        {
            "event": "label_apply_worker_started",
            "checked_at": iso_now(),
            "worker_id": args.worker_id,
            "batch_size": args.batch_size,
            "lease_seconds": args.lease_seconds,
            "verify_timeout_seconds": args.verify_timeout_seconds,
            "verify_interval_seconds": args.verify_interval_seconds,
            "max_attempts": args.max_attempts,
            "backoff_base_seconds": args.backoff_base_seconds,
        }
    )

    while True:
        jobs = lease_label_work_items(
            worker_id=args.worker_id,
            batch_size=args.batch_size,
            lease_seconds=args.lease_seconds,
        )

        if not jobs:
            time.sleep(args.idle_sleep_seconds)
            continue

        for job in jobs:
            job_id = job["id"]
            post_url = job["post_url"]
            label_value = job["label_value"]
            attempt_count = int(job["attempt_count"] or 0)

            print_json_line(
                {
                    "event": "label_apply_started",
                    "checked_at": iso_now(),
                    "worker_id": args.worker_id,
                    "job_id": job_id,
                    "post_url": post_url,
                    "label_value": label_value,
                    "attempt_count": attempt_count,
                }
            )

            cmd = [
                python_bin,
                str(repo_root / "scripts" / "manual_publish_and_verify.py"),
                "--post-url",
                post_url,
                "--label-value",
                label_value,
                "--verify-timeout-seconds",
                str(args.verify_timeout_seconds),
                "--verify-interval-seconds",
                str(args.verify_interval_seconds),
                "--skip-subscriber-check",
                "--json",
            ]

            result: dict[str, Any] | None = None
            error_text: str | None = None

            try:
                completed = run_subprocess(
                    cmd,
                    timeout_seconds=max(args.verify_timeout_seconds + 30, 60),
                )

                stdout_text = completed.stdout.strip()
                stderr_text = completed.stderr.strip()

                if completed.returncode not in (0, 2):
                    error_text = f"subprocess_stderr rc={completed.returncode}: {stderr_text or stdout_text}"
                else:
                    result = parse_result(stdout_text)

                    if verification_visible_enough(result):
                        mark_label_work_item_published(
                            job_id=job_id,
                            result=result,
                        )

                        print_json_line(
                            {
                                "event": "label_apply_succeeded",
                                "checked_at": iso_now(),
                                "worker_id": args.worker_id,
                                "job_id": job_id,
                                "post_url": post_url,
                                "label_value": label_value,
                                "ozone_event_id": (result.get("ozone_response") or {}).get("id"),
                                "ozone_created_at": (result.get("ozone_response") or {}).get("createdAt"),
                                "final_forced_found_label": final_forced_found_label(result),
                                "final_query_found_label": final_query_found_label(result),
                                "manual_success": result.get("success"),
                            }
                        )
                        continue

                    error_text = f"verification_unsuccessful rc={completed.returncode}"

            except Exception as exc:
                error_text = str(exc)

            assert error_text is not None

            new_state, next_attempt = mark_label_work_item_retry(
                job_id=job_id,
                result=result,
                error_text=error_text,
                max_attempts=args.max_attempts,
                backoff_base_seconds=args.backoff_base_seconds,
            )

            print_json_line(
                {
                    "event": "label_apply_failed",
                    "checked_at": iso_now(),
                    "worker_id": args.worker_id,
                    "job_id": job_id,
                    "post_url": post_url,
                    "label_value": label_value,
                    "attempt_count_after_failure": next_attempt,
                    "new_state": new_state,
                    "error": truncate_error(error_text, 400),
                    "final_forced_found_label": final_forced_found_label(result) if result else None,
                    "final_query_found_label": final_query_found_label(result) if result else None,
                }
            )


if __name__ == "__main__":
    main()