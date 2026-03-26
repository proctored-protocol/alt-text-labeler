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

from app.db import SessionLocal, init_db
from app.services.label_work_queue import (
    lease_label_work_items,
    mark_label_work_item_dead,
    mark_label_work_item_retry,
    mark_label_work_item_visible,
    next_backoff_seconds,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def print_json_line(data: dict[str, Any]) -> None:
    print(json.dumps(data, ensure_ascii=False, default=str), flush=True)


def run_subprocess_json(cmd: list[str], *, timeout_seconds: int) -> tuple[int, dict[str, Any] | None, str, str]:
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

    if not stdout:
        return completed.returncode, None, stdout, stderr

    try:
        parsed = json.loads(stdout)
        return completed.returncode, parsed, stdout, stderr
    except json.JSONDecodeError:
        return completed.returncode, None, stdout, stderr


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Lease label_work_item rows and process them through the proven manual publish+verify path."
    )
    parser.add_argument("--worker-id", default=None)
    parser.add_argument("--batch-size", type=int, default=5)
    parser.add_argument("--lease-seconds", type=int, default=180)
    parser.add_argument("--idle-sleep-seconds", type=float, default=1.0)
    parser.add_argument("--verify-timeout-seconds", type=int, default=10)
    parser.add_argument("--verify-interval-seconds", type=int, default=1)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--backoff-base-seconds", type=int, default=15)
    args = parser.parse_args()

    init_db()

    repo_root = Path(__file__).resolve().parent.parent
    python_bin = sys.executable
    worker_id = args.worker_id or f"{socket.gethostname()}:{Path(sys.argv[0]).name}:{os_getpid_safe()}"

    print_json_line(
        {
            "event": "label_apply_worker_started",
            "checked_at": iso_now(),
            "worker_id": worker_id,
            "batch_size": args.batch_size,
            "lease_seconds": args.lease_seconds,
            "verify_timeout_seconds": args.verify_timeout_seconds,
            "verify_interval_seconds": args.verify_interval_seconds,
            "max_attempts": args.max_attempts,
            "backoff_base_seconds": args.backoff_base_seconds,
        }
    )

    while True:
        with SessionLocal() as session:
            jobs = lease_label_work_items(
                session,
                worker_id=worker_id,
                batch_size=args.batch_size,
                lease_seconds=args.lease_seconds,
            )
            session.commit()

        if not jobs:
            time.sleep(args.idle_sleep_seconds)
            continue

        for job in jobs:
            post_url = job.get("post_url")
            label_value = job.get("label_value")
            job_id = job["id"]
            attempt_count = int(job.get("attempt_count") or 0)

            if not post_url:
                with SessionLocal() as session:
                    mark_label_work_item_dead(
                        session,
                        job_id=job_id,
                        result=None,
                        error_text="Missing post_url on leased work item",
                    )
                    session.commit()

                print_json_line(
                    {
                        "event": "label_apply_dead",
                        "checked_at": iso_now(),
                        "worker_id": worker_id,
                        "job_id": job_id,
                        "post_url": post_url,
                        "label_value": label_value,
                        "record_created_at": job.get("record_created_at"),
                        "error": "Missing post_url on leased work item",
                    }
                )
                continue

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
                "--json",
            ]

            rc = -1
            result = None
            stdout = ""
            stderr = ""

            try:
                rc, result, stdout, stderr = run_subprocess_json(
                    cmd,
                    timeout_seconds=max(args.verify_timeout_seconds + 30, 60),
                )

                if rc == 0 and result is not None and result.get("success") is True:
                    with SessionLocal() as session:
                        mark_label_work_item_visible(
                            session,
                            job_id=job_id,
                            result=result,
                        )
                        session.commit()

                    attempts = result.get("verification_attempts") or []
                    final_summary = attempts[-1]["summary"] if attempts else {}

                    print_json_line(
                        {
                            "event": "label_apply_visible",
                            "checked_at": iso_now(),
                            "worker_id": worker_id,
                            "job_id": job_id,
                            "post_url": post_url,
                            "record_created_at": job.get("record_created_at"),
                            "label_value": label_value,
                            "ozone_event_id": (result.get("ozone_response") or {}).get("id"),
                            "ozone_created_at": (result.get("ozone_response") or {}).get("createdAt"),
                            "final_forced_found_label": (final_summary.get("forced_hydration") or {}).get("found_label"),
                            "final_query_found_label": (final_summary.get("query_labels") or {}).get("found_label"),
                            "final_subscriber_found_label": (final_summary.get("subscriber_hydration") or {}).get("found_label"),
                            "message": "visible",
                        }
                    )
                    continue

                error_text = build_error_text(rc=rc, result=result, stdout=stdout, stderr=stderr)

                if attempt_count >= args.max_attempts:
                    with SessionLocal() as session:
                        mark_label_work_item_dead(
                            session,
                            job_id=job_id,
                            result=result,
                            error_text=error_text,
                        )
                        session.commit()

                    print_json_line(
                        {
                            "event": "label_apply_dead",
                            "checked_at": iso_now(),
                            "worker_id": worker_id,
                            "job_id": job_id,
                            "post_url": post_url,
                            "record_created_at": job.get("record_created_at"),
                            "label_value": label_value,
                            "final_forced_found_label": extract_final_forced(result),
                            "ozone_created_at": extract_ozone_created_at(result),
                            "error": error_text,
                        }
                    )
                else:
                    delay_seconds = next_backoff_seconds(
                        attempt_count=attempt_count,
                        base_seconds=args.backoff_base_seconds,
                    )
                    with SessionLocal() as session:
                        mark_label_work_item_retry(
                            session,
                            job_id=job_id,
                            result=result,
                            error_text=error_text,
                            delay_seconds=delay_seconds,
                        )
                        session.commit()

                    print_json_line(
                        {
                            "event": "label_apply_retry",
                            "checked_at": iso_now(),
                            "worker_id": worker_id,
                            "job_id": job_id,
                            "post_url": post_url,
                            "record_created_at": job.get("record_created_at"),
                            "label_value": label_value,
                            "final_forced_found_label": extract_final_forced(result),
                            "ozone_created_at": extract_ozone_created_at(result),
                            "delay_seconds": delay_seconds,
                            "error": error_text,
                        }
                    )

            except Exception as exc:
                error_text = f"{type(exc).__name__}: {exc}"

                if attempt_count >= args.max_attempts:
                    with SessionLocal() as session:
                        mark_label_work_item_dead(
                            session,
                            job_id=job_id,
                            result=result,
                            error_text=error_text,
                        )
                        session.commit()
                else:
                    delay_seconds = next_backoff_seconds(
                        attempt_count=attempt_count,
                        base_seconds=args.backoff_base_seconds,
                    )
                    with SessionLocal() as session:
                        mark_label_work_item_retry(
                            session,
                            job_id=job_id,
                            result=result,
                            error_text=error_text,
                            delay_seconds=delay_seconds,
                        )
                        session.commit()

                print_json_line(
                    {
                        "event": "label_apply_exception",
                        "checked_at": iso_now(),
                        "worker_id": worker_id,
                        "job_id": job_id,
                        "post_url": post_url,
                        "record_created_at": job.get("record_created_at"),
                        "label_value": label_value,
                        "error": error_text,
                    }
                )


def os_getpid_safe() -> int:
    try:
        import os
        return os.getpid()
    except Exception:
        return 0


def extract_ozone_created_at(result: dict[str, Any] | None) -> str | None:
    if not result:
        return None
    return (result.get("ozone_response") or {}).get("createdAt")


def extract_final_forced(result: dict[str, Any] | None) -> bool | None:
    if not result:
        return None
    attempts = result.get("verification_attempts") or []
    if not attempts:
        return None
    final_summary = (attempts[-1] or {}).get("summary") or {}
    return (final_summary.get("forced_hydration") or {}).get("found_label")


def build_error_text(
    *,
    rc: int,
    result: dict[str, Any] | None,
    stdout: str,
    stderr: str,
) -> str:
    if result is not None and result.get("success") is False:
        return f"verification_unsuccessful rc={rc}"
    if stdout and not result:
        return f"non_json_stdout rc={rc}: {stdout[:500]}"
    if stderr:
        return f"subprocess_stderr rc={rc}: {stderr[:500]}"
    return f"subprocess_failed rc={rc}"


if __name__ == "__main__":
    main()