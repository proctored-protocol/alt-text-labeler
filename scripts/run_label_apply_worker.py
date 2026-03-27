from __future__ import annotations

import argparse
import json
import socket
import time
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

from app.db import SessionLocal
from scripts.manual_publish_and_verify import HTTPJSONError, publish_label_via_ozone


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


def is_rate_limit_error(exc: Exception) -> bool:
    if isinstance(exc, HTTPJSONError) and exc.code == 429:
        return True
    msg = str(exc)
    return "HTTP Error 429" in msg or "HTTP 429" in msg or "Too Many Requests" in msg


def next_backoff_seconds(*, attempt_count_after_failure: int, base_seconds: int) -> int:
    return min(base_seconds * (2 ** max(0, attempt_count_after_failure - 1)), 1800)


def lease_label_work_items(
    *,
    session,
    worker_id: str,
    batch_size: int,
    lease_seconds: int,
) -> list[dict[str, Any]]:
    rows = session.execute(
        text(
            """
            WITH candidates AS (
                SELECT id
                FROM label_work_item
                WHERE
                    (
                        state = 'queued'
                        AND next_attempt_at <= NOW()
                    )
                    OR (
                        state = 'leased'
                        AND leased_until IS NOT NULL
                        AND leased_until < NOW()
                    )
                ORDER BY next_attempt_at ASC, id ASC
                LIMIT :batch_size
                FOR UPDATE SKIP LOCKED
            )
            UPDATE label_work_item AS lwi
            SET
                state = 'leased',
                attempt_count = lwi.attempt_count + 1,
                leased_until = NOW() + (:lease_seconds * INTERVAL '1 second'),
                leased_by = :worker_id,
                updated_at = NOW()
            FROM candidates
            WHERE lwi.id = candidates.id
            RETURNING
                lwi.id,
                lwi.uri,
                lwi.cid,
                lwi.label_value,
                lwi.post_url,
                lwi.record_created_at,
                lwi.evaluated_at,
                lwi.state,
                lwi.attempt_count,
                lwi.next_attempt_at,
                lwi.leased_until,
                lwi.leased_by,
                lwi.ozone_event_id,
                lwi.ozone_created_at,
                lwi.final_forced_status_code,
                lwi.final_query_status_code,
                lwi.final_subscriber_status_code,
                lwi.final_forced_found_label,
                lwi.final_query_found_label,
                lwi.final_subscriber_found_label,
                lwi.manual_success,
                lwi.label_visible_at,
                lwi.last_error,
                lwi.raw_result_json,
                lwi.created_at,
                lwi.updated_at
            """
        ),
        {
            "worker_id": worker_id,
            "batch_size": batch_size,
            "lease_seconds": lease_seconds,
        },
    ).mappings().all()

    return [dict(row) for row in rows]


def mark_label_work_item_emitted(
    *,
    session,
    work_item_id: int,
    ozone_response: dict[str, Any],
    verification_delay_seconds: int,
) -> None:
    raw_result = {
        "phase": "emitted",
        "ozone_response": ozone_response,
    }

    session.execute(
        text(
            """
            UPDATE label_work_item
            SET
                state = 'published_pending_verification',
                leased_until = NULL,
                leased_by = NULL,
                next_attempt_at = NOW() + (:verification_delay_seconds * INTERVAL '1 second'),
                ozone_event_id = :ozone_event_id,
                ozone_created_at = :ozone_created_at,
                last_error = NULL,
                raw_result_json = :raw_result_json,
                updated_at = NOW()
            WHERE id = :work_item_id
            """
        ),
        {
            "work_item_id": work_item_id,
            "verification_delay_seconds": verification_delay_seconds,
            "ozone_event_id": ozone_response.get("id"),
            "ozone_created_at": ozone_response.get("createdAt"),
            "raw_result_json": json.dumps(raw_result, ensure_ascii=False, default=str),
        },
    )


def mark_label_work_item_retry(
    *,
    session,
    work_item_id: int,
    error_text: str,
    delay_seconds: int,
) -> None:
    raw_result = {
        "phase": "publish_failed",
        "error": error_text,
    }

    session.execute(
        text(
            """
            UPDATE label_work_item
            SET
                state = 'queued',
                leased_until = NULL,
                leased_by = NULL,
                next_attempt_at = NOW() + (:delay_seconds * INTERVAL '1 second'),
                last_error = :last_error,
                raw_result_json = :raw_result_json,
                updated_at = NOW()
            WHERE id = :work_item_id
            """
        ),
        {
            "work_item_id": work_item_id,
            "delay_seconds": delay_seconds,
            "last_error": truncate_error(error_text),
            "raw_result_json": json.dumps(raw_result, ensure_ascii=False, default=str),
        },
    )


def mark_label_work_item_dead(
    *,
    session,
    work_item_id: int,
    error_text: str,
) -> None:
    raw_result = {
        "phase": "publish_dead",
        "error": error_text,
    }

    session.execute(
        text(
            """
            UPDATE label_work_item
            SET
                state = 'dead',
                leased_until = NULL,
                leased_by = NULL,
                manual_success = FALSE,
                last_error = :last_error,
                raw_result_json = :raw_result_json,
                updated_at = NOW()
            WHERE id = :work_item_id
            """
        ),
        {
            "work_item_id": work_item_id,
            "last_error": truncate_error(error_text),
            "raw_result_json": json.dumps(raw_result, ensure_ascii=False, default=str),
        },
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Lease label_work_item rows and emit labels only.")
    parser.add_argument("--worker-id", default=f"apply-{socket.gethostname()}")
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--lease-seconds", type=int, default=180)
    parser.add_argument("--verification-delay-seconds", type=int, default=30)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--backoff-base-seconds", type=int, default=15)
    parser.add_argument("--rate-limit-cooldown-seconds", type=int, default=60)
    parser.add_argument("--rate-limit-backoff-multiplier", type=int, default=8)
    parser.add_argument("--publish-sleep-seconds", type=float, default=0.25)
    parser.add_argument("--idle-sleep-seconds", type=float, default=1.0)
    args = parser.parse_args()

    cooldown_until = 0.0

    print_json_line(
        {
            "event": "label_apply_worker_started",
            "checked_at": iso_now(),
            "worker_id": args.worker_id,
            "batch_size": args.batch_size,
            "lease_seconds": args.lease_seconds,
            "verification_delay_seconds": args.verification_delay_seconds,
            "max_attempts": args.max_attempts,
            "backoff_base_seconds": args.backoff_base_seconds,
            "rate_limit_cooldown_seconds": args.rate_limit_cooldown_seconds,
            "rate_limit_backoff_multiplier": args.rate_limit_backoff_multiplier,
            "publish_sleep_seconds": args.publish_sleep_seconds,
        }
    )

    while True:
        now_mono = time.monotonic()
        if now_mono < cooldown_until:
            time.sleep(min(args.idle_sleep_seconds, cooldown_until - now_mono))
            continue

        with SessionLocal() as session:
            jobs = lease_label_work_items(
                session=session,
                worker_id=args.worker_id,
                batch_size=args.batch_size,
                lease_seconds=args.lease_seconds,
            )
            session.commit()

        if not jobs:
            time.sleep(args.idle_sleep_seconds)
            continue

        rate_limited_this_batch = False

        for job in jobs:
            work_item_id = int(job["id"])
            attempt_count = int(job.get("attempt_count") or 0)

            post_url = job["post_url"]
            at_uri = job["uri"]
            cid = job["cid"]
            label_value = job["label_value"]

            print_json_line(
                {
                    "event": "label_apply_started",
                    "checked_at": iso_now(),
                    "worker_id": args.worker_id,
                    "work_item_id": work_item_id,
                    "post_url": post_url,
                    "post_uri": at_uri,
                    "label_value": label_value,
                    "attempt_count": attempt_count,
                }
            )

            try:
                ozone_response = publish_label_via_ozone(
                    at_uri=at_uri,
                    cid=cid,
                    label_value=label_value,
                )

                with SessionLocal() as session:
                    mark_label_work_item_emitted(
                        session=session,
                        work_item_id=work_item_id,
                        ozone_response=ozone_response,
                        verification_delay_seconds=args.verification_delay_seconds,
                    )
                    session.commit()

                print_json_line(
                    {
                        "event": "label_apply_emitted",
                        "checked_at": iso_now(),
                        "worker_id": args.worker_id,
                        "work_item_id": work_item_id,
                        "post_url": post_url,
                        "post_uri": at_uri,
                        "label_value": label_value,
                        "ozone_event_id": ozone_response.get("id"),
                        "ozone_created_at": ozone_response.get("createdAt"),
                    }
                )

            except Exception as exc:
                error_text = str(exc)
                rate_limited = is_rate_limit_error(exc)

                with SessionLocal() as session:
                    if attempt_count >= args.max_attempts:
                        mark_label_work_item_dead(
                            session=session,
                            work_item_id=work_item_id,
                            error_text=error_text,
                        )
                    else:
                        base_seconds = args.backoff_base_seconds
                        if rate_limited:
                            base_seconds *= args.rate_limit_backoff_multiplier

                        delay_seconds = next_backoff_seconds(
                            attempt_count_after_failure=attempt_count,
                            base_seconds=base_seconds,
                        )

                        mark_label_work_item_retry(
                            session=session,
                            work_item_id=work_item_id,
                            error_text=error_text,
                            delay_seconds=delay_seconds,
                        )
                    session.commit()

                if rate_limited:
                    cooldown_until = max(
                        cooldown_until,
                        time.monotonic() + args.rate_limit_cooldown_seconds,
                    )
                    rate_limited_this_batch = True

                    print_json_line(
                        {
                            "event": "label_apply_rate_limited",
                            "checked_at": iso_now(),
                            "worker_id": args.worker_id,
                            "work_item_id": work_item_id,
                            "post_url": post_url,
                            "cooldown_seconds": args.rate_limit_cooldown_seconds,
                            "error": truncate_error(error_text),
                        }
                    )

                print_json_line(
                    {
                        "event": "label_apply_failed",
                        "checked_at": iso_now(),
                        "worker_id": args.worker_id,
                        "work_item_id": work_item_id,
                        "post_url": post_url,
                        "post_uri": at_uri,
                        "label_value": label_value,
                        "error": truncate_error(error_text),
                    }
                )

                if args.publish_sleep_seconds > 0:
                    time.sleep(args.publish_sleep_seconds)

                if rate_limited_this_batch:
                    break

                continue

            if args.publish_sleep_seconds > 0:
                time.sleep(args.publish_sleep_seconds)

        if rate_limited_this_batch:
            continue


if __name__ == "__main__":
    main()