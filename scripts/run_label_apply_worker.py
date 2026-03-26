from __future__ import annotations

import argparse
import json
import socket
import time
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

from app.db import SessionLocal
from app.services.publish_queue import (
    lease_publish_jobs,
    mark_publish_job_dead,
    mark_publish_job_published,
    mark_publish_job_retry,
    next_backoff_seconds,
)
from scripts.manual_publish_and_verify import (
    HTTPJSONError,
    ScriptSettings,
    publish_label_via_ozone,
    summarize_snapshot,
    verify_once,
)


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


def is_not_found_error(exc: Exception) -> bool:
    msg = str(exc)
    return "getPosts returned no posts for" in msg


def is_rate_limit_error(exc: Exception) -> bool:
    if isinstance(exc, HTTPJSONError) and exc.code == 429:
        return True

    msg = str(exc)
    return "HTTP Error 429" in msg or "HTTP 429" in msg or "Too Many Requests" in msg


def fetch_label_work_item(
    *,
    session,
    uri: str,
    cid: str,
    label_value: str,
) -> dict[str, Any] | None:
    row = session.execute(
        text(
            """
            SELECT
                id,
                uri,
                cid,
                label_value,
                post_url,
                record_created_at,
                evaluated_at,
                state,
                attempt_count,
                next_attempt_at,
                leased_until,
                leased_by,
                ozone_event_id,
                ozone_created_at,
                final_forced_status_code,
                final_query_status_code,
                final_subscriber_status_code,
                final_forced_found_label,
                final_query_found_label,
                final_subscriber_found_label,
                manual_success,
                label_visible_at,
                last_error,
                raw_result_json,
                created_at,
                updated_at
            FROM label_work_item
            WHERE uri = :uri
              AND cid = :cid
              AND label_value = :label_value
            ORDER BY id DESC
            LIMIT 1
            """
        ),
        {
            "uri": uri,
            "cid": cid,
            "label_value": label_value,
        },
    ).mappings().first()

    return dict(row) if row is not None else None


def mark_label_work_item_leased(
    *,
    session,
    work_item_id: int,
    worker_id: str,
    lease_seconds: int,
) -> None:
    session.execute(
        text(
            """
            UPDATE label_work_item
            SET
                state = 'leased',
                leased_until = NOW() + (:lease_seconds * INTERVAL '1 second'),
                leased_by = :worker_id,
                updated_at = NOW()
            WHERE id = :work_item_id
            """
        ),
        {
            "work_item_id": work_item_id,
            "worker_id": worker_id,
            "lease_seconds": lease_seconds,
        },
    )


def extract_final_fields(final_summary: dict[str, Any] | None) -> dict[str, Any]:
    summary = final_summary or {}
    query_labels = summary.get("query_labels") or {}
    forced_hydration = summary.get("forced_hydration") or {}
    subscriber_hydration = summary.get("subscriber_hydration") or {}

    return {
        "final_forced_status_code": forced_hydration.get("status_code"),
        "final_query_status_code": query_labels.get("status_code"),
        "final_subscriber_status_code": subscriber_hydration.get("status_code"),
        "final_forced_found_label": forced_hydration.get("found_label"),
        "final_query_found_label": query_labels.get("found_label"),
        "final_subscriber_found_label": subscriber_hydration.get("found_label"),
    }


def update_label_work_item_success(
    *,
    session,
    work_item_id: int,
    ozone_response: dict[str, Any],
    final_summary: dict[str, Any],
) -> None:
    fields = extract_final_fields(final_summary)

    raw_result = {
        "ozone_response": ozone_response,
        "final_summary": final_summary,
        "success": True,
    }

    session.execute(
        text(
            """
            UPDATE label_work_item
            SET
                state = 'published',
                leased_until = NULL,
                leased_by = NULL,
                ozone_event_id = :ozone_event_id,
                ozone_created_at = :ozone_created_at,
                final_forced_status_code = :final_forced_status_code,
                final_query_status_code = :final_query_status_code,
                final_subscriber_status_code = :final_subscriber_status_code,
                final_forced_found_label = :final_forced_found_label,
                final_query_found_label = :final_query_found_label,
                final_subscriber_found_label = :final_subscriber_found_label,
                manual_success = TRUE,
                label_visible_at = NOW(),
                last_error = NULL,
                raw_result_json = :raw_result_json,
                updated_at = NOW()
            WHERE id = :work_item_id
            """
        ),
        {
            "work_item_id": work_item_id,
            "ozone_event_id": ozone_response.get("id"),
            "ozone_created_at": ozone_response.get("createdAt"),
            "final_forced_status_code": fields["final_forced_status_code"],
            "final_query_status_code": fields["final_query_status_code"],
            "final_subscriber_status_code": fields["final_subscriber_status_code"],
            "final_forced_found_label": fields["final_forced_found_label"],
            "final_query_found_label": fields["final_query_found_label"],
            "final_subscriber_found_label": fields["final_subscriber_found_label"],
            "raw_result_json": json.dumps(raw_result, ensure_ascii=False, default=str),
        },
    )


def update_label_work_item_retry(
    *,
    session,
    work_item_id: int,
    error_text: str,
    ozone_response: dict[str, Any] | None = None,
    final_summary: dict[str, Any] | None = None,
) -> None:
    fields = extract_final_fields(final_summary)

    raw_result = {
        "ozone_response": ozone_response,
        "final_summary": final_summary,
        "success": False,
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
                ozone_event_id = COALESCE(:ozone_event_id, ozone_event_id),
                ozone_created_at = COALESCE(:ozone_created_at, ozone_created_at),
                final_forced_status_code = COALESCE(:final_forced_status_code, final_forced_status_code),
                final_query_status_code = COALESCE(:final_query_status_code, final_query_status_code),
                final_subscriber_status_code = COALESCE(:final_subscriber_status_code, final_subscriber_status_code),
                final_forced_found_label = COALESCE(:final_forced_found_label, final_forced_found_label),
                final_query_found_label = COALESCE(:final_query_found_label, final_query_found_label),
                final_subscriber_found_label = COALESCE(:final_subscriber_found_label, final_subscriber_found_label),
                manual_success = FALSE,
                last_error = :last_error,
                raw_result_json = :raw_result_json,
                updated_at = NOW()
            WHERE id = :work_item_id
            """
        ),
        {
            "work_item_id": work_item_id,
            "ozone_event_id": (ozone_response or {}).get("id"),
            "ozone_created_at": (ozone_response or {}).get("createdAt"),
            "final_forced_status_code": fields["final_forced_status_code"],
            "final_query_status_code": fields["final_query_status_code"],
            "final_subscriber_status_code": fields["final_subscriber_status_code"],
            "final_forced_found_label": fields["final_forced_found_label"],
            "final_query_found_label": fields["final_query_found_label"],
            "final_subscriber_found_label": fields["final_subscriber_found_label"],
            "last_error": truncate_error(error_text),
            "raw_result_json": json.dumps(raw_result, ensure_ascii=False, default=str),
        },
    )


def update_label_work_item_dead(
    *,
    session,
    work_item_id: int,
    error_text: str,
) -> None:
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
                updated_at = NOW()
            WHERE id = :work_item_id
            """
        ),
        {
            "work_item_id": work_item_id,
            "last_error": truncate_error(error_text),
        },
    )


def verification_succeeded(final_summary: dict[str, Any] | None) -> bool:
    summary = final_summary or {}
    query_ok = ((summary.get("query_labels") or {}).get("found_label") is True)
    forced_ok = ((summary.get("forced_hydration") or {}).get("found_label") is True)
    return query_ok and forced_ok


def main() -> None:
    parser = argparse.ArgumentParser(description="Lease label work items and publish labels in-process.")
    parser.add_argument("--worker-id", default=f"apply-{socket.gethostname()}")
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--lease-seconds", type=int, default=180)
    parser.add_argument("--verify-timeout-seconds", type=int, default=10)
    parser.add_argument("--verify-interval-seconds", type=int, default=1)
    parser.add_argument("--request-timeout-seconds", type=int, default=30)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--backoff-base-seconds", type=int, default=15)
    parser.add_argument("--rate-limit-cooldown-seconds", type=int, default=60)
    parser.add_argument("--rate-limit-backoff-multiplier", type=int, default=8)
    parser.add_argument("--publish-sleep-seconds", type=float, default=0.25)
    parser.add_argument("--idle-sleep-seconds", type=float, default=1.0)
    args = parser.parse_args()

    script_settings = ScriptSettings()
    labeler_did = script_settings.verifier_labeler_did

    cooldown_until = 0.0

    print_json_line(
        {
            "event": "label_apply_worker_started",
            "checked_at": iso_now(),
            "worker_id": args.worker_id,
            "batch_size": args.batch_size,
            "lease_seconds": args.lease_seconds,
            "verify_timeout_seconds": args.verify_timeout_seconds,
            "verify_interval_seconds": args.verify_interval_seconds,
            "request_timeout_seconds": args.request_timeout_seconds,
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

        hydrated_jobs: list[tuple[dict[str, Any], dict[str, Any]]] = []

        with SessionLocal() as session:
            leased_publish_jobs = lease_publish_jobs(
                session,
                worker_id=args.worker_id,
                batch_size=args.batch_size,
                lease_seconds=args.lease_seconds,
            )

            for publish_job in leased_publish_jobs:
                publish_job = dict(publish_job)
                publish_job_id = int(publish_job["id"])

                work_item = fetch_label_work_item(
                    session=session,
                    uri=publish_job["uri"],
                    cid=publish_job["cid"],
                    label_value=publish_job["label_value"],
                )

                if work_item is None:
                    mark_publish_job_dead(
                        session=session,
                        job_id=publish_job_id,
                        error_text="matching label_work_item row missing",
                    )
                    print_json_line(
                        {
                            "event": "label_apply_dead",
                            "checked_at": iso_now(),
                            "worker_id": args.worker_id,
                            "publish_job_id": publish_job_id,
                            "error": "matching label_work_item row missing",
                        }
                    )
                    continue

                work_item_id = int(work_item["id"])

                if work_item["state"] == "published":
                    mark_publish_job_published(session=session, job_id=publish_job_id)
                    print_json_line(
                        {
                            "event": "label_apply_already_published",
                            "checked_at": iso_now(),
                            "worker_id": args.worker_id,
                            "publish_job_id": publish_job_id,
                            "work_item_id": work_item_id,
                            "post_url": work_item["post_url"],
                            "label_value": work_item["label_value"],
                        }
                    )
                    continue

                mark_label_work_item_leased(
                    session=session,
                    work_item_id=work_item_id,
                    worker_id=args.worker_id,
                    lease_seconds=args.lease_seconds,
                )
                hydrated_jobs.append((publish_job, work_item))

            session.commit()

        if not hydrated_jobs:
            time.sleep(args.idle_sleep_seconds)
            continue

        rate_limited_this_batch = False

        for publish_job, work_item in hydrated_jobs:
            publish_job_id = int(publish_job["id"])
            publish_attempt_count = int(publish_job.get("attempt_count") or 0)

            work_item_id = int(work_item["id"])
            post_url = work_item["post_url"]
            at_uri = work_item["uri"]
            cid = work_item["cid"]
            label_value = work_item["label_value"]

            print_json_line(
                {
                    "event": "label_apply_started",
                    "checked_at": iso_now(),
                    "worker_id": args.worker_id,
                    "publish_job_id": publish_job_id,
                    "work_item_id": work_item_id,
                    "post_url": post_url,
                    "post_uri": at_uri,
                    "label_value": label_value,
                    "attempt_count": publish_attempt_count,
                }
            )

            ozone_response: dict[str, Any] | None = None
            final_summary: dict[str, Any] | None = None

            try:
                ozone_response = publish_label_via_ozone(
                    at_uri=at_uri,
                    cid=cid,
                    label_value=label_value,
                )

                deadline = time.monotonic() + args.verify_timeout_seconds
                while True:
                    snapshot = verify_once(
                        at_uri=at_uri,
                        label_value=label_value,
                        labeler_did=labeler_did,
                        timeout=args.request_timeout_seconds,
                        skip_forced_check=False,
                        skip_subscriber_check=True,
                    )
                    final_summary = summarize_snapshot(snapshot)

                    if verification_succeeded(final_summary):
                        break

                    if time.monotonic() >= deadline:
                        break

                    time.sleep(args.verify_interval_seconds)

            except Exception as exc:
                error_text = str(exc)
                rate_limited = is_rate_limit_error(exc)

                with SessionLocal() as session:
                    if is_not_found_error(exc):
                        update_label_work_item_dead(
                            session=session,
                            work_item_id=work_item_id,
                            error_text=error_text,
                        )
                        mark_publish_job_dead(
                            session=session,
                            job_id=publish_job_id,
                            error_text=truncate_error(error_text),
                        )
                    else:
                        next_attempt = publish_attempt_count + 1
                        if next_attempt >= args.max_attempts:
                            update_label_work_item_dead(
                                session=session,
                                work_item_id=work_item_id,
                                error_text=error_text,
                            )
                            mark_publish_job_dead(
                                session=session,
                                job_id=publish_job_id,
                                error_text=truncate_error(error_text),
                            )
                        else:
                            base_seconds = args.backoff_base_seconds
                            if rate_limited:
                                base_seconds *= args.rate_limit_backoff_multiplier

                            delay_seconds = next_backoff_seconds(
                                attempt_count_after_failure=next_attempt,
                                base_seconds=base_seconds,
                            )

                            update_label_work_item_retry(
                                session=session,
                                work_item_id=work_item_id,
                                error_text=error_text,
                                ozone_response=ozone_response,
                                final_summary=final_summary,
                            )
                            mark_publish_job_retry(
                                session=session,
                                job_id=publish_job_id,
                                error_text=truncate_error(error_text),
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
                            "publish_job_id": publish_job_id,
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
                        "publish_job_id": publish_job_id,
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

            if verification_succeeded(final_summary):
                with SessionLocal() as session:
                    update_label_work_item_success(
                        session=session,
                        work_item_id=work_item_id,
                        ozone_response=ozone_response or {},
                        final_summary=final_summary or {},
                    )
                    mark_publish_job_published(session=session, job_id=publish_job_id)
                    session.commit()

                print_json_line(
                    {
                        "event": "label_apply_succeeded",
                        "checked_at": iso_now(),
                        "worker_id": args.worker_id,
                        "publish_job_id": publish_job_id,
                        "work_item_id": work_item_id,
                        "post_url": post_url,
                        "post_uri": at_uri,
                        "label_value": label_value,
                        "ozone_event_id": (ozone_response or {}).get("id"),
                        "ozone_created_at": (ozone_response or {}).get("createdAt"),
                        "final_summary": final_summary,
                    }
                )
            else:
                error_text = "verification_unsuccessful"

                with SessionLocal() as session:
                    next_attempt = publish_attempt_count + 1
                    if next_attempt >= args.max_attempts:
                        update_label_work_item_dead(
                            session=session,
                            work_item_id=work_item_id,
                            error_text=error_text,
                        )
                        mark_publish_job_dead(
                            session=session,
                            job_id=publish_job_id,
                            error_text=error_text,
                        )
                    else:
                        delay_seconds = next_backoff_seconds(
                            attempt_count_after_failure=next_attempt,
                            base_seconds=args.backoff_base_seconds,
                        )
                        update_label_work_item_retry(
                            session=session,
                            work_item_id=work_item_id,
                            error_text=error_text,
                            ozone_response=ozone_response,
                            final_summary=final_summary,
                        )
                        mark_publish_job_retry(
                            session=session,
                            job_id=publish_job_id,
                            error_text=error_text,
                            delay_seconds=delay_seconds,
                        )
                    session.commit()

                print_json_line(
                    {
                        "event": "label_apply_retry_scheduled",
                        "checked_at": iso_now(),
                        "worker_id": args.worker_id,
                        "publish_job_id": publish_job_id,
                        "work_item_id": work_item_id,
                        "post_url": post_url,
                        "post_uri": at_uri,
                        "label_value": label_value,
                        "ozone_event_id": (ozone_response or {}).get("id"),
                        "ozone_created_at": (ozone_response or {}).get("createdAt"),
                        "final_summary": final_summary,
                        "error": error_text,
                    }
                )

            if args.publish_sleep_seconds > 0:
                time.sleep(args.publish_sleep_seconds)

        if rate_limited_this_batch:
            continue


if __name__ == "__main__":
    main()