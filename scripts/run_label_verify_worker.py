from __future__ import annotations

import argparse
import json
import socket
import time
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

from app.db import SessionLocal
from scripts.manual_publish_and_verify import ScriptSettings, summarize_snapshot, verify_once


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


def lease_verification_items(
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
                        state = 'published_pending_verification'
                        AND next_attempt_at <= NOW()
                    )
                    OR (
                        state = 'verifying'
                        AND leased_until IS NOT NULL
                        AND leased_until < NOW()
                    )
                ORDER BY ozone_created_at ASC NULLS LAST, id ASC
                LIMIT :batch_size
                FOR UPDATE SKIP LOCKED
            )
            UPDATE label_work_item AS lwi
            SET
                state = 'verifying',
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


def verification_succeeded(final_summary: dict[str, Any] | None) -> bool:
    summary = final_summary or {}
    query_ok = ((summary.get("query_labels") or {}).get("found_label") is True)
    forced_ok = ((summary.get("forced_hydration") or {}).get("found_label") is True)
    return query_ok and forced_ok


def verification_age_seconds(ozone_created_at: Any) -> float:
    if ozone_created_at is None:
        return 0.0
    if isinstance(ozone_created_at, datetime):
        created = ozone_created_at.astimezone(timezone.utc)
    else:
        created = datetime.fromisoformat(str(ozone_created_at).replace("Z", "+00:00")).astimezone(timezone.utc)
    return max(0.0, (utc_now() - created).total_seconds())


def mark_label_work_item_verified(
    *,
    session,
    work_item_id: int,
    final_summary: dict[str, Any],
) -> None:
    fields = extract_final_fields(final_summary)
    raw_result = {
        "phase": "verified",
        "final_summary": final_summary,
    }

    session.execute(
        text(
            """
            UPDATE label_work_item
            SET
                state = 'published',
                leased_until = NULL,
                leased_by = NULL,
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
            "final_forced_status_code": fields["final_forced_status_code"],
            "final_query_status_code": fields["final_query_status_code"],
            "final_subscriber_status_code": fields["final_subscriber_status_code"],
            "final_forced_found_label": fields["final_forced_found_label"],
            "final_query_found_label": fields["final_query_found_label"],
            "final_subscriber_found_label": fields["final_subscriber_found_label"],
            "raw_result_json": json.dumps(raw_result, ensure_ascii=False, default=str),
        },
    )


def mark_label_work_item_verification_pending(
    *,
    session,
    work_item_id: int,
    final_summary: dict[str, Any] | None,
    delay_seconds: int,
    error_text: str | None = None,
) -> None:
    fields = extract_final_fields(final_summary)
    raw_result = {
        "phase": "verification_pending",
        "final_summary": final_summary,
        "error": error_text,
    }

    session.execute(
        text(
            """
            UPDATE label_work_item
            SET
                state = 'published_pending_verification',
                leased_until = NULL,
                leased_by = NULL,
                next_attempt_at = NOW() + (:delay_seconds * INTERVAL '1 second'),
                final_forced_status_code = :final_forced_status_code,
                final_query_status_code = :final_query_status_code,
                final_subscriber_status_code = :final_subscriber_status_code,
                final_forced_found_label = :final_forced_found_label,
                final_query_found_label = :final_query_found_label,
                final_subscriber_found_label = :final_subscriber_found_label,
                last_error = CASE
                    WHEN :last_error IS NULL THEN NULL
                    ELSE :last_error
                END,
                raw_result_json = :raw_result_json,
                updated_at = NOW()
            WHERE id = :work_item_id
            """
        ),
        {
            "work_item_id": work_item_id,
            "delay_seconds": delay_seconds,
            "final_forced_status_code": fields["final_forced_status_code"],
            "final_query_status_code": fields["final_query_status_code"],
            "final_subscriber_status_code": fields["final_subscriber_status_code"],
            "final_forced_found_label": fields["final_forced_found_label"],
            "final_query_found_label": fields["final_query_found_label"],
            "final_subscriber_found_label": fields["final_subscriber_found_label"],
            "last_error": truncate_error(error_text) if error_text else None,
            "raw_result_json": json.dumps(raw_result, ensure_ascii=False, default=str),
        },
    )


def mark_label_work_item_verification_failed(
    *,
    session,
    work_item_id: int,
    final_summary: dict[str, Any] | None,
    error_text: str,
) -> None:
    fields = extract_final_fields(final_summary)
    raw_result = {
        "phase": "verification_failed",
        "final_summary": final_summary,
        "error": error_text,
    }

    session.execute(
        text(
            """
            UPDATE label_work_item
            SET
                state = 'verification_failed',
                leased_until = NULL,
                leased_by = NULL,
                final_forced_status_code = :final_forced_status_code,
                final_query_status_code = :final_query_status_code,
                final_subscriber_status_code = :final_subscriber_status_code,
                final_forced_found_label = :final_forced_found_label,
                final_query_found_label = :final_query_found_label,
                final_subscriber_found_label = :final_subscriber_found_label,
                manual_success = FALSE,
                last_error = :last_error,
                raw_result_json = :raw_result_json,
                updated_at = NOW()
            WHERE id = :work_item_id
            """
        ),
        {
            "work_item_id": work_item_id,
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify label visibility separately from emit workers.")
    parser.add_argument("--worker-id", default=f"verify-{socket.gethostname()}")
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--lease-seconds", type=int, default=180)
    parser.add_argument("--request-timeout-seconds", type=int, default=30)
    parser.add_argument("--verify-retry-seconds", type=int, default=30)
    parser.add_argument("--verification-max-age-seconds", type=int, default=1800)
    parser.add_argument("--idle-sleep-seconds", type=float, default=1.0)
    args = parser.parse_args()

    script_settings = ScriptSettings()
    labeler_did = script_settings.verifier_labeler_did

    print_json_line(
        {
            "event": "label_verify_worker_started",
            "checked_at": iso_now(),
            "worker_id": args.worker_id,
            "batch_size": args.batch_size,
            "lease_seconds": args.lease_seconds,
            "request_timeout_seconds": args.request_timeout_seconds,
            "verify_retry_seconds": args.verify_retry_seconds,
            "verification_max_age_seconds": args.verification_max_age_seconds,
        }
    )

    while True:
        with SessionLocal() as session:
            jobs = lease_verification_items(
                session=session,
                worker_id=args.worker_id,
                batch_size=args.batch_size,
                lease_seconds=args.lease_seconds,
            )
            session.commit()

        if not jobs:
            time.sleep(args.idle_sleep_seconds)
            continue

        for job in jobs:
            work_item_id = int(job["id"])
            post_url = job["post_url"]
            at_uri = job["uri"]
            label_value = job["label_value"]
            ozone_created_at = job["ozone_created_at"]

            print_json_line(
                {
                    "event": "label_verify_started",
                    "checked_at": iso_now(),
                    "worker_id": args.worker_id,
                    "work_item_id": work_item_id,
                    "post_url": post_url,
                    "post_uri": at_uri,
                    "label_value": label_value,
                    "ozone_created_at": ozone_created_at,
                }
            )

            final_summary: dict[str, Any] | None = None

            try:
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
                    with SessionLocal() as session:
                        mark_label_work_item_verified(
                            session=session,
                            work_item_id=work_item_id,
                            final_summary=final_summary,
                        )
                        session.commit()

                    print_json_line(
                        {
                            "event": "label_verify_succeeded",
                            "checked_at": iso_now(),
                            "worker_id": args.worker_id,
                            "work_item_id": work_item_id,
                            "post_url": post_url,
                            "post_uri": at_uri,
                            "label_value": label_value,
                            "final_summary": final_summary,
                        }
                    )
                    continue

                age_seconds = verification_age_seconds(ozone_created_at)
                if age_seconds >= args.verification_max_age_seconds:
                    with SessionLocal() as session:
                        mark_label_work_item_verification_failed(
                            session=session,
                            work_item_id=work_item_id,
                            final_summary=final_summary,
                            error_text="verification_unsuccessful",
                        )
                        session.commit()

                    print_json_line(
                        {
                            "event": "label_verify_failed_terminal",
                            "checked_at": iso_now(),
                            "worker_id": args.worker_id,
                            "work_item_id": work_item_id,
                            "post_url": post_url,
                            "post_uri": at_uri,
                            "label_value": label_value,
                            "age_seconds": age_seconds,
                            "final_summary": final_summary,
                            "error": "verification_unsuccessful",
                        }
                    )
                    continue

                with SessionLocal() as session:
                    mark_label_work_item_verification_pending(
                        session=session,
                        work_item_id=work_item_id,
                        final_summary=final_summary,
                        delay_seconds=args.verify_retry_seconds,
                        error_text=None,
                    )
                    session.commit()

                print_json_line(
                    {
                        "event": "label_verify_retry_scheduled",
                        "checked_at": iso_now(),
                        "worker_id": args.worker_id,
                        "work_item_id": work_item_id,
                        "post_url": post_url,
                        "post_uri": at_uri,
                        "label_value": label_value,
                        "age_seconds": age_seconds,
                        "final_summary": final_summary,
                    }
                )

            except Exception as exc:
                error_text = str(exc)
                age_seconds = verification_age_seconds(ozone_created_at)

                with SessionLocal() as session:
                    if age_seconds >= args.verification_max_age_seconds:
                        mark_label_work_item_verification_failed(
                            session=session,
                            work_item_id=work_item_id,
                            final_summary=final_summary,
                            error_text=error_text,
                        )
                    else:
                        mark_label_work_item_verification_pending(
                            session=session,
                            work_item_id=work_item_id,
                            final_summary=final_summary,
                            delay_seconds=args.verify_retry_seconds,
                            error_text=error_text,
                        )
                    session.commit()

                print_json_line(
                    {
                        "event": "label_verify_failed",
                        "checked_at": iso_now(),
                        "worker_id": args.worker_id,
                        "work_item_id": work_item_id,
                        "post_url": post_url,
                        "post_uri": at_uri,
                        "label_value": label_value,
                        "age_seconds": age_seconds,
                        "error": truncate_error(error_text),
                    }
                )


if __name__ == "__main__":
    main()