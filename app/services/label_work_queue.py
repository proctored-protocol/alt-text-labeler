from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session


def enqueue_label_work_item(
    session: Session,
    *,
    uri: str,
    cid: str,
    label_value: str,
    post_url: str | None,
    record_created_at: str | None,
    evaluated_at: str | None,
) -> dict[str, Any]:
    session.execute(
        text(
            """
            INSERT INTO label_work_item (
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
                last_error,
                raw_result_json,
                created_at,
                updated_at
            ) VALUES (
                :uri,
                :cid,
                :label_value,
                :post_url,
                :record_created_at,
                :evaluated_at,
                'queued',
                0,
                NOW(),
                NULL,
                NULL,
                NULL,
                NULL,
                NOW(),
                NOW()
            )
            ON CONFLICT (uri, cid, label_value) DO NOTHING
            """
        ),
        {
            "uri": uri,
            "cid": cid,
            "label_value": label_value,
            "post_url": post_url,
            "record_created_at": record_created_at,
            "evaluated_at": evaluated_at,
        },
    )

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
            WHERE uri = :uri AND cid = :cid AND label_value = :label_value
            """
        ),
        {
            "uri": uri,
            "cid": cid,
            "label_value": label_value,
        },
    ).mappings().one()

    return dict(row)


def lease_label_work_items(
    session: Session,
    *,
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
            UPDATE label_work_item AS j
            SET
                state = 'leased',
                attempt_count = j.attempt_count + 1,
                leased_until = NOW() + (:lease_seconds * INTERVAL '1 second'),
                leased_by = :worker_id,
                updated_at = NOW()
            FROM candidates
            WHERE j.id = candidates.id
            RETURNING
                j.id,
                j.uri,
                j.cid,
                j.label_value,
                j.post_url,
                j.record_created_at,
                j.evaluated_at,
                j.state,
                j.attempt_count,
                j.next_attempt_at,
                j.leased_until,
                j.leased_by,
                j.ozone_event_id,
                j.ozone_created_at,
                j.final_forced_status_code,
                j.final_query_status_code,
                j.final_subscriber_status_code,
                j.final_forced_found_label,
                j.final_query_found_label,
                j.final_subscriber_found_label,
                j.manual_success,
                j.label_visible_at,
                j.last_error,
                j.raw_result_json,
                j.created_at,
                j.updated_at
            """
        ),
        {
            "worker_id": worker_id,
            "batch_size": batch_size,
            "lease_seconds": lease_seconds,
        },
    ).mappings().all()

    return [dict(row) for row in rows]


def _extract_result_fields(result: dict[str, Any]) -> dict[str, Any]:
    ozone_response = result.get("ozone_response") or {}
    attempts = result.get("verification_attempts") or []

    final_summary: dict[str, Any] = {}
    if attempts:
        final_summary = (attempts[-1] or {}).get("summary") or {}

    query_labels = final_summary.get("query_labels") or {}
    forced_hydration = final_summary.get("forced_hydration") or {}
    subscriber_hydration = final_summary.get("subscriber_hydration") or {}

    return {
        "ozone_event_id": ozone_response.get("id"),
        "ozone_created_at": ozone_response.get("createdAt"),
        "final_forced_status_code": forced_hydration.get("status_code"),
        "final_query_status_code": query_labels.get("status_code"),
        "final_subscriber_status_code": subscriber_hydration.get("status_code"),
        "final_forced_found_label": forced_hydration.get("found_label"),
        "final_query_found_label": query_labels.get("found_label"),
        "final_subscriber_found_label": subscriber_hydration.get("found_label"),
        "manual_success": result.get("success"),
        "raw_result_json": json.dumps(result, ensure_ascii=False, default=str),
    }


def mark_label_work_item_visible(
    session: Session,
    *,
    job_id: int,
    result: dict[str, Any],
) -> None:
    fields = _extract_result_fields(result)

    session.execute(
        text(
            """
            UPDATE label_work_item
            SET
                state = 'visible',
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
                manual_success = :manual_success,
                label_visible_at = NOW(),
                last_error = NULL,
                raw_result_json = :raw_result_json,
                updated_at = NOW()
            WHERE id = :job_id
            """
        ),
        {
            "job_id": job_id,
            **fields,
        },
    )


def mark_label_work_item_retry(
    session: Session,
    *,
    job_id: int,
    result: dict[str, Any] | None,
    error_text: str,
    delay_seconds: int,
) -> None:
    fields = _extract_result_fields(result) if result is not None else {
        "ozone_event_id": None,
        "ozone_created_at": None,
        "final_forced_status_code": None,
        "final_query_status_code": None,
        "final_subscriber_status_code": None,
        "final_forced_found_label": None,
        "final_query_found_label": None,
        "final_subscriber_found_label": None,
        "manual_success": None,
        "raw_result_json": None,
    }

    session.execute(
        text(
            """
            UPDATE label_work_item
            SET
                state = 'queued',
                next_attempt_at = NOW() + (:delay_seconds * INTERVAL '1 second'),
                leased_until = NULL,
                leased_by = NULL,
                ozone_event_id = COALESCE(:ozone_event_id, ozone_event_id),
                ozone_created_at = COALESCE(CAST(:ozone_created_at AS timestamptz), ozone_created_at),
                final_forced_status_code = :final_forced_status_code,
                final_query_status_code = :final_query_status_code,
                final_subscriber_status_code = :final_subscriber_status_code,
                final_forced_found_label = :final_forced_found_label,
                final_query_found_label = :final_query_found_label,
                final_subscriber_found_label = :final_subscriber_found_label,
                manual_success = :manual_success,
                last_error = :last_error,
                raw_result_json = COALESCE(:raw_result_json, raw_result_json),
                updated_at = NOW()
            WHERE id = :job_id
            """
        ),
        {
            "job_id": job_id,
            "delay_seconds": delay_seconds,
            "last_error": error_text,
            **fields,
        },
    )


def mark_label_work_item_dead(
    session: Session,
    *,
    job_id: int,
    result: dict[str, Any] | None,
    error_text: str,
) -> None:
    fields = _extract_result_fields(result) if result is not None else {
        "ozone_event_id": None,
        "ozone_created_at": None,
        "final_forced_status_code": None,
        "final_query_status_code": None,
        "final_subscriber_status_code": None,
        "final_forced_found_label": None,
        "final_query_found_label": None,
        "final_subscriber_found_label": None,
        "manual_success": None,
        "raw_result_json": None,
    }

    session.execute(
        text(
            """
            UPDATE label_work_item
            SET
                state = 'dead',
                leased_until = NULL,
                leased_by = NULL,
                ozone_event_id = COALESCE(:ozone_event_id, ozone_event_id),
                ozone_created_at = COALESCE(CAST(:ozone_created_at AS timestamptz), ozone_created_at),
                final_forced_status_code = :final_forced_status_code,
                final_query_status_code = :final_query_status_code,
                final_subscriber_status_code = :final_subscriber_status_code,
                final_forced_found_label = :final_forced_found_label,
                final_query_found_label = :final_query_found_label,
                final_subscriber_found_label = :final_subscriber_found_label,
                manual_success = :manual_success,
                last_error = :last_error,
                raw_result_json = COALESCE(:raw_result_json, raw_result_json),
                updated_at = NOW()
            WHERE id = :job_id
            """
        ),
        {
            "job_id": job_id,
            "last_error": error_text,
            **fields,
        },
    )


def next_backoff_seconds(*, attempt_count: int, base_seconds: int) -> int:
    return min(base_seconds * (2 ** max(0, attempt_count - 1)), 1800)


def count_by_state(session: Session) -> list[dict[str, Any]]:
    rows = session.execute(
        text(
            """
            SELECT state, count(*) AS n
            FROM label_work_item
            GROUP BY state
            ORDER BY state
            """
        )
    ).mappings().all()
    return [dict(row) for row in rows]