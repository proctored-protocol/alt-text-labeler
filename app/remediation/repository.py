from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def jsonb_param(value: dict | list | None) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False)


@dataclass(frozen=True, slots=True)
class LeasedRemediationRef:
    id: int
    publish_job_id: int
    uri: str
    cid: str
    label_value: str
    published_at: datetime
    attempt_no: int


def seed_remediation_jobs(
    session: Session,
    *,
    first_delay_seconds: int,
    now: datetime | None = None,
) -> int:
    now = now or utc_now()

    result = session.execute(
        text("""
            INSERT INTO visibility_remediation (
                publish_job_id,
                status,
                attempt_count,
                next_attempt_at,
                created_at,
                updated_at
            )
            SELECT
                pj.id,
                'pending',
                0,
                pj.published_at + (:first_delay_seconds * INTERVAL '1 second'),
                :now,
                :now
            FROM visibility_check vc
            JOIN publish_job pj
              ON pj.id = vc.publish_job_id
            LEFT JOIN visibility_remediation vr
              ON vr.publish_job_id = pj.id
            WHERE vc.status = 'not_visible'
              AND pj.status = 'published'
              AND pj.published_at IS NOT NULL
              AND vr.id IS NULL
        """),
        {
            "now": now,
            "first_delay_seconds": first_delay_seconds,
        },
    )
    return int(result.rowcount or 0)


def count_remediation_backlog(
    session: Session,
    *,
    now: datetime | None = None,
) -> int:
    now = now or utc_now()

    result = session.execute(
        text("""
            SELECT COUNT(*) AS n
            FROM visibility_remediation vr
            WHERE (
                    (vr.status = 'pending' AND vr.next_attempt_at <= :now)
                 OR (vr.status = 'leased' AND vr.lease_until IS NOT NULL AND vr.lease_until < :now)
            )
        """),
        {"now": now},
    ).mappings().one()

    return int(result["n"])


def lease_remediation_batch(
    session: Session,
    *,
    worker_name: str,
    batch_size: int,
    lease_seconds: int,
    max_attempts: int,
    now: datetime | None = None,
) -> list[LeasedRemediationRef]:
    now = now or utc_now()
    lease_until = now + timedelta(seconds=lease_seconds)

    rows = session.execute(
        text("""
            SELECT
                vr.id,
                vr.publish_job_id,
                vr.attempt_count,
                pj.uri,
                pj.cid,
                pj.label_value,
                pj.published_at
            FROM visibility_remediation vr
            JOIN publish_job pj
              ON pj.id = vr.publish_job_id
            WHERE vr.attempt_count < :max_attempts
              AND (
                    (vr.status = 'pending' AND vr.next_attempt_at <= :now)
                 OR (vr.status = 'leased' AND vr.lease_until IS NOT NULL AND vr.lease_until < :now)
              )
            ORDER BY
                vr.attempt_count DESC,
                vr.next_attempt_at ASC,
                vr.id ASC
            LIMIT :batch_size
            FOR UPDATE OF vr SKIP LOCKED
        """),
        {
            "now": now,
            "batch_size": batch_size,
            "max_attempts": max_attempts,
        },
    ).mappings().all()

    leased: list[LeasedRemediationRef] = []

    for row in rows:
        session.execute(
            text("""
                UPDATE visibility_remediation
                SET
                    status = 'leased',
                    lease_owner = :worker_name,
                    lease_until = :lease_until,
                    updated_at = :now
                WHERE id = :id
            """),
            {
                "id": row["id"],
                "worker_name": worker_name,
                "lease_until": lease_until,
                "now": now,
            },
        )

        attempt_no = int(row["attempt_count"]) + 1

        leased.append(
            LeasedRemediationRef(
                id=int(row["id"]),
                publish_job_id=int(row["publish_job_id"]),
                uri=str(row["uri"]),
                cid=str(row["cid"]),
                label_value=str(row["label_value"]),
                published_at=row["published_at"],
                attempt_no=attempt_no,
            )
        )

    return leased


def get_leased_remediation_for_worker(
    session: Session,
    *,
    remediation_id: int,
    worker_name: str,
):
    return session.execute(
        text("""
            SELECT
                vr.id,
                vr.publish_job_id,
                vr.status,
                vr.attempt_count,
                pj.uri,
                pj.cid,
                pj.label_value,
                pj.published_at
            FROM visibility_remediation vr
            JOIN publish_job pj
              ON pj.id = vr.publish_job_id
            WHERE vr.id = :id
              AND vr.status = 'leased'
              AND vr.lease_owner = :worker_name
            FOR UPDATE OF vr
        """),
        {
            "id": remediation_id,
            "worker_name": worker_name,
        },
    ).mappings().one_or_none()


def mark_remediation_visible(
    session: Session,
    *,
    remediation_id: int,
    attempt_no: int,
    now: datetime,
    http_status: int | None,
    response_json: dict | list | None,
    unlabel_event_id: str | None,
    relabel_event_id: str | None,
) -> None:
    if attempt_no == 1:
        session.execute(
            text("""
                UPDATE visibility_remediation
                SET
                    status = 'visible_after_first',
                    attempt_count = 1,
                    first_attempt_at = COALESCE(first_attempt_at, :now),
                    first_found_label = TRUE,
                    first_unlabel_event_id = COALESCE(:unlabel_event_id, first_unlabel_event_id),
                    first_relabel_event_id = COALESCE(:relabel_event_id, first_relabel_event_id),
                    resolved_at = :now,
                    last_checked_at = :now,
                    last_http_status = :http_status,
                    last_error_code = NULL,
                    last_error_text = NULL,
                    last_response_json = CAST(:response_json AS jsonb),
                    lease_owner = NULL,
                    lease_until = NULL,
                    updated_at = :now
                WHERE id = :id
            """),
            {
                "id": remediation_id,
                "now": now,
                "http_status": http_status,
                "response_json": jsonb_param(response_json),
                "unlabel_event_id": unlabel_event_id,
                "relabel_event_id": relabel_event_id,
            },
        )
        return

    session.execute(
        text("""
            UPDATE visibility_remediation
            SET
                status = 'visible_after_second',
                attempt_count = 2,
                second_attempt_at = COALESCE(second_attempt_at, :now),
                second_found_label = TRUE,
                second_unlabel_event_id = COALESCE(:unlabel_event_id, second_unlabel_event_id),
                second_relabel_event_id = COALESCE(:relabel_event_id, second_relabel_event_id),
                resolved_at = :now,
                last_checked_at = :now,
                last_http_status = :http_status,
                last_error_code = NULL,
                last_error_text = NULL,
                last_response_json = CAST(:response_json AS jsonb),
                lease_owner = NULL,
                lease_until = NULL,
                updated_at = :now
            WHERE id = :id
        """),
        {
            "id": remediation_id,
            "now": now,
            "http_status": http_status,
            "response_json": jsonb_param(response_json),
            "unlabel_event_id": unlabel_event_id,
            "relabel_event_id": relabel_event_id,
        },
    )


def mark_remediation_not_found(
    session: Session,
    *,
    remediation_id: int,
    attempt_no: int,
    now: datetime,
    http_status: int | None,
    error_code: str,
    error_text: str,
    response_json: dict | list | None,
    unlabel_event_id: str | None,
    relabel_event_id: str | None,
) -> None:
    if attempt_no == 1:
        session.execute(
            text("""
                UPDATE visibility_remediation
                SET
                    status = 'not_found',
                    attempt_count = 1,
                    first_attempt_at = COALESCE(first_attempt_at, :now),
                    first_found_label = FALSE,
                    first_unlabel_event_id = COALESCE(:unlabel_event_id, first_unlabel_event_id),
                    first_relabel_event_id = COALESCE(:relabel_event_id, first_relabel_event_id),
                    resolved_at = :now,
                    last_checked_at = :now,
                    last_http_status = :http_status,
                    last_error_code = :error_code,
                    last_error_text = :error_text,
                    last_response_json = CAST(:response_json AS jsonb),
                    lease_owner = NULL,
                    lease_until = NULL,
                    updated_at = :now
                WHERE id = :id
            """),
            {
                "id": remediation_id,
                "now": now,
                "http_status": http_status,
                "error_code": error_code,
                "error_text": error_text,
                "response_json": jsonb_param(response_json),
                "unlabel_event_id": unlabel_event_id,
                "relabel_event_id": relabel_event_id,
            },
        )
        return

    session.execute(
        text("""
            UPDATE visibility_remediation
            SET
                status = 'not_found',
                attempt_count = 2,
                second_attempt_at = COALESCE(second_attempt_at, :now),
                second_found_label = FALSE,
                second_unlabel_event_id = COALESCE(:unlabel_event_id, second_unlabel_event_id),
                second_relabel_event_id = COALESCE(:relabel_event_id, second_relabel_event_id),
                resolved_at = :now,
                last_checked_at = :now,
                last_http_status = :http_status,
                last_error_code = :error_code,
                last_error_text = :error_text,
                last_response_json = CAST(:response_json AS jsonb),
                lease_owner = NULL,
                lease_until = NULL,
                updated_at = :now
            WHERE id = :id
        """),
        {
            "id": remediation_id,
            "now": now,
            "http_status": http_status,
            "error_code": error_code,
            "error_text": error_text,
            "response_json": jsonb_param(response_json),
            "unlabel_event_id": unlabel_event_id,
            "relabel_event_id": relabel_event_id,
        },
    )


def mark_remediation_schedule_second(
    session: Session,
    *,
    remediation_id: int,
    published_at: datetime,
    second_delay_seconds: int,
    now: datetime,
    http_status: int | None,
    response_json: dict | list | None,
    error_code: str,
    error_text: str,
    unlabel_event_id: str | None,
    relabel_event_id: str | None,
) -> None:
    next_attempt_at = max(
        published_at + timedelta(seconds=second_delay_seconds),
        now,
    )

    session.execute(
        text("""
            UPDATE visibility_remediation
            SET
                status = 'pending',
                attempt_count = 1,
                next_attempt_at = :next_attempt_at,
                first_attempt_at = COALESCE(first_attempt_at, :now),
                first_found_label = FALSE,
                first_unlabel_event_id = COALESCE(:unlabel_event_id, first_unlabel_event_id),
                first_relabel_event_id = COALESCE(:relabel_event_id, first_relabel_event_id),
                last_checked_at = :now,
                last_http_status = :http_status,
                last_error_code = :error_code,
                last_error_text = :error_text,
                last_response_json = CAST(:response_json AS jsonb),
                lease_owner = NULL,
                lease_until = NULL,
                updated_at = :now
            WHERE id = :id
        """),
        {
            "id": remediation_id,
            "now": now,
            "next_attempt_at": next_attempt_at,
            "http_status": http_status,
            "error_code": error_code,
            "error_text": error_text,
            "response_json": jsonb_param(response_json),
            "unlabel_event_id": unlabel_event_id,
            "relabel_event_id": relabel_event_id,
        },
    )


def mark_remediation_gave_up(
    session: Session,
    *,
    remediation_id: int,
    now: datetime,
    http_status: int | None,
    response_json: dict | list | None,
    error_code: str,
    error_text: str,
    unlabel_event_id: str | None,
    relabel_event_id: str | None,
) -> None:
    session.execute(
        text("""
            UPDATE visibility_remediation
            SET
                status = 'gave_up',
                attempt_count = 2,
                second_attempt_at = COALESCE(second_attempt_at, :now),
                second_found_label = FALSE,
                second_unlabel_event_id = COALESCE(:unlabel_event_id, second_unlabel_event_id),
                second_relabel_event_id = COALESCE(:relabel_event_id, second_relabel_event_id),
                resolved_at = :now,
                last_checked_at = :now,
                last_http_status = :http_status,
                last_error_code = :error_code,
                last_error_text = :error_text,
                last_response_json = CAST(:response_json AS jsonb),
                lease_owner = NULL,
                lease_until = NULL,
                updated_at = :now
            WHERE id = :id
        """),
        {
            "id": remediation_id,
            "now": now,
            "http_status": http_status,
            "error_code": error_code,
            "error_text": error_text,
            "response_json": jsonb_param(response_json),
            "unlabel_event_id": unlabel_event_id,
            "relabel_event_id": relabel_event_id,
        },
    )