from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.models import WorkerHeartbeat


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def jsonb_param(value: dict | list | None) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False)


@dataclass(frozen=True, slots=True)
class LeasedVisibilityCheckRef:
    id: int
    publish_job_id: int
    uri: str
    cid: str
    label_value: str
    published_at: datetime


def seed_visibility_checks(
    session: Session,
    *,
    max_age_seconds: int,
    initial_delay_seconds: int,
    now: datetime | None = None,
) -> int:
    now = now or utc_now()

    result = session.execute(
        text("""
            INSERT INTO visibility_check (
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
                pj.published_at + (:initial_delay_seconds * INTERVAL '1 second'),
                :now,
                :now
            FROM publish_job pj
            LEFT JOIN visibility_check vc
                ON vc.publish_job_id = pj.id
            WHERE pj.status = 'published'
              AND pj.published_at IS NOT NULL
              AND pj.published_at >= (:now - (:max_age_seconds * INTERVAL '1 second'))
              AND vc.id IS NULL
        """),
        {
            "now": now,
            "max_age_seconds": max_age_seconds,
            "initial_delay_seconds": initial_delay_seconds,
        },
    )
    return int(result.rowcount or 0)


def mark_old_pending_timeouts(
    session: Session,
    *,
    max_age_seconds: int,
    now: datetime | None = None,
) -> int:
    now = now or utc_now()

    result = session.execute(
        text("""
            UPDATE visibility_check vc
            SET
                status = 'timeout',
                lease_owner = NULL,
                lease_until = NULL,
                last_checked_at = :now,
                last_error_code = 'visibility_timeout',
                last_error_text = 'baseline forced hydration check not completed within max age window',
                updated_at = :now
            FROM publish_job pj
            WHERE pj.id = vc.publish_job_id
              AND pj.status = 'published'
              AND vc.status IN ('pending', 'leased')
              AND pj.published_at IS NOT NULL
              AND pj.published_at < (:now - (:max_age_seconds * INTERVAL '1 second'))
        """),
        {
            "now": now,
            "max_age_seconds": max_age_seconds,
        },
    )
    return int(result.rowcount or 0)


def count_visibility_backlog(
    session: Session,
    *,
    now: datetime | None = None,
) -> int:
    now = now or utc_now()

    result = session.execute(
        text("""
            SELECT COUNT(*) AS n
            FROM visibility_check vc
            JOIN publish_job pj
              ON pj.id = vc.publish_job_id
            WHERE pj.status = 'published'
              AND (
                    (vc.status = 'pending' AND vc.next_attempt_at <= :now)
                 OR (vc.status = 'leased' AND vc.lease_until IS NOT NULL AND vc.lease_until < :now)
              )
        """),
        {"now": now},
    ).mappings().one()

    return int(result["n"])


def lease_visibility_batch(
    session: Session,
    *,
    worker_name: str,
    batch_size: int,
    lease_seconds: int,
    max_attempts: int,
    now: datetime | None = None,
) -> list[LeasedVisibilityCheckRef]:
    now = now or utc_now()
    lease_until = now + timedelta(seconds=lease_seconds)

    rows = session.execute(
        text("""
            SELECT
                vc.id,
                vc.publish_job_id,
                pj.uri,
                pj.cid,
                pj.label_value,
                pj.published_at
            FROM visibility_check vc
            JOIN publish_job pj
              ON pj.id = vc.publish_job_id
            WHERE pj.status = 'published'
              AND vc.attempt_count < :max_attempts
              AND (
                    (vc.status = 'pending' AND vc.next_attempt_at <= :now)
                 OR (vc.status = 'leased' AND vc.lease_until IS NOT NULL AND vc.lease_until < :now)
              )
            ORDER BY vc.next_attempt_at ASC, pj.published_at ASC, vc.id ASC
            LIMIT :batch_size
            FOR UPDATE OF vc SKIP LOCKED
        """),
        {
            "now": now,
            "batch_size": batch_size,
            "max_attempts": max_attempts,
        },
    ).mappings().all()

    leased: list[LeasedVisibilityCheckRef] = []

    for row in rows:
        session.execute(
            text("""
                UPDATE visibility_check
                SET
                    status = 'leased',
                    attempt_count = attempt_count + 1,
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

        leased.append(
            LeasedVisibilityCheckRef(
                id=int(row["id"]),
                publish_job_id=int(row["publish_job_id"]),
                uri=str(row["uri"]),
                cid=str(row["cid"]),
                label_value=str(row["label_value"]),
                published_at=row["published_at"],
            )
        )

    return leased


def get_leased_visibility_check_for_worker(
    session: Session,
    *,
    visibility_check_id: int,
    worker_name: str,
):
    return session.execute(
        text("""
            SELECT
                vc.id,
                vc.publish_job_id,
                vc.status,
                vc.attempt_count,
                vc.last_checked_at,
                vc.last_http_status,
                vc.last_error_code,
                vc.last_error_text,
                pj.uri,
                pj.cid,
                pj.label_value,
                pj.published_at
            FROM visibility_check vc
            JOIN publish_job pj
              ON pj.id = vc.publish_job_id
            WHERE vc.id = :id
              AND vc.status = 'leased'
              AND vc.lease_owner = :worker_name
            FOR UPDATE OF vc
        """),
        {
            "id": visibility_check_id,
            "worker_name": worker_name,
        },
    ).mappings().one_or_none()


def mark_visibility_visible(
    session: Session,
    *,
    visibility_check_id: int,
    now: datetime,
    http_status: int,
    response_json: dict | list | None,
) -> None:
    session.execute(
        text("""
            UPDATE visibility_check
            SET
                status = 'visible',
                visible_at = COALESCE(visible_at, :now),
                forced_found = TRUE,
                forced_status_code = :http_status,
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
            "id": visibility_check_id,
            "now": now,
            "http_status": http_status,
            "response_json": jsonb_param(response_json),
        },
    )


def mark_visibility_not_visible(
    session: Session,
    *,
    visibility_check_id: int,
    now: datetime,
    http_status: int,
    response_json: dict | list | None,
) -> None:
    session.execute(
        text("""
            UPDATE visibility_check
            SET
                status = 'not_visible',
                forced_found = FALSE,
                forced_status_code = :http_status,
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
            "id": visibility_check_id,
            "now": now,
            "http_status": http_status,
            "response_json": jsonb_param(response_json),
        },
    )


def mark_visibility_not_found(
    session: Session,
    *,
    visibility_check_id: int,
    now: datetime,
    http_status: int | None,
    error_code: str,
    error_text: str,
    response_json: dict | list | None,
) -> None:
    session.execute(
        text("""
            UPDATE visibility_check
            SET
                status = 'not_found',
                forced_found = FALSE,
                forced_status_code = :http_status,
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
            "id": visibility_check_id,
            "now": now,
            "http_status": http_status,
            "error_code": error_code,
            "error_text": error_text,
            "response_json": jsonb_param(response_json),
        },
    )


def mark_visibility_retry_or_error(
    session: Session,
    *,
    visibility_check_id: int,
    attempt_count: int,
    published_at: datetime | None,
    now: datetime,
    retry_seconds: int,
    max_age_seconds: int,
    max_attempts: int,
    http_status: int | None,
    error_code: str,
    error_text: str,
    response_json: dict | list | None,
    retryable: bool,
) -> None:
    timed_out = False
    if published_at is not None:
        timed_out = (now - published_at).total_seconds() >= max_age_seconds

    if timed_out:
        session.execute(
            text("""
                UPDATE visibility_check
                SET
                    status = 'timeout',
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
                "id": visibility_check_id,
                "now": now,
                "http_status": http_status,
                "error_code": error_code,
                "error_text": error_text,
                "response_json": jsonb_param(response_json),
            },
        )
        return

    if retryable and attempt_count < max_attempts:
        next_check_at = now + timedelta(seconds=retry_seconds)
        session.execute(
            text("""
                UPDATE visibility_check
                SET
                    status = 'pending',
                    next_check_at = :next_check_at,
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
                "id": visibility_check_id,
                "now": now,
                "next_check_at": next_check_at,
                "http_status": http_status,
                "error_code": error_code,
                "error_text": error_text,
                "response_json": jsonb_param(response_json),
            },
        )
        return

    session.execute(
        text("""
            UPDATE visibility_check
            SET
                status = 'error',
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
            "id": visibility_check_id,
            "now": now,
            "http_status": http_status,
            "error_code": error_code,
            "error_text": error_text,
            "response_json": jsonb_param(response_json),
        },
    )


def upsert_worker_heartbeat(
    session: Session,
    *,
    worker_name: str,
    stage: str,
    status: str,
    started_at: datetime | None,
    heartbeat_at: datetime,
    host: str | None,
    pid: int | None,
    lease_count: int | None,
    backlog_depth: int | None,
    last_error_code: str | None = None,
    last_error_text: str | None = None,
    meta_json: dict | None = None,
) -> None:
    stmt = (
        insert(WorkerHeartbeat)
        .values(
            worker_name=worker_name,
            stage=stage,
            host=host,
            pid=pid,
            status=status,
            started_at=started_at,
            heartbeat_at=heartbeat_at,
            lease_count=lease_count,
            backlog_depth=backlog_depth,
            last_error_code=last_error_code,
            last_error_text=last_error_text,
            meta_json=meta_json,
        )
        .on_conflict_do_update(
            index_elements=[WorkerHeartbeat.worker_name],
            set_={
                "stage": stage,
                "host": host,
                "pid": pid,
                "status": status,
                "started_at": func.coalesce(WorkerHeartbeat.started_at, started_at),
                "heartbeat_at": heartbeat_at,
                "lease_count": lease_count,
                "backlog_depth": backlog_depth,
                "last_error_code": last_error_code,
                "last_error_text": last_error_text,
                "meta_json": meta_json,
                "updated_at": func.now(),
            },
        )
    )
    session.execute(stmt)