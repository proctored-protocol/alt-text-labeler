from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def enqueue_publish_job(session: Session, *, uri: str, cid: str, label_value: str) -> dict[str, Any]:
    session.execute(
        text(
            """
            INSERT INTO publish_job (
                uri, cid, label_value, state, attempt_count,
                next_attempt_at, leased_until, leased_by, last_error,
                created_at, updated_at
            ) VALUES (
                :uri, :cid, :label_value, 'pending', 0,
                NOW(), NULL, NULL, NULL,
                NOW(), NOW()
            )
            ON CONFLICT (uri, cid, label_value) DO NOTHING
            """
        ),
        {
            "uri": uri,
            "cid": cid,
            "label_value": label_value,
        },
    )

    row = session.execute(
        text(
            """
            SELECT
                id, uri, cid, label_value, state, attempt_count,
                next_attempt_at, leased_until, leased_by, last_error,
                created_at, updated_at
            FROM publish_job
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


def lease_publish_jobs(
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
                FROM publish_job
                WHERE
                    (
                        state = 'pending'
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
            UPDATE publish_job AS j
            SET
                state = 'leased',
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
                j.state,
                j.attempt_count,
                j.next_attempt_at,
                j.leased_until,
                j.leased_by,
                j.last_error,
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


def mark_publish_job_published(session: Session, *, job_id: int) -> None:
    session.execute(
        text(
            """
            UPDATE publish_job
            SET
                state = 'published',
                leased_until = NULL,
                leased_by = NULL,
                last_error = NULL,
                updated_at = NOW()
            WHERE id = :job_id
            """
        ),
        {"job_id": job_id},
    )


def mark_publish_job_retry(
    session: Session,
    *,
    job_id: int,
    error_text: str,
    delay_seconds: int,
) -> None:
    session.execute(
        text(
            """
            UPDATE publish_job
            SET
                state = 'pending',
                attempt_count = attempt_count + 1,
                next_attempt_at = NOW() + (:delay_seconds * INTERVAL '1 second'),
                leased_until = NULL,
                leased_by = NULL,
                last_error = :error_text,
                updated_at = NOW()
            WHERE id = :job_id
            """
        ),
        {
            "job_id": job_id,
            "error_text": error_text,
            "delay_seconds": delay_seconds,
        },
    )


def mark_publish_job_dead(
    session: Session,
    *,
    job_id: int,
    error_text: str,
) -> None:
    session.execute(
        text(
            """
            UPDATE publish_job
            SET
                state = 'dead',
                attempt_count = attempt_count + 1,
                leased_until = NULL,
                leased_by = NULL,
                last_error = :error_text,
                updated_at = NOW()
            WHERE id = :job_id
            """
        ),
        {
            "job_id": job_id,
            "error_text": error_text,
        },
    )


def next_backoff_seconds(*, attempt_count_after_failure: int, base_seconds: int) -> int:
    return min(base_seconds * (2 ** max(0, attempt_count_after_failure - 1)), 1800)