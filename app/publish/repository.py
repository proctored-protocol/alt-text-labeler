from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import and_, func, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.models import PublishAttempt, PublishJob, WorkerHeartbeat


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class LeasedPublishJobRef:
    id: int
    uri: str
    label_value: str


def lease_publish_batch(
    session: Session,
    *,
    worker_name: str,
    batch_size: int,
    lease_seconds: int,
    max_attempts: int,
    now: datetime | None = None,
) -> list[LeasedPublishJobRef]:
    now = now or utc_now()
    lease_until = now + timedelta(seconds=lease_seconds)

    rows = (
        session.execute(
            select(PublishJob)
            .where(
                or_(
                    and_(
                        PublishJob.status == "pending",
                        PublishJob.next_attempt_at <= now,
                    ),
                    and_(
                        PublishJob.status == "leased",
                        PublishJob.lease_until.is_not(None),
                        PublishJob.lease_until < now,
                    ),
                ),
                PublishJob.attempt_count < max_attempts,
            )
            .order_by(PublishJob.id.asc())
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
        .scalars()
        .all()
    )

    leased: list[LeasedPublishJobRef] = []

    for row in rows:
        row.status = "leased"
        row.lease_owner = worker_name
        row.lease_until = lease_until

        leased.append(
            LeasedPublishJobRef(
                id=row.id,
                uri=row.uri,
                label_value=row.label_value,
            )
        )

    session.flush()
    return leased


def get_leased_publish_job_for_worker(
    session: Session,
    *,
    publish_job_id: int,
    worker_name: str,
) -> PublishJob | None:
    return (
        session.execute(
            select(PublishJob)
            .where(
                PublishJob.id == publish_job_id,
                PublishJob.status == "leased",
                PublishJob.lease_owner == worker_name,
            )
            .with_for_update()
        )
        .scalar_one_or_none()
    )


def mark_attempt_started(row: PublishJob) -> int:
    row.attempt_count += 1
    return int(row.attempt_count)


def insert_publish_attempt(
    session: Session,
    *,
    publish_job_id: int,
    attempt_no: int,
    attempted_at: datetime,
    success: bool,
    http_status: int | None,
    error_code: str | None,
    error_text: str | None,
    response_json: dict | None,
) -> None:
    session.add(
        PublishAttempt(
            publish_job_id=publish_job_id,
            attempt_no=attempt_no,
            attempted_at=attempted_at,
            success=success,
            http_status=http_status,
            error_code=error_code,
            error_text=error_text,
            response_json=response_json,
        )
    )


def mark_job_published(
    row: PublishJob,
    *,
    published_at: datetime,
) -> None:
    row.status = "published"
    row.published_at = published_at
    row.lease_owner = None
    row.lease_until = None
    row.last_error_code = None
    row.last_error_text = None


def mark_job_retry_or_error(
    row: PublishJob,
    *,
    error_code: str,
    error_text: str,
    max_attempts: int,
    backoff_base_seconds: int,
    now: datetime | None = None,
) -> None:
    now = now or utc_now()

    row.last_error_code = error_code
    row.last_error_text = error_text
    row.lease_owner = None
    row.lease_until = None

    if row.attempt_count >= max_attempts:
        row.status = "error"
        return

    backoff_seconds = backoff_base_seconds * (2 ** max(0, row.attempt_count - 1))
    row.status = "pending"
    row.next_attempt_at = now + timedelta(seconds=backoff_seconds)


def count_publish_backlog(session: Session, *, now: datetime | None = None) -> int:
    now = now or utc_now()

    stmt = select(func.count()).select_from(PublishJob).where(
        or_(
            and_(
                PublishJob.status == "pending",
                PublishJob.next_attempt_at <= now,
            ),
            and_(
                PublishJob.status == "leased",
                PublishJob.lease_until.is_not(None),
                PublishJob.lease_until < now,
            ),
        )
    )

    return int(session.execute(stmt).scalar_one())


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