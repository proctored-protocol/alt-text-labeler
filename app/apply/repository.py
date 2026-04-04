from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import and_, func, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.models import (
    IntakeItem,
    LabelDecision,
    ManualOverride,
    PublishJob,
    WorkerHeartbeat,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class LeasedIntakeItemRef:
    id: int
    uri: str
    firehose_seq: int


def lease_apply_batch(
    session: Session,
    *,
    worker_name: str,
    batch_size: int,
    lease_seconds: int,
    max_attempts: int,
    now: datetime | None = None,
) -> list[LeasedIntakeItemRef]:
    now = now or utc_now()
    lease_until = now + timedelta(seconds=lease_seconds)

    rows = (
        session.execute(
            select(IntakeItem)
            .where(
                or_(
                    and_(
                        IntakeItem.apply_status == "pending",
                        IntakeItem.apply_next_attempt_at <= now,
                    ),
                    and_(
                        IntakeItem.apply_status == "leased",
                        IntakeItem.apply_lease_until.is_not(None),
                        IntakeItem.apply_lease_until < now,
                    ),
                ),
                IntakeItem.apply_attempt_count < max_attempts,
            )
            .order_by(IntakeItem.firehose_seq.asc(), IntakeItem.id.asc())
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
        .scalars()
        .all()
    )

    leased: list[LeasedIntakeItemRef] = []

    for row in rows:
        row.apply_status = "leased"
        row.apply_attempt_count += 1
        row.apply_lease_owner = worker_name
        row.apply_lease_until = lease_until

        leased.append(
            LeasedIntakeItemRef(
                id=row.id,
                uri=row.uri,
                firehose_seq=row.firehose_seq,
            )
        )

    session.flush()
    return leased


def get_leased_item_for_worker(
    session: Session,
    *,
    intake_item_id: int,
    worker_name: str,
) -> IntakeItem | None:
    return (
        session.execute(
            select(IntakeItem)
            .where(
                IntakeItem.id == intake_item_id,
                IntakeItem.apply_status == "leased",
                IntakeItem.apply_lease_owner == worker_name,
            )
            .with_for_update()
        )
        .scalar_one_or_none()
    )


def get_active_manual_override(
    session: Session,
    *,
    uri: str,
    now: datetime | None = None,
) -> ManualOverride | None:
    now = now or utc_now()

    return (
        session.execute(
            select(ManualOverride).where(
                ManualOverride.uri == uri,
                or_(
                    ManualOverride.expires_at.is_(None),
                    ManualOverride.expires_at > now,
                ),
            )
        )
        .scalar_one_or_none()
    )


def upsert_label_decision(
    session: Session,
    *,
    intake_item_id: int,
    uri: str,
    cid: str,
    rule_version: str,
    image_count: int,
    usable_alt_count: int,
    decision_outcome: str,
    decision_reason: str | None,
    publish_required: bool,
    override_applied: bool,
) -> int:
    stmt = (
        insert(LabelDecision)
        .values(
            intake_item_id=intake_item_id,
            uri=uri,
            cid=cid,
            rule_version=rule_version,
            image_count=image_count,
            usable_alt_count=usable_alt_count,
            decision_outcome=decision_outcome,
            decision_reason=decision_reason,
            publish_required=publish_required,
            override_applied=override_applied,
        )
        .on_conflict_do_update(
            index_elements=[LabelDecision.intake_item_id],
            set_={
                "uri": uri,
                "cid": cid,
                "rule_version": rule_version,
                "image_count": image_count,
                "usable_alt_count": usable_alt_count,
                "decision_outcome": decision_outcome,
                "decision_reason": decision_reason,
                "publish_required": publish_required,
                "override_applied": override_applied,
                "evaluated_at": func.now(),
                "updated_at": func.now(),
            },
        )
        .returning(LabelDecision.id)
    )

    return int(session.execute(stmt).scalar_one())


def ensure_publish_job(
    session: Session,
    *,
    label_decision_id: int,
    uri: str,
    cid: str,
    label_value: str,
) -> None:
    stmt = (
        insert(PublishJob)
        .values(
            label_decision_id=label_decision_id,
            uri=uri,
            cid=cid,
            label_value=label_value,
            status="pending",
        )
        .on_conflict_do_nothing(
            index_elements=[PublishJob.label_decision_id],
        )
    )
    session.execute(stmt)


def mark_item_applied(row: IntakeItem) -> None:
    row.apply_status = "applied"
    row.apply_lease_owner = None
    row.apply_lease_until = None


def mark_item_skipped(row: IntakeItem) -> None:
    row.apply_status = "skipped"
    row.apply_lease_owner = None
    row.apply_lease_until = None


def mark_item_retry_or_error(
    row: IntakeItem,
    *,
    error_code: str,
    error_text: str,
    max_attempts: int,
    backoff_base_seconds: int,
    now: datetime | None = None,
) -> None:
    now = now or utc_now()

    row.last_apply_error_code = error_code
    row.last_apply_error_text = error_text
    row.apply_lease_owner = None
    row.apply_lease_until = None

    if row.apply_attempt_count >= max_attempts:
        row.apply_status = "error"
        return

    backoff_seconds = backoff_base_seconds * (2 ** max(0, row.apply_attempt_count - 1))
    row.apply_status = "pending"
    row.apply_next_attempt_at = now + timedelta(seconds=backoff_seconds)


def count_apply_backlog(session: Session, *, now: datetime | None = None) -> int:
    now = now or utc_now()

    stmt = select(func.count()).select_from(IntakeItem).where(
        or_(
            and_(
                IntakeItem.apply_status == "pending",
                IntakeItem.apply_next_attempt_at <= now,
            ),
            and_(
                IntakeItem.apply_status == "leased",
                IntakeItem.apply_lease_until.is_not(None),
                IntakeItem.apply_lease_until < now,
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