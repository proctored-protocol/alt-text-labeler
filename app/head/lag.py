from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.head.store import get_consumer_state, get_latest_head_sample
from app.models import FirehoseHeadSample


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class ConsumerLagSnapshot:
    consumer_name: str
    consumer_status: str | None
    cursor_seq: int | None
    cursor_observed_at: datetime | None

    latest_head_seq: int | None
    latest_head_bucket_second: datetime | None

    seq_gap_to_head: int | None
    lag_seconds_estimate: float | None
    matched_bucket_second: datetime | None

    head_freshness_seconds: float | None
    consumer_freshness_seconds: float | None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def get_first_head_sample_covering_seq(
    session: Session,
    *,
    cursor_seq: int,
) -> FirehoseHeadSample | None:
    return session.execute(
        select(FirehoseHeadSample)
        .where(FirehoseHeadSample.head_seq >= cursor_seq)
        .order_by(FirehoseHeadSample.bucket_second.asc())
        .limit(1)
    ).scalar_one_or_none()


def get_consumer_lag_snapshot(
    session: Session,
    *,
    consumer_name: str,
) -> ConsumerLagSnapshot:
    consumer = get_consumer_state(session, consumer_name)
    latest_head = get_latest_head_sample(session)

    latest_head_seq = latest_head.head_seq if latest_head else None
    latest_head_bucket_second = latest_head.bucket_second if latest_head else None

    cursor_seq = consumer.cursor_seq if consumer else None
    cursor_observed_at = consumer.cursor_observed_at if consumer else None
    consumer_status = consumer.status if consumer else None

    seq_gap_to_head: int | None = None
    lag_seconds_estimate: float | None = None
    matched_bucket_second: datetime | None = None

    if latest_head_seq is not None and cursor_seq is not None:
        seq_gap_to_head = latest_head_seq - cursor_seq

        covering = get_first_head_sample_covering_seq(session, cursor_seq=cursor_seq)
        if covering is not None and latest_head_bucket_second is not None:
            matched_bucket_second = covering.bucket_second
            lag_seconds_estimate = max(
                0.0,
                (latest_head_bucket_second - covering.bucket_second).total_seconds(),
            )
        elif seq_gap_to_head <= 0:
            lag_seconds_estimate = 0.0

    head_freshness_seconds = None
    if latest_head_bucket_second is not None:
        head_freshness_seconds = max(
            0.0,
            (utc_now() - latest_head_bucket_second.astimezone(timezone.utc)).total_seconds(),
        )

    consumer_freshness_seconds = None
    if cursor_observed_at is not None:
        consumer_freshness_seconds = max(
            0.0,
            (utc_now() - cursor_observed_at.astimezone(timezone.utc)).total_seconds(),
        )

    return ConsumerLagSnapshot(
        consumer_name=consumer_name,
        consumer_status=consumer_status,
        cursor_seq=cursor_seq,
        cursor_observed_at=cursor_observed_at,
        latest_head_seq=latest_head_seq,
        latest_head_bucket_second=latest_head_bucket_second,
        seq_gap_to_head=seq_gap_to_head,
        lag_seconds_estimate=lag_seconds_estimate,
        matched_bucket_second=matched_bucket_second,
        head_freshness_seconds=head_freshness_seconds,
        consumer_freshness_seconds=consumer_freshness_seconds,
    )