from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.models import ConsumerState, FirehoseHeadSample


HEAD_TRACKER_CONSUMER_NAME = "head_tracker"
HEAD_TRACKER_CONSUMER_TYPE = "head_tracker"


@dataclass(frozen=True, slots=True)
class HeadBucketCounts:
    commit_count: int = 0
    post_count: int = 0
    image_post_count: int = 0
    missing_alt_post_count: int = 0
    partial_alt_post_count: int = 0
    gif_post_count: int = 0
    video_post_count: int = 0


def upsert_consumer_state(
    session: Session,
    *,
    consumer_name: str,
    consumer_type: str,
    stream_name: str,
    status: str,
    cursor_seq: int | None,
    cursor_observed_at: datetime | None,
    started_at: datetime | None = None,
    last_error_code: str | None = None,
    last_error_text: str | None = None,
) -> None:
    insert_stmt = insert(ConsumerState).values(
        consumer_name=consumer_name,
        consumer_type=consumer_type,
        stream_name=stream_name,
        cursor_seq=cursor_seq,
        cursor_observed_at=cursor_observed_at,
        status=status,
        last_error_code=last_error_code,
        last_error_text=last_error_text,
        started_at=started_at,
    )

    excluded = insert_stmt.excluded

    set_map: dict[str, object] = {
        "consumer_type": excluded.consumer_type,
        "stream_name": excluded.stream_name,
        "cursor_seq": excluded.cursor_seq,
        "cursor_observed_at": excluded.cursor_observed_at,
        "status": excluded.status,
        "last_error_code": excluded.last_error_code,
        "last_error_text": excluded.last_error_text,
        "updated_at": func.now(),
    }

    if started_at is not None:
        set_map["started_at"] = func.coalesce(ConsumerState.started_at, excluded.started_at)

    session.execute(
        insert_stmt.on_conflict_do_update(
            index_elements=[ConsumerState.consumer_name],
            set_=set_map,
        )
    )


def record_head_sample(
    session: Session,
    *,
    bucket_second: datetime,
    head_seq: int,
    counts: HeadBucketCounts,
) -> None:
    insert_stmt = insert(FirehoseHeadSample).values(
        bucket_second=bucket_second,
        head_seq=head_seq,
        commit_count=counts.commit_count,
        post_count=counts.post_count,
        image_post_count=counts.image_post_count,
        missing_alt_post_count=counts.missing_alt_post_count,
        partial_alt_post_count=counts.partial_alt_post_count,
        gif_post_count=counts.gif_post_count,
        video_post_count=counts.video_post_count,
    )

    excluded = insert_stmt.excluded

    session.execute(
        insert_stmt.on_conflict_do_update(
            index_elements=[FirehoseHeadSample.bucket_second],
            set_={
                "head_seq": func.greatest(FirehoseHeadSample.head_seq, excluded.head_seq),
                "commit_count": FirehoseHeadSample.commit_count + excluded.commit_count,
                "post_count": FirehoseHeadSample.post_count + excluded.post_count,
                "image_post_count": FirehoseHeadSample.image_post_count + excluded.image_post_count,
                "missing_alt_post_count": FirehoseHeadSample.missing_alt_post_count + excluded.missing_alt_post_count,
                "partial_alt_post_count": FirehoseHeadSample.partial_alt_post_count + excluded.partial_alt_post_count,
                "gif_post_count": FirehoseHeadSample.gif_post_count + excluded.gif_post_count,
                "video_post_count": FirehoseHeadSample.video_post_count + excluded.video_post_count,
                "updated_at": func.now(),
            },
        )
    )


def get_consumer_state(session: Session, consumer_name: str) -> ConsumerState | None:
    return session.execute(
        select(ConsumerState).where(ConsumerState.consumer_name == consumer_name)
    ).scalar_one_or_none()


def get_latest_head_sample(session: Session) -> FirehoseHeadSample | None:
    return session.execute(
        select(FirehoseHeadSample).order_by(FirehoseHeadSample.bucket_second.desc()).limit(1)
    ).scalar_one_or_none()