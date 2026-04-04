from __future__ import annotations

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.models import ConsumerState, IntakeItem
from app.schemas import ParsedPostCreate


INTAKE_CONSUMER_NAME = "intake"
INTAKE_CONSUMER_TYPE = "intake"


def get_consumer_state(session: Session, consumer_name: str) -> ConsumerState | None:
    return session.execute(
        select(ConsumerState).where(ConsumerState.consumer_name == consumer_name)
    ).scalar_one_or_none()


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


def upsert_intake_items(
    session: Session,
    *,
    posts: list[ParsedPostCreate],
    firehose_seq: int,
    firehose_observed_at: datetime,
) -> None:
    if not posts:
        return

    rows = [
        {
            "uri": post.uri,
            "cid": post.cid,
            "author_did": post.author_did,
            "repo_did": post.repo_did,
            "record_created_at": post.record_created_at,
            "firehose_seq": firehose_seq,
            "firehose_observed_at": firehose_observed_at,
            "raw_embed_type": post.raw_embed_type,
            "image_count": post.image_count,
            "image_alts_json": post.image_alts,
        }
        for post in posts
    ]

    insert_stmt = insert(IntakeItem).values(rows)
    excluded = insert_stmt.excluded

    session.execute(
        insert_stmt.on_conflict_do_update(
            index_elements=[IntakeItem.uri],
            set_={
                "cid": excluded.cid,
                "author_did": excluded.author_did,
                "repo_did": excluded.repo_did,
                "record_created_at": excluded.record_created_at,
                "firehose_seq": func.greatest(IntakeItem.firehose_seq, excluded.firehose_seq),
                "firehose_observed_at": func.greatest(
                    IntakeItem.firehose_observed_at,
                    excluded.firehose_observed_at,
                ),
                "raw_embed_type": excluded.raw_embed_type,
                "image_count": excluded.image_count,
                "image_alts_json": excluded.image_alts_json,
                "updated_at": func.now(),
            },
        )
    )