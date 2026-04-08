from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.models import ConsumerState, IntakeItem


INTAKE_CONSUMER_NAME = "intake"
INTAKE_CONSUMER_TYPE = "intake"


@dataclass(frozen=True, slots=True)
class BufferedIntakePost:
    uri: str
    cid: str
    author_did: str
    repo_did: str
    record_created_at: datetime | None
    firehose_seq: int
    firehose_observed_at: datetime
    raw_embed_type: str | None
    image_count: int
    image_alts_json: Any


def _clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    return value.replace("\x00", "")


def _clean_jsonish(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return value.replace("\x00", "")
    if isinstance(value, list):
        return [_clean_jsonish(v) for v in value]
    if isinstance(value, tuple):
        return [_clean_jsonish(v) for v in value]
    if isinstance(value, dict):
        cleaned: dict[str, Any] = {}
        for k, v in value.items():
            cleaned_key = "" if k is None else str(k).replace("\x00", "")
            cleaned[cleaned_key] = _clean_jsonish(v)
        return cleaned
    return value


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
        last_error_code=_clean_text(last_error_code),
        last_error_text=_clean_text(last_error_text),
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
    rows: list[BufferedIntakePost],
) -> int:
    if not rows:
        return 0

    insert_rows = [
        {
            "uri": _clean_text(row.uri),
            "cid": _clean_text(row.cid),
            "author_did": _clean_text(row.author_did),
            "repo_did": _clean_text(row.repo_did),
            "record_created_at": row.record_created_at,
            "firehose_seq": row.firehose_seq,
            "firehose_observed_at": row.firehose_observed_at,
            "raw_embed_type": _clean_text(row.raw_embed_type),
            "image_count": row.image_count,
            "image_alts_json": _clean_jsonish(row.image_alts_json),
        }
        for row in rows
    ]

    insert_stmt = insert(IntakeItem).values(insert_rows)
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

    return len(insert_rows)