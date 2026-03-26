from datetime import datetime

from sqlalchemy import BigInteger, Boolean, DateTime, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class FirehoseCursor(Base):
    __tablename__ = "firehose_cursor"

    stream_name: Mapped[str] = mapped_column(String(100), primary_key=True)
    last_seq: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class PostEvaluation(Base):
    __tablename__ = "post_evaluation"

    uri: Mapped[str] = mapped_column(String(512), primary_key=True)
    cid: Mapped[str] = mapped_column(String(128), nullable=False)
    author_did: Mapped[str] = mapped_column(String(255), nullable=False)
    repo_did: Mapped[str] = mapped_column(String(255), nullable=False)
    image_count: Mapped[int] = mapped_column(Integer, nullable=False)
    usable_alt_count: Mapped[int] = mapped_column(Integer, nullable=False)
    derived_label: Mapped[str | None] = mapped_column(String(128), nullable=True)
    record_created_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    raw_embed_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    evaluated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    last_seen_seq: Mapped[int | None] = mapped_column(BigInteger, nullable=True)


class LabelPublication(Base):
    __tablename__ = "label_publication"
    __table_args__ = (
        UniqueConstraint("uri", "cid", "label_value", name="uq_label_publication_triplet"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    uri: Mapped[str] = mapped_column(String(512), nullable=False)
    cid: Mapped[str] = mapped_column(String(128), nullable=False)
    label_value: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)


class ManualOverride(Base):
    __tablename__ = "manual_override"

    uri: Mapped[str] = mapped_column(String(512), primary_key=True)
    override_type: Mapped[str] = mapped_column(String(32), nullable=False)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class LabelWorkItem(Base):
    __tablename__ = "label_work_item"
    __table_args__ = (
        UniqueConstraint("uri", "cid", "label_value", name="uq_label_work_item_triplet"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    uri: Mapped[str] = mapped_column(String(512), nullable=False)
    cid: Mapped[str] = mapped_column(String(128), nullable=False)
    label_value: Mapped[str] = mapped_column(String(128), nullable=False)

    post_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    record_created_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    evaluated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    state: Mapped[str] = mapped_column(String(32), nullable=False, default="queued")
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    leased_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    leased_by: Mapped[str | None] = mapped_column(String(255), nullable=True)

    ozone_event_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    ozone_created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    final_forced_status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    final_query_status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    final_subscriber_status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)

    final_forced_found_label: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    final_query_found_label: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    final_subscriber_found_label: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    manual_success: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    label_visible_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_result_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )