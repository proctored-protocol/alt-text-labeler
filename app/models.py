from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


class ConsumerState(Base):
    __tablename__ = "consumer_state"
    __table_args__ = (
        Index("ix_consumer_state_consumer_type", "consumer_type"),
        Index("ix_consumer_state_status", "status"),
        Index("ix_consumer_state_updated_at", "updated_at"),
    )

    consumer_name: Mapped[str] = mapped_column(Text, primary_key=True)
    consumer_type: Mapped[str] = mapped_column(String(64), nullable=False)
    stream_name: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        default="subscribe_repos",
        server_default="subscribe_repos",
    )

    cursor_seq: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    cursor_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    status: Mapped[str] = mapped_column(String(32), nullable=False, default="starting", server_default="starting")

    last_error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_error_text: Mapped[str | None] = mapped_column(Text, nullable=True)

    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class FirehoseHeadSample(Base):
    __tablename__ = "firehose_head_sample"
    __table_args__ = (
        CheckConstraint("commit_count >= 0", name="ck_firehose_head_sample_commit_count_nonnegative"),
        CheckConstraint("post_count >= 0", name="ck_firehose_head_sample_post_count_nonnegative"),
        CheckConstraint("image_post_count >= 0", name="ck_firehose_head_sample_image_post_count_nonnegative"),
        CheckConstraint("missing_alt_post_count >= 0", name="ck_firehose_head_sample_missing_alt_post_count_nonnegative"),
        CheckConstraint("partial_alt_post_count >= 0", name="ck_firehose_head_sample_partial_alt_post_count_nonnegative"),
        CheckConstraint("gif_post_count >= 0", name="ck_firehose_head_sample_gif_post_count_nonnegative"),
        CheckConstraint("video_post_count >= 0", name="ck_firehose_head_sample_video_post_count_nonnegative"),
        Index("ix_firehose_head_sample_head_seq", "head_seq"),
        Index("ix_firehose_head_sample_updated_at", "updated_at"),
    )

    bucket_second: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    head_seq: Mapped[int] = mapped_column(BigInteger, nullable=False)

    commit_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    post_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    image_post_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    missing_alt_post_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    partial_alt_post_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    gif_post_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    video_post_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class IntakeItem(Base):
    __tablename__ = "intake_item"
    __table_args__ = (
        UniqueConstraint("uri", name="uq_intake_item_uri"),
        CheckConstraint("image_count >= 0", name="ck_intake_item_image_count_nonnegative"),
        CheckConstraint("apply_attempt_count >= 0", name="ck_intake_item_apply_attempt_count_nonnegative"),
        Index("ix_intake_item_firehose_seq", "firehose_seq"),
        Index("ix_intake_item_record_created_at", "record_created_at"),
        Index("ix_intake_item_apply_status_next_attempt_at", "apply_status", "apply_next_attempt_at"),
        Index("ix_intake_item_apply_lease_until", "apply_lease_until"),
        Index("ix_intake_item_created_at", "created_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    uri: Mapped[str] = mapped_column(Text, nullable=False)
    cid: Mapped[str] = mapped_column(Text, nullable=False)

    author_did: Mapped[str] = mapped_column(Text, nullable=False)
    repo_did: Mapped[str] = mapped_column(Text, nullable=False)

    record_created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    firehose_seq: Mapped[int] = mapped_column(BigInteger, nullable=False)
    firehose_observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    raw_embed_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    image_count: Mapped[int] = mapped_column(Integer, nullable=False)
    image_alts_json: Mapped[list[str | None]] = mapped_column(JSONB, nullable=False, default=list)

    apply_status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending", server_default="pending")
    apply_attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    apply_next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    apply_lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    apply_lease_owner: Mapped[str | None] = mapped_column(String(255), nullable=True)

    last_apply_error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_apply_error_text: Mapped[str | None] = mapped_column(Text, nullable=True)

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

    label_decision: Mapped[LabelDecision | None] = relationship(
        "LabelDecision",
        back_populates="intake_item",
        cascade="all, delete-orphan",
        uselist=False,
    )


class LabelDecision(Base):
    __tablename__ = "label_decision"
    __table_args__ = (
        UniqueConstraint("intake_item_id", name="uq_label_decision_intake_item_id"),
        Index("ix_label_decision_decision_outcome", "decision_outcome"),
        Index("ix_label_decision_publish_required", "publish_required"),
        Index("ix_label_decision_evaluated_at", "evaluated_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    intake_item_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("intake_item.id", ondelete="CASCADE"),
        nullable=False,
    )

    uri: Mapped[str] = mapped_column(Text, nullable=False)
    cid: Mapped[str] = mapped_column(Text, nullable=False)

    rule_version: Mapped[str] = mapped_column(String(64), nullable=False)

    image_count: Mapped[int] = mapped_column(Integer, nullable=False)
    usable_alt_count: Mapped[int] = mapped_column(Integer, nullable=False)

    decision_outcome: Mapped[str] = mapped_column(String(64), nullable=False)
    decision_reason: Mapped[str | None] = mapped_column(String(128), nullable=True)

    publish_required: Mapped[bool] = mapped_column(Boolean, nullable=False)
    override_applied: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")

    evaluated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
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

    intake_item: Mapped[IntakeItem] = relationship("IntakeItem", back_populates="label_decision")
    publish_job: Mapped[PublishJob | None] = relationship(
        "PublishJob",
        back_populates="label_decision",
        cascade="all, delete-orphan",
        uselist=False,
    )


class PublishJob(Base):
    __tablename__ = "publish_job"
    __table_args__ = (
        UniqueConstraint("label_decision_id", name="uq_publish_job_label_decision_id"),
        CheckConstraint("attempt_count >= 0", name="ck_publish_job_attempt_count_nonnegative"),
        Index("ix_publish_job_status_next_attempt_at", "status", "next_attempt_at"),
        Index("ix_publish_job_lease_until", "lease_until"),
        Index("ix_publish_job_published_at", "published_at"),
        Index("ix_publish_job_label_value", "label_value"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    label_decision_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("label_decision.id", ondelete="CASCADE"),
        nullable=False,
    )

    uri: Mapped[str] = mapped_column(Text, nullable=False)
    cid: Mapped[str] = mapped_column(Text, nullable=False)
    label_value: Mapped[str] = mapped_column(String(64), nullable=False)

    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending", server_default="pending")
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_owner: Mapped[str | None] = mapped_column(String(255), nullable=True)

    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    external_event_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    external_created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    last_error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_error_text: Mapped[str | None] = mapped_column(Text, nullable=True)

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

    label_decision: Mapped[LabelDecision] = relationship("LabelDecision", back_populates="publish_job")
    publish_attempts: Mapped[list[PublishAttempt]] = relationship(
        "PublishAttempt",
        back_populates="publish_job",
        cascade="all, delete-orphan",
        order_by="PublishAttempt.attempt_no",
    )
    visibility_check: Mapped[VisibilityCheck | None] = relationship(
        "VisibilityCheck",
        back_populates="publish_job",
        cascade="all, delete-orphan",
        uselist=False,
    )
    visibility_remediation: Mapped[VisibilityRemediation | None] = relationship(
        "VisibilityRemediation",
        back_populates="publish_job",
        cascade="all, delete-orphan",
        uselist=False,
    )


class PublishAttempt(Base):
    __tablename__ = "publish_attempt"
    __table_args__ = (
        UniqueConstraint("publish_job_id", "attempt_no", name="uq_publish_attempt_job_attempt_no"),
        Index("ix_publish_attempt_publish_job_id", "publish_job_id"),
        Index("ix_publish_attempt_started_at", "started_at"),
        Index("ix_publish_attempt_result_status", "result_status"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    publish_job_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("publish_job.id", ondelete="CASCADE"),
        nullable=False,
    )
    attempt_no: Mapped[int] = mapped_column(Integer, nullable=False)

    worker_name: Mapped[str | None] = mapped_column(String(255), nullable=True)

    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    result_status: Mapped[str] = mapped_column(String(32), nullable=False)

    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)

    external_event_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    external_created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    response_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)

    publish_job: Mapped[PublishJob] = relationship("PublishJob", back_populates="publish_attempts")


class VisibilityCheck(Base):
    __tablename__ = "visibility_check"
    __table_args__ = (
        UniqueConstraint("publish_job_id", name="uq_visibility_check_publish_job_id"),
        CheckConstraint("attempt_count >= 0", name="ck_visibility_check_attempt_count_nonnegative"),
        Index("ix_visibility_check_status_next_attempt_at", "status", "next_attempt_at"),
        Index("ix_visibility_check_lease_until", "lease_until"),
        Index("ix_visibility_check_visible_at", "visible_at"),
        Index("ix_visibility_check_last_checked_at", "last_checked_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    publish_job_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("publish_job.id", ondelete="CASCADE"),
        nullable=False,
    )

    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending", server_default="pending")
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_owner: Mapped[str | None] = mapped_column(String(255), nullable=True)

    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    visible_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    forced_found: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    query_found: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    subscriber_found: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    forced_status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    query_status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    subscriber_status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)

    last_error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_error_text: Mapped[str | None] = mapped_column(Text, nullable=True)

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

    publish_job: Mapped[PublishJob] = relationship("PublishJob", back_populates="visibility_check")


class VisibilityRemediation(Base):
    __tablename__ = "visibility_remediation"
    __table_args__ = (
        UniqueConstraint("publish_job_id", name="uq_visibility_remediation_publish_job_id"),
        CheckConstraint("attempt_count >= 0", name="ck_visibility_remediation_attempt_count_nonnegative"),
        Index("ix_visibility_remediation_status_next_attempt_at", "status", "next_attempt_at"),
        Index("ix_visibility_remediation_lease_until", "lease_until"),
        Index("ix_visibility_remediation_resolved_at", "resolved_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    publish_job_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("publish_job.id", ondelete="CASCADE"),
        nullable=False,
    )

    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending", server_default="pending")
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_owner: Mapped[str | None] = mapped_column(String(255), nullable=True)

    first_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    first_found_label: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    first_unlabel_event_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    first_relabel_event_id: Mapped[str | None] = mapped_column(Text, nullable=True)

    second_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    second_found_label: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    second_unlabel_event_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    second_relabel_event_id: Mapped[str | None] = mapped_column(Text, nullable=True)

    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_error_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_response_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)

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

    publish_job: Mapped[PublishJob] = relationship("PublishJob", back_populates="visibility_remediation")


class WorkerHeartbeat(Base):
    __tablename__ = "worker_heartbeat"
    __table_args__ = (
        Index("ix_worker_heartbeat_stage", "stage"),
        Index("ix_worker_heartbeat_heartbeat_at", "heartbeat_at"),
        Index("ix_worker_heartbeat_stage_status", "stage", "status"),
    )

    worker_name: Mapped[str] = mapped_column(Text, primary_key=True)
    stage: Mapped[str] = mapped_column(String(32), nullable=False)

    host: Mapped[str | None] = mapped_column(String(255), nullable=True)
    pid: Mapped[int | None] = mapped_column(Integer, nullable=True)

    status: Mapped[str] = mapped_column(String(32), nullable=False)

    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    lease_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    backlog_depth: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    last_error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_error_text: Mapped[str | None] = mapped_column(Text, nullable=True)

    meta_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class ControlActionLog(Base):
    __tablename__ = "control_action_log"
    __table_args__ = (
        Index("ix_control_action_log_created_at", "created_at"),
        Index("ix_control_action_log_stage_action_type", "stage", "action_type"),
        Index("ix_control_action_log_actor", "actor"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    actor: Mapped[str] = mapped_column(String(64), nullable=False)
    stage: Mapped[str | None] = mapped_column(String(32), nullable=True)
    action_type: Mapped[str] = mapped_column(String(64), nullable=False)

    reason_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    reason_text: Mapped[str | None] = mapped_column(Text, nullable=True)

    before_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)
    after_json: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class ManualOverride(Base):
    __tablename__ = "manual_override"
    __table_args__ = (
        UniqueConstraint("uri", name="uq_manual_override_uri"),
        Index("ix_manual_override_expires_at", "expires_at"),
        Index("ix_manual_override_override_type", "override_type"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    uri: Mapped[str] = mapped_column(Text, nullable=False)
    override_type: Mapped[str] = mapped_column(String(64), nullable=False)

    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[str | None] = mapped_column(String(255), nullable=True)

    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

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