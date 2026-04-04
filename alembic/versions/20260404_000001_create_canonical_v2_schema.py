from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260404_000001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "consumer_state",
        sa.Column("consumer_name", sa.Text(), nullable=False),
        sa.Column("consumer_type", sa.String(length=64), nullable=False),
        sa.Column("stream_name", sa.String(length=128), nullable=False, server_default="subscribe_repos"),
        sa.Column("cursor_seq", sa.BigInteger(), nullable=True),
        sa.Column("cursor_observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="starting"),
        sa.Column("last_error_code", sa.String(length=128), nullable=True),
        sa.Column("last_error_text", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("consumer_name"),
    )
    op.create_index("ix_consumer_state_consumer_type", "consumer_state", ["consumer_type"], unique=False)
    op.create_index("ix_consumer_state_status", "consumer_state", ["status"], unique=False)
    op.create_index("ix_consumer_state_updated_at", "consumer_state", ["updated_at"], unique=False)

    op.create_table(
        "firehose_head_sample",
        sa.Column("bucket_second", sa.DateTime(timezone=True), nullable=False),
        sa.Column("head_seq", sa.BigInteger(), nullable=False),
        sa.Column("commit_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("post_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("image_post_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("missing_alt_post_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("partial_alt_post_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("gif_post_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("video_post_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint("commit_count >= 0", name="ck_firehose_head_sample_commit_count_nonnegative"),
        sa.CheckConstraint("post_count >= 0", name="ck_firehose_head_sample_post_count_nonnegative"),
        sa.CheckConstraint("image_post_count >= 0", name="ck_firehose_head_sample_image_post_count_nonnegative"),
        sa.CheckConstraint("missing_alt_post_count >= 0", name="ck_firehose_head_sample_missing_alt_post_count_nonnegative"),
        sa.CheckConstraint("partial_alt_post_count >= 0", name="ck_firehose_head_sample_partial_alt_post_count_nonnegative"),
        sa.CheckConstraint("gif_post_count >= 0", name="ck_firehose_head_sample_gif_post_count_nonnegative"),
        sa.CheckConstraint("video_post_count >= 0", name="ck_firehose_head_sample_video_post_count_nonnegative"),
        sa.PrimaryKeyConstraint("bucket_second"),
    )
    op.create_index("ix_firehose_head_sample_head_seq", "firehose_head_sample", ["head_seq"], unique=False)
    op.create_index("ix_firehose_head_sample_updated_at", "firehose_head_sample", ["updated_at"], unique=False)

    op.create_table(
        "intake_item",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("uri", sa.Text(), nullable=False),
        sa.Column("cid", sa.Text(), nullable=False),
        sa.Column("author_did", sa.Text(), nullable=False),
        sa.Column("repo_did", sa.Text(), nullable=False),
        sa.Column("record_created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("firehose_seq", sa.BigInteger(), nullable=False),
        sa.Column("firehose_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_embed_type", sa.String(length=128), nullable=True),
        sa.Column("image_count", sa.Integer(), nullable=False),
        sa.Column("image_alts_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("apply_status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("apply_attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("apply_next_attempt_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("apply_lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("apply_lease_owner", sa.String(length=255), nullable=True),
        sa.Column("last_apply_error_code", sa.String(length=128), nullable=True),
        sa.Column("last_apply_error_text", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint("image_count >= 0", name="ck_intake_item_image_count_nonnegative"),
        sa.CheckConstraint("apply_attempt_count >= 0", name="ck_intake_item_apply_attempt_count_nonnegative"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("uri", name="uq_intake_item_uri"),
    )
    op.create_index("ix_intake_item_firehose_seq", "intake_item", ["firehose_seq"], unique=False)
    op.create_index("ix_intake_item_record_created_at", "intake_item", ["record_created_at"], unique=False)
    op.create_index("ix_intake_item_apply_status_next_attempt_at", "intake_item", ["apply_status", "apply_next_attempt_at"], unique=False)
    op.create_index("ix_intake_item_apply_lease_until", "intake_item", ["apply_lease_until"], unique=False)
    op.create_index("ix_intake_item_created_at", "intake_item", ["created_at"], unique=False)

    op.create_table(
        "worker_heartbeat",
        sa.Column("worker_name", sa.Text(), nullable=False),
        sa.Column("stage", sa.String(length=32), nullable=False),
        sa.Column("host", sa.String(length=255), nullable=True),
        sa.Column("pid", sa.Integer(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("lease_count", sa.Integer(), nullable=True),
        sa.Column("backlog_depth", sa.BigInteger(), nullable=True),
        sa.Column("last_error_code", sa.String(length=128), nullable=True),
        sa.Column("last_error_text", sa.Text(), nullable=True),
        sa.Column("meta_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("worker_name"),
    )
    op.create_index("ix_worker_heartbeat_stage", "worker_heartbeat", ["stage"], unique=False)
    op.create_index("ix_worker_heartbeat_heartbeat_at", "worker_heartbeat", ["heartbeat_at"], unique=False)
    op.create_index("ix_worker_heartbeat_stage_status", "worker_heartbeat", ["stage", "status"], unique=False)

    op.create_table(
        "control_action_log",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("actor", sa.String(length=64), nullable=False),
        sa.Column("stage", sa.String(length=32), nullable=True),
        sa.Column("action_type", sa.String(length=64), nullable=False),
        sa.Column("reason_code", sa.String(length=128), nullable=True),
        sa.Column("reason_text", sa.Text(), nullable=True),
        sa.Column("before_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("after_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_control_action_log_created_at", "control_action_log", ["created_at"], unique=False)
    op.create_index("ix_control_action_log_stage_action_type", "control_action_log", ["stage", "action_type"], unique=False)
    op.create_index("ix_control_action_log_actor", "control_action_log", ["actor"], unique=False)

    op.create_table(
        "manual_override",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("uri", sa.Text(), nullable=False),
        sa.Column("override_type", sa.String(length=64), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("created_by", sa.String(length=255), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("uri", name="uq_manual_override_uri"),
    )
    op.create_index("ix_manual_override_expires_at", "manual_override", ["expires_at"], unique=False)
    op.create_index("ix_manual_override_override_type", "manual_override", ["override_type"], unique=False)

    op.create_table(
        "label_decision",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("intake_item_id", sa.BigInteger(), nullable=False),
        sa.Column("uri", sa.Text(), nullable=False),
        sa.Column("cid", sa.Text(), nullable=False),
        sa.Column("rule_version", sa.String(length=64), nullable=False),
        sa.Column("image_count", sa.Integer(), nullable=False),
        sa.Column("usable_alt_count", sa.Integer(), nullable=False),
        sa.Column("decision_outcome", sa.String(length=64), nullable=False),
        sa.Column("decision_reason", sa.String(length=128), nullable=True),
        sa.Column("publish_required", sa.Boolean(), nullable=False),
        sa.Column("override_applied", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("evaluated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["intake_item_id"], ["intake_item.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("intake_item_id", name="uq_label_decision_intake_item_id"),
    )
    op.create_index("ix_label_decision_decision_outcome", "label_decision", ["decision_outcome"], unique=False)
    op.create_index("ix_label_decision_publish_required", "label_decision", ["publish_required"], unique=False)
    op.create_index("ix_label_decision_evaluated_at", "label_decision", ["evaluated_at"], unique=False)

    op.create_table(
        "publish_job",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("label_decision_id", sa.BigInteger(), nullable=False),
        sa.Column("uri", sa.Text(), nullable=False),
        sa.Column("cid", sa.Text(), nullable=False),
        sa.Column("label_value", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_owner", sa.String(length=255), nullable=True),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("external_event_id", sa.Text(), nullable=True),
        sa.Column("external_created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error_code", sa.String(length=128), nullable=True),
        sa.Column("last_error_text", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint("attempt_count >= 0", name="ck_publish_job_attempt_count_nonnegative"),
        sa.ForeignKeyConstraint(["label_decision_id"], ["label_decision.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("label_decision_id", name="uq_publish_job_label_decision_id"),
    )
    op.create_index("ix_publish_job_status_next_attempt_at", "publish_job", ["status", "next_attempt_at"], unique=False)
    op.create_index("ix_publish_job_lease_until", "publish_job", ["lease_until"], unique=False)
    op.create_index("ix_publish_job_published_at", "publish_job", ["published_at"], unique=False)
    op.create_index("ix_publish_job_label_value", "publish_job", ["label_value"], unique=False)

    op.create_table(
        "publish_attempt",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("publish_job_id", sa.BigInteger(), nullable=False),
        sa.Column("attempt_no", sa.Integer(), nullable=False),
        sa.Column("worker_name", sa.String(length=255), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("result_status", sa.String(length=32), nullable=False),
        sa.Column("http_status", sa.Integer(), nullable=True),
        sa.Column("error_code", sa.String(length=128), nullable=True),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.Column("external_event_id", sa.Text(), nullable=True),
        sa.Column("external_created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("response_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.ForeignKeyConstraint(["publish_job_id"], ["publish_job.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("publish_job_id", "attempt_no", name="uq_publish_attempt_job_attempt_no"),
    )
    op.create_index("ix_publish_attempt_publish_job_id", "publish_attempt", ["publish_job_id"], unique=False)
    op.create_index("ix_publish_attempt_started_at", "publish_attempt", ["started_at"], unique=False)
    op.create_index("ix_publish_attempt_result_status", "publish_attempt", ["result_status"], unique=False)

    op.create_table(
        "visibility_check",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("publish_job_id", sa.BigInteger(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_owner", sa.String(length=255), nullable=True),
        sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("visible_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("forced_found", sa.Boolean(), nullable=True),
        sa.Column("query_found", sa.Boolean(), nullable=True),
        sa.Column("subscriber_found", sa.Boolean(), nullable=True),
        sa.Column("forced_status_code", sa.Integer(), nullable=True),
        sa.Column("query_status_code", sa.Integer(), nullable=True),
        sa.Column("subscriber_status_code", sa.Integer(), nullable=True),
        sa.Column("last_error_code", sa.String(length=128), nullable=True),
        sa.Column("last_error_text", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint("attempt_count >= 0", name="ck_visibility_check_attempt_count_nonnegative"),
        sa.ForeignKeyConstraint(["publish_job_id"], ["publish_job.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("publish_job_id", name="uq_visibility_check_publish_job_id"),
    )
    op.create_index("ix_visibility_check_status_next_attempt_at", "visibility_check", ["status", "next_attempt_at"], unique=False)
    op.create_index("ix_visibility_check_lease_until", "visibility_check", ["lease_until"], unique=False)
    op.create_index("ix_visibility_check_visible_at", "visibility_check", ["visible_at"], unique=False)
    op.create_index("ix_visibility_check_last_checked_at", "visibility_check", ["last_checked_at"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_visibility_check_last_checked_at", table_name="visibility_check")
    op.drop_index("ix_visibility_check_visible_at", table_name="visibility_check")
    op.drop_index("ix_visibility_check_lease_until", table_name="visibility_check")
    op.drop_index("ix_visibility_check_status_next_attempt_at", table_name="visibility_check")
    op.drop_table("visibility_check")

    op.drop_index("ix_publish_attempt_result_status", table_name="publish_attempt")
    op.drop_index("ix_publish_attempt_started_at", table_name="publish_attempt")
    op.drop_index("ix_publish_attempt_publish_job_id", table_name="publish_attempt")
    op.drop_table("publish_attempt")

    op.drop_index("ix_publish_job_label_value", table_name="publish_job")
    op.drop_index("ix_publish_job_published_at", table_name="publish_job")
    op.drop_index("ix_publish_job_lease_until", table_name="publish_job")
    op.drop_index("ix_publish_job_status_next_attempt_at", table_name="publish_job")
    op.drop_table("publish_job")

    op.drop_index("ix_label_decision_evaluated_at", table_name="label_decision")
    op.drop_index("ix_label_decision_publish_required", table_name="label_decision")
    op.drop_index("ix_label_decision_decision_outcome", table_name="label_decision")
    op.drop_table("label_decision")

    op.drop_index("ix_manual_override_override_type", table_name="manual_override")
    op.drop_index("ix_manual_override_expires_at", table_name="manual_override")
    op.drop_table("manual_override")

    op.drop_index("ix_control_action_log_actor", table_name="control_action_log")
    op.drop_index("ix_control_action_log_stage_action_type", table_name="control_action_log")
    op.drop_index("ix_control_action_log_created_at", table_name="control_action_log")
    op.drop_table("control_action_log")

    op.drop_index("ix_worker_heartbeat_stage_status", table_name="worker_heartbeat")
    op.drop_index("ix_worker_heartbeat_heartbeat_at", table_name="worker_heartbeat")
    op.drop_index("ix_worker_heartbeat_stage", table_name="worker_heartbeat")
    op.drop_table("worker_heartbeat")

    op.drop_index("ix_intake_item_created_at", table_name="intake_item")
    op.drop_index("ix_intake_item_apply_lease_until", table_name="intake_item")
    op.drop_index("ix_intake_item_apply_status_next_attempt_at", table_name="intake_item")
    op.drop_index("ix_intake_item_record_created_at", table_name="intake_item")
    op.drop_index("ix_intake_item_firehose_seq", table_name="intake_item")
    op.drop_table("intake_item")

    op.drop_index("ix_firehose_head_sample_updated_at", table_name="firehose_head_sample")
    op.drop_index("ix_firehose_head_sample_head_seq", table_name="firehose_head_sample")
    op.drop_table("firehose_head_sample")

    op.drop_index("ix_consumer_state_updated_at", table_name="consumer_state")
    op.drop_index("ix_consumer_state_status", table_name="consumer_state")
    op.drop_index("ix_consumer_state_consumer_type", table_name="consumer_state")
    op.drop_table("consumer_state")