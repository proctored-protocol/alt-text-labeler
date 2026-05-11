"""add label write rate limiting and publish retry diagnostics

Revision ID: 20260511_000004
Revises: 20260413_000003
Create Date: 2026-05-11
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260511_000004"
down_revision = "20260413_000003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "label_write_rate_limit_bucket",
        sa.Column("scope", sa.Text(), nullable=False),
        sa.Column("bucket_started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("bucket_seconds", sa.Integer(), nullable=False),
        sa.Column("used_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("limit_count", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint("bucket_seconds > 0", name="ck_label_write_rl_bucket_seconds_positive"),
        sa.CheckConstraint("used_count >= 0", name="ck_label_write_rl_used_count_nonnegative"),
        sa.CheckConstraint("limit_count > 0", name="ck_label_write_rl_limit_count_positive"),
        sa.PrimaryKeyConstraint("scope", "bucket_started_at", "bucket_seconds", name="pk_label_write_rate_limit_bucket"),
    )
    op.create_index(
        "ix_label_write_rl_bucket_started_at",
        "label_write_rate_limit_bucket",
        ["bucket_started_at"],
        unique=False,
    )

    op.create_table(
        "label_write_rate_limit_cooldown",
        sa.Column("scope", sa.Text(), nullable=False),
        sa.Column("cooldown_until", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason_code", sa.String(length=128), nullable=True),
        sa.Column("http_status", sa.Integer(), nullable=True),
        sa.Column("last_error_text", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("scope", name="pk_label_write_rate_limit_cooldown"),
    )
    op.create_index(
        "ix_label_write_rl_cooldown_until",
        "label_write_rate_limit_cooldown",
        ["cooldown_until"],
        unique=False,
    )

    op.add_column(
        "publish_attempt",
        sa.Column("retry_after_seconds", sa.Integer(), nullable=True),
    )
    op.create_index(
        "ix_publish_attempt_http_status_started_at",
        "publish_attempt",
        ["http_status", "started_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_publish_attempt_http_status_started_at", table_name="publish_attempt")
    op.drop_column("publish_attempt", "retry_after_seconds")
    op.drop_index("ix_label_write_rl_cooldown_until", table_name="label_write_rate_limit_cooldown")
    op.drop_table("label_write_rate_limit_cooldown")
    op.drop_index("ix_label_write_rl_bucket_started_at", table_name="label_write_rate_limit_bucket")
    op.drop_table("label_write_rate_limit_bucket")
