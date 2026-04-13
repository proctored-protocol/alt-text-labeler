"""add visibility_remediation table and publish_job uri index

Revision ID: 20260413_000003
Revises: 20260405_000002
Create Date: 2026-04-13 18:30:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260413_000003"
down_revision = "20260405_000002"
branch_labels = None
depends_on = None


def _has_table(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _has_index(inspector: sa.Inspector, table_name: str, index_name: str) -> bool:
    return index_name in {i["name"] for i in inspector.get_indexes(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not _has_table(inspector, "visibility_remediation"):
        op.create_table(
            "visibility_remediation",
            sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
            sa.Column("publish_job_id", sa.BigInteger(), nullable=False),

            sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
            sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
            sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
            sa.Column("lease_owner", sa.String(length=255), nullable=True),

            sa.Column("first_attempt_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("first_found_label", sa.Boolean(), nullable=True),
            sa.Column("first_unlabel_event_id", sa.Text(), nullable=True),
            sa.Column("first_relabel_event_id", sa.Text(), nullable=True),

            sa.Column("second_attempt_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("second_found_label", sa.Boolean(), nullable=True),
            sa.Column("second_unlabel_event_id", sa.Text(), nullable=True),
            sa.Column("second_relabel_event_id", sa.Text(), nullable=True),

            sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),

            sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_http_status", sa.Integer(), nullable=True),
            sa.Column("last_error_code", sa.String(length=128), nullable=True),
            sa.Column("last_error_text", sa.Text(), nullable=True),
            sa.Column("last_response_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),

            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),

            sa.CheckConstraint(
                "attempt_count >= 0",
                name="ck_visibility_remediation_attempt_count_nonnegative",
            ),
            sa.ForeignKeyConstraint(
                ["publish_job_id"],
                ["publish_job.id"],
                ondelete="CASCADE",
            ),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("publish_job_id", name="uq_visibility_remediation_publish_job_id"),
        )

    inspector = sa.inspect(bind)

    if not _has_index(inspector, "visibility_remediation", "ix_visibility_remediation_status_next_attempt_at"):
        op.create_index(
            "ix_visibility_remediation_status_next_attempt_at",
            "visibility_remediation",
            ["status", "next_attempt_at"],
            unique=False,
        )

    inspector = sa.inspect(bind)

    if not _has_index(inspector, "visibility_remediation", "ix_visibility_remediation_lease_until"):
        op.create_index(
            "ix_visibility_remediation_lease_until",
            "visibility_remediation",
            ["lease_until"],
            unique=False,
        )

    inspector = sa.inspect(bind)

    if not _has_index(inspector, "visibility_remediation", "ix_visibility_remediation_resolved_at"):
        op.create_index(
            "ix_visibility_remediation_resolved_at",
            "visibility_remediation",
            ["resolved_at"],
            unique=False,
        )

    inspector = sa.inspect(bind)

    if not _has_index(inspector, "publish_job", "ix_publish_job_uri"):
        op.create_index(
            "ix_publish_job_uri",
            "publish_job",
            ["uri"],
            unique=False,
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if _has_index(inspector, "publish_job", "ix_publish_job_uri"):
        op.drop_index("ix_publish_job_uri", table_name="publish_job")

    inspector = sa.inspect(bind)

    if _has_table(inspector, "visibility_remediation"):
        if _has_index(inspector, "visibility_remediation", "ix_visibility_remediation_resolved_at"):
            op.drop_index("ix_visibility_remediation_resolved_at", table_name="visibility_remediation")

        inspector = sa.inspect(bind)
        if _has_index(inspector, "visibility_remediation", "ix_visibility_remediation_lease_until"):
            op.drop_index("ix_visibility_remediation_lease_until", table_name="visibility_remediation")

        inspector = sa.inspect(bind)
        if _has_index(inspector, "visibility_remediation", "ix_visibility_remediation_status_next_attempt_at"):
            op.drop_index("ix_visibility_remediation_status_next_attempt_at", table_name="visibility_remediation")

        op.drop_table("visibility_remediation")