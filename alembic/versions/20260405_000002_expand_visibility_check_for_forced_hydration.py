"""expand visibility_check for forced hydration verifier

Revision ID: 20260405_000002
Revises: 20260404_000001
Create Date: 2026-04-05 12:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260405_000002"
down_revision = "20260404_000001"
branch_labels = None
depends_on = None


def _has_column(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    return column_name in {c["name"] for c in inspector.get_columns(table_name)}


def _has_index(inspector: sa.Inspector, table_name: str, index_name: str) -> bool:
    return index_name in {i["name"] for i in inspector.get_indexes(table_name)}


def _has_unique(inspector: sa.Inspector, table_name: str, constraint_name: str) -> bool:
    return constraint_name in {u["name"] for u in inspector.get_unique_constraints(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not _has_column(inspector, "visibility_check", "publish_job_id"):
        op.add_column("visibility_check", sa.Column("publish_job_id", sa.BigInteger(), nullable=True))

    inspector = sa.inspect(bind)

    wanted_columns = [
        ("status", sa.String(length=32), False, sa.text("'pending'")),
        ("check_count", sa.Integer(), False, sa.text("0")),
        ("next_check_at", sa.DateTime(timezone=True), False, sa.text("now()")),
        ("lease_owner", sa.String(length=255), True, None),
        ("lease_until", sa.DateTime(timezone=True), True, None),
        ("first_forced_visible_at", sa.DateTime(timezone=True), True, None),
        ("last_forced_visible_at", sa.DateTime(timezone=True), True, None),
        ("last_checked_at", sa.DateTime(timezone=True), True, None),
        ("last_http_status", sa.Integer(), True, None),
        ("last_error_code", sa.String(length=128), True, None),
        ("last_error_text", sa.Text(), True, None),
        ("last_response_json", postgresql.JSONB(astext_type=sa.Text()), True, None),
        ("created_at", sa.DateTime(timezone=True), False, sa.text("now()")),
        ("updated_at", sa.DateTime(timezone=True), False, sa.text("now()")),
    ]

    for name, type_, nullable, server_default in wanted_columns:
        if not _has_column(inspector, "visibility_check", name):
            op.add_column(
                "visibility_check",
                sa.Column(
                    name,
                    type_,
                    nullable=nullable,
                    server_default=server_default,
                ),
            )
            inspector = sa.inspect(bind)

    if not _has_unique(inspector, "visibility_check", "uq_visibility_check_publish_job_id"):
        op.create_unique_constraint(
            "uq_visibility_check_publish_job_id",
            "visibility_check",
            ["publish_job_id"],
        )

    inspector = sa.inspect(bind)

    if not _has_index(inspector, "visibility_check", "ix_visibility_check_status_next_check_at"):
        op.create_index(
            "ix_visibility_check_status_next_check_at",
            "visibility_check",
            ["status", "next_check_at"],
            unique=False,
        )

    inspector = sa.inspect(bind)

    if not _has_index(inspector, "visibility_check", "ix_visibility_check_lease_until"):
        op.create_index(
            "ix_visibility_check_lease_until",
            "visibility_check",
            ["lease_until"],
            unique=False,
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if _has_index(inspector, "visibility_check", "ix_visibility_check_lease_until"):
        op.drop_index("ix_visibility_check_lease_until", table_name="visibility_check")

    inspector = sa.inspect(bind)
    if _has_index(inspector, "visibility_check", "ix_visibility_check_status_next_check_at"):
        op.drop_index("ix_visibility_check_status_next_check_at", table_name="visibility_check")

    inspector = sa.inspect(bind)
    if _has_unique(inspector, "visibility_check", "uq_visibility_check_publish_job_id"):
        op.drop_constraint("uq_visibility_check_publish_job_id", "visibility_check", type_="unique")

    inspector = sa.inspect(bind)

    for column_name in [
        "updated_at",
        "created_at",
        "last_response_json",
        "last_error_text",
        "last_error_code",
        "last_http_status",
        "last_checked_at",
        "last_forced_visible_at",
        "first_forced_visible_at",
        "lease_until",
        "lease_owner",
        "next_check_at",
        "check_count",
        "status",
        "publish_job_id",
    ]:
        if _has_column(inspector, "visibility_check", column_name):
            op.drop_column("visibility_check", column_name)
            inspector = sa.inspect(bind)