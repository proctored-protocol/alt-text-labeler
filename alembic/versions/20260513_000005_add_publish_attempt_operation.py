"""add publish_attempt operation for remediation label writes

Revision ID: 20260513_000005
Revises: 20260511_000004
Create Date: 2026-05-13 02:30:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260513_000005"
down_revision = "20260511_000004"
branch_labels = None
depends_on = None


def _has_column(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    return column_name in {c["name"] for c in inspector.get_columns(table_name)}


def _has_constraint(inspector: sa.Inspector, table_name: str, constraint_name: str) -> bool:
    constraints = inspector.get_unique_constraints(table_name)
    return constraint_name in {c["name"] for c in constraints}


def _has_index(inspector: sa.Inspector, table_name: str, index_name: str) -> bool:
    return index_name in {i["name"] for i in inspector.get_indexes(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not _has_column(inspector, "publish_attempt", "operation"):
        op.add_column(
            "publish_attempt",
            sa.Column(
                "operation",
                sa.String(length=32),
                nullable=False,
                server_default="publish",
            ),
        )

    inspector = sa.inspect(bind)

    if _has_constraint(inspector, "publish_attempt", "uq_publish_attempt_job_attempt_no"):
        op.drop_constraint(
            "uq_publish_attempt_job_attempt_no",
            "publish_attempt",
            type_="unique",
        )

    inspector = sa.inspect(bind)

    if not _has_constraint(inspector, "publish_attempt", "uq_publish_attempt_job_attempt_no_operation"):
        op.create_unique_constraint(
            "uq_publish_attempt_job_attempt_no_operation",
            "publish_attempt",
            ["publish_job_id", "attempt_no", "operation"],
        )

    inspector = sa.inspect(bind)

    if not _has_index(inspector, "publish_attempt", "ix_publish_attempt_operation_started_at"):
        op.create_index(
            "ix_publish_attempt_operation_started_at",
            "publish_attempt",
            ["operation", "started_at"],
            unique=False,
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if _has_index(inspector, "publish_attempt", "ix_publish_attempt_operation_started_at"):
        op.drop_index("ix_publish_attempt_operation_started_at", table_name="publish_attempt")

    inspector = sa.inspect(bind)

    if _has_constraint(inspector, "publish_attempt", "uq_publish_attempt_job_attempt_no_operation"):
        op.drop_constraint(
            "uq_publish_attempt_job_attempt_no_operation",
            "publish_attempt",
            type_="unique",
        )

    inspector = sa.inspect(bind)

    if not _has_constraint(inspector, "publish_attempt", "uq_publish_attempt_job_attempt_no"):
        op.create_unique_constraint(
            "uq_publish_attempt_job_attempt_no",
            "publish_attempt",
            ["publish_job_id", "attempt_no"],
        )

    inspector = sa.inspect(bind)

    if _has_column(inspector, "publish_attempt", "operation"):
        op.drop_column("publish_attempt", "operation")
"""add publish_attempt operation for remediation label writes

Revision ID: 20260513_000005
Revises: 20260511_000004
Create Date: 2026-05-13 02:30:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260513_000005"
down_revision = "20260511_000004"
branch_labels = None
depends_on = None


def _has_column(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    return column_name in {c["name"] for c in inspector.get_columns(table_name)}


def _has_constraint(inspector: sa.Inspector, table_name: str, constraint_name: str) -> bool:
    constraints = inspector.get_unique_constraints(table_name)
    return constraint_name in {c["name"] for c in constraints}


def _has_index(inspector: sa.Inspector, table_name: str, index_name: str) -> bool:
    return index_name in {i["name"] for i in inspector.get_indexes(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not _has_column(inspector, "publish_attempt", "operation"):
        op.add_column(
            "publish_attempt",
            sa.Column(
                "operation",
                sa.String(length=32),
                nullable=False,
                server_default="publish",
            ),
        )

    inspector = sa.inspect(bind)

    if _has_constraint(inspector, "publish_attempt", "uq_publish_attempt_job_attempt_no"):
        op.drop_constraint(
            "uq_publish_attempt_job_attempt_no",
            "publish_attempt",
            type_="unique",
        )

    inspector = sa.inspect(bind)

    if not _has_constraint(inspector, "publish_attempt", "uq_publish_attempt_job_attempt_no_operation"):
        op.create_unique_constraint(
            "uq_publish_attempt_job_attempt_no_operation",
            "publish_attempt",
            ["publish_job_id", "attempt_no", "operation"],
        )

    inspector = sa.inspect(bind)

    if not _has_index(inspector, "publish_attempt", "ix_publish_attempt_operation_started_at"):
        op.create_index(
            "ix_publish_attempt_operation_started_at",
            "publish_attempt",
            ["operation", "started_at"],
            unique=False,
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if _has_index(inspector, "publish_attempt", "ix_publish_attempt_operation_started_at"):
        op.drop_index("ix_publish_attempt_operation_started_at", table_name="publish_attempt")

    inspector = sa.inspect(bind)

    if _has_constraint(inspector, "publish_attempt", "uq_publish_attempt_job_attempt_no_operation"):
        op.drop_constraint(
            "uq_publish_attempt_job_attempt_no_operation",
            "publish_attempt",
            type_="unique",
        )

    inspector = sa.inspect(bind)

    if not _has_constraint(inspector, "publish_attempt", "uq_publish_attempt_job_attempt_no"):
        op.create_unique_constraint(
            "uq_publish_attempt_job_attempt_no",
            "publish_attempt",
            ["publish_job_id", "attempt_no"],
        )

    inspector = sa.inspect(bind)

    if _has_column(inspector, "publish_attempt", "operation"):
        op.drop_column("publish_attempt", "operation")
