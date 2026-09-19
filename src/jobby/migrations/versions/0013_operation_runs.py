"""Add durable background operation status records.

Revision ID: 0013_operation_runs
Revises: 0012_consolidate_workflow_indexes
"""

from alembic import op
import sqlalchemy as sa


revision = "0013_operation_runs"
down_revision = "0012_consolidate_workflow_indexes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "operation_runs",
        sa.Column("kind", sa.String(length=80), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("request_json", sa.JSON(), nullable=False),
        sa.Column("result_json", sa.JSON(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_operation_runs_kind", "operation_runs", ["kind"])
    op.create_index("ix_operation_runs_status", "operation_runs", ["status"])
    op.create_index(
        "ix_operation_runs_status_created", "operation_runs", ["status", "created_at"]
    )


def downgrade() -> None:
    op.drop_table("operation_runs")
