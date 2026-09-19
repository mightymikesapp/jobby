"""Release 0.6 verified backup and maintenance history.

Revision ID: 0009_release_0_6
Revises: 0008_release_0_5
"""

from alembic import op
import sqlalchemy as sa


revision = "0009_release_0_6"
down_revision = "0008_release_0_5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "backup_records",
        sa.Column("backup_kind", sa.String(length=40), nullable=False),
        sa.Column("path", sa.Text(), nullable=False),
        sa.Column("plaintext_sha256", sa.String(length=64), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("verified_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("external", sa.Boolean(), nullable=False),
        sa.Column("recovery_tested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_backup_records_backup_kind", "backup_records", ["backup_kind"])
    op.create_index(
        "ix_backup_records_plaintext_sha256",
        "backup_records",
        ["plaintext_sha256"],
    )
    op.create_table(
        "maintenance_runs",
        sa.Column("kind", sa.String(length=80), nullable=False),
        sa.Column("status", sa.String(length=40), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("result_json", sa.JSON(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_maintenance_runs_status", "maintenance_runs", ["status"])
    op.create_index(
        "ix_maintenance_kind_started",
        "maintenance_runs",
        ["kind", "started_at"],
    )


def downgrade() -> None:
    op.drop_table("maintenance_runs")
    op.drop_table("backup_records")
