"""Add one-time hash-bound approval intents for non-human mutations.

Revision ID: 0011_mutation_approvals
Revises: 0010_headless_workflows
"""

from alembic import op
import sqlalchemy as sa


revision = "0011_mutation_approvals"
down_revision = "0010_headless_workflows"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "mutation_approvals",
        sa.Column("action", sa.String(length=200), nullable=False),
        sa.Column("payload_hash", sa.String(length=64), nullable=False),
        sa.Column("actor", sa.String(length=100), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("consumed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_mutation_approvals_action", "mutation_approvals", ["action"])
    op.create_index(
        "ix_mutation_approvals_payload_hash", "mutation_approvals", ["payload_hash"]
    )
    op.create_index(
        "ix_mutation_approvals_actor_status", "mutation_approvals", ["actor", "status"]
    )
    op.create_index("ix_mutation_approvals_status", "mutation_approvals", ["status"])
    op.create_index(
        "ix_mutation_approvals_expires_at", "mutation_approvals", ["expires_at"]
    )


def downgrade() -> None:
    op.drop_table("mutation_approvals")
