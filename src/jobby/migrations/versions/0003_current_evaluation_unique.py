"""Enforce one current evaluation per job.

Revision ID: 0003_current_eval
Revises: 0002_materials_offers
"""

from alembic import op
import sqlalchemy as sa


revision = "0003_current_eval"
down_revision = "0002_materials_offers"
branch_labels = None
depends_on = None


INDEX_NAME = "ix_evaluation_job_current"


def upgrade() -> None:
    connection = op.get_bind()
    # Preserve the most recently updated row when repairing a pre-constraint
    # database that already contains multiple current evaluations.
    connection.execute(
        sa.text(
            """
            WITH ranked AS (
                SELECT id,
                       ROW_NUMBER() OVER (
                           PARTITION BY job_id
                           ORDER BY updated_at DESC, created_at DESC, id DESC
                       ) AS position
                FROM evaluations
                WHERE is_current = 1
            )
            UPDATE evaluations
            SET is_current = 0
            WHERE id IN (SELECT id FROM ranked WHERE position > 1)
            """
        )
    )
    connection.execute(
        sa.text(
            """
            UPDATE jobs
            SET latest_score = (
                SELECT score
                FROM evaluations
                WHERE evaluations.job_id = jobs.id
                  AND evaluations.is_current = 1
            )
            WHERE EXISTS (
                SELECT 1
                FROM evaluations
                WHERE evaluations.job_id = jobs.id
                  AND evaluations.is_current = 1
            )
            """
        )
    )
    indexes = {
        item["name"]: item for item in sa.inspect(connection).get_indexes("evaluations")
    }
    existing = indexes.get(INDEX_NAME)
    if existing is not None and not existing.get("unique"):
        op.drop_index(INDEX_NAME, table_name="evaluations")
        existing = None
    if existing is None:
        op.create_index(
            INDEX_NAME,
            "evaluations",
            ["job_id"],
            unique=True,
            sqlite_where=sa.text("is_current = 1"),
        )


def downgrade() -> None:
    indexes = {
        item["name"]: item
        for item in sa.inspect(op.get_bind()).get_indexes("evaluations")
    }
    if INDEX_NAME in indexes:
        op.drop_index(INDEX_NAME, table_name="evaluations")
    op.create_index(
        INDEX_NAME,
        "evaluations",
        ["job_id", "is_current"],
        unique=False,
    )
