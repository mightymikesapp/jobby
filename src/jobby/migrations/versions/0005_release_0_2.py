"""Release 0.2 safety, URL, compensation, and evaluation contracts.

Revision ID: 0005_release_0_2
Revises: 0004_search_ai_cache
"""

from alembic import op
import sqlalchemy as sa


revision = "0005_release_0_2"
down_revision = "0004_search_ai_cache"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("jobs", sa.Column("launch_url", sa.Text(), nullable=True))
    op.add_column("jobs", sa.Column("comparison_url", sa.Text(), nullable=True))
    op.add_column(
        "jobs",
        sa.Column(
            "compensation_period",
            sa.String(length=20),
            server_default=sa.text("'unknown'"),
            nullable=False,
        ),
    )
    op.add_column(
        "jobs",
        sa.Column(
            "compensation_confidence",
            sa.Float(),
            server_default=sa.text("0"),
            nullable=False,
        ),
    )
    op.add_column("jobs", sa.Column("compensation_evidence", sa.Text(), nullable=True))
    # Existing values are copied byte-for-byte. In particular, no URL
    # normalizer is run during migration.
    op.execute(
        sa.text(
            "UPDATE jobs SET launch_url = canonical_url, comparison_url = canonical_url"
        )
    )
    op.create_index("ix_jobs_comparison_url", "jobs", ["comparison_url"])

    op.add_column(
        "evaluations", sa.Column("fingerprint", sa.String(length=64), nullable=True)
    )
    op.add_column(
        "evaluations", sa.Column("payload_hash", sa.String(length=64), nullable=True)
    )
    op.add_column(
        "evaluations",
        sa.Column(
            "ranker_version",
            sa.String(length=100),
            server_default=sa.text("'deterministic-v1'"),
            nullable=False,
        ),
    )
    op.add_column(
        "evaluations",
        sa.Column(
            "evaluation_kind",
            sa.String(length=40),
            server_default=sa.text("'automatic'"),
            nullable=False,
        ),
    )
    op.add_column(
        "evaluations", sa.Column("reference_key", sa.String(length=200), nullable=True)
    )
    op.create_index("ix_evaluations_fingerprint", "evaluations", ["fingerprint"])
    op.create_index("ix_evaluations_payload_hash", "evaluations", ["payload_hash"])
    op.create_index("ix_evaluations_reference_key", "evaluations", ["reference_key"])
    op.create_index(
        "ix_evaluation_job_fingerprint",
        "evaluations",
        ["job_id", "fingerprint"],
        unique=True,
        sqlite_where=sa.text(
            "fingerprint IS NOT NULL AND evaluation_kind = 'automatic'"
        ),
    )

    op.create_table(
        "evaluation_compaction_batches",
        sa.Column("manifest_hash", sa.String(length=64), nullable=False),
        sa.Column("backup_path", sa.Text(), nullable=False),
        sa.Column("candidate_count", sa.Integer(), nullable=False),
        sa.Column("removed_count", sa.Integer(), nullable=False),
        sa.Column("retained_count", sa.Integer(), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_evaluation_compaction_batches_manifest_hash",
        "evaluation_compaction_batches",
        ["manifest_hash"],
        unique=True,
    )
    op.create_table(
        "evaluation_compaction_ledger",
        sa.Column("batch_id", sa.String(length=36), nullable=False),
        sa.Column("removed_evaluation_id", sa.String(length=36), nullable=False),
        sa.Column("retained_evaluation_id", sa.String(length=36), nullable=False),
        sa.Column("job_id", sa.String(length=36), nullable=False),
        sa.Column("payload_hash", sa.String(length=64), nullable=False),
        sa.Column("manifest_hash", sa.String(length=64), nullable=False),
        sa.Column("removed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "batch_id", "removed_evaluation_id", name="uq_compaction_removed_eval"
        ),
        sa.ForeignKeyConstraint(
            ["batch_id"],
            ["evaluation_compaction_batches.id"],
            ondelete="RESTRICT",
        ),
    )
    op.create_index(
        "ix_evaluation_compaction_ledger_batch_id",
        "evaluation_compaction_ledger",
        ["batch_id"],
    )
    op.create_index(
        "ix_evaluation_compaction_ledger_job_id",
        "evaluation_compaction_ledger",
        ["job_id"],
    )
    op.create_index(
        "ix_evaluation_compaction_ledger_payload_hash",
        "evaluation_compaction_ledger",
        ["payload_hash"],
    )


def downgrade() -> None:
    op.drop_table("evaluation_compaction_ledger")
    op.drop_table("evaluation_compaction_batches")
    op.drop_index("ix_evaluation_job_fingerprint", table_name="evaluations")
    op.drop_index("ix_evaluations_reference_key", table_name="evaluations")
    op.drop_index("ix_evaluations_payload_hash", table_name="evaluations")
    op.drop_index("ix_evaluations_fingerprint", table_name="evaluations")
    op.drop_column("evaluations", "reference_key")
    op.drop_column("evaluations", "evaluation_kind")
    op.drop_column("evaluations", "ranker_version")
    op.drop_column("evaluations", "payload_hash")
    op.drop_column("evaluations", "fingerprint")
    op.drop_index("ix_jobs_comparison_url", table_name="jobs")
    op.drop_column("jobs", "compensation_evidence")
    op.drop_column("jobs", "compensation_confidence")
    op.drop_column("jobs", "compensation_period")
    op.drop_column("jobs", "comparison_url")
    op.drop_column("jobs", "launch_url")
