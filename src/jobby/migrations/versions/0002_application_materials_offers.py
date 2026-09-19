"""Persist application materials and offer snapshots.

Revision ID: 0002_materials_offers
Revises: 0001_initial

``0001_initial`` bootstraps from package metadata for fresh installations.
Conditional creation lets this migration work both for a fresh upgrade (where
those tables may already exist) and for a database stamped at the earlier 0001
schema.
"""

from alembic import op
import sqlalchemy as sa


revision = "0002_materials_offers"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    tables = set(sa.inspect(bind).get_table_names())

    if "application_materials" not in tables:
        op.create_table(
            "application_materials",
            sa.Column("application_id", sa.String(length=36), nullable=False),
            sa.Column("document_version_id", sa.String(length=36), nullable=False),
            sa.Column("purpose", sa.String(length=100), nullable=False),
            sa.Column("used_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("id", sa.String(length=36), nullable=False),
            sa.ForeignKeyConstraint(
                ["application_id"],
                ["applications.id"],
                ondelete="CASCADE",
            ),
            sa.ForeignKeyConstraint(
                ["document_version_id"],
                ["document_versions.id"],
                ondelete="RESTRICT",
            ),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint(
                "application_id",
                "document_version_id",
                "purpose",
                name="uq_application_material_purpose",
            ),
        )
        op.create_index(
            "ix_application_materials_application_id",
            "application_materials",
            ["application_id"],
            unique=False,
        )
        op.create_index(
            "ix_application_materials_document_version_id",
            "application_materials",
            ["document_version_id"],
            unique=False,
        )
        tables.add("application_materials")

    if "offers" not in tables:
        op.create_table(
            "offers",
            sa.Column("application_id", sa.String(length=36), nullable=False),
            sa.Column("base_salary", sa.Float(), nullable=False),
            sa.Column("annual_bonus", sa.Float(), nullable=False),
            sa.Column("annualized_equity", sa.Float(), nullable=False),
            sa.Column("currency", sa.String(length=3), nullable=False),
            sa.Column("cost_of_living_index", sa.Float(), nullable=False),
            sa.Column("stress_score", sa.Float(), nullable=False),
            sa.Column("terms", sa.JSON(), nullable=False),
            sa.Column("decision", sa.String(length=100), nullable=True),
            sa.Column("offered_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("id", sa.String(length=36), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.ForeignKeyConstraint(
                ["application_id"],
                ["applications.id"],
                ondelete="CASCADE",
            ),
            sa.PrimaryKeyConstraint("id"),
        )
        op.create_index(
            "ix_offers_application_id", "offers", ["application_id"], unique=False
        )
        op.create_index("ix_offers_decision", "offers", ["decision"], unique=False)
        op.create_index(
            "ix_offers_application_decision",
            "offers",
            ["application_id", "decision"],
            unique=False,
        )


def downgrade() -> None:
    tables = set(sa.inspect(op.get_bind()).get_table_names())
    if "offers" in tables:
        op.drop_table("offers")
    if "application_materials" in tables:
        op.drop_table("application_materials")
