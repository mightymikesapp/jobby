"""Release 0.4 capture authority and application command-center contracts.

Revision ID: 0007_release_0_4
Revises: 0006_release_0_3
"""

from alembic import op
import sqlalchemy as sa


revision = "0007_release_0_4"
down_revision = "0006_release_0_3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column(
            "authoritative_fields",
            sa.JSON(),
            server_default=sa.text("'[]'"),
            nullable=False,
        ),
    )
    with op.get_context().autocommit_block():
        raw = op.get_bind()
        raw.exec_driver_sql("PRAGMA foreign_keys=OFF")
        raw.exec_driver_sql(
            """
            CREATE TABLE _jobby_applications_copy AS
            SELECT job_id, current_stage, submission_channel, submitted_at,
                   follow_up_at, rejection_reason, notes, import_key,
                   id, created_at, updated_at
            FROM applications
            """
        )
        raw.exec_driver_sql("DROP TABLE applications")
        raw.exec_driver_sql(
            """
            CREATE TABLE applications (
                job_id VARCHAR(36) NOT NULL,
                current_stage VARCHAR(10) NOT NULL,
                submission_channel VARCHAR(200),
                submitted_at DATETIME,
                follow_up_at DATETIME,
                rejection_reason TEXT,
                notes TEXT,
                import_key VARCHAR(64),
                id VARCHAR(36) NOT NULL,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                applied_evaluation_id VARCHAR(36),
                applied_score FLOAT,
                applied_ranker_version VARCHAR(100),
                applied_sources JSON DEFAULT '[]' NOT NULL,
                PRIMARY KEY (id),
                FOREIGN KEY(job_id) REFERENCES jobs (id) ON DELETE RESTRICT,
                CONSTRAINT fk_applications_applied_evaluation
                    FOREIGN KEY(applied_evaluation_id) REFERENCES evaluations (id)
                    ON DELETE SET NULL
            )
            """
        )
        raw.exec_driver_sql(
            """
            INSERT INTO applications(
                job_id, current_stage, submission_channel, submitted_at,
                follow_up_at, rejection_reason, notes, import_key,
                id, created_at, updated_at, applied_evaluation_id,
                applied_score, applied_ranker_version, applied_sources
            )
            SELECT job_id, current_stage, submission_channel, submitted_at,
                   follow_up_at, rejection_reason, notes, import_key,
                   id, created_at, updated_at, NULL, NULL, NULL, '[]'
            FROM _jobby_applications_copy
            """
        )
        raw.exec_driver_sql("DROP TABLE _jobby_applications_copy")
        raw.exec_driver_sql("PRAGMA foreign_keys=ON")
        if raw.exec_driver_sql("PRAGMA foreign_key_check").first() is not None:
            raise RuntimeError(
                "application migration introduced a foreign-key violation"
            )
    op.create_index("ix_applications_job_id", "applications", ["job_id"])
    op.create_index("ix_applications_current_stage", "applications", ["current_stage"])
    op.create_index("ix_applications_follow_up_at", "applications", ["follow_up_at"])
    op.create_index(
        "ix_applications_import_key", "applications", ["import_key"], unique=True
    )
    op.create_index(
        "ix_applications_applied_evaluation_id",
        "applications",
        ["applied_evaluation_id"],
    )

    op.add_column(
        "tasks", sa.Column("automation_key", sa.String(length=200), nullable=True)
    )
    op.create_index("ix_tasks_automation_key", "tasks", ["automation_key"], unique=True)

    op.create_table(
        "application_contacts",
        sa.Column("application_id", sa.String(length=36), nullable=False),
        sa.Column("contact_id", sa.String(length=36), nullable=False),
        sa.Column("role", sa.String(length=200), nullable=True),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "application_id", "contact_id", name="uq_application_contact"
        ),
        sa.ForeignKeyConstraint(
            ["application_id"], ["applications.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["contact_id"], ["contacts.id"], ondelete="CASCADE"),
    )
    op.create_index(
        "ix_application_contacts_application_id",
        "application_contacts",
        ["application_id"],
    )
    op.create_index(
        "ix_application_contacts_contact_id",
        "application_contacts",
        ["contact_id"],
    )


def downgrade() -> None:
    op.drop_table("application_contacts")
    op.drop_index("ix_tasks_automation_key", table_name="tasks")
    op.drop_column("tasks", "automation_key")
    op.drop_index("ix_applications_applied_evaluation_id", table_name="applications")

    # ``applied_evaluation_id`` owns a named foreign key and applications has
    # many incoming references. Recreate the pre-0.4 shape under a temporary
    # FK suspension, then verify every surviving relationship before commit.
    with op.get_context().autocommit_block():
        raw = op.get_bind()
        raw.exec_driver_sql("PRAGMA foreign_keys=OFF")
        try:
            raw.exec_driver_sql(
                """
                CREATE TABLE _jobby_applications_downgrade AS
                SELECT job_id, current_stage, submission_channel, submitted_at,
                       follow_up_at, rejection_reason, notes, import_key,
                       id, created_at, updated_at
                FROM applications
                """
            )
            raw.exec_driver_sql("DROP TABLE applications")
            raw.exec_driver_sql(
                """
                CREATE TABLE applications (
                    job_id VARCHAR(36) NOT NULL,
                    current_stage VARCHAR(10) NOT NULL,
                    submission_channel VARCHAR(200),
                    submitted_at DATETIME,
                    follow_up_at DATETIME,
                    rejection_reason TEXT,
                    notes TEXT,
                    import_key VARCHAR(64),
                    id VARCHAR(36) NOT NULL,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL,
                    PRIMARY KEY (id),
                    FOREIGN KEY(job_id) REFERENCES jobs (id) ON DELETE RESTRICT
                )
                """
            )
            raw.exec_driver_sql(
                """
                INSERT INTO applications(
                    job_id, current_stage, submission_channel, submitted_at,
                    follow_up_at, rejection_reason, notes, import_key,
                    id, created_at, updated_at
                )
                SELECT job_id, current_stage, submission_channel, submitted_at,
                       follow_up_at, rejection_reason, notes, import_key,
                       id, created_at, updated_at
                FROM _jobby_applications_downgrade
                """
            )
            raw.exec_driver_sql("DROP TABLE _jobby_applications_downgrade")
        finally:
            raw.exec_driver_sql("PRAGMA foreign_keys=ON")
        if raw.exec_driver_sql("PRAGMA foreign_key_check").first() is not None:
            raise RuntimeError(
                "application downgrade introduced a foreign-key violation"
            )
    op.create_index("ix_applications_job_id", "applications", ["job_id"])
    op.create_index("ix_applications_current_stage", "applications", ["current_stage"])
    op.create_index("ix_applications_follow_up_at", "applications", ["follow_up_at"])
    op.create_index(
        "ix_applications_import_key", "applications", ["import_key"], unique=True
    )
    op.drop_column("jobs", "authoritative_fields")
