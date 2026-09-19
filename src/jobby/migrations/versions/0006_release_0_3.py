"""Release 0.3 discovery profiles, source telemetry, and source state.

Revision ID: 0006_release_0_3
Revises: 0005_release_0_2
"""

from alembic import op
import sqlalchemy as sa


revision = "0006_release_0_3"
down_revision = "0005_release_0_2"
branch_labels = None
depends_on = None

DEFAULT_PROFILE_ID = "00000000-0000-4000-8000-000000000003"


def upgrade() -> None:
    op.create_table(
        "scan_profiles",
        sa.Column("name", sa.String(length=300), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("source_selectors", sa.JSON(), nullable=False),
        sa.Column("query_pack", sa.JSON(), nullable=False),
        sa.Column("location_filters", sa.JSON(), nullable=False),
        sa.Column("role_filters", sa.JSON(), nullable=False),
        sa.Column("hydration_policy", sa.String(length=40), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.create_index("ix_scan_profiles_enabled", "scan_profiles", ["enabled"])

    # Recreate explicitly: SQLite cannot ALTER in an FK, and Alembic's generic
    # batch rename leaves quoted table SQL that is not schema-equivalent to a
    # fresh metadata build.
    with op.get_context().autocommit_block():
        raw = op.get_bind()
        raw.exec_driver_sql("PRAGMA foreign_keys=OFF")
        raw.exec_driver_sql(
            """
            CREATE TABLE _jobby_scan_runs_copy AS
            SELECT status, "query", requested_sources, source_results,
                   started_at, finished_at, discovered_count, error_summary,
                   id, created_at, updated_at
            FROM scan_runs
            """
        )
        raw.exec_driver_sql("DROP TABLE scan_runs")
        raw.exec_driver_sql(
            """
            CREATE TABLE scan_runs (
                status VARCHAR(9) NOT NULL,
                "query" TEXT,
                requested_sources JSON NOT NULL,
                source_results JSON NOT NULL,
                started_at DATETIME,
                finished_at DATETIME,
                discovered_count INTEGER NOT NULL,
                error_summary TEXT,
                id VARCHAR(36) NOT NULL,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                scan_kind VARCHAR(40) DEFAULT 'manual' NOT NULL,
                profile_id VARCHAR(36),
                fts_sync_status VARCHAR(40),
                fts_sync_error TEXT,
                PRIMARY KEY (id),
                CONSTRAINT fk_scan_runs_profile_id_scan_profiles
                    FOREIGN KEY(profile_id) REFERENCES scan_profiles (id)
                    ON DELETE SET NULL
            )
            """
        )
        raw.exec_driver_sql(
            """
            INSERT INTO scan_runs(
                status, "query", requested_sources, source_results,
                started_at, finished_at, discovered_count, error_summary,
                id, created_at, updated_at, scan_kind, profile_id,
                fts_sync_status, fts_sync_error
            )
            SELECT status, "query", requested_sources, source_results,
                   started_at, finished_at, discovered_count, error_summary,
                   id, created_at, updated_at, 'manual', NULL, NULL, NULL
            FROM _jobby_scan_runs_copy
            """
        )
        raw.exec_driver_sql("DROP TABLE _jobby_scan_runs_copy")
        raw.exec_driver_sql("PRAGMA foreign_keys=ON")
        if raw.exec_driver_sql("PRAGMA foreign_key_check").first() is not None:
            raise RuntimeError("scan-run migration introduced a foreign-key violation")
    op.create_index("ix_scan_runs_scan_kind", "scan_runs", ["scan_kind"])
    op.create_index("ix_scan_runs_profile_id", "scan_runs", ["profile_id"])

    op.add_column(
        "source_observations",
        sa.Column("snapshot_hash", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "source_observations",
        sa.Column(
            "observation_kind",
            sa.String(length=40),
            server_default=sa.text("'content'"),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_source_observations_snapshot_hash",
        "source_observations",
        ["snapshot_hash"],
    )

    op.create_table(
        "saved_discovery_views",
        sa.Column("name", sa.String(length=300), nullable=False),
        sa.Column("filters_json", sa.JSON(), nullable=False),
        sa.Column("sort_key", sa.String(length=80), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.create_table(
        "discovery_review_cursors",
        sa.Column("view_id", sa.String(length=36), nullable=False),
        sa.Column("reviewed_through", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reviewed_job_id", sa.String(length=36), nullable=True),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("view_id", name="uq_discovery_review_cursor_view"),
        sa.ForeignKeyConstraint(
            ["view_id"], ["saved_discovery_views.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["reviewed_job_id"], ["jobs.id"], ondelete="SET NULL"),
    )
    op.create_index(
        "ix_discovery_review_cursors_view_id",
        "discovery_review_cursors",
        ["view_id"],
    )

    op.create_table(
        "source_runs",
        sa.Column("scan_run_id", sa.String(length=36), nullable=False),
        sa.Column("source", sa.String(length=200), nullable=False),
        sa.Column("attempted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("succeeded_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_seconds", sa.Float(), nullable=False),
        sa.Column("result_count", sa.Integer(), nullable=False),
        sa.Column("reported_total", sa.Integer(), nullable=True),
        sa.Column("retries", sa.Integer(), nullable=False),
        sa.Column("complete", sa.Boolean(), nullable=False),
        sa.Column("failure_class", sa.String(length=100), nullable=True),
        sa.Column("anomaly_state", sa.String(length=40), nullable=False),
        sa.Column("failure_streak", sa.Integer(), nullable=False),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.ForeignKeyConstraint(["scan_run_id"], ["scan_runs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_source_runs_scan_run_id", "source_runs", ["scan_run_id"])
    op.create_index("ix_source_runs_source", "source_runs", ["source"])
    op.create_index(
        "ix_source_runs_source_attempt", "source_runs", ["source", "attempted_at"]
    )
    op.create_index("ix_source_runs_failure_class", "source_runs", ["failure_class"])
    op.create_index("ix_source_runs_anomaly_state", "source_runs", ["anomaly_state"])

    op.create_table(
        "source_health",
        sa.Column("source", sa.String(length=200), nullable=False),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_complete_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_result_count", sa.Integer(), nullable=True),
        sa.Column("last_reported_total", sa.Integer(), nullable=True),
        sa.Column("failure_streak", sa.Integer(), nullable=False),
        sa.Column("last_failure_class", sa.String(length=100), nullable=True),
        sa.Column("anomaly_state", sa.String(length=40), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source"),
    )
    op.create_index(
        "ix_source_health_anomaly_state", "source_health", ["anomaly_state"]
    )

    op.create_table(
        "job_source_states",
        sa.Column("job_id", sa.String(length=36), nullable=False),
        sa.Column("source", sa.String(length=200), nullable=False),
        sa.Column("source_job_id", sa.String(length=500), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("seen_count", sa.Integer(), nullable=False),
        sa.Column("last_content_hash", sa.String(length=64), nullable=True),
        sa.Column("last_snapshot_hash", sa.String(length=64), nullable=True),
        sa.Column("last_source_run_id", sa.String(length=36), nullable=True),
        sa.Column("is_live", sa.Boolean(), nullable=True),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source", "source_job_id", name="uq_job_source_state_identity"
        ),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["last_source_run_id"], ["source_runs.id"], ondelete="SET NULL"
        ),
    )
    op.create_index("ix_job_source_states_job_id", "job_source_states", ["job_id"])
    op.create_index(
        "ix_job_source_state_job_seen",
        "job_source_states",
        ["job_id", "last_seen_at"],
    )
    op.create_index(
        "ix_job_source_states_last_content_hash",
        "job_source_states",
        ["last_content_hash"],
    )
    op.create_index(
        "ix_job_source_states_last_snapshot_hash",
        "job_source_states",
        ["last_snapshot_hash"],
    )
    op.create_index(
        "ix_job_source_states_last_source_run_id",
        "job_source_states",
        ["last_source_run_id"],
    )

    # The seed is deliberately disabled and exists only for review/editing.
    op.execute(
        sa.text(
            """
            INSERT INTO scan_profiles(
                name, description, source_selectors, query_pack,
                location_filters, role_filters, hydration_policy, enabled,
                id, created_at, updated_at
            ) VALUES (
                'Focused legal AI / IP / policy',
                'Seeded for review; scheduling remains disabled until explicitly enabled.',
                '["all"]',
                '["legal AI", "intellectual property", "AI policy", "legal technology"]',
                '["United States", "remote"]',
                '["legal", "policy", "IP", "governance", "technology"]',
                'focused', 0, '00000000-0000-4000-8000-000000000003',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
            """
        )
    )


def downgrade() -> None:
    op.drop_table("job_source_states")
    op.drop_table("source_health")
    op.drop_table("source_runs")
    op.drop_table("discovery_review_cursors")
    op.drop_table("saved_discovery_views")
    op.drop_index(
        "ix_source_observations_snapshot_hash", table_name="source_observations"
    )
    op.drop_column("source_observations", "observation_kind")
    op.drop_column("source_observations", "snapshot_hash")
    op.drop_index("ix_scan_runs_profile_id", table_name="scan_runs")
    op.drop_index("ix_scan_runs_scan_kind", table_name="scan_runs")

    # ``profile_id`` owns a named foreign key. SQLite cannot drop that column
    # independently, so restore the complete pre-0.3 table while preserving
    # rows and every incoming reference to ``scan_runs.id``.
    with op.get_context().autocommit_block():
        raw = op.get_bind()
        raw.exec_driver_sql("PRAGMA foreign_keys=OFF")
        try:
            raw.exec_driver_sql(
                """
                CREATE TABLE _jobby_scan_runs_downgrade AS
                SELECT status, "query", requested_sources, source_results,
                       started_at, finished_at, discovered_count, error_summary,
                       id, created_at, updated_at
                FROM scan_runs
                """
            )
            raw.exec_driver_sql("DROP TABLE scan_runs")
            raw.exec_driver_sql(
                """
                CREATE TABLE scan_runs (
                    status VARCHAR(9) NOT NULL,
                    "query" TEXT,
                    requested_sources JSON NOT NULL,
                    source_results JSON NOT NULL,
                    started_at DATETIME,
                    finished_at DATETIME,
                    discovered_count INTEGER NOT NULL,
                    error_summary TEXT,
                    id VARCHAR(36) NOT NULL,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL,
                    PRIMARY KEY (id)
                )
                """
            )
            raw.exec_driver_sql(
                """
                INSERT INTO scan_runs(
                    status, "query", requested_sources, source_results,
                    started_at, finished_at, discovered_count, error_summary,
                    id, created_at, updated_at
                )
                SELECT status, "query", requested_sources, source_results,
                       started_at, finished_at, discovered_count, error_summary,
                       id, created_at, updated_at
                FROM _jobby_scan_runs_downgrade
                """
            )
            raw.exec_driver_sql("DROP TABLE _jobby_scan_runs_downgrade")
        finally:
            raw.exec_driver_sql("PRAGMA foreign_keys=ON")
        if raw.exec_driver_sql("PRAGMA foreign_key_check").first() is not None:
            raise RuntimeError("scan-run downgrade introduced a foreign-key violation")
    op.drop_table("scan_profiles")
