"""Add derived-search synchronization and guarded AI cache state.

Revision ID: 0004_search_ai_cache
Revises: 0003_current_eval
"""

from alembic import op
import sqlalchemy as sa


revision = "0004_search_ai_cache"
down_revision = "0003_current_eval"
branch_labels = None
depends_on = None


SEARCH_INDEX_TRIGGER_SQL = (
    """
    CREATE TRIGGER search_jobs_insert
    AFTER INSERT ON jobs
    BEGIN
        UPDATE search_index_state SET generation = generation + 1 WHERE id = 1;
        INSERT OR IGNORE INTO search_index_changes(job_id, generation)
        SELECT NEW.id, generation FROM search_index_state WHERE id = 1;
    END
    """,
    """
    CREATE TRIGGER search_jobs_update
    AFTER UPDATE OF title, description, category, company_id ON jobs
    BEGIN
        UPDATE search_index_state SET generation = generation + 1 WHERE id = 1;
        INSERT OR IGNORE INTO search_index_changes(job_id, generation)
        SELECT NEW.id, generation FROM search_index_state WHERE id = 1;
    END
    """,
    """
    CREATE TRIGGER search_jobs_delete
    AFTER DELETE ON jobs
    BEGIN
        UPDATE search_index_state SET generation = generation + 1 WHERE id = 1;
        INSERT OR IGNORE INTO search_index_changes(job_id, generation)
        SELECT OLD.id, generation FROM search_index_state WHERE id = 1;
    END
    """,
    """
    CREATE TRIGGER search_companies_rename
    AFTER UPDATE OF name ON companies
    WHEN OLD.name IS NOT NEW.name
    BEGIN
        UPDATE search_index_state SET generation = generation + 1 WHERE id = 1;
        INSERT OR IGNORE INTO search_index_changes(job_id, generation)
        SELECT id, (SELECT generation FROM search_index_state WHERE id = 1)
        FROM jobs WHERE company_id = NEW.id;
    END
    """,
)


def upgrade() -> None:
    connection = op.get_bind()

    op.create_table(
        "ai_cache_entries",
        sa.Column("provider", sa.String(80), nullable=False),
        sa.Column("model", sa.String(200), nullable=False),
        sa.Column("purpose", sa.String(100), nullable=False),
        sa.Column("prompt_version", sa.String(100), nullable=False),
        sa.Column("output_schema_hash", sa.String(64), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("max_output_tokens", sa.Integer(), nullable=False),
        sa.Column("cache_key", sa.String(64), nullable=False),
        sa.Column("output_json", sa.JSON(), nullable=False),
        sa.Column(
            "source_ai_run_id",
            sa.String(36),
            sa.ForeignKey("ai_runs.id", ondelete="SET NULL"),
        ),
        sa.Column("expires_at", sa.DateTime(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("invalidated_at", sa.DateTime()),
        sa.Column("id", sa.String(36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("cache_key", name="uq_ai_cache_key"),
        sa.UniqueConstraint(
            "provider",
            "model",
            "purpose",
            "prompt_version",
            "output_schema_hash",
            "request_hash",
            "max_output_tokens",
            name="uq_ai_cache_identity",
        ),
    )
    # SQLite cannot add a non-null column without retaining a server default,
    # and Alembic batch rename leaves a quoted table name in sqlite_master.
    # Recreate explicitly with FK enforcement briefly disabled in an Alembic
    # autocommit block. Incoming FKs keep their original ``ai_runs`` target,
    # and the immediate foreign-key check verifies the result before migration
    # continues.
    with op.get_context().autocommit_block():
        raw = op.get_bind()
        raw.exec_driver_sql("PRAGMA foreign_keys=OFF")
        raw.exec_driver_sql(
            """
            CREATE TABLE _jobby_ai_runs_copy AS
            SELECT purpose, provider, model, prompt_version, input_hash,
                   input_tokens, output_tokens, cached_tokens, output_json,
                   approval_state, approved_at, error,
                   0 AS cache_hit, NULL AS cache_entry_id,
                   NULL AS source_ai_run_id, id, created_at, updated_at
            FROM ai_runs
            """
        )
        raw.exec_driver_sql("DROP TABLE ai_runs")
        raw.exec_driver_sql(
            """
            CREATE TABLE ai_runs (
                purpose VARCHAR(100) NOT NULL,
                provider VARCHAR(80) NOT NULL,
                model VARCHAR(200) NOT NULL,
                prompt_version VARCHAR(100) NOT NULL,
                input_hash VARCHAR(64) NOT NULL,
                input_tokens INTEGER,
                output_tokens INTEGER,
                cached_tokens INTEGER,
                output_json JSON NOT NULL,
                approval_state VARCHAR(14) NOT NULL,
                approved_at DATETIME,
                error TEXT,
                cache_hit BOOLEAN NOT NULL,
                cache_entry_id VARCHAR(36),
                source_ai_run_id VARCHAR(36),
                id VARCHAR(36) NOT NULL,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                PRIMARY KEY (id),
                FOREIGN KEY(cache_entry_id) REFERENCES ai_cache_entries (id) ON DELETE SET NULL,
                FOREIGN KEY(source_ai_run_id) REFERENCES ai_runs (id) ON DELETE SET NULL
            )
            """
        )
        raw.exec_driver_sql(
            """
            INSERT INTO ai_runs(
                purpose, provider, model, prompt_version, input_hash,
                input_tokens, output_tokens, cached_tokens, output_json,
                approval_state, approved_at, error, cache_hit, cache_entry_id,
                source_ai_run_id, id, created_at, updated_at
            )
            SELECT purpose, provider, model, prompt_version, input_hash,
                   input_tokens, output_tokens, cached_tokens, output_json,
                   approval_state, approved_at, error, cache_hit, cache_entry_id,
                   source_ai_run_id, id, created_at, updated_at
            FROM _jobby_ai_runs_copy
            """
        )
        raw.exec_driver_sql("DROP TABLE _jobby_ai_runs_copy")
        raw.exec_driver_sql("PRAGMA foreign_keys=ON")
        if raw.exec_driver_sql("PRAGMA foreign_key_check").first() is not None:
            raise RuntimeError("AI cache migration introduced a foreign-key violation")
    op.create_index("ix_ai_runs_input_hash", "ai_runs", ["input_hash"])
    op.create_index("ix_ai_runs_purpose", "ai_runs", ["purpose"])
    op.create_index("ix_ai_runs_cache_entry_id", "ai_runs", ["cache_entry_id"])
    op.create_index("ix_ai_runs_source_ai_run_id", "ai_runs", ["source_ai_run_id"])
    op.create_index(
        "ix_ai_cache_entries_source_ai_run_id",
        "ai_cache_entries",
        ["source_ai_run_id"],
    )
    op.create_index(
        "ix_ai_cache_active_expiry",
        "ai_cache_entries",
        ["active", "expires_at"],
    )

    op.create_table(
        "search_index_state",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint("id = 1", name="ck_search_index_state_singleton"),
    )
    op.create_table(
        "search_index_changes",
        sa.Column("job_id", sa.String(36), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("job_id", "generation"),
    )
    op.create_index(
        "ix_search_index_changes_generation",
        "search_index_changes",
        ["generation"],
    )
    connection.execute(
        sa.text("INSERT INTO search_index_state(id, generation) VALUES (1, 0)")
    )
    for statement in SEARCH_INDEX_TRIGGER_SQL:
        connection.exec_driver_sql(statement)
    # Existing databases need one rebuild generation. Fresh databases simply
    # retain generation zero and will build an empty cache on first use.
    existing_jobs = int(
        connection.execute(sa.text("SELECT count(*) FROM jobs")).scalar_one()
    )
    if existing_jobs:
        connection.execute(
            sa.text("UPDATE search_index_state SET generation = 1 WHERE id = 1")
        )
        connection.execute(
            sa.text(
                "INSERT INTO search_index_changes(job_id, generation) "
                "SELECT id, 1 FROM jobs"
            )
        )


def downgrade() -> None:
    connection = op.get_bind()
    for name in (
        "search_companies_rename",
        "search_jobs_delete",
        "search_jobs_update",
        "search_jobs_insert",
    ):
        connection.exec_driver_sql(f"DROP TRIGGER IF EXISTS {name}")
    op.drop_index(
        "ix_search_index_changes_generation", table_name="search_index_changes"
    )
    op.drop_table("search_index_changes")
    op.drop_table("search_index_state")

    connection.execute(sa.text("DELETE FROM ai_cache_entries"))
    op.drop_index("ix_ai_runs_source_ai_run_id", table_name="ai_runs")
    op.drop_index("ix_ai_runs_cache_entry_id", table_name="ai_runs")
    with op.batch_alter_table("ai_runs", recreate="always") as batch:
        batch.drop_column("source_ai_run_id")
        batch.drop_column("cache_entry_id")
        batch.drop_column("cache_hit")
    op.drop_index("ix_ai_cache_entries_source_ai_run_id", table_name="ai_cache_entries")
    op.drop_index("ix_ai_cache_active_expiry", table_name="ai_cache_entries")
    op.drop_table("ai_cache_entries")
