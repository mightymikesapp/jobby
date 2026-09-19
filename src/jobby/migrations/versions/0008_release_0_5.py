"""Release 0.5 canonical duplicate groups, alert inbox, and analytics state.

Revision ID: 0008_release_0_5
Revises: 0007_release_0_4
"""

from uuid import UUID, uuid5

from alembic import op
import sqlalchemy as sa


revision = "0008_release_0_5"
down_revision = "0007_release_0_4"
branch_labels = None
depends_on = None

_CANONICAL_GROUP_NAMESPACE = UUID("af34ef7f-ffcb-5f61-8baf-cc3544978ac0")


def _deterministic_id(kind: str, *parts: str) -> str:
    """Return a collision-resistant UUID whose input encoding is unambiguous."""
    encoded = kind + "|" + "".join(f"{len(part)}:{part}" for part in parts)
    return str(uuid5(_CANONICAL_GROUP_NAMESPACE, encoded))


def _migrate_confirmed_duplicate_groups(raw: sa.Connection) -> None:
    relationships = (
        raw.exec_driver_sql(
            """
        SELECT job_id, duplicate_job_id, id, created_at, updated_at
        FROM duplicate_relationships
        WHERE confirmed = 1
        ORDER BY job_id, duplicate_job_id, id
        """
        )
        .mappings()
        .all()
    )
    if not relationships:
        return

    adjacency: dict[str, set[str]] = {}
    for relationship in relationships:
        job_id = str(relationship["job_id"])
        duplicate_job_id = str(relationship["duplicate_job_id"])
        adjacency.setdefault(job_id, set()).add(duplicate_job_id)
        adjacency.setdefault(duplicate_job_id, set()).add(job_id)

    job_to_group: dict[str, str] = {}
    unseen = set(adjacency)
    while unseen:
        frontier = [min(unseen)]
        component: set[str] = set()
        while frontier:
            job_id = frontier.pop()
            if job_id in component:
                continue
            component.add(job_id)
            frontier.extend(sorted(adjacency[job_id] - component, reverse=True))

        unseen.difference_update(component)
        members = sorted(component)
        group_id = _deterministic_id("canonical-group", *members)
        component_relationships = [
            relationship
            for relationship in relationships
            if str(relationship["job_id"]) in component
        ]
        created_at = min(
            relationship["created_at"] for relationship in component_relationships
        )
        updated_at = max(
            relationship["updated_at"] for relationship in component_relationships
        )
        canonical_job_id = members[0]

        raw.exec_driver_sql(
            """
            INSERT INTO canonical_job_groups(
                canonical_job_id, notes, id, created_at, updated_at
            ) VALUES (?, 'Migrated confirmed duplicate component', ?, ?, ?)
            """,
            (canonical_job_id, group_id, created_at, updated_at),
        )
        for job_id in members:
            member_id = _deterministic_id("canonical-member", group_id, job_id)
            raw.exec_driver_sql(
                """
                INSERT INTO canonical_job_members(
                    group_id, job_id, is_canonical, hidden_by_default,
                    id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    group_id,
                    job_id,
                    int(job_id == canonical_job_id),
                    int(job_id != canonical_job_id),
                    member_id,
                    created_at,
                    updated_at,
                ),
            )
            job_to_group[job_id] = group_id

    for relationship in relationships:
        job_id = str(relationship["job_id"])
        duplicate_job_id = str(relationship["duplicate_job_id"])
        group_id = job_to_group[job_id]
        if job_to_group[duplicate_job_id] != group_id:
            raise RuntimeError(
                "confirmed duplicate endpoints were assigned to different groups"
            )
        raw.exec_driver_sql(
            """
            UPDATE duplicate_relationships
            SET canonical_group_id = ?
            WHERE id = ?
            """,
            (group_id, relationship["id"]),
        )

    invalid_relationship = raw.exec_driver_sql(
        """
        SELECT d.id
        FROM duplicate_relationships AS d
        WHERE d.confirmed = 1
          AND (
              d.canonical_group_id IS NULL
              OR NOT EXISTS (
                  SELECT 1 FROM canonical_job_members AS m
                  WHERE m.group_id = d.canonical_group_id
                    AND m.job_id = d.job_id
              )
              OR NOT EXISTS (
                  SELECT 1 FROM canonical_job_members AS m
                  WHERE m.group_id = d.canonical_group_id
                    AND m.job_id = d.duplicate_job_id
              )
          )
        LIMIT 1
        """
    ).first()
    if invalid_relationship is not None:
        raise RuntimeError(
            "confirmed duplicate migration produced an incomplete canonical group"
        )


def upgrade() -> None:
    op.create_table(
        "canonical_job_groups",
        sa.Column("canonical_job_id", sa.String(length=36), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("canonical_job_id"),
        sa.ForeignKeyConstraint(["canonical_job_id"], ["jobs.id"], ondelete="RESTRICT"),
    )
    op.create_table(
        "canonical_job_members",
        sa.Column("group_id", sa.String(length=36), nullable=False),
        sa.Column("job_id", sa.String(length=36), nullable=False),
        sa.Column("is_canonical", sa.Boolean(), nullable=False),
        sa.Column("hidden_by_default", sa.Boolean(), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("job_id", name="uq_canonical_job_member_job"),
        sa.ForeignKeyConstraint(
            ["group_id"], ["canonical_job_groups.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="RESTRICT"),
    )
    op.create_index(
        "ix_canonical_job_members_group_id", "canonical_job_members", ["group_id"]
    )
    op.create_index(
        "ix_canonical_job_members_job_id", "canonical_job_members", ["job_id"]
    )
    op.create_index(
        "ix_canonical_job_member_group",
        "canonical_job_members",
        ["group_id", "is_canonical"],
    )

    with op.get_context().autocommit_block():
        raw = op.get_bind()
        raw.exec_driver_sql("PRAGMA foreign_keys=OFF")
        raw.exec_driver_sql(
            """
            CREATE TABLE _jobby_duplicates_copy AS
            SELECT job_id, duplicate_job_id, rule, similarity, confirmed,
                   id, created_at, updated_at
            FROM duplicate_relationships
            """
        )
        raw.exec_driver_sql("DROP TABLE duplicate_relationships")
        raw.exec_driver_sql(
            """
            CREATE TABLE duplicate_relationships (
                job_id VARCHAR(36) NOT NULL,
                duplicate_job_id VARCHAR(36) NOT NULL,
                rule VARCHAR(80) NOT NULL,
                similarity FLOAT,
                confirmed BOOLEAN NOT NULL,
                id VARCHAR(36) NOT NULL,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                resolution VARCHAR(20) DEFAULT 'pending' NOT NULL,
                comparison_identity VARCHAR(64),
                resolved_at DATETIME,
                canonical_group_id VARCHAR(36),
                PRIMARY KEY (id),
                CONSTRAINT uq_duplicate_pair UNIQUE (job_id, duplicate_job_id),
                FOREIGN KEY(job_id) REFERENCES jobs (id) ON DELETE CASCADE,
                FOREIGN KEY(duplicate_job_id) REFERENCES jobs (id) ON DELETE CASCADE,
                CONSTRAINT fk_duplicate_relationships_group
                    FOREIGN KEY(canonical_group_id)
                    REFERENCES canonical_job_groups (id) ON DELETE SET NULL
            )
            """
        )
        raw.exec_driver_sql(
            """
            INSERT INTO duplicate_relationships(
                job_id, duplicate_job_id, rule, similarity, confirmed,
                id, created_at, updated_at, resolution, comparison_identity,
                resolved_at, canonical_group_id
            )
            SELECT job_id, duplicate_job_id, rule, similarity, confirmed,
                   id, created_at, updated_at,
                   CASE WHEN confirmed = 1 THEN 'confirmed' ELSE 'pending' END,
                   NULL, CASE WHEN confirmed = 1 THEN updated_at ELSE NULL END, NULL
            FROM _jobby_duplicates_copy
            """
        )
        raw.exec_driver_sql("DROP TABLE _jobby_duplicates_copy")
        raw.exec_driver_sql("PRAGMA foreign_keys=ON")
        if raw.exec_driver_sql("PRAGMA foreign_key_check").first() is not None:
            raise RuntimeError("duplicate migration introduced a foreign-key violation")
    op.create_index(
        "ix_duplicate_relationships_job_id", "duplicate_relationships", ["job_id"]
    )
    op.create_index(
        "ix_duplicate_relationships_duplicate_job_id",
        "duplicate_relationships",
        ["duplicate_job_id"],
    )
    op.create_index(
        "ix_duplicate_relationships_resolution",
        "duplicate_relationships",
        ["resolution"],
    )
    op.create_index(
        "ix_duplicate_relationships_comparison_identity",
        "duplicate_relationships",
        ["comparison_identity"],
    )
    op.create_index(
        "ix_duplicate_relationships_canonical_group_id",
        "duplicate_relationships",
        ["canonical_group_id"],
    )
    # Legacy confirmations form an undirected graph. Materialize one group per
    # connected component so overlapping pairs never violate the one-group-per-
    # job invariant. IDs depend only on sorted component membership, remain
    # within the persisted 36-character contract, and are stable on rehearsal.
    _migrate_confirmed_duplicate_groups(op.get_bind())

    op.add_column(
        "alerts", sa.Column("fingerprint", sa.String(length=64), nullable=True)
    )
    op.add_column(
        "alerts",
        sa.Column(
            "recurrence_count",
            sa.Integer(),
            server_default=sa.text("1"),
            nullable=False,
        ),
    )
    op.add_column(
        "alerts",
        sa.Column("last_recurred_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "alerts", sa.Column("snoozed_until", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column(
        "alerts", sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column("alerts", sa.Column("resolution_reason", sa.Text(), nullable=True))
    op.add_column(
        "alerts", sa.Column("entity_type", sa.String(length=100), nullable=True)
    )
    op.add_column(
        "alerts", sa.Column("entity_id", sa.String(length=100), nullable=True)
    )
    op.create_index("ix_alerts_fingerprint", "alerts", ["fingerprint"], unique=True)
    op.create_index("ix_alerts_snoozed_until", "alerts", ["snoozed_until"])
    op.create_index("ix_alerts_resolved_at", "alerts", ["resolved_at"])
    op.create_index("ix_alerts_entity_type", "alerts", ["entity_type"])
    op.create_index("ix_alerts_entity_id", "alerts", ["entity_id"])


def downgrade() -> None:
    op.drop_index("ix_alerts_entity_id", table_name="alerts")
    op.drop_index("ix_alerts_entity_type", table_name="alerts")
    op.drop_index("ix_alerts_resolved_at", table_name="alerts")
    op.drop_index("ix_alerts_snoozed_until", table_name="alerts")
    op.drop_index("ix_alerts_fingerprint", table_name="alerts")
    op.drop_column("alerts", "entity_id")
    op.drop_column("alerts", "entity_type")
    op.drop_column("alerts", "resolution_reason")
    op.drop_column("alerts", "resolved_at")
    op.drop_column("alerts", "snoozed_until")
    op.drop_column("alerts", "last_recurred_at")
    op.drop_column("alerts", "recurrence_count")
    op.drop_column("alerts", "fingerprint")
    op.drop_index(
        "ix_duplicate_relationships_canonical_group_id",
        table_name="duplicate_relationships",
    )
    op.drop_index(
        "ix_duplicate_relationships_comparison_identity",
        table_name="duplicate_relationships",
    )
    op.drop_index(
        "ix_duplicate_relationships_resolution", table_name="duplicate_relationships"
    )
    op.drop_table("canonical_job_members")

    # The canonical-group column owns a named foreign key. Rebuild the legacy
    # pair table rather than asking SQLite to drop a constrained column, while
    # preserving all legacy relationship fields and incoming job references.
    with op.get_context().autocommit_block():
        raw = op.get_bind()
        raw.exec_driver_sql("PRAGMA foreign_keys=OFF")
        try:
            raw.exec_driver_sql(
                """
                CREATE TABLE _jobby_duplicates_downgrade AS
                SELECT job_id, duplicate_job_id, rule, similarity, confirmed,
                       id, created_at, updated_at
                FROM duplicate_relationships
                """
            )
            raw.exec_driver_sql("DROP TABLE duplicate_relationships")
            raw.exec_driver_sql(
                """
                CREATE TABLE duplicate_relationships (
                    job_id VARCHAR(36) NOT NULL,
                    duplicate_job_id VARCHAR(36) NOT NULL,
                    rule VARCHAR(80) NOT NULL,
                    similarity FLOAT,
                    confirmed BOOLEAN NOT NULL,
                    id VARCHAR(36) NOT NULL,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL,
                    PRIMARY KEY (id),
                    CONSTRAINT uq_duplicate_pair UNIQUE (job_id, duplicate_job_id),
                    FOREIGN KEY(job_id) REFERENCES jobs (id) ON DELETE CASCADE,
                    FOREIGN KEY(duplicate_job_id) REFERENCES jobs (id) ON DELETE CASCADE
                )
                """
            )
            raw.exec_driver_sql(
                """
                INSERT INTO duplicate_relationships(
                    job_id, duplicate_job_id, rule, similarity, confirmed,
                    id, created_at, updated_at
                )
                SELECT job_id, duplicate_job_id, rule, similarity, confirmed,
                       id, created_at, updated_at
                FROM _jobby_duplicates_downgrade
                """
            )
            raw.exec_driver_sql("DROP TABLE _jobby_duplicates_downgrade")
        finally:
            raw.exec_driver_sql("PRAGMA foreign_keys=ON")
        if raw.exec_driver_sql("PRAGMA foreign_key_check").first() is not None:
            raise RuntimeError("duplicate downgrade introduced a foreign-key violation")
    op.create_index(
        "ix_duplicate_relationships_job_id", "duplicate_relationships", ["job_id"]
    )
    op.create_index(
        "ix_duplicate_relationships_duplicate_job_id",
        "duplicate_relationships",
        ["duplicate_job_id"],
    )
    op.drop_table("canonical_job_groups")
