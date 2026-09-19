from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from alembic import command
from alembic.config import Config


MIGRATIONS = Path(__file__).resolve().parents[2] / "src" / "jobby" / "migrations"


def _config(path: Path) -> Config:
    config = Config()
    config.set_main_option("script_location", str(MIGRATIONS))
    config.set_main_option(
        "sqlalchemy.url", f"sqlite+pysqlite:///{path}".replace("%", "%%")
    )
    return config


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {
        str(row[1])
        for row in connection.execute(f'PRAGMA table_info("{table}")').fetchall()
    }


def _tables(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }


def _indexes(connection: sqlite3.Connection, table: str) -> set[str]:
    return {
        str(row[1])
        for row in connection.execute(f'PRAGMA index_list("{table}")').fetchall()
    }


def _assert_healthy(connection: sqlite3.Connection, revision: str) -> None:
    assert connection.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
    assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    assert connection.execute("SELECT version_num FROM alembic_version").fetchone() == (
        revision,
    )


def _insert_job(
    connection: sqlite3.Connection, job_id: str, title: str, now: str
) -> None:
    connection.execute(
        """
        INSERT INTO jobs(
            company_id, title, normalized_title, status, salary_currency,
            discovered_at, consecutive_misses, explicit_closure,
            liveness_known, manual_status_locked, id, created_at, updated_at
        ) VALUES (
            'company-1', ?, ?, 'discovered', 'USD', ?, 0, 0, 0, 0, ?, ?, ?
        )
        """,
        (title, title.casefold(), now, job_id, now, now),
    )


def test_0006_downgrade_recreates_scan_runs_and_preserves_incoming_references(
    tmp_path: Path,
) -> None:
    path = tmp_path / "release-0.3.sqlite3"
    config = _config(path)
    command.upgrade(config, "0006_release_0_3")
    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute(
            """
            INSERT INTO scan_runs(
                status, "query", requested_sources, source_results,
                discovered_count, id, created_at, updated_at,
                scan_kind, profile_id, fts_sync_status
            ) VALUES (
                'queued', 'legal AI', '[]', '{}', 3, 'scan-1', ?, ?,
                'focused', '00000000-0000-4000-8000-000000000003', 'complete'
            )
            """,
            (now, now),
        )
        connection.execute(
            """
            INSERT INTO agent_runs(
                status, scan_run_id, summary, id, created_at, updated_at
            ) VALUES ('queued', 'scan-1', '{}', 'agent-1', ?, ?)
            """,
            (now, now),
        )

    command.downgrade(config, "0005_release_0_2")

    with sqlite3.connect(path) as connection:
        _assert_healthy(connection, "0005_release_0_2")
        assert connection.execute(
            "SELECT \"query\", discovered_count FROM scan_runs WHERE id = 'scan-1'"
        ).fetchone() == ("legal AI", 3)
        assert connection.execute(
            "SELECT scan_run_id FROM agent_runs WHERE id = 'agent-1'"
        ).fetchone() == ("scan-1",)
        assert {
            "scan_kind",
            "profile_id",
            "fts_sync_status",
            "fts_sync_error",
        }.isdisjoint(_columns(connection, "scan_runs"))
        assert "scan_profiles" not in _tables(connection)


def test_0007_downgrade_recreates_applications_and_preserves_workflow_rows(
    tmp_path: Path,
) -> None:
    path = tmp_path / "release-0.4.sqlite3"
    config = _config(path)
    command.upgrade(config, "0007_release_0_4")
    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute(
            """
            INSERT INTO companies(
                name, normalized_name, id, created_at, updated_at
            ) VALUES ('Example', 'example', 'company-1', ?, ?)
            """,
            (now, now),
        )
        _insert_job(connection, "job-1", "Policy Counsel", now)
        connection.execute(
            """
            INSERT INTO evaluations(
                job_id, score, components, gates, evidence, warnings,
                confidence, automatic_skip, manual_override, locked,
                is_current, id, created_at, updated_at
            ) VALUES (
                'job-1', 8.5, '{}', '[]', '[]', '[]',
                1.0, 0, 0, 0, 1, 'evaluation-1', ?, ?
            )
            """,
            (now, now),
        )
        connection.execute(
            """
            INSERT INTO applications(
                job_id, current_stage, notes, id, created_at, updated_at,
                applied_evaluation_id, applied_score,
                applied_ranker_version, applied_sources
            ) VALUES (
                'job-1', 'applied', 'keep this note', 'application-1', ?, ?,
                'evaluation-1', 8.5, 'deterministic-v1', '[{"source":"manual"}]'
            )
            """,
            (now, now),
        )
        connection.execute(
            """
            INSERT INTO tasks(
                title, status, application_id, id, created_at, updated_at,
                automation_key
            ) VALUES (
                'Follow up', 'pending', 'application-1', 'task-1', ?, ?,
                'follow-up:application-1'
            )
            """,
            (now, now),
        )
        connection.execute(
            """
            INSERT INTO stage_events(
                application_id, to_stage, occurred_at, actor, source, id
            ) VALUES (
                'application-1', 'applied', ?, 'user', 'manual', 'event-1'
            )
            """,
            (now,),
        )

    command.downgrade(config, "0006_release_0_3")

    with sqlite3.connect(path) as connection:
        _assert_healthy(connection, "0006_release_0_3")
        assert connection.execute(
            "SELECT job_id, current_stage, notes FROM applications "
            "WHERE id = 'application-1'"
        ).fetchone() == ("job-1", "applied", "keep this note")
        assert connection.execute(
            "SELECT application_id FROM tasks WHERE id = 'task-1'"
        ).fetchone() == ("application-1",)
        assert connection.execute(
            "SELECT application_id FROM stage_events WHERE id = 'event-1'"
        ).fetchone() == ("application-1",)
        assert {
            "applied_evaluation_id",
            "applied_score",
            "applied_ranker_version",
            "applied_sources",
        }.isdisjoint(_columns(connection, "applications"))
        assert "automation_key" not in _columns(connection, "tasks")
        assert "application_contacts" not in _tables(connection)
        assert {
            "ix_applications_job_id",
            "ix_applications_current_stage",
            "ix_applications_follow_up_at",
            "ix_applications_import_key",
        } <= _indexes(connection, "applications")


def test_0008_upgrade_merges_overlapping_confirmations_deterministically(
    tmp_path: Path,
) -> None:
    path = tmp_path / "release-0.5-overlap.sqlite3"
    config = _config(path)
    command.upgrade(config, "0007_release_0_4")
    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute(
            """
            INSERT INTO companies(
                name, normalized_name, id, created_at, updated_at
            ) VALUES ('Example', 'example', 'company-1', ?, ?)
            """,
            (now, now),
        )
        for job_id, title in (
            ("job-a", "Policy Counsel"),
            ("job-b", "Counsel, Policy"),
            ("job-c", "AI Policy Counsel"),
        ):
            _insert_job(connection, job_id, title, now)
        for job_id, duplicate_job_id, relationship_id in (
            (
                "job-a",
                "job-b",
                "11111111-1111-4111-8111-111111111111",
            ),
            (
                "job-b",
                "job-c",
                "22222222-2222-4222-8222-222222222222",
            ),
        ):
            connection.execute(
                """
                INSERT INTO duplicate_relationships(
                    job_id, duplicate_job_id, rule, similarity, confirmed,
                    id, created_at, updated_at
                ) VALUES (?, ?, 'legacy-confirmed', 1.0, 1, ?, ?, ?)
                """,
                (job_id, duplicate_job_id, relationship_id, now, now),
            )

    def migrated_snapshot() -> tuple[
        tuple[object, ...], tuple[tuple[object, ...], ...]
    ]:
        with sqlite3.connect(path) as connection:
            _assert_healthy(connection, "0008_release_0_5")
            group = connection.execute(
                """
                SELECT canonical_job_id, id
                FROM canonical_job_groups
                """
            ).fetchone()
            assert group is not None
            members = tuple(
                connection.execute(
                    """
                    SELECT group_id, job_id, is_canonical, hidden_by_default, id
                    FROM canonical_job_members
                    ORDER BY job_id
                    """
                ).fetchall()
            )
            relationships = connection.execute(
                """
                SELECT job_id, duplicate_job_id, canonical_group_id
                FROM duplicate_relationships
                WHERE confirmed = 1
                ORDER BY job_id, duplicate_job_id
                """
            ).fetchall()
            invalid_relationships = connection.execute(
                """
                SELECT COUNT(*)
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
                """
            ).fetchone()

        assert group[0] == "job-a"
        assert len(str(group[1])) <= 36
        assert len(members) == 3
        assert {row[1] for row in members} == {"job-a", "job-b", "job-c"}
        assert len({row[1] for row in members}) == len(members)
        assert [row[1] for row in members if row[2]] == ["job-a"]
        assert all(row[3] == (row[1] != "job-a") for row in members)
        assert all(row[0] == group[1] for row in members)
        assert all(len(str(row[4])) <= 36 for row in members)
        assert len({row[2] for row in relationships}) == 1
        assert all(row[2] == group[1] for row in relationships)
        assert invalid_relationships == (0,)
        return group, members

    command.upgrade(config, "0008_release_0_5")
    first_snapshot = migrated_snapshot()

    # A rehearsal, rollback, and subsequent apply must derive the same IDs
    # without relying on relationship insertion order or generated suffixes.
    command.downgrade(config, "0007_release_0_4")
    with sqlite3.connect(path) as connection:
        _assert_healthy(connection, "0007_release_0_4")
        assert connection.execute(
            "SELECT COUNT(*) FROM duplicate_relationships WHERE confirmed = 1"
        ).fetchone() == (2,)
    command.upgrade(config, "0008_release_0_5")
    assert migrated_snapshot() == first_snapshot


def test_0008_downgrade_recreates_duplicates_and_preserves_legacy_alert_data(
    tmp_path: Path,
) -> None:
    path = tmp_path / "release-0.5.sqlite3"
    config = _config(path)
    command.upgrade(config, "0008_release_0_5")
    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute(
            """
            INSERT INTO companies(
                name, normalized_name, id, created_at, updated_at
            ) VALUES ('Example', 'example', 'company-1', ?, ?)
            """,
            (now, now),
        )
        _insert_job(connection, "job-1", "Policy Counsel", now)
        _insert_job(connection, "job-2", "Counsel, Policy", now)
        connection.execute(
            """
            INSERT INTO canonical_job_groups(
                canonical_job_id, notes, id, created_at, updated_at
            ) VALUES ('job-1', 'reviewed', 'group-1', ?, ?)
            """,
            (now, now),
        )
        connection.execute(
            """
            INSERT INTO canonical_job_members(
                group_id, job_id, is_canonical, hidden_by_default,
                id, created_at, updated_at
            ) VALUES ('group-1', 'job-1', 1, 0, 'member-1', ?, ?)
            """,
            (now, now),
        )
        connection.execute(
            """
            INSERT INTO duplicate_relationships(
                job_id, duplicate_job_id, rule, similarity, confirmed,
                id, created_at, updated_at, resolution,
                comparison_identity, resolved_at, canonical_group_id
            ) VALUES (
                'job-1', 'job-2', 'exact-url', 1.0, 1,
                'duplicate-1', ?, ?, 'confirmed',
                'comparison-1', ?, 'group-1'
            )
            """,
            (now, now, now),
        )
        connection.execute(
            """
            INSERT INTO alerts(
                severity, title, message, job_id, id, created_at, updated_at,
                fingerprint, recurrence_count, entity_type, entity_id
            ) VALUES (
                'info', 'Duplicate reviewed', 'Preserve this alert', 'job-1',
                'alert-1', ?, ?, 'fingerprint-1', 3, 'job', 'job-1'
            )
            """,
            (now, now),
        )

    command.downgrade(config, "0007_release_0_4")

    with sqlite3.connect(path) as connection:
        _assert_healthy(connection, "0007_release_0_4")
        assert connection.execute(
            "SELECT job_id, duplicate_job_id, rule, confirmed "
            "FROM duplicate_relationships WHERE id = 'duplicate-1'"
        ).fetchone() == ("job-1", "job-2", "exact-url", 1)
        assert connection.execute(
            "SELECT title, message, job_id FROM alerts WHERE id = 'alert-1'"
        ).fetchone() == (
            "Duplicate reviewed",
            "Preserve this alert",
            "job-1",
        )
        assert {
            "resolution",
            "comparison_identity",
            "resolved_at",
            "canonical_group_id",
        }.isdisjoint(_columns(connection, "duplicate_relationships"))
        assert {
            "fingerprint",
            "recurrence_count",
            "last_recurred_at",
            "snoozed_until",
            "resolved_at",
            "resolution_reason",
            "entity_type",
            "entity_id",
        }.isdisjoint(_columns(connection, "alerts"))
        assert "canonical_job_members" not in _tables(connection)
        assert "canonical_job_groups" not in _tables(connection)
        assert {
            "ix_duplicate_relationships_job_id",
            "ix_duplicate_relationships_duplicate_job_id",
        } <= _indexes(connection, "duplicate_relationships")
