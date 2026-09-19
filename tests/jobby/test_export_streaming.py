from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from sqlalchemy import event, func, select

from jobby.db import Database
from jobby.exporter import _export_json, export_data
from jobby.models import Company, Job, Location


def test_json_export_streams_core_rows_without_populating_the_orm_identity_map(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "streaming.sqlite3")
    database.initialize()
    with database.session() as session:
        company = Company(name="Scale Co", normalized_name="scale co")
        session.add(company)
        session.flush()
        company_id = company.id
        session.execute(
            Job.__table__.insert(),
            [
                {
                    "id": f"job-{index:05}",
                    "company_id": company_id,
                    "title": f"Role {index:05}",
                    "normalized_title": f"role {index:05}",
                }
                for index in range(2_500)
            ],
        )

    loaded_jobs = 0

    def loaded(_target, _context) -> None:
        nonlocal loaded_jobs
        loaded_jobs += 1

    event.listen(Job, "load", loaded)
    try:
        destination = export_data(database, "json", tmp_path / "streaming.json")
    finally:
        event.remove(Job, "load", loaded)

    assert loaded_jobs == 0
    payload = json.loads(destination.read_text(encoding="utf-8"))
    assert len(payload["jobs"]) == 2_500
    database.dispose()


def test_json_export_uses_one_wal_snapshot_across_every_table(tmp_path: Path) -> None:
    database = Database(tmp_path / "snapshot.sqlite3")
    database.initialize()
    with database.session() as session:
        session.add(Company(name="Initial", normalized_name="initial"))

    injected = False

    def inject_after_companies(
        _connection,
        _cursor,
        statement,
        _parameters,
        _context,
        _many,
    ) -> None:
        nonlocal injected
        if injected or "from companies" not in statement.casefold():
            return
        injected = True
        writer = sqlite3.connect(database.path, timeout=5)
        try:
            writer.execute(
                """
                INSERT INTO locations (
                    display_name, normalized_key, remote, id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "Late location",
                    "late location",
                    0,
                    "late-location",
                    "2026-07-14 12:00:00",
                    "2026-07-14 12:00:00",
                ),
            )
            writer.commit()
        finally:
            writer.close()

    event.listen(database.engine, "after_cursor_execute", inject_after_companies)
    try:
        output = tmp_path / "snapshot.json"
        _export_json(database, output)
    finally:
        event.remove(database.engine, "after_cursor_execute", inject_after_companies)

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert injected is True
    assert payload["locations"] == []
    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(Location)) == 1
    database.dispose()
