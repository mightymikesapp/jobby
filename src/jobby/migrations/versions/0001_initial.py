"""Create the frozen initial local-first domain schema.

Revision ID: 0001_initial
Revises: None
"""

import json
import re
from importlib.resources import files

from alembic import op


revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


BASELINE_SCHEMA = "jobby-sqlite-baseline-v1"
BASELINE_RESOURCE = "migrations/baseline_v0001.json"


def _baseline() -> dict[str, object]:
    resource = files("jobby").joinpath(BASELINE_RESOURCE)
    payload = json.loads(resource.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("schema") != BASELINE_SCHEMA:
        raise RuntimeError("Jobby baseline migration resource is invalid")
    return payload


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "sqlite":
        raise RuntimeError("Jobby V1 migrations support SQLite only")
    statements = _baseline().get("upgrade")
    if not isinstance(statements, list) or not statements:
        raise RuntimeError("Jobby baseline migration has no schema statements")
    for statement in statements:
        if not isinstance(statement, str) or not statement.strip():
            raise RuntimeError("Jobby baseline migration contains invalid SQL")
        bind.exec_driver_sql(statement)


def downgrade() -> None:
    tables = _baseline().get("tables")
    if not isinstance(tables, list) or not tables:
        raise RuntimeError("Jobby baseline migration has no table list")
    for table in reversed(tables):
        if not isinstance(table, str) or not re.fullmatch(r"[a-z][a-z0-9_]*", table):
            raise RuntimeError("Jobby baseline migration contains an invalid table")
        op.drop_table(table)
