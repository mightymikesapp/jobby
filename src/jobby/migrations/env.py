from __future__ import annotations

from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from jobby.models import Base


config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    provided_connection = config.attributes.get("connection")
    if provided_connection is not None:
        _run_with_connection(provided_connection)
        return
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        _run_with_connection(connection)


def _run_with_connection(connection) -> None:
    if connection.dialect.name == "sqlite":
        connection.exec_driver_sql("PRAGMA foreign_keys=ON")
        # SQLAlchemy 2 autobegins on PRAGMA. Commit it before handing the
        # connection to Alembic so the revision stamp is not rolled back when
        # this connection closes (SQLite DDL itself would survive).
        connection.commit()
    context.configure(
        connection=connection, target_metadata=target_metadata, compare_type=True
    )
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
