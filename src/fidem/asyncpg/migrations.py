from collections import Counter, defaultdict
from collections.abc import Collection, Mapping, MutableMapping, MutableSequence, Sequence
from dataclasses import dataclass
from hashlib import sha256
from importlib.resources.abc import Traversable
from logging import getLogger
from typing import Final, cast

from asyncpg import Connection, Pool

from fidem.errors import Error

__all__ = [
    "DuplicateMigrationSectionError",
    "MigrationChangedError",
    "MigrationSection",
    "MigrationSql",
    "MigrationVersion",
    "SectionName",
    "load_section",
    "migrate",
]

_logger = getLogger(__name__)

type SectionName = str
type MigrationVersion = int
type MigrationSql = str
type _Checksum = str

# Advisory, because the table of applied migrations may not exist yet, and per transaction, so that a pooler in transaction mode cannot leave it taken.
_LOCK_SQL: Final = f"SELECT pg_advisory_xact_lock({int.from_bytes(b'fidem')})"

# `CREATE SCHEMA IF NOT EXISTS` requires the privilege to create schemas in the database even when the schema exists.
_BOOTSTRAP_SQL: Final = """
DO $$
BEGIN
    IF to_regnamespace('fidem') IS NULL THEN
        CREATE SCHEMA fidem;
    END IF;
END
$$;
CREATE TABLE IF NOT EXISTS fidem.migrations (
    section text NOT NULL,
    version integer NOT NULL,
    checksum text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (section, version)
);
"""


@dataclass(frozen=True, slots=True)
class MigrationSection:
    """Migrations of the tables of one component, the version of a migration is its position starting from 1."""

    name: SectionName
    # Not `Sequence[MigrationSql]`, because a single string is one too and would make each character a migration.
    migrations: list[MigrationSql] | tuple[MigrationSql, ...]


def load_section(name: SectionName, directory: Traversable) -> MigrationSection:
    """Reads the `.sql` files of the directory in the order of their names."""
    paths = sorted((path for path in directory.iterdir() if path.name.endswith(".sql")), key=lambda path: path.name)
    return MigrationSection(name=name, migrations=tuple(path.read_text(encoding="utf-8") for path in paths))


class DuplicateMigrationSectionError(Error):
    def __init__(self, names: Collection[SectionName]) -> None:
        super().__init__(names)


class MigrationChangedError(Error):
    def __init__(self, section: SectionName, version: MigrationVersion) -> None:
        super().__init__(section, version)


@dataclass(frozen=True, slots=True)
class _PendingMigration:
    section: SectionName
    version: MigrationVersion
    sql: MigrationSql


async def migrate(pool: Pool, sections: Sequence[MigrationSection]) -> None:
    """Applies the pending migrations of the sections in their order, each in its own transaction."""
    duplicates = [name for name, count in Counter(section.name for section in sections).items() if count > 1]
    if duplicates:
        raise DuplicateMigrationSectionError(duplicates)
    async with pool.acquire() as proxy:
        connection = cast("Connection", proxy)
        async with connection.transaction():
            await connection.execute(_LOCK_SQL)
            await connection.execute(_BOOTSTRAP_SQL)
        for migration in _find_pending(sections, await _fetch_checksums(connection)):
            await _apply(connection, migration)


async def _fetch_checksums(connection: Connection) -> Mapping[SectionName, Mapping[MigrationVersion, _Checksum]]:
    checksums: MutableMapping[SectionName, MutableMapping[MigrationVersion, _Checksum]] = defaultdict(dict)
    for row in await connection.fetch("SELECT section, version, checksum FROM fidem.migrations"):
        checksums[row["section"]][row["version"]] = row["checksum"]
    return checksums


def _find_pending(
    sections: Sequence[MigrationSection],
    checksums: Mapping[SectionName, Mapping[MigrationVersion, _Checksum]],
) -> Sequence[_PendingMigration]:
    """Fails on a changed applied migration before any pending one is applied."""
    pending: MutableSequence[_PendingMigration] = []
    for section in sections:
        applied = checksums.get(section.name, {})
        for version, sql in enumerate(section.migrations, start=1):
            if version not in applied:
                pending.append(_PendingMigration(section=section.name, version=version, sql=sql))
            elif applied[version] != _checksum(sql):
                raise MigrationChangedError(section.name, version)
    return pending


async def _apply(connection: Connection, migration: _PendingMigration) -> None:
    async with connection.transaction():
        await connection.execute(_LOCK_SQL)
        # A concurrent run may have applied the migration after this one found it pending.
        applied_checksum = await connection.fetchval(
            "SELECT checksum FROM fidem.migrations WHERE section = $1 AND version = $2",
            migration.section,
            migration.version,
        )
        if applied_checksum is not None:
            if applied_checksum != _checksum(migration.sql):
                raise MigrationChangedError(migration.section, migration.version)
            return
        await connection.execute(migration.sql)
        await connection.execute(
            "INSERT INTO fidem.migrations (section, version, checksum) VALUES ($1, $2, $3)",
            migration.section,
            migration.version,
            _checksum(migration.sql),
        )
    _logger.info("Applied migration %s version %s", migration.section, migration.version)


def _checksum(sql: MigrationSql) -> _Checksum:
    return sha256(sql.encode()).hexdigest()
