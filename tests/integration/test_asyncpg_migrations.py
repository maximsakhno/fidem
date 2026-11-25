from asyncio import gather
from collections.abc import AsyncIterator, Mapping
from hashlib import sha256
from logging import INFO
from uuid import uuid4

import pytest
import pytest_asyncio
from asyncpg import DivisionByZeroError, Pool, create_pool

from fidem.asyncpg import DuplicateMigrationSectionError, MigrationChangedError, MigrationSection, migrate

pytestmark = [
    pytest.mark.asyncio(loop_scope="session"),
    # The first test also waits for the container to start, possibly after pulling its image.
    pytest.mark.timeout(60),
]


def unique_name() -> str:
    return f"test_{uuid4().hex}"


def database_dsn(postgres_dsn: str, database: str, user: str = "postgres") -> str:
    server = postgres_dsn.rsplit("/", 1)[0].replace("postgres@", f"{user}@", 1)
    return f"{server}/{database}"


@pytest_asyncio.fixture(loop_scope="session")
async def empty_database(pool: Pool) -> str:
    """A new database, because other tests have already created the schema of fidem in the shared one."""
    name = unique_name()
    await pool.execute(f"CREATE DATABASE {name}")
    return name


@pytest_asyncio.fixture(loop_scope="session")
async def empty_pool(empty_database: str, postgres_dsn: str) -> AsyncIterator[Pool]:
    async with create_pool(database_dsn(postgres_dsn, empty_database), min_size=1, max_size=1) as empty_pool:
        yield empty_pool


async def fetch_applied(pool: Pool, section: str) -> Mapping[int, str]:
    rows = await pool.fetch("SELECT version, checksum FROM fidem.migrations WHERE section = $1", section)
    return {row["version"]: row["checksum"] for row in rows}


async def table_exists(pool: Pool, table: str) -> bool:
    return await pool.fetchval("SELECT to_regclass($1) IS NOT NULL", table)


async def test_migrate_creates_schema_and_records_migrations(empty_pool: Pool) -> None:
    table = unique_name()
    sql = f"CREATE TABLE {table} (id int)"
    section = MigrationSection(name=unique_name(), migrations=(sql,))

    await migrate(empty_pool, [section])

    assert await table_exists(empty_pool, table)
    assert await fetch_applied(empty_pool, section.name) == {1: sha256(sql.encode()).hexdigest()}


async def test_migrate_needs_only_privileges_on_existing_schema(
    empty_pool: Pool,
    empty_database: str,
    postgres_dsn: str,
) -> None:
    await migrate(empty_pool, [])
    # A new role cannot create schemas in the database.
    role = unique_name()
    await empty_pool.execute(f"CREATE ROLE {role} LOGIN")
    await empty_pool.execute(f"ALTER SCHEMA fidem OWNER TO {role}")
    await empty_pool.execute(f"ALTER TABLE fidem.migrations OWNER TO {role}")
    section = MigrationSection(name=unique_name(), migrations=(f"CREATE TABLE fidem.{unique_name()} (id int)",))

    async with create_pool(database_dsn(postgres_dsn, empty_database, user=role), min_size=1, max_size=1) as role_pool:
        await migrate(role_pool, [section])

    assert (await fetch_applied(empty_pool, section.name)).keys() == {1}


async def test_migrate_applies_only_new_migrations(pool: Pool) -> None:
    table = unique_name()
    create = f"CREATE TABLE {table} (id int)"
    name = unique_name()

    extended = MigrationSection(name=name, migrations=(create, f"ALTER TABLE {table} ADD COLUMN value text"))

    await migrate(pool, [MigrationSection(name=name, migrations=(create,))])
    await migrate(pool, [extended])
    await migrate(pool, [extended])

    assert (await fetch_applied(pool, name)).keys() == {1, 2}
    assert await pool.fetchval("SELECT count(*) FROM information_schema.columns WHERE table_name = $1", table) == 2


async def test_migrate_applies_sections_in_their_order_with_own_versions(pool: Pool) -> None:
    table = unique_name()
    first = MigrationSection(name=unique_name(), migrations=(f"CREATE TABLE {table} (id int)",))
    second = MigrationSection(name=unique_name(), migrations=(f"ALTER TABLE {table} ADD COLUMN value text",))

    await migrate(pool, [first, second])

    assert (await fetch_applied(pool, first.name)).keys() == {1}
    assert (await fetch_applied(pool, second.name)).keys() == {1}


async def test_migrate_rejects_changed_migration_before_applying_any(pool: Pool) -> None:
    changed = MigrationSection(name=unique_name(), migrations=("SELECT 1",))
    other_table = unique_name()
    other = MigrationSection(name=unique_name(), migrations=(f"CREATE TABLE {other_table} (id int)",))
    await migrate(pool, [changed])

    with pytest.raises(MigrationChangedError) as exc_info:
        await migrate(pool, [other, MigrationSection(name=changed.name, migrations=("SELECT 2",))])

    assert exc_info.value.args == (changed.name, 1)
    assert not await table_exists(pool, other_table)
    assert await fetch_applied(pool, other.name) == {}


async def test_failed_migration_rolls_back_only_itself(pool: Pool) -> None:
    applied_table = unique_name()
    failed_table = unique_name()
    name = unique_name()
    create = f"CREATE TABLE {applied_table} (id int)"

    with pytest.raises(DivisionByZeroError):
        await migrate(
            pool=pool,
            sections=[
                MigrationSection(name=name, migrations=(create, f"CREATE TABLE {failed_table} (id int); SELECT 1 / 0"))
            ],
        )

    assert await table_exists(pool, applied_table)
    assert not await table_exists(pool, failed_table)
    assert (await fetch_applied(pool, name)).keys() == {1}
    await migrate(pool, [MigrationSection(name=name, migrations=(create, f"CREATE TABLE {failed_table} (id int)"))])
    assert await table_exists(pool, failed_table)


async def test_migrate_skips_versions_unknown_to_code(pool: Pool) -> None:
    name = unique_name()
    await migrate(pool, [MigrationSection(name=name, migrations=("SELECT 1", "SELECT 2"))])

    await migrate(pool, [MigrationSection(name=name, migrations=("SELECT 1",))])

    assert (await fetch_applied(pool, name)).keys() == {1, 2}


async def test_migrate_rejects_duplicate_sections_before_touching_database(empty_pool: Pool) -> None:
    section = MigrationSection(name=unique_name(), migrations=("SELECT 1",))

    with pytest.raises(DuplicateMigrationSectionError) as exc_info:
        await migrate(empty_pool, [section, section])

    assert exc_info.value.args == ([section.name],)
    assert not await table_exists(empty_pool, "fidem.migrations")


async def test_concurrent_migrations_apply_each_migration_once(pool: Pool) -> None:
    table = unique_name()
    # Without the lock, the second run would also create the table while the first one sleeps and fail.
    section = MigrationSection(name=unique_name(), migrations=(f"CREATE TABLE {table} (id int); SELECT pg_sleep(0.1)",))

    await gather(migrate(pool, [section]), migrate(pool, [section]), migrate(pool, [section]))

    assert (await fetch_applied(pool, section.name)).keys() == {1}


async def test_migrate_logs_applied_migrations(pool: Pool, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(INFO, logger="fidem.asyncpg.migrations")
    section = MigrationSection(name=unique_name(), migrations=("SELECT 1", "SELECT 2"))

    await migrate(pool, [section])
    await migrate(pool, [section])

    assert caplog.messages == [
        f"Applied migration {section.name} version 1",
        f"Applied migration {section.name} version 2",
    ]
