from asyncio import sleep, timeout
from collections.abc import AsyncIterator, Iterator
from typing import Final

import pytest
import pytest_asyncio
from asyncpg import CannotConnectNowError, Pool, create_pool
from testcontainers.core.container import DockerContainer

from fidem.asyncpg import OUTBOX_SECTION, migrate

POSTGRES_IMAGE: Final = "postgres:17-alpine"
POSTGRES_PORT: Final = 5432
POSTGRES_DATA: Final = "/pgdata/data"
# Durability costs disk writes that tests never need, and the data directory lives in memory anyway.
POSTGRES_SETTINGS: Final = {
    "listen_addresses": "'*'",
    "fsync": "off",
    "synchronous_commit": "off",
    "full_page_writes": "off",
    "wal_level": "minimal",
    "max_wal_senders": "0",
    "checkpoint_timeout": "1d",
    "autovacuum": "off",
    "jit": "off",
}
# The entrypoint of the image starts and stops a temporary server after initdb, which doubles the startup time.
POSTGRES_COMMAND: Final = " && ".join(
    [
        f"initdb --pgdata={POSTGRES_DATA} --username=postgres --auth=trust --no-sync --locale=C --encoding=UTF8 > /dev/null",
        f"echo 'host all all all trust' >> {POSTGRES_DATA}/pg_hba.conf",
        f"exec postgres -D {POSTGRES_DATA} {' '.join(f'-c {name}={value}' for name, value in POSTGRES_SETTINGS.items())}",
    ]
)


@pytest.fixture(scope="session")
def postgres_dsn() -> Iterator[str]:
    container = (
        DockerContainer(POSTGRES_IMAGE)
        .with_kwargs(user="postgres")
        .with_exposed_ports(POSTGRES_PORT)
        .with_command(["sh", "-c", POSTGRES_COMMAND])
    )
    # The postgres user cannot write to a mount owned by root.
    container.tmpfs["/pgdata"] = "mode=1777"
    with container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(POSTGRES_PORT)
        yield f"postgresql://postgres@{host}:{port}/postgres"


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def pool(postgres_dsn: str) -> AsyncIterator[Pool]:
    async with await _connect(postgres_dsn) as pool:
        await pool.execute("CREATE TABLE items (id text PRIMARY KEY, value text NOT NULL)")
        yield pool


@pytest_asyncio.fixture(loop_scope="session")
async def outbox_pool(pool: Pool) -> Pool:
    await migrate(pool, [OUTBOX_SECTION])
    return pool


async def _connect(dsn: str) -> Pool:
    """Polls the server often because the container does not report when it is ready."""
    async with timeout(10):
        while True:
            try:
                return await create_pool(dsn, min_size=1, max_size=4)
            except (OSError, CannotConnectNowError):
                await sleep(0.02)
