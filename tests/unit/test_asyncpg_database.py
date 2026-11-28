from collections.abc import AsyncGenerator
from unittest.mock import Mock

from asyncpg import Pool

from fidem import dependency
from fidem.asyncpg import AsyncpgConnectionDef, AsyncpgDatabase


@dependency
async def pool() -> AsyncGenerator[Pool]:
    yield Mock(spec=Pool)


class TestAsyncpgDatabase:
    def test_bundles_pool_and_connection(self) -> None:
        database = AsyncpgDatabase(pool)

        bundled_pool, connection_def = database.definitions

        assert bundled_pool is pool
        assert isinstance(connection_def, AsyncpgConnectionDef)
