from uuid import uuid4

import pytest
import pytest_asyncio
from asyncpg import InvalidTextRepresentationError, Pool, UniqueViolationError

from fidem.asyncpg import OUTBOX_SECTION, migrate

pytestmark = [
    pytest.mark.asyncio(loop_scope="session"),
    # The first test also waits for the container to start, possibly after pulling its image.
    pytest.mark.timeout(60),
]

INSERT_SQL = """
INSERT INTO fidem.outbox (id, event_id, source_message_id, handler, event_name, event_version, payload)
VALUES ($1, 'event', 'message', 'handler', 'test.event', 1, '{"field": 1}')
"""


@pytest_asyncio.fixture(loop_scope="session")
async def outbox_pool(pool: Pool) -> Pool:
    await migrate(pool, [OUTBOX_SECTION])
    return pool


async def test_section_creates_outbox_with_index_of_pending_rows(outbox_pool: Pool) -> None:
    index = await outbox_pool.fetchval(
        "SELECT indexdef FROM pg_indexes WHERE schemaname = 'fidem' AND indexname = 'outbox_pending'"
    )

    assert (
        index
        == "CREATE INDEX outbox_pending ON fidem.outbox USING btree (available_at) WHERE (status = 'pending'::fidem.outbox_status)"
    )


async def test_new_row_is_pending_and_available_now(outbox_pool: Pool) -> None:
    row_id = uuid4().hex

    # `now()` is the start of the transaction, so the defaults equal it only inside the inserting one.
    async with outbox_pool.acquire() as connection, connection.transaction():
        await connection.execute(INSERT_SQL, row_id)
        row = await connection.fetchrow(
            """
            SELECT status::text, attempts, available_at = now() AS available_now, created_at = now() AS created_now,
                last_error, processed_at, payload->'field' AS field
            FROM fidem.outbox WHERE id = $1
            """,
            row_id,
        )

    assert row is not None
    assert dict(row) == {
        "status": "pending",
        "attempts": 0,
        "available_now": True,
        "created_now": True,
        "last_error": None,
        "processed_at": None,
        "field": "1",
    }


async def test_outbox_rejects_unknown_status(outbox_pool: Pool) -> None:
    row_id = uuid4().hex
    await outbox_pool.execute(INSERT_SQL, row_id)

    with pytest.raises(InvalidTextRepresentationError):
        await outbox_pool.execute("UPDATE fidem.outbox SET status = 'unknown' WHERE id = $1", row_id)


async def test_outbox_rejects_duplicate_id(outbox_pool: Pool) -> None:
    row_id = uuid4().hex
    await outbox_pool.execute(INSERT_SQL, row_id)

    with pytest.raises(UniqueViolationError):
        await outbox_pool.execute(INSERT_SQL, row_id)
