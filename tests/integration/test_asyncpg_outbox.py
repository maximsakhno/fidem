from collections.abc import AsyncGenerator, Collection, Sequence
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import pytest
import pytest_asyncio
from asyncpg import Connection, InvalidTextRepresentationError, Pool, Record, UniqueViolationError

from fidem.asyncpg import OUTBOX_SECTION, AsyncpgConnectionDef, AsyncpgOutboxEffectDef, migrate
from fidem.di.effects import EffectDependencyDef
from fidem.effects import DatabaseWriteEffect
from fidem.environment import EffectDef, EnvironmentImpl
from fidem.messages import AtomicEffectful, Event, StagedEffectful
from fidem.operation_engine.definitions import (
    AtomicCommandDef,
    CommandDef,
    EventDef,
    StagedCommandDef,
    StagedEventHandlerDef,
)
from fidem.operation_engine.engine import OperationEngineImpl
from fidem.pydantic import PydanticSerializer
from fidem.serialization import Serializer
from fidem.transactions import WriteTransaction, WriteTransactionEffectful
from tests.messages import TestCommand

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


@dataclass(frozen=True, slots=True)
class ItemSaved(Event):
    item_id: str


@dataclass(frozen=True, slots=True)
class SaveItemEffect(DatabaseWriteEffect[None]):
    item_id: str


@dataclass(frozen=True, slots=True)
class FailEffect(DatabaseWriteEffect[None]):
    pass


async def handle_save_item(connection: Connection, effect: SaveItemEffect) -> None:
    await connection.execute("INSERT INTO items (id, value) VALUES ($1, 'value')", effect.item_id)


async def handle_fail() -> None:
    raise ValueError("effect_error")


def handle_item_saved() -> StagedEffectful[None]:
    yield from ()


EVENT_DEFS = [
    EventDef(
        name="test.item_saved",
        event_type=ItemSaved,
        version=3,
        handler_defs=[
            StagedEventHandlerDef("audit", handle_item_saved),
            StagedEventHandlerDef("notify", handle_item_saved),
        ],
    ),
]


@WriteTransaction
def save_item(item_id: str, fail: bool = False) -> WriteTransactionEffectful[None, ItemSaved]:
    yield from SaveItemEffect(item_id)
    yield from ItemSaved(item_id)
    if fail:
        yield from FailEffect()


class TestWriteOutbox:
    async def test_writes_events_of_atomic_command_in_its_transaction(self, outbox_pool: Pool) -> None:
        def handler() -> AtomicEffectful[None, ItemSaved]:
            yield from ()
            return save_item(item_id)

        item_id = uuid4().hex
        message_id = uuid4().hex
        environment = make_environment(outbox_pool, [AtomicCommandDef("test", TestCommand, handler)])

        async with environment:
            await environment.handle(TestCommand(), message_id=message_id)

        rows = await fetch_rows(outbox_pool, message_id)
        event_id = rows[0]["event_id"]
        assert [dict(row) for row in rows] == [
            {
                "id": f"{event_id}:{handler}",
                "event_id": event_id,
                "handler": handler,
                "event_name": "test.item_saved",
                "event_version": 3,
                "item_id": item_id,
                "status": "pending",
            }
            for handler in ["audit", "notify"]
        ]
        # Rows written by one transaction have its id in `xmin`.
        transaction_ids = await outbox_pool.fetch(
            """
            SELECT xmin::text FROM items WHERE id = $1
            UNION SELECT xmin::text FROM fidem.outbox WHERE source_message_id = $2
            """,
            item_id,
            message_id,
        )
        assert len(transaction_ids) == 1

    async def test_rolls_back_events_with_failed_transaction(self, outbox_pool: Pool) -> None:
        def handler() -> AtomicEffectful[None, ItemSaved]:
            yield from ()
            return save_item(item_id, fail=True)

        item_id = uuid4().hex
        message_id = uuid4().hex
        environment = make_environment(outbox_pool, [AtomicCommandDef("test", TestCommand, handler)])

        async with environment:
            with pytest.raises(ValueError, match="effect_error"):
                await environment.handle(TestCommand(), message_id=message_id)

        assert await fetch_rows(outbox_pool, message_id) == []

    async def test_skips_rows_written_by_previous_execution_of_same_message(self, outbox_pool: Pool) -> None:
        def handler() -> StagedEffectful[None, ItemSaved]:
            yield from ItemSaved(item_id)

        item_id = uuid4().hex
        message_id = uuid4().hex
        environment = make_environment(outbox_pool, [StagedCommandDef("test", TestCommand, handler)])

        async with environment:
            await environment.handle(TestCommand(), message_id=message_id)
            await environment.handle(TestCommand(), message_id=message_id)

        rows = await fetch_rows(outbox_pool, message_id)
        assert [(row["handler"], row["item_id"]) for row in rows] == [("audit", item_id), ("notify", item_id)]


def make_environment(pool: Pool, command_defs: Collection[CommandDef[Any]]) -> EnvironmentImpl:
    # The pool outlives the environment because starting it for each test is slower.
    async def shared_pool() -> AsyncGenerator[Pool]:
        yield pool

    async def serializer() -> AsyncGenerator[Serializer]:
        yield PydanticSerializer()

    return EnvironmentImpl(
        operation_engine=OperationEngineImpl(command_defs=command_defs, event_defs=EVENT_DEFS),
        effect_defs=[
            EffectDef(SaveItemEffect, handle_save_item),
            EffectDef(FailEffect, handle_fail),
            AsyncpgOutboxEffectDef(),
        ],
        dependency_defs=[
            EffectDependencyDef(shared_pool),
            EffectDependencyDef(serializer),
            AsyncpgConnectionDef(),
        ],
    )


async def fetch_rows(pool: Pool, message_id: str) -> Sequence[Record]:
    return await pool.fetch(
        """
        SELECT id, event_id, handler, event_name, event_version, payload->>'item_id' AS item_id, status::text
        FROM fidem.outbox WHERE source_message_id = $1 ORDER BY handler
        """,
        message_id,
    )
