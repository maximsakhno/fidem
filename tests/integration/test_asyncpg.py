from collections.abc import AsyncGenerator, Collection, Sequence
from contextlib import suppress
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import pytest
from asyncpg import Connection, DivisionByZeroError, Pool, ReadOnlySQLTransactionError

from fidem.asyncpg import AsyncpgConnectionDef, AsyncpgTransactionOptions, TransactionRolledBackError
from fidem.core.di.effects import EffectDependencyDef
from fidem.core.effects import DatabaseReadEffect, DatabaseWriteEffect, Effect, Effectful
from fidem.core.environment import EffectDef, EnvironmentImpl
from fidem.core.messages import AtomicEffectful, QueryEffectful
from fidem.core.operation_engine.definitions import AtomicCommandDef, CommandDef, QueryDef, StagedCommandDef
from fidem.core.operation_engine.engine import OperationEngineImpl
from fidem.core.transactions import (
    ReadTransaction,
    ReadTransactionEffectful,
    WriteTransaction,
    WriteTransactionEffectful,
)
from tests.messages import TestCommand, TestQuery

pytestmark = [
    pytest.mark.asyncio(loop_scope="session"),
    # The first test also waits for the container to start, possibly after pulling its image.
    pytest.mark.timeout(60),
]


@dataclass(frozen=True, slots=True)
class TransactionState:
    in_transaction: bool
    isolation: str
    read_only: bool


NO_TRANSACTION = TransactionState(in_transaction=False, isolation="read committed", read_only=False)


@dataclass(frozen=True, slots=True)
class GetItemEffect(DatabaseReadEffect[str | None]):
    item_id: str


@dataclass(frozen=True, slots=True)
class GetItemAndChangeItConcurrentlyEffect(DatabaseReadEffect[str | None]):
    item_id: str
    value: str


@dataclass(frozen=True, slots=True)
class SaveItemEffect(DatabaseWriteEffect[None]):
    item_id: str
    value: str


@dataclass(frozen=True, slots=True)
class SaveItemAsReadEffect(DatabaseReadEffect[None]):
    item_id: str
    value: str


@dataclass(frozen=True, slots=True)
class ReadTransactionStateEffect(DatabaseReadEffect[TransactionState]):
    pass


@dataclass(frozen=True, slots=True)
class WriteTransactionStateEffect(DatabaseWriteEffect[TransactionState]):
    pass


@dataclass(frozen=True, slots=True)
class FailEffect(DatabaseWriteEffect[None]):
    pass


@dataclass(frozen=True, slots=True)
class FailQueryEffect(DatabaseWriteEffect[None]):
    pass


async def handle_get_item(connection: Connection, effect: GetItemEffect) -> str | None:
    return await connection.fetchval("SELECT value FROM items WHERE id = $1", effect.item_id)


# Another connection commits the change while the transaction of the effect is open.
async def handle_get_item_and_change_it_concurrently(
    connection: Connection,
    pool: Pool,
    effect: GetItemAndChangeItConcurrentlyEffect,
) -> str | None:
    value = await connection.fetchval("SELECT value FROM items WHERE id = $1", effect.item_id)
    await pool.execute("UPDATE items SET value = $2 WHERE id = $1", effect.item_id, effect.value)
    return value


async def handle_save_item(connection: Connection, effect: SaveItemEffect) -> None:
    await connection.execute("INSERT INTO items (id, value) VALUES ($1, $2)", effect.item_id, effect.value)


async def handle_save_item_as_read(connection: Connection, effect: SaveItemAsReadEffect) -> None:
    await connection.execute("INSERT INTO items (id, value) VALUES ($1, $2)", effect.item_id, effect.value)


async def handle_transaction_state(connection: Connection) -> TransactionState:
    return TransactionState(
        in_transaction=connection.is_in_transaction(),
        isolation=str(await connection.fetchval("SHOW transaction_isolation")),
        read_only=await connection.fetchval("SHOW transaction_read_only") == "on",
    )


async def handle_fail(connection: Connection) -> None:
    raise ValueError("effect_error")


async def handle_fail_query(connection: Connection) -> None:
    await connection.execute("SELECT 1 / 0")


@ReadTransaction
def read_state() -> ReadTransactionEffectful[TransactionState]:
    return (yield from ReadTransactionStateEffect())


@ReadTransaction
def read_other_state() -> ReadTransactionEffectful[TransactionState]:
    return (yield from ReadTransactionStateEffect())


@WriteTransaction
def write_state() -> WriteTransactionEffectful[TransactionState]:
    return (yield from WriteTransactionStateEffect())


@WriteTransaction
def write_other_state() -> WriteTransactionEffectful[TransactionState]:
    return (yield from WriteTransactionStateEffect())


def get_state_in_read_transaction() -> QueryEffectful[TransactionState]:
    return (yield from read_state())


def get_state_in_write_transaction() -> AtomicEffectful[TransactionState]:
    yield from ()
    return write_state()


class TestTransactions:
    async def test_commits_transaction_of_atomic_command(self, pool: Pool) -> None:
        @WriteTransaction
        def save_items(value: str | None) -> WriteTransactionEffectful[str | None]:
            yield from SaveItemEffect(first_id, "first")
            yield from SaveItemEffect(second_id, "second")
            return value

        def handler() -> AtomicEffectful[str | None]:
            value = yield from GetItemEffect(first_id)
            return save_items(value)

        first_id = uuid4().hex
        second_id = uuid4().hex
        environment = make_environment(pool, command_defs=[AtomicCommandDef("test", TestCommand, handler)])

        async with environment:
            result = await environment.handle(TestCommand())

        assert result is None
        assert await fetch_values(pool, first_id, second_id) == ["first", "second"]

    async def test_rolls_back_transaction_of_atomic_command_when_it_fails(self, pool: Pool) -> None:
        @WriteTransaction
        def save_item() -> WriteTransactionEffectful[None]:
            yield from SaveItemEffect(item_id, "value")
            yield from FailEffect()

        def handler() -> AtomicEffectful[None]:
            yield from ()
            return save_item()

        item_id = uuid4().hex
        environment = make_environment(pool, command_defs=[AtomicCommandDef("test", TestCommand, handler)])

        async with environment:
            with pytest.raises(ValueError, match="effect_error"):
                await environment.handle(TestCommand())

        assert await fetch_values(pool, item_id) == [None]

    async def test_raises_transaction_rolled_back_error_when_transaction_continues_after_failed_query(
        self,
        pool: Pool,
    ) -> None:
        @WriteTransaction
        def save_item() -> WriteTransactionEffectful[None]:
            yield from SaveItemEffect(item_id, "value")
            with suppress(DivisionByZeroError):
                yield from FailQueryEffect()

        def handler() -> AtomicEffectful[None]:
            yield from ()
            return save_item()

        item_id = uuid4().hex
        environment = make_environment(pool, command_defs=[AtomicCommandDef("test", TestCommand, handler)])

        async with environment:
            with pytest.raises(TransactionRolledBackError):
                await environment.handle(TestCommand())

        assert await fetch_values(pool, item_id) == [None]

    @pytest.mark.parametrize(
        argnames=("connection_def", "expected_values"),
        argvalues=[
            pytest.param(AsyncpgConnectionDef(), ["initial", "initial"], id="default_repeatable_read"),
            pytest.param(
                AsyncpgConnectionDef(read_options=AsyncpgTransactionOptions(isolation="read_committed")),
                ["initial", "changed"],
                id="read_committed",
            ),
        ],
    )
    async def test_reads_one_snapshot_in_read_transaction_unless_isolation_is_read_committed(
        self,
        pool: Pool,
        connection_def: AsyncpgConnectionDef,
        expected_values: Sequence[str],
    ) -> None:
        @ReadTransaction
        def read_twice(item_id: str) -> ReadTransactionEffectful[Sequence[str | None]]:
            first_value = yield from GetItemAndChangeItConcurrentlyEffect(item_id, "changed")
            second_value = yield from GetItemEffect(item_id)
            return [first_value, second_value]

        def handler() -> QueryEffectful[Sequence[str | None]]:
            return (yield from read_twice(item_id))

        item_id = uuid4().hex
        await pool.execute("INSERT INTO items (id, value) VALUES ($1, 'initial')", item_id)
        environment = make_environment(
            pool=pool,
            query_defs=[QueryDef(TestQuery, handler)],
            connection_def=connection_def,
        )

        async with environment:
            result = await environment.handle(TestQuery())

        assert result == expected_values
        assert await fetch_values(pool, item_id) == ["changed"]

    async def test_raises_error_when_read_transaction_writes(self, pool: Pool) -> None:
        @ReadTransaction
        def save_item() -> ReadTransactionEffectful[None]:
            yield from SaveItemAsReadEffect(item_id, "value")

        def handler() -> QueryEffectful[None]:
            yield from save_item()

        item_id = uuid4().hex
        environment = make_environment(pool, query_defs=[QueryDef(TestQuery, handler)])

        async with environment:
            with pytest.raises(ReadOnlySQLTransactionError):
                await environment.handle(TestQuery())

        assert await fetch_values(pool, item_id) == [None]

    async def test_commits_each_transaction_of_staged_command(self, pool: Pool) -> None:
        @WriteTransaction
        def save_item(item_id: str) -> WriteTransactionEffectful[None]:
            yield from SaveItemEffect(item_id, "value")

        def handler() -> Effectful[None, Effect[Any]]:
            yield from save_item(first_id)
            with suppress(DivisionByZeroError):
                yield from fail_query()
            yield from save_item(second_id)

        @WriteTransaction
        def fail_query() -> WriteTransactionEffectful[None]:
            yield from SaveItemEffect(failed_id, "value")
            yield from FailQueryEffect()

        first_id = uuid4().hex
        failed_id = uuid4().hex
        second_id = uuid4().hex
        environment = make_environment(pool, command_defs=[StagedCommandDef("test", TestCommand, handler)])

        async with environment:
            await environment.handle(TestCommand())

        assert await fetch_values(pool, first_id, failed_id, second_id) == ["value", None, "value"]


class TestTransactionOptions:
    @pytest.mark.parametrize(
        argnames="connection_def",
        argvalues=[
            pytest.param(AsyncpgConnectionDef(), id="default"),
            pytest.param(
                AsyncpgConnectionDef(read_options=AsyncpgTransactionOptions(isolation="serializable")),
                id="serializable_read_options",
            ),
        ],
    )
    async def test_runs_read_effect_outside_transaction_without_database_transaction(
        self,
        pool: Pool,
        connection_def: AsyncpgConnectionDef,
    ) -> None:
        def handler() -> QueryEffectful[TransactionState]:
            return (yield from ReadTransactionStateEffect())

        environment = make_environment(pool, query_defs=[QueryDef(TestQuery, handler)], connection_def=connection_def)

        async with environment:
            result = await environment.handle(TestQuery())

        assert result == NO_TRANSACTION

    @pytest.mark.parametrize(
        argnames=("connection_def", "expected_state"),
        argvalues=[
            pytest.param(
                AsyncpgConnectionDef(),
                TransactionState(in_transaction=True, isolation="repeatable read", read_only=True),
                id="default_repeatable_read",
            ),
            pytest.param(
                AsyncpgConnectionDef(read_options=AsyncpgTransactionOptions(isolation="serializable")),
                TransactionState(in_transaction=True, isolation="serializable", read_only=True),
                id="read_options",
            ),
            pytest.param(
                AsyncpgConnectionDef(read_options=AsyncpgTransactionOptions(isolation="read_committed")),
                NO_TRANSACTION,
                id="read_committed_without_transaction",
            ),
            pytest.param(
                AsyncpgConnectionDef(write_options=AsyncpgTransactionOptions(isolation="serializable")),
                TransactionState(in_transaction=True, isolation="repeatable read", read_only=True),
                id="write_options_ignored",
            ),
            pytest.param(
                AsyncpgConnectionDef(
                    read_options=AsyncpgTransactionOptions(isolation="repeatable_read"),
                    transaction_options={read_state: AsyncpgTransactionOptions(isolation="serializable")},
                ),
                TransactionState(in_transaction=True, isolation="serializable", read_only=True),
                id="transaction_options",
            ),
            pytest.param(
                AsyncpgConnectionDef(
                    transaction_options={read_other_state: AsyncpgTransactionOptions(isolation="serializable")},
                ),
                TransactionState(in_transaction=True, isolation="repeatable read", read_only=True),
                id="other_transaction_options_ignored",
            ),
        ],
    )
    async def test_runs_read_transaction_with_its_options(
        self,
        pool: Pool,
        connection_def: AsyncpgConnectionDef,
        expected_state: TransactionState,
    ) -> None:
        environment = make_environment(
            pool=pool,
            query_defs=[QueryDef(TestQuery, get_state_in_read_transaction)],
            connection_def=connection_def,
        )

        async with environment:
            result = await environment.handle(TestQuery())

        assert result == expected_state

    @pytest.mark.parametrize(
        argnames=("connection_def", "expected_isolation"),
        argvalues=[
            pytest.param(AsyncpgConnectionDef(), "read committed", id="default_read_committed"),
            pytest.param(
                AsyncpgConnectionDef(write_options=AsyncpgTransactionOptions(isolation="repeatable_read")),
                "repeatable read",
                id="write_options",
            ),
            pytest.param(
                AsyncpgConnectionDef(read_options=AsyncpgTransactionOptions(isolation="serializable")),
                "read committed",
                id="read_options_ignored",
            ),
            pytest.param(
                AsyncpgConnectionDef(
                    write_options=AsyncpgTransactionOptions(isolation="repeatable_read"),
                    transaction_options={write_state: AsyncpgTransactionOptions(isolation="serializable")},
                ),
                "serializable",
                id="transaction_options",
            ),
            pytest.param(
                AsyncpgConnectionDef(
                    transaction_options={write_other_state: AsyncpgTransactionOptions(isolation="serializable")},
                ),
                "read committed",
                id="other_transaction_options_ignored",
            ),
        ],
    )
    async def test_runs_write_transaction_with_its_options(
        self,
        pool: Pool,
        connection_def: AsyncpgConnectionDef,
        expected_isolation: str,
    ) -> None:
        environment = make_environment(
            pool=pool,
            command_defs=[AtomicCommandDef("test", TestCommand, get_state_in_write_transaction)],
            connection_def=connection_def,
        )

        async with environment:
            result = await environment.handle(TestCommand())

        assert result == TransactionState(in_transaction=True, isolation=expected_isolation, read_only=False)

    @pytest.mark.parametrize(
        argnames=("connection_def", "expected_isolation"),
        argvalues=[
            pytest.param(AsyncpgConnectionDef(), "read committed", id="default_read_committed"),
            pytest.param(
                AsyncpgConnectionDef(write_options=AsyncpgTransactionOptions(isolation="serializable")),
                "serializable",
                id="write_options",
            ),
        ],
    )
    async def test_runs_write_effect_outside_transaction_with_write_options(
        self,
        pool: Pool,
        connection_def: AsyncpgConnectionDef,
        expected_isolation: str,
    ) -> None:
        def handler() -> Effectful[TransactionState, Effect[Any]]:
            yield from SaveItemEffect(item_id, "value")
            return (yield from WriteTransactionStateEffect())

        item_id = uuid4().hex
        environment = make_environment(
            pool=pool,
            command_defs=[StagedCommandDef("test", TestCommand, handler)],
            connection_def=connection_def,
        )

        async with environment:
            result = await environment.handle(TestCommand())

        assert result == TransactionState(in_transaction=True, isolation=expected_isolation, read_only=False)
        assert await fetch_values(pool, item_id) == ["value"]


def make_environment(
    pool: Pool,
    command_defs: Collection[CommandDef[Any]] = (),
    query_defs: Collection[QueryDef[Any]] = (),
    connection_def: AsyncpgConnectionDef | None = None,
) -> EnvironmentImpl:
    # The pool outlives the environment because starting it for each test is slower.
    async def shared_pool() -> AsyncGenerator[Pool]:
        yield pool

    return EnvironmentImpl(
        operation_engine=OperationEngineImpl(command_defs=command_defs, query_defs=query_defs),
        effect_defs=[
            EffectDef(GetItemEffect, handle_get_item),
            EffectDef(GetItemAndChangeItConcurrentlyEffect, handle_get_item_and_change_it_concurrently),
            EffectDef(SaveItemEffect, handle_save_item),
            EffectDef(SaveItemAsReadEffect, handle_save_item_as_read),
            EffectDef(ReadTransactionStateEffect, handle_transaction_state),
            EffectDef(WriteTransactionStateEffect, handle_transaction_state),
            EffectDef(FailEffect, handle_fail),
            EffectDef(FailQueryEffect, handle_fail_query),
        ],
        dependency_defs=[
            EffectDependencyDef(shared_pool),
            AsyncpgConnectionDef() if connection_def is None else connection_def,
        ],
    )


async def fetch_values(pool: Pool, *item_ids: str) -> Sequence[str | None]:
    return [await pool.fetchval("SELECT value FROM items WHERE id = $1", item_id) for item_id in item_ids]
