from collections.abc import AsyncGenerator, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import pytest
from asyncpg import Connection, DivisionByZeroError, Pool

from fidem.asyncpg import AsyncpgConnectionDef, AsyncpgTransactionOptions, IsolationLevel, TransactionRolledBackError
from fidem.di.effects import EffectDependencyDef
from fidem.effects import DatabaseReadEffect, DatabaseWriteEffect, Effect, Effectful
from fidem.environment import EffectDef, EnvironmentImpl
from fidem.messages import AtomicEffectful, Command, Message, TransactionEffectful
from fidem.operation_engine import AtomicCommandDef, OperationEngineImpl, StagedCommandDef
from tests.messages import TestCommand

pytestmark = [
    pytest.mark.asyncio(loop_scope="session"),
    # The first test also waits for the container to start, possibly after pulling its image.
    pytest.mark.timeout(60),
]


@dataclass(frozen=True, slots=True)
class TransactionState:
    in_transaction: bool
    isolation: str


@dataclass(frozen=True, slots=True)
class GetItemEffect(DatabaseReadEffect[str | None]):
    item_id: str


@dataclass(frozen=True, slots=True)
class SaveItemEffect(DatabaseWriteEffect[None]):
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


async def handle_save_item(connection: Connection, effect: SaveItemEffect) -> None:
    await connection.execute("INSERT INTO items (id, value) VALUES ($1, $2)", effect.item_id, effect.value)


async def handle_transaction_state(connection: Connection) -> TransactionState:
    return TransactionState(
        in_transaction=connection.is_in_transaction(),
        isolation=str(await connection.fetchval("SHOW transaction_isolation")),
    )


async def handle_fail(connection: Connection) -> None:
    raise ValueError("effect_error")


async def handle_fail_query(connection: Connection) -> None:
    await connection.execute("SELECT 1 / 0")


class TestAsyncpgConnectionDef:
    async def test_commits_transaction_of_atomic_command(self, pool: Pool) -> None:
        def handler() -> AtomicEffectful[str | None]:
            value = yield from GetItemEffect(first_id)

            def transaction() -> TransactionEffectful[str | None]:
                yield from SaveItemEffect(first_id, "first")
                yield from SaveItemEffect(second_id, "second")
                return value

            return transaction

        first_id = uuid4().hex
        second_id = uuid4().hex
        environment = make_environment(pool, AtomicCommandDef("test", TestCommand, handler))

        async with environment:
            result = await environment.execute(TestCommand())

        assert result is None
        assert await fetch_values(pool, first_id, second_id) == ["first", "second"]

    async def test_rolls_back_transaction_of_atomic_command_when_it_fails(self, pool: Pool) -> None:
        def handler() -> AtomicEffectful[None]:
            yield from ()

            def transaction() -> TransactionEffectful[None]:
                yield from SaveItemEffect(item_id, "value")
                yield from FailEffect()

            return transaction

        item_id = uuid4().hex
        environment = make_environment(pool, AtomicCommandDef("test", TestCommand, handler))

        async with environment:
            with pytest.raises(ValueError, match="effect_error"):
                await environment.execute(TestCommand())

        assert await fetch_values(pool, item_id) == [None]

    async def test_raises_transaction_rolled_back_error_when_transaction_continues_after_failed_query(
        self,
        pool: Pool,
    ) -> None:
        def handler() -> AtomicEffectful[None]:
            yield from ()

            def transaction() -> TransactionEffectful[None]:
                yield from SaveItemEffect(item_id, "value")
                with suppress(DivisionByZeroError):
                    yield from FailQueryEffect()

            return transaction

        item_id = uuid4().hex
        environment = make_environment(pool, AtomicCommandDef("test", TestCommand, handler))

        async with environment:
            with pytest.raises(TransactionRolledBackError):
                await environment.execute(TestCommand())

        assert await fetch_values(pool, item_id) == [None]

    async def test_commits_write_effect_outside_transaction_in_its_own_transaction(self, pool: Pool) -> None:
        def handler() -> Effectful[TransactionState, Effect[Any]]:
            yield from SaveItemEffect(item_id, "value")
            return (yield from WriteTransactionStateEffect())

        item_id = uuid4().hex
        environment = make_environment(pool, StagedCommandDef("test", TestCommand, handler))

        async with environment:
            result = await environment.execute(TestCommand())

        assert result == TransactionState(in_transaction=True, isolation="read committed")
        assert await fetch_values(pool, item_id) == ["value"]

    @pytest.mark.parametrize(
        argnames=("isolation", "expected_state"),
        argvalues=[
            ("read_committed", TransactionState(in_transaction=False, isolation="read committed")),
            ("repeatable_read", TransactionState(in_transaction=True, isolation="repeatable read")),
            ("serializable", TransactionState(in_transaction=True, isolation="serializable")),
        ],
    )
    async def test_runs_read_effect_outside_transaction_only_when_isolation_is_read_committed(
        self,
        pool: Pool,
        isolation: IsolationLevel,
        expected_state: TransactionState,
    ) -> None:
        def handler() -> Effectful[TransactionState, Effect[Any]]:
            return (yield from ReadTransactionStateEffect())

        environment = make_environment(pool, StagedCommandDef("test", TestCommand, handler), isolation)

        async with environment:
            result = await environment.execute(TestCommand())

        assert result == expected_state

    @pytest.mark.parametrize(
        argnames=("isolation", "expected_isolation"),
        argvalues=[
            ("read_committed", "read committed"),
            ("repeatable_read", "repeatable read"),
            ("serializable", "serializable"),
        ],
    )
    async def test_runs_transaction_with_isolation(
        self,
        pool: Pool,
        isolation: IsolationLevel,
        expected_isolation: str,
    ) -> None:
        environment = make_environment(
            pool, AtomicCommandDef("test", TestCommand, read_state_in_transaction), isolation
        )

        async with environment:
            result = await environment.execute(TestCommand())

        assert result == TransactionState(in_transaction=True, isolation=expected_isolation)

    async def test_runs_transaction_of_atomic_command_with_its_options(self, pool: Pool) -> None:
        environment = make_environment(
            pool=pool,
            command_def=AtomicCommandDef("test", TestCommand, read_state_in_transaction),
            message_options={TestCommand: AsyncpgTransactionOptions(isolation="serializable")},
        )

        async with environment:
            result = await environment.execute(TestCommand())

        assert result == TransactionState(in_transaction=True, isolation="serializable")

    async def test_does_not_run_transaction_with_options_of_other_command(self, pool: Pool) -> None:
        environment = make_environment(
            pool=pool,
            command_def=AtomicCommandDef("test", TestCommand, read_state_in_transaction),
            message_options={OtherCommand: AsyncpgTransactionOptions(isolation="serializable")},
        )

        async with environment:
            result = await environment.execute(TestCommand())

        assert result == TransactionState(in_transaction=True, isolation="read committed")

    @pytest.mark.parametrize(
        argnames=("effect", "expected_state"),
        argvalues=[
            (ReadTransactionStateEffect(), TransactionState(in_transaction=False, isolation="read committed")),
            (WriteTransactionStateEffect(), TransactionState(in_transaction=True, isolation="serializable")),
        ],
    )
    async def test_runs_only_write_effect_outside_transaction_with_options_of_command(
        self,
        pool: Pool,
        effect: ReadTransactionStateEffect | WriteTransactionStateEffect,
        expected_state: TransactionState,
    ) -> None:
        def handler() -> Effectful[TransactionState, Effect[Any]]:
            return (yield from effect)

        environment = make_environment(
            pool=pool,
            command_def=StagedCommandDef("test", TestCommand, handler),
            message_options={TestCommand: AsyncpgTransactionOptions(isolation="serializable")},
        )

        async with environment:
            result = await environment.execute(TestCommand())

        assert result == expected_state


def read_state_in_transaction() -> AtomicEffectful[TransactionState]:
    yield from ()

    def transaction() -> TransactionEffectful[TransactionState]:
        return (yield from ReadTransactionStateEffect())

    return transaction


class OtherCommand(Command[str]):
    pass


def make_environment(
    pool: Pool,
    command_def: AtomicCommandDef[Any] | StagedCommandDef[Any],
    isolation: IsolationLevel = "read_committed",
    message_options: Mapping[type[Message[Any]], AsyncpgTransactionOptions] | None = None,
) -> EnvironmentImpl:
    # The pool outlives the environment because starting it for each test is slower.
    async def shared_pool() -> AsyncGenerator[Pool]:
        yield pool

    return EnvironmentImpl(
        operation_engine=OperationEngineImpl(command_defs=[command_def]),
        effect_defs=[
            EffectDef(GetItemEffect, handle_get_item),
            EffectDef(SaveItemEffect, handle_save_item),
            EffectDef(ReadTransactionStateEffect, handle_transaction_state),
            EffectDef(WriteTransactionStateEffect, handle_transaction_state),
            EffectDef(FailEffect, handle_fail),
            EffectDef(FailQueryEffect, handle_fail_query),
        ],
        dependency_defs=[
            EffectDependencyDef(shared_pool),
            AsyncpgConnectionDef(AsyncpgTransactionOptions(isolation=isolation), message_options),
        ],
    )


async def fetch_values(pool: Pool, *item_ids: str) -> Sequence[str | None]:
    return [await pool.fetchval("SELECT value FROM items WHERE id = $1", item_id) for item_id in item_ids]
