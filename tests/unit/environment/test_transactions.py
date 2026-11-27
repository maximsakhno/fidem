from asyncio import CancelledError, Event, create_task
from collections.abc import AsyncGenerator, Awaitable, Callable, MutableSequence, Sequence
from typing import Any

import pytest

from fidem.di.effects import EffectDependencyDef, EffectDependencyLifetime
from fidem.environment import CallNext, EffectDef, EffectMiddlewareDef, EnvironmentImpl
from fidem.messages import AtomicEffectful, MessageContext, StagedEffectful
from fidem.operation_engine.definitions import AtomicCommandDef, StagedCommandDef
from fidem.operation_engine.engine import OperationEngineImpl
from fidem.operation_engine.instructions import EffectCall, ExecutionPlan, OperationEngine, RunTransactionInstruction
from fidem.transactions import TransactionContext, WriteTransaction, WriteTransactionEffectful
from tests.effects import TestDatabaseReadEffect, TestDatabaseWriteEffect
from tests.messages import TestCommand
from tests.transactions import read_transaction, write_transaction
from tests.unit.environment.helpers import make_instruction, make_operation_engine_mock


class Connection:
    pass


def write_plan() -> ExecutionPlan[object]:
    return (yield make_instruction(TestDatabaseWriteEffect()))


def read_plan() -> ExecutionPlan[object]:
    return (yield make_instruction(TestDatabaseReadEffect()))


class TestTransactions:
    async def test_runs_effects_of_transaction_with_one_connection_and_returns_its_result(self) -> None:
        def transaction_plan() -> ExecutionPlan[object]:
            first_result = yield make_instruction(TestDatabaseWriteEffect())
            second_result = yield make_instruction(TestDatabaseReadEffect())
            return f"{first_result} {second_result}"

        events: MutableSequence[str] = []
        connections: MutableSequence[Connection] = []
        environment = self._make_environment(
            operation_engine=make_operation_engine_mock(self._in_transaction(transaction_plan)),
            handler=self._make_handler(connections),
            connection_def=self._make_connection_def(events),
        )

        async with environment:
            result = await environment.execute(TestCommand())

        assert result == "effect_result effect_result"
        assert events == ["begin read_only=False", "commit"]
        assert len(connections) == 2
        assert connections[0] is connections[1]

    async def test_rolls_back_transaction_and_throws_error_into_execution_plan_when_transaction_fails(self) -> None:
        async def handler(connection: Connection) -> str:
            raise ValueError("effect_error")

        events: MutableSequence[str] = []
        environment = self._make_environment(
            operation_engine=make_operation_engine_mock(self._in_transaction(write_plan)),
            handler=handler,
            connection_def=self._make_connection_def(events),
        )

        async with environment:
            result = await environment.execute(TestCommand())

        assert result == "effect_error"
        assert events == ["begin read_only=False", "rollback"]

    async def test_throws_commit_error_into_execution_plan(self) -> None:
        async def connection() -> AsyncGenerator[Connection]:
            yield Connection()
            raise ValueError("commit_error")

        environment = self._make_environment(
            operation_engine=make_operation_engine_mock(self._in_transaction(write_plan)),
            handler=self._make_handler([]),
            connection_def=EffectDependencyDef(connection, lifetime=EffectDependencyLifetime.TRANSACTION),
        )

        async with environment:
            result = await environment.execute(TestCommand())

        assert result == "commit_error"

    async def test_does_not_resolve_transaction_dependencies_when_no_effect_needs_them(self) -> None:
        async def handler() -> str:
            return "effect_result"

        events: MutableSequence[str] = []
        environment = self._make_environment(
            operation_engine=make_operation_engine_mock(self._in_transaction(write_plan)),
            handler=handler,
            connection_def=self._make_connection_def(events),
        )

        async with environment:
            result = await environment.execute(TestCommand())

        assert result == "effect_result"
        assert events == []

    @pytest.mark.timeout(5)
    async def test_rolls_back_transaction_when_execution_cancelled(self) -> None:
        async def handler(connection: Connection) -> str:
            started.set()
            await Event().wait()
            return "unreachable"

        started = Event()
        events: MutableSequence[str] = []
        environment = self._make_environment(
            operation_engine=make_operation_engine_mock(self._in_transaction(write_plan)),
            handler=handler,
            connection_def=self._make_connection_def(events),
        )

        async with environment:
            task = create_task(environment.execute(TestCommand()))
            await started.wait()
            task.cancel()
            with pytest.raises(CancelledError):
                await task

        assert events == ["begin read_only=False", "rollback"]

    async def test_raises_not_implemented_error_when_transaction_is_nested(self) -> None:
        def transaction_plan() -> ExecutionPlan[object]:
            return (yield RunTransactionInstruction(transaction=write_transaction, plan=write_plan()))

        environment = self._make_environment(
            operation_engine=make_operation_engine_mock(self._in_transaction(transaction_plan)),
            handler=self._make_handler([]),
            connection_def=self._make_connection_def([]),
        )

        async with environment:
            with pytest.raises(NotImplementedError):
                await environment.execute(TestCommand())

    async def test_runs_each_database_effect_outside_transaction_in_its_own_transaction(self) -> None:
        def command_handler() -> StagedEffectful[str]:
            yield from TestDatabaseReadEffect()
            yield from TestDatabaseWriteEffect()
            return "command_result"

        events: MutableSequence[str] = []
        connections: MutableSequence[Connection] = []
        environment = self._make_environment(
            operation_engine=OperationEngineImpl(command_defs=[StagedCommandDef("test", TestCommand, command_handler)]),
            handler=self._make_handler(connections),
            connection_def=self._make_connection_def(events),
        )

        async with environment:
            await environment.execute(TestCommand())

        assert events == ["begin read_only=True", "commit", "begin read_only=False", "commit"]
        assert connections[0] is not connections[1]

    async def test_runs_retries_of_database_effect_outside_transaction_in_its_implicit_transaction(self) -> None:
        async def handler(connection: Connection) -> str:
            connections.append(connection)
            if len(connections) == 1:
                raise ValueError("effect_error")
            return "effect_result"

        async def retry(effect_call: EffectCall, call_next: CallNext) -> object:
            try:
                return await call_next()
            except ValueError:
                return await call_next()

        def command_handler() -> StagedEffectful[str]:
            return (yield from TestDatabaseWriteEffect())

        events: MutableSequence[str] = []
        connections: MutableSequence[Connection] = []
        environment = self._make_environment(
            operation_engine=OperationEngineImpl(command_defs=[StagedCommandDef("test", TestCommand, command_handler)]),
            handler=handler,
            connection_def=self._make_connection_def(events),
            middleware_defs=[EffectMiddlewareDef(retry)],
        )

        async with environment:
            result = await environment.execute(TestCommand())

        assert result == "effect_result"
        assert events == ["begin read_only=False", "commit"]
        assert connections[0] is connections[1]

    async def test_executes_atomic_command_with_reads_outside_transaction(self) -> None:
        @WriteTransaction
        def transaction(read_result: str) -> WriteTransactionEffectful[str]:
            first_result = yield from TestDatabaseWriteEffect()
            second_result = yield from TestDatabaseWriteEffect()
            return f"{read_result} {first_result} {second_result}"

        def handler() -> AtomicEffectful[str]:
            read_result = yield from TestDatabaseReadEffect()
            return transaction(read_result)

        events: MutableSequence[str] = []
        connections: MutableSequence[Connection] = []
        environment = self._make_environment(
            operation_engine=OperationEngineImpl(command_defs=[AtomicCommandDef("test", TestCommand, handler)]),
            handler=self._make_handler(connections),
            connection_def=self._make_connection_def(events),
        )

        async with environment:
            result = await environment.execute(TestCommand())

        assert result == "effect_result effect_result effect_result"
        assert events == ["begin read_only=True", "commit", "begin read_only=False", "commit"]
        assert connections[0] is not connections[1]
        assert connections[1] is connections[2]

    async def test_passes_transaction_context_of_each_transaction_kind_into_factory(self) -> None:
        async def connection(transaction_context: TransactionContext) -> AsyncGenerator[Connection]:
            transaction_contexts.append(transaction_context)
            yield Connection()

        def execution_plan(command: TestCommand, message_context: MessageContext) -> ExecutionPlan[object]:
            yield RunTransactionInstruction(transaction=read_transaction, plan=read_plan())
            yield RunTransactionInstruction(transaction=write_transaction, plan=write_plan())
            return "command_result"

        transaction_contexts: MutableSequence[TransactionContext] = []
        environment = self._make_environment(
            operation_engine=make_operation_engine_mock(execution_plan),
            handler=self._make_handler([]),
            connection_def=EffectDependencyDef(connection, lifetime=EffectDependencyLifetime.TRANSACTION),
        )

        async with environment:
            await environment.execute(TestCommand())

        assert transaction_contexts == [
            TransactionContext(transaction=read_transaction),
            TransactionContext(transaction=write_transaction),
        ]

    @staticmethod
    def _make_environment(
        operation_engine: OperationEngine,
        handler: Callable[..., Awaitable[object]],
        connection_def: EffectDependencyDef[Any],
        middleware_defs: Sequence[EffectMiddlewareDef] | None = None,
    ) -> EnvironmentImpl:
        return EnvironmentImpl(
            operation_engine=operation_engine,
            effect_defs=[
                EffectDef(TestDatabaseReadEffect, handler, middleware_defs),
                EffectDef(TestDatabaseWriteEffect, handler, middleware_defs),
            ],
            dependency_defs=[connection_def],
        )

    @staticmethod
    def _in_transaction(plan: Callable[[], ExecutionPlan[object]]) -> Callable[..., ExecutionPlan[object]]:
        """Makes an execution plan that runs the plan in a transaction and returns its result or error."""

        def execution_plan(command: TestCommand, message_context: MessageContext) -> ExecutionPlan[object]:
            try:
                return (yield RunTransactionInstruction(transaction=write_transaction, plan=plan()))
            except ValueError as exc:
                return str(exc)

        return execution_plan

    @staticmethod
    def _make_handler(connections: MutableSequence[Connection]) -> Callable[..., Awaitable[object]]:
        async def handler(connection: Connection) -> str:
            connections.append(connection)
            return "effect_result"

        return handler

    @staticmethod
    def _make_connection_def(events: MutableSequence[str]) -> EffectDependencyDef[Connection]:
        async def connection(transaction_context: TransactionContext) -> AsyncGenerator[Connection]:
            events.append(f"begin read_only={transaction_context.read_only}")
            try:
                yield Connection()
            except BaseException:
                events.append("rollback")
                raise
            events.append("commit")

        return EffectDependencyDef(connection, lifetime=EffectDependencyLifetime.TRANSACTION)
