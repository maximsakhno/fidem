from collections.abc import MutableSequence
from typing import Any

import pytest

from fidem.core.di.core import UnsupportedFunctionError
from fidem.core.effects import Effect, Effectful
from fidem.core.guards import GuardDef
from fidem.core.messages import QueryEffect, QueryEffectful
from fidem.core.operation_engine.definitions import (
    AtomicCommandDef,
    MessageMiddlewareDef,
    MessageMiddlewareEffectful,
    Proceed,
    StagedCommandDef,
)
from fidem.core.operation_engine.errors import UnsupportedEffectError
from fidem.core.operation_engine.instructions import HandledEffect, RunTransactionInstruction
from fidem.core.testing import YieldStep, assert_generator
from fidem.core.transactions import (
    IMPLICIT_READ_TRANSACTION,
    IMPLICIT_WRITE_TRANSACTION,
    AnyTransaction,
    ReadTransaction,
    ReadTransactionEffectful,
    WriteTransaction,
    WriteTransactionEffectful,
)
from tests.effects import TestDatabaseReadEffect, TestDatabaseWriteEffect, TestExternalReadEffect, TestInternalEffect
from tests.ids import type_name
from tests.messages import TestCommand, TestEvent, TestQuery
from tests.transactions import read_transaction, write_transaction
from tests.unit.operation_engine.helpers import (
    MESSAGE_CONTEXT,
    Service,
    assert_runs_in_transaction,
    collect_effect_ids,
    make_engine,
    make_expected_instruction,
    make_query_engine,
    run_plan,
)


class TestTransactions:
    @pytest.mark.parametrize(
        argnames=("effect", "transaction"),
        argvalues=[
            (TestDatabaseReadEffect(), IMPLICIT_READ_TRANSACTION),
            (TestDatabaseWriteEffect(), IMPLICIT_WRITE_TRANSACTION),
        ],
        ids=type_name,
    )
    def test_runs_database_effect_outside_transaction_in_implicit_transaction(
        self,
        effect: HandledEffect[str],
        transaction: AnyTransaction,
    ) -> None:
        def handler() -> Effectful[str, Effect[Any]]:
            effect_result = yield from effect
            return f"command_result {effect_result}"

        command = TestCommand()
        execution_plan = make_engine(handler).handle(command, MESSAGE_CONTEXT)

        assert_runs_in_transaction(
            instruction=execution_plan.send(None),
            transaction=transaction,
            effect_instruction=make_expected_instruction(effect, command),
            result="effect_result",
        )
        with pytest.raises(StopIteration) as exc_info:
            execution_plan.send("effect_result")
        assert exc_info.value.value == "command_result effect_result"

    def test_runs_read_transaction_with_its_arguments_in_run_transaction_instruction(self) -> None:
        @ReadTransaction
        def transaction(prefix: str, suffix: str) -> ReadTransactionEffectful[str]:
            first_result = yield from first_read
            second_result = yield from second_read
            return f"{prefix} {first_result} {second_result} {suffix}"

        def handler() -> QueryEffectful[str]:
            return (yield from transaction("prefix", suffix="suffix"))

        first_read = TestDatabaseReadEffect()
        second_read = TestDatabaseReadEffect()
        query = TestQuery()
        execution_plan = make_query_engine(handler).handle(query, MESSAGE_CONTEXT)

        instruction = execution_plan.send(None)
        assert isinstance(instruction, RunTransactionInstruction)
        assert instruction.transaction is transaction
        assert_generator(
            generator=instruction.plan,
            steps=[
                YieldStep(make_expected_instruction(first_read, query), "first"),
                YieldStep(make_expected_instruction(second_read, query), "second"),
            ],
            return_value="prefix first second suffix",
        )
        with pytest.raises(StopIteration) as exc_info:
            execution_plan.send("transaction_result")
        assert exc_info.value.value == "transaction_result"

    @pytest.mark.parametrize("handler_kind", ["staged", "atomic", "guard", "middleware"])
    def test_runs_read_transaction_yielded_by_any_part_of_command(self, handler_kind: str) -> None:
        def guard() -> Effectful[Service, QueryEffect]:
            if handler_kind == "guard":
                yield from read_transaction()
            return Service()

        def middleware() -> MessageMiddlewareEffectful[object]:
            if handler_kind == "middleware":
                yield from read_transaction()
            return (yield from Proceed())

        def handler(service: Service) -> Effectful[object, Effect[Any]]:
            if handler_kind in {"staged", "atomic"}:
                yield from read_transaction()
            return write_transaction() if handler_kind == "atomic" else "command_result"

        transactions: MutableSequence[object] = []
        engine = make_engine(
            handler,
            guard_defs=[GuardDef(guard)],
            middleware_defs=[MessageMiddlewareDef("test", middleware)],
            command_def_type=AtomicCommandDef if handler_kind == "atomic" else StagedCommandDef,
        )

        run_plan(engine.handle(TestCommand(), MESSAGE_CONTEXT), on_transaction=transactions.append)

        expected_transactions = (
            [read_transaction, write_transaction] if handler_kind == "atomic" else [read_transaction]
        )
        assert transactions == expected_transactions

    def test_runs_each_transaction_yielded_by_staged_handler(self) -> None:
        def handler() -> Effectful[str, Effect[Any]]:
            first_result = yield from write_transaction()
            second_result = yield from read_transaction()
            third_result = yield from write_transaction()
            return f"{first_result} {second_result} {third_result}"

        transactions: MutableSequence[object] = []
        effect_types: MutableSequence[type[Effect[Any]]] = []

        result = run_plan(
            execution_plan=make_engine(handler).handle(TestCommand(), MESSAGE_CONTEXT),
            call_effect_handler=lambda instruction: (
                effect_types.append(type(instruction.call.effect)) or "effect_result"
            ),
            on_transaction=transactions.append,
        )

        assert result == "effect_result effect_result effect_result"
        assert transactions == [write_transaction, read_transaction, write_transaction]
        assert effect_types == [TestDatabaseWriteEffect, TestDatabaseReadEffect, TestDatabaseWriteEffect]

    def test_runs_new_generator_each_time_same_transaction_effect_is_yielded(self) -> None:
        def handler() -> Effectful[str, Effect[Any]]:
            transaction = read_transaction()
            first_result = yield from transaction
            second_result = yield from transaction
            return f"{first_result} {second_result}"

        result = run_plan(
            execution_plan=make_engine(handler).handle(TestCommand(), MESSAGE_CONTEXT),
            call_effect_handler=lambda instruction: "effect_result",
        )

        assert result == "effect_result effect_result"

    @pytest.mark.parametrize(
        argnames="effect",
        argvalues=[
            TestDatabaseWriteEffect(),
            TestInternalEffect(),
            TestExternalReadEffect(),
            TestEvent(),
            read_transaction(),
            write_transaction(),
        ],
        ids=type_name,
    )
    def test_raises_unsupported_effect_error_when_read_transaction_yields_unsupported_effect(
        self,
        effect: Effect[Any],
    ) -> None:
        @ReadTransaction  # pyright: ignore[reportArgumentType] - checks what the transaction type forbids
        def transaction() -> Effectful[str, Effect[Any]]:
            return (yield from effect)

        def handler() -> Effectful[str, Effect[Any]]:
            return (yield from transaction())

        with pytest.raises(UnsupportedEffectError) as exc_info:
            run_plan(make_engine(handler).handle(TestCommand(), MESSAGE_CONTEXT))

        assert exc_info.value.args == (effect,)

    def test_throws_transaction_error_into_staged_handler(self) -> None:
        def handler() -> Effectful[str, Effect[Any]]:
            try:
                yield from write_transaction()
            except ValueError as exc:
                return f"caught {exc}"
            return "unreachable"

        execution_plan = make_engine(handler).handle(TestCommand(), MESSAGE_CONTEXT)
        assert isinstance(execution_plan.send(None), RunTransactionInstruction)

        with pytest.raises(StopIteration) as exc_info:
            execution_plan.throw(ValueError("commit_error"))

        assert exc_info.value.value == "caught commit_error"

    def test_throws_error_of_transaction_call_into_staged_handler(self) -> None:
        @WriteTransaction
        def transaction(value: str) -> WriteTransactionEffectful[str]:
            return (yield from TestDatabaseWriteEffect())

        def handler() -> Effectful[str, Effect[Any]]:
            try:
                yield from transaction()  # pyright: ignore[reportCallIssue] - checks arguments that do not bind
            except TypeError:
                return "caught"
            return "unreachable"

        result = run_plan(make_engine(handler).handle(TestCommand(), MESSAGE_CONTEXT))

        assert result == "caught"

    def test_generates_transaction_effect_ids_like_effects_yielded_by_handler(self) -> None:
        @ReadTransaction
        def transaction() -> ReadTransactionEffectful[str]:
            yield from TestDatabaseReadEffect()
            return (yield from TestDatabaseReadEffect())

        def transaction_handler() -> Effectful[str, Effect[Any]]:
            yield from TestInternalEffect()
            yield from transaction()
            return (yield from TestInternalEffect())

        def plain_handler() -> Effectful[str, Effect[Any]]:
            yield from TestInternalEffect()
            yield from TestDatabaseReadEffect()
            yield from TestDatabaseReadEffect()
            return (yield from TestInternalEffect())

        transaction_effect_ids = collect_effect_ids(
            make_engine(transaction_handler).handle(TestCommand(), MESSAGE_CONTEXT)
        )
        plain_effect_ids = collect_effect_ids(make_engine(plain_handler).handle(TestCommand(), MESSAGE_CONTEXT))

        assert transaction_effect_ids == plain_effect_ids

    @pytest.mark.parametrize("transaction_type", [ReadTransaction, WriteTransaction])
    def test_raises_unsupported_function_error_when_transaction_function_is_not_generator_function(
        self,
        transaction_type: type[ReadTransaction[..., Any]] | type[WriteTransaction[..., Any]],
    ) -> None:
        def function() -> str:
            return "not_a_generator"

        with pytest.raises(UnsupportedFunctionError) as exc_info:
            transaction_type(function)  # pyright: ignore[reportArgumentType] - checks a wrong function

        assert exc_info.value.args == (function,)
