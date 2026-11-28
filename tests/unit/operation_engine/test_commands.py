from collections.abc import MutableSequence
from dataclasses import dataclass
from typing import Any

import pytest

from fidem.core.di.core import DependencyNotFoundError, UnsupportedFunctionError
from fidem.core.effects import Effect, Effectful
from fidem.core.guards import GuardDef
from fidem.core.messages import AtomicEffectful, Command, MessageContext, QueryEffect
from fidem.core.operation_engine.definitions import (
    AtomicCommandDef,
    ExecutionMode,
    MessageMiddlewareDef,
    MessageMiddlewareEffectful,
    Proceed,
    StagedCommandDef,
)
from fidem.core.operation_engine.engine import OperationEngineImpl
from fidem.core.operation_engine.errors import (
    DuplicateCommandDefError,
    MessageNotDefinedError,
    UnsupportedAtomicResultError,
    UnsupportedEffectError,
)
from fidem.core.operation_engine.instructions import (
    CallEffectHandlerInstruction,
    HandledEffect,
    RunTransactionInstruction,
)
from fidem.core.testing import YieldStep, assert_generator
from fidem.core.transactions import IMPLICIT_READ_TRANSACTION, WriteTransaction, WriteTransactionEffectful
from tests.effects import (
    TestControlEffect,
    TestDatabaseReadEffect,
    TestDatabaseWriteEffect,
    TestExternalReadEffect,
    TestExternalWriteEffect,
    TestInternalEffect,
)
from tests.ids import type_name
from tests.messages import TestCommand, TestEvent
from tests.transactions import read_transaction, write_transaction
from tests.unit.operation_engine.helpers import (
    MESSAGE_CONTEXT,
    Service,
    assert_runs_in_transaction,
    collect_effect_ids,
    handle_with_three_effects,
    make_engine,
    make_expected_instruction,
    run_plan,
)


@dataclass(frozen=True, slots=True)
class HandlerCall:
    context: MessageContext
    command: TestCommand


def not_a_transaction() -> WriteTransactionEffectful[str]:
    return (yield from TestDatabaseWriteEffect())


def raise_effect_error(instruction: CallEffectHandlerInstruction[Any]) -> object:
    raise ValueError("effect_error")


class TestExecuteEphemeral:
    def test_passes_message_and_message_context_into_handler_and_returns_its_result(self) -> None:
        def handler(context: MessageContext, command: TestCommand) -> Effectful[str, Effect[Any]]:
            calls.append(HandlerCall(context=context, command=command))
            yield from ()
            return "command_result"

        calls: MutableSequence[HandlerCall] = []
        command = TestCommand()
        engine = make_engine(handler)

        assert_generator(
            generator=engine.handle(command, MessageContext(message_id="test_message_id")),
            return_value="command_result",
        )
        assert calls == [HandlerCall(context=MessageContext(message_id="test_message_id"), command=command)]

    @pytest.mark.parametrize(
        argnames="effect",
        argvalues=[TestInternalEffect(), TestExternalReadEffect(), TestExternalWriteEffect()],
        ids=type_name,
    )
    def test_yields_call_effect_handler_instruction_when_handler_yields_effect(
        self,
        effect: HandledEffect[str],
    ) -> None:
        def handler() -> Effectful[str, Effect[Any]]:
            effect_result = yield from effect
            return f"command_result {effect_result}"

        command = TestCommand()
        engine = make_engine(handler)

        assert_generator(
            generator=engine.handle(command, MessageContext(message_id="test_message_id")),
            steps=[YieldStep(make_expected_instruction(effect, command), "effect_result")],
            return_value="command_result effect_result",
        )

    @pytest.mark.parametrize("effect", [TestControlEffect(), TestCommand(), Proceed()], ids=type_name)
    def test_raises_unsupported_effect_error_when_handler_yields_unsupported_effect(self, effect: Effect[Any]) -> None:
        def handler() -> Effectful[str, Effect[Any]]:
            yield from effect
            return "unreachable"

        engine = make_engine(handler)

        with pytest.raises(UnsupportedEffectError) as exc_info:
            engine.handle(TestCommand(), MESSAGE_CONTEXT).send(None)

        assert exc_info.value.args == (effect,)

    def test_does_not_throw_unsupported_effect_error_into_handler(self) -> None:
        def handler() -> Effectful[str, Effect[Any]]:
            nonlocal is_handler_closed
            try:
                yield from TestControlEffect()
            except Exception:  # noqa: BLE001 - checks that no error reaches the handler
                return "swallowed"
            finally:
                is_handler_closed = True
            return "unreachable"

        is_handler_closed = False
        engine = make_engine(handler)

        with pytest.raises(UnsupportedEffectError):
            engine.handle(TestCommand(), MESSAGE_CONTEXT).send(None)

        assert is_handler_closed

    def test_closes_handler_when_execution_plan_closed(self) -> None:
        def handler() -> Effectful[str, Effect[Any]]:
            nonlocal is_handler_closed
            try:
                yield from TestInternalEffect()
            finally:
                is_handler_closed = True
            return "command_result"

        is_handler_closed = False
        engine = make_engine(handler)
        execution_plan = engine.handle(TestCommand(), MESSAGE_CONTEXT)
        execution_plan.send(None)

        execution_plan.close()

        assert is_handler_closed

    def test_generates_same_effect_ids_for_same_message_id(self) -> None:
        engine = make_engine(handle_with_three_effects)

        effect_ids1 = collect_effect_ids(engine.handle(TestCommand(), MessageContext(message_id="test_message_id")))
        effect_ids2 = collect_effect_ids(engine.handle(TestCommand(), MessageContext(message_id="test_message_id")))

        assert effect_ids1 == effect_ids2

    def test_generates_unique_effect_ids_within_command(self) -> None:
        engine = make_engine(handle_with_three_effects)

        effect_ids = collect_effect_ids(engine.handle(TestCommand(), MessageContext(message_id="test_message_id")))

        assert len(set(effect_ids)) == 3

    def test_generates_different_effect_ids_for_different_message_ids(self) -> None:
        engine = make_engine(handle_with_three_effects)

        effect_ids1 = collect_effect_ids(engine.handle(TestCommand(), MessageContext(message_id="test_message_id1")))
        effect_ids2 = collect_effect_ids(engine.handle(TestCommand(), MessageContext(message_id="test_message_id2")))

        assert set(effect_ids1).isdisjoint(effect_ids2)

    def test_throws_instruction_error_into_handler(self) -> None:
        def handler() -> Effectful[str, Effect[Any]]:
            try:
                yield from TestInternalEffect()
            except ValueError as exc:
                return str(exc)
            return "unreachable"

        engine = make_engine(handler)
        execution_plan = engine.handle(TestCommand(), MESSAGE_CONTEXT)
        execution_plan.send(None)

        with pytest.raises(StopIteration) as exc_info:
            execution_plan.throw(ValueError("effect_error"))

        assert exc_info.value.value == "effect_error"

    def test_raises_instruction_error_when_handler_does_not_handle_it(self) -> None:
        engine = make_engine(handle_with_three_effects)
        execution_plan = engine.handle(TestCommand(), MESSAGE_CONTEXT)
        execution_plan.send(None)
        effect_error = ValueError("effect_error")

        with pytest.raises(ValueError, match="effect_error") as exc_info:
            execution_plan.throw(effect_error)

        assert exc_info.value is effect_error

    def test_raises_command_not_defined_error_when_command_not_defined(self) -> None:
        engine = OperationEngineImpl(command_defs=[])

        with pytest.raises(MessageNotDefinedError) as exc_info:
            engine.handle(TestCommand(), MESSAGE_CONTEXT).send(None)

        assert exc_info.value.args == (TestCommand,)

    def test_raises_not_implemented_error_when_command_is_durable(self) -> None:
        command_def = StagedCommandDef(
            name="test",
            message_type=TestCommand,
            handler=handle_with_three_effects,
            execution_mode=ExecutionMode.DURABLE,
        )
        engine = OperationEngineImpl(command_defs=[command_def])

        with pytest.raises(NotImplementedError):
            engine.handle(TestCommand(), MESSAGE_CONTEXT).send(None)


class TestExecuteAtomic:
    def test_runs_reads_and_then_transaction_in_run_transaction_instruction(self) -> None:
        @WriteTransaction
        def transaction(read_result: str) -> WriteTransactionEffectful[str]:
            write_result = yield from write
            return f"{read_result} {write_result}"

        def handler() -> AtomicEffectful[str]:
            read_result = yield from read
            return transaction(read_result)

        read = TestDatabaseReadEffect()
        write = TestDatabaseWriteEffect()
        command = TestCommand()
        execution_plan = make_engine(handler, command_def_type=AtomicCommandDef).handle(command, MESSAGE_CONTEXT)

        assert_runs_in_transaction(
            instruction=execution_plan.send(None),
            transaction=IMPLICIT_READ_TRANSACTION,
            effect_instruction=make_expected_instruction(read, command),
            result="read_result",
        )
        instruction = execution_plan.send("read_result")
        assert isinstance(instruction, RunTransactionInstruction)
        assert instruction.transaction is transaction
        assert_generator(
            generator=instruction.plan,
            steps=[YieldStep(make_expected_instruction(write, command), "write_result")],
            return_value="read_result write_result",
        )
        with pytest.raises(StopIteration) as exc_info:
            execution_plan.send("read_result write_result")
        assert exc_info.value.value == "read_result write_result"

    def test_yields_external_write_effect_returned_by_handler_and_returns_its_result(self) -> None:
        def handler() -> AtomicEffectful[str]:
            yield from read
            return write

        read = TestExternalReadEffect()
        write = TestExternalWriteEffect()
        command = TestCommand()

        assert_generator(
            generator=make_engine(handler, command_def_type=AtomicCommandDef).handle(command, MESSAGE_CONTEXT),
            steps=[
                YieldStep(make_expected_instruction(read, command), "read_result"),
                YieldStep(make_expected_instruction(write, command), "write_result"),
            ],
            return_value="write_result",
        )

    @pytest.mark.parametrize(
        argnames="effect",
        argvalues=[
            TestDatabaseWriteEffect(),
            TestExternalWriteEffect(),
            TestControlEffect(),
            TestEvent(),
            TestCommand(),
            Proceed(),
            write_transaction(),
        ],
        ids=type_name,
    )
    def test_raises_unsupported_effect_error_when_handler_yields_unsupported_effect(self, effect: Effect[Any]) -> None:
        def handler() -> Effectful[object, Effect[Any]]:
            yield from effect
            return write_transaction()

        engine = make_engine(handler, command_def_type=AtomicCommandDef)

        with pytest.raises(UnsupportedEffectError) as exc_info:
            run_plan(engine.handle(TestCommand(), MESSAGE_CONTEXT))

        assert exc_info.value.args == (effect,)

    @pytest.mark.parametrize(
        argnames="effect",
        argvalues=[
            TestInternalEffect(),
            TestExternalReadEffect(),
            TestExternalWriteEffect(),
            read_transaction(),
            write_transaction(),
        ],
        ids=type_name,
    )
    def test_raises_unsupported_effect_error_when_transaction_yields_unsupported_effect(
        self,
        effect: Effect[Any],
    ) -> None:
        @WriteTransaction  # pyright: ignore[reportArgumentType] - checks what the transaction type forbids
        def transaction() -> Effectful[str, Effect[Any]]:
            yield from effect
            return "command_result"

        def handler() -> Effectful[object, Effect[Any]]:
            yield from ()
            return transaction()

        engine = make_engine(handler, command_def_type=AtomicCommandDef)

        with pytest.raises(UnsupportedEffectError) as exc_info:
            run_plan(engine.handle(TestCommand(), MESSAGE_CONTEXT))

        assert exc_info.value.args == (effect,)

    @pytest.mark.parametrize(
        argnames="result",
        argvalues=["not_a_write", TestDatabaseWriteEffect(), read_transaction(), write_transaction, not_a_transaction],
        ids=type_name,
    )
    def test_raises_unsupported_atomic_result_error_when_handler_returns_neither_transaction_nor_external_write(
        self,
        result: object,
    ) -> None:
        def handler() -> Effectful[object, Effect[Any]]:
            yield from ()
            return result

        engine = make_engine(handler, command_def_type=AtomicCommandDef)

        with pytest.raises(UnsupportedAtomicResultError) as exc_info:
            run_plan(engine.handle(TestCommand(), MESSAGE_CONTEXT))

        assert exc_info.value.args == (result,)

    def test_does_not_throw_unsupported_atomic_result_error_into_middleware(self) -> None:
        def middleware() -> MessageMiddlewareEffectful[object]:
            try:
                return (yield from Proceed())
            except Exception:  # noqa: BLE001 - checks that no error reaches the middleware
                return "swallowed"

        def handler() -> Effectful[object, Effect[Any]]:
            yield from ()
            return "not_a_write"

        engine = make_engine(
            handler, command_def_type=AtomicCommandDef, middleware_defs=[MessageMiddlewareDef("test", middleware)]
        )

        with pytest.raises(UnsupportedAtomicResultError):
            run_plan(engine.handle(TestCommand(), MESSAGE_CONTEXT))

    def test_throws_effect_error_into_transaction(self) -> None:
        @WriteTransaction
        def transaction() -> WriteTransactionEffectful[str]:
            try:
                yield from TestDatabaseWriteEffect()
            except ValueError as exc:
                return str(exc)
            return "unreachable"

        def handler() -> AtomicEffectful[str]:
            yield from ()
            return transaction()

        engine = make_engine(handler, command_def_type=AtomicCommandDef)

        result = run_plan(engine.handle(TestCommand(), MESSAGE_CONTEXT), raise_effect_error)

        assert result == "effect_error"

    def test_throws_transaction_error_into_middleware(self) -> None:
        def middleware() -> MessageMiddlewareEffectful[object]:
            try:
                return (yield from Proceed())
            except ValueError as exc:
                return str(exc)

        def handler() -> AtomicEffectful[str]:
            yield from ()
            return write_transaction()

        engine = make_engine(
            handler, command_def_type=AtomicCommandDef, middleware_defs=[MessageMiddlewareDef("test", middleware)]
        )

        result = run_plan(engine.handle(TestCommand(), MESSAGE_CONTEXT), raise_effect_error)

        assert result == "effect_error"

    @pytest.mark.parametrize(
        argnames="write", argvalues=[TestDatabaseWriteEffect(), TestExternalWriteEffect()], ids=type_name
    )
    def test_generates_write_effect_ids_after_read_effect_ids_like_staged_handler(
        self,
        write: TestDatabaseWriteEffect | TestExternalWriteEffect,
    ) -> None:
        @WriteTransaction
        def transaction() -> WriteTransactionEffectful[str]:
            return (yield from TestDatabaseWriteEffect())

        def atomic_handler() -> Effectful[object, Effect[Any]]:
            yield from TestInternalEffect()
            yield from TestDatabaseReadEffect()
            return write if isinstance(write, TestExternalWriteEffect) else transaction()

        def staged_handler() -> Effectful[str, Effect[Any]]:
            yield from TestInternalEffect()
            yield from TestDatabaseReadEffect()
            return (yield from write)

        atomic_effect_ids = collect_effect_ids(
            make_engine(atomic_handler, command_def_type=AtomicCommandDef).handle(TestCommand(), MESSAGE_CONTEXT)
        )
        staged_effect_ids = collect_effect_ids(make_engine(staged_handler).handle(TestCommand(), MESSAGE_CONTEXT))

        assert atomic_effect_ids == staged_effect_ids

    def test_runs_both_phases_on_each_proceed_with_same_effect_ids(self) -> None:
        def middleware() -> MessageMiddlewareEffectful[object]:
            first_result = yield from Proceed()
            second_result = yield from Proceed()
            return f"{first_result} {second_result}"

        def handler() -> AtomicEffectful[str]:
            yield from TestDatabaseReadEffect()
            return write_transaction()

        effect_ids: MutableSequence[str] = []
        engine = make_engine(
            handler, command_def_type=AtomicCommandDef, middleware_defs=[MessageMiddlewareDef("test", middleware)]
        )

        result = run_plan(
            execution_plan=engine.handle(TestCommand(), MESSAGE_CONTEXT),
            call_effect_handler=lambda instruction: (
                effect_ids.append(instruction.call.effect_context.effect_id) or "effect_result"
            ),
        )

        assert result == "effect_result effect_result"
        assert len(effect_ids) == 4
        assert effect_ids[:2] == effect_ids[2:]

    def test_runs_guards_before_handler_and_passes_their_values_into_it(self) -> None:
        def service() -> Effectful[Service, QueryEffect]:
            yield from TestInternalEffect()
            return Service()

        def handler(service: Service) -> AtomicEffectful[str]:
            services.append(service)
            yield from TestDatabaseReadEffect()
            return write_transaction()

        services: MutableSequence[Service] = []
        effect_types: MutableSequence[type[Effect[Any]]] = []
        engine = make_engine(handler, command_def_type=AtomicCommandDef, guard_defs=[GuardDef(service)])

        run_plan(
            execution_plan=engine.handle(TestCommand(), MESSAGE_CONTEXT),
            call_effect_handler=lambda instruction: effect_types.append(type(instruction.call.effect)),
        )

        assert len(services) == 1
        assert effect_types == [TestInternalEffect, TestDatabaseReadEffect, TestDatabaseWriteEffect]


class TestCommandDefinition:
    def test_raises_duplicate_command_def_error_when_define_commands_with_same_type(self) -> None:
        class OtherCommand(Command[str]):
            pass

        duplicate_command_defs = [
            StagedCommandDef("test1", TestCommand, handle_with_three_effects),
            StagedCommandDef("test2", TestCommand, handle_with_three_effects),
        ]
        other_command_defs = [
            StagedCommandDef("other", OtherCommand, handle_with_three_effects),
        ]

        with pytest.raises(DuplicateCommandDefError) as exc_info:
            OperationEngineImpl(command_defs=duplicate_command_defs + other_command_defs)

        assert exc_info.value.args == (duplicate_command_defs,)

    def test_raises_unsupported_function_error_when_handler_is_not_generator_function(self) -> None:
        def handler() -> str:
            return "command_result"

        with pytest.raises(UnsupportedFunctionError):
            make_engine(handler)  # pyright: ignore[reportArgumentType] - checks a wrong handler

    def test_raises_dependency_not_found_error_when_handler_parameter_has_no_guard(self) -> None:
        def handler(service: Service) -> Effectful[str, Effect[Any]]:
            yield from ()
            return "command_result"

        with pytest.raises(DependencyNotFoundError) as exc_info:
            make_engine(handler)

        assert exc_info.value.args == (handler, "service", Service)
