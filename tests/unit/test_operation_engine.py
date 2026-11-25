from collections.abc import Callable, Collection, MutableSequence, Sequence
from dataclasses import dataclass
from typing import Any
from unittest.mock import ANY

import pytest

from fidem.di.core import DependencyNotFoundError, UnsupportedFunctionError
from fidem.effects import (
    Effect,
    EffectContext,
    Effectful,
)
from fidem.guards import GuardDef
from fidem.messages import (
    AtomicEffectful,
    Command,
    Event,
    Message,
    MessageContext,
    QueryEffect,
    QueryEffectful,
    StagedEffectful,
    StagedHandler,
)
from fidem.operation_engine import (
    AtomicCommandDef,
    AtomicEventHandlerDef,
    CallEffectHandlerInstruction,
    CommandNotDefinedError,
    DuplicateCommandDefError,
    DuplicateEventDefError,
    DuplicateEventHandlerDefError,
    DuplicateMessageMiddlewareDefError,
    DuplicateQueryDefError,
    EventDef,
    EventNotDefinedError,
    ExecutionMode,
    ExecutionPlan,
    HandledEffect,
    MessageMiddlewareDef,
    MessageMiddlewareEffectful,
    OperationEngineImpl,
    Proceed,
    QueryDef,
    QueryNotDefinedError,
    RunTransactionInstruction,
    StagedCommandDef,
    StagedEventHandlerDef,
    UnsupportedAtomicResultError,
    UnsupportedEffectError,
)
from fidem.outbox import OutboxEvent, WriteOutboxEffect
from fidem.testing import YeildStep, assert_generator
from fidem.transactions import ReadTransaction, ReadTransactionEffectful, WriteTransaction, WriteTransactionEffectful
from tests.effects import (
    TestControlEffect,
    TestDatabaseReadEffect,
    TestDatabaseWriteEffect,
    TestExternalReadEffect,
    TestExternalWriteEffect,
    TestInternalEffect,
)
from tests.messages import TestCommand, TestEvent, TestQuery
from tests.stubs import run
from tests.transactions import read_transaction, write_transaction


class Service:
    pass


class OtherService:
    pass


@dataclass(frozen=True, slots=True)
class HandlerCall:
    context: MessageContext
    command: TestCommand


def not_a_transaction() -> WriteTransactionEffectful[str]:
    return (yield from TestDatabaseWriteEffect())


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
            generator=engine.execute(command, MessageContext(message_id="test_message_id")),
            return_value="command_result",
        )
        assert calls == [HandlerCall(context=MessageContext(message_id="test_message_id"), command=command)]

    @pytest.mark.parametrize(
        argnames="effect",
        argvalues=[TestInternalEffect(), TestExternalReadEffect(), TestDatabaseWriteEffect()],
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
            generator=engine.execute(command, MessageContext(message_id="test_message_id")),
            steps=[YeildStep(make_expected_instruction(effect, command), "effect_result")],
            return_value="command_result effect_result",
        )

    @pytest.mark.parametrize("effect", [TestControlEffect(), TestCommand(), Proceed()])
    def test_raises_unsupported_effect_error_when_handler_yields_unsupported_effect(self, effect: Effect[Any]) -> None:
        def handler() -> Effectful[str, Effect[Any]]:
            yield from effect
            return "unreachable"

        engine = make_engine(handler)

        with pytest.raises(UnsupportedEffectError) as exc_info:
            engine.execute(TestCommand(), MESSAGE_CONTEXT).send(None)

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
            engine.execute(TestCommand(), MESSAGE_CONTEXT).send(None)

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
        execution_plan = engine.execute(TestCommand(), MESSAGE_CONTEXT)
        execution_plan.send(None)

        execution_plan.close()

        assert is_handler_closed

    def test_generates_same_effect_ids_for_same_message_id(self) -> None:
        engine = make_engine(handle_with_three_effects)

        effect_ids1 = collect_effect_ids(engine.execute(TestCommand(), MessageContext(message_id="test_message_id")))
        effect_ids2 = collect_effect_ids(engine.execute(TestCommand(), MessageContext(message_id="test_message_id")))

        assert effect_ids1 == effect_ids2

    def test_generates_unique_effect_ids_within_command(self) -> None:
        engine = make_engine(handle_with_three_effects)

        effect_ids = collect_effect_ids(engine.execute(TestCommand(), MessageContext(message_id="test_message_id")))

        assert len(set(effect_ids)) == 3

    def test_generates_different_effect_ids_for_different_message_ids(self) -> None:
        engine = make_engine(handle_with_three_effects)

        effect_ids1 = collect_effect_ids(engine.execute(TestCommand(), MessageContext(message_id="test_message_id1")))
        effect_ids2 = collect_effect_ids(engine.execute(TestCommand(), MessageContext(message_id="test_message_id2")))

        assert set(effect_ids1).isdisjoint(effect_ids2)

    def test_throws_instruction_error_into_handler(self) -> None:
        def handler() -> Effectful[str, Effect[Any]]:
            try:
                yield from TestInternalEffect()
            except ValueError as exc:
                return str(exc)
            return "unreachable"

        engine = make_engine(handler)
        execution_plan = engine.execute(TestCommand(), MESSAGE_CONTEXT)
        execution_plan.send(None)

        with pytest.raises(StopIteration) as exc_info:
            execution_plan.throw(ValueError("effect_error"))

        assert exc_info.value.value == "effect_error"

    def test_raises_instruction_error_when_handler_does_not_handle_it(self) -> None:
        engine = make_engine(handle_with_three_effects)
        execution_plan = engine.execute(TestCommand(), MESSAGE_CONTEXT)
        execution_plan.send(None)
        effect_error = ValueError("effect_error")

        with pytest.raises(ValueError, match="effect_error") as exc_info:
            execution_plan.throw(effect_error)

        assert exc_info.value is effect_error

    def test_raises_command_not_defined_error_when_command_not_defined(self) -> None:
        engine = OperationEngineImpl(command_defs=[])

        with pytest.raises(CommandNotDefinedError) as exc_info:
            engine.execute(TestCommand(), MESSAGE_CONTEXT).send(None)

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
            engine.execute(TestCommand(), MESSAGE_CONTEXT).send(None)


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
        execution_plan = make_engine(handler, command_def_type=AtomicCommandDef).execute(command, MESSAGE_CONTEXT)

        assert execution_plan.send(None) == make_expected_instruction(read, command)
        instruction = execution_plan.send("read_result")
        assert isinstance(instruction, RunTransactionInstruction)
        assert instruction.transaction is transaction
        assert_generator(
            generator=instruction.plan,
            steps=[YeildStep(make_expected_instruction(write, command), "write_result")],
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
            generator=make_engine(handler, command_def_type=AtomicCommandDef).execute(command, MESSAGE_CONTEXT),
            steps=[
                YeildStep(make_expected_instruction(read, command), "read_result"),
                YeildStep(make_expected_instruction(write, command), "write_result"),
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
    )
    def test_raises_unsupported_effect_error_when_handler_yields_unsupported_effect(self, effect: Effect[Any]) -> None:
        def handler() -> Effectful[object, Effect[Any]]:
            yield from effect
            return write_transaction()

        engine = make_engine(handler, command_def_type=AtomicCommandDef)

        with pytest.raises(UnsupportedEffectError) as exc_info:
            run_plan(engine.execute(TestCommand(), MESSAGE_CONTEXT))

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
            run_plan(engine.execute(TestCommand(), MESSAGE_CONTEXT))

        assert exc_info.value.args == (effect,)

    @pytest.mark.parametrize(
        argnames="result",
        argvalues=["not_a_write", TestDatabaseWriteEffect(), read_transaction(), write_transaction, not_a_transaction],
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
            run_plan(engine.execute(TestCommand(), MESSAGE_CONTEXT))

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
            run_plan(engine.execute(TestCommand(), MESSAGE_CONTEXT))

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

        result = run_plan(engine.execute(TestCommand(), MESSAGE_CONTEXT), raise_effect_error)

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

        result = run_plan(engine.execute(TestCommand(), MESSAGE_CONTEXT), raise_effect_error)

        assert result == "effect_error"

    @pytest.mark.parametrize(argnames="write", argvalues=[TestDatabaseWriteEffect(), TestExternalWriteEffect()])
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
            make_engine(atomic_handler, command_def_type=AtomicCommandDef).execute(TestCommand(), MESSAGE_CONTEXT)
        )
        staged_effect_ids = collect_effect_ids(make_engine(staged_handler).execute(TestCommand(), MESSAGE_CONTEXT))

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
            execution_plan=engine.execute(TestCommand(), MESSAGE_CONTEXT),
            call_effect_handler=lambda instruction: (
                effect_ids.append(instruction.effect_context.effect_id) or "effect_result"
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
            execution_plan=engine.execute(TestCommand(), MESSAGE_CONTEXT),
            call_effect_handler=lambda instruction: effect_types.append(type(instruction.effect)),
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


class TestGuards:
    def test_runs_guards_with_their_effects_before_handler(self) -> None:
        def other(context: MessageContext) -> Effectful[OtherService, QueryEffect]:
            events.append(f"other {context.message_id}")
            assert (yield from internal_effect) == "internal_result"
            return OtherService()

        def service(other: OtherService) -> Effectful[Service, QueryEffect]:
            events.append("service")
            assert (yield from external_effect) == "external_result"
            return Service()

        def handler(service: Service, other: OtherService) -> Effectful[str, Effect[Any]]:
            events.append("handler")
            yield from ()
            return f"{type(service).__name__} {type(other).__name__}"

        events: MutableSequence[str] = []
        internal_effect = TestInternalEffect()
        external_effect = TestExternalReadEffect()
        command = TestCommand()
        engine = make_engine(handler, [GuardDef(service), GuardDef(other)])

        assert_generator(
            generator=engine.execute(command, MessageContext(message_id="test_message_id")),
            steps=[
                YeildStep(make_expected_instruction(internal_effect, command), "internal_result"),
                YeildStep(make_expected_instruction(external_effect, command), "external_result"),
            ],
            return_value="Service OtherService",
        )
        assert events == ["other test_message_id", "service", "handler"]

    def test_runs_guards_for_each_message(self) -> None:
        def service() -> Effectful[Service, QueryEffect]:
            yield from ()
            return Service()

        def handler(service: Service) -> Effectful[Service, Effect[Any]]:
            yield from ()
            return service

        engine = make_engine(handler, [GuardDef(service)])

        first = run(engine.execute(TestCommand(), MESSAGE_CONTEXT))
        second = run(engine.execute(TestCommand(), MESSAGE_CONTEXT))

        assert first is not second

    def test_throws_effect_error_into_guard(self) -> None:
        def service() -> Effectful[Service, QueryEffect]:
            try:
                yield from TestInternalEffect()
            except ValueError:
                return Service()
            raise AssertionError("unreachable")

        def handler(service: Service) -> Effectful[str, Effect[Any]]:
            yield from ()
            return type(service).__name__

        engine = make_engine(handler, [GuardDef(service)])
        execution_plan = engine.execute(TestCommand(), MESSAGE_CONTEXT)
        execution_plan.send(None)

        with pytest.raises(StopIteration) as exc_info:
            execution_plan.throw(ValueError("effect_error"))

        assert exc_info.value.value == "Service"

    def test_raises_guard_error_and_does_not_call_handler(self) -> None:
        def service() -> Effectful[Service, QueryEffect]:
            yield from ()
            raise PermissionError("forbidden")

        def handler(service: Service) -> Effectful[str, Effect[Any]]:
            nonlocal is_handler_called
            is_handler_called = True
            yield from ()
            return "command_result"

        is_handler_called = False
        engine = make_engine(handler, [GuardDef(service)])

        with pytest.raises(PermissionError, match="forbidden"):
            engine.execute(TestCommand(), MESSAGE_CONTEXT).send(None)

        assert not is_handler_called

    @pytest.mark.parametrize(
        argnames="effect",
        argvalues=[
            TestDatabaseWriteEffect(),
            TestControlEffect(),
            TestCommand(),
            TestEvent(),
            Proceed(),
            write_transaction(),
        ],
    )
    def test_raises_unsupported_effect_error_when_guard_yields_not_guard_effect(
        self,
        effect: Effect[Any],
    ) -> None:
        def service() -> Effectful[Service, QueryEffect]:
            nonlocal is_guard_closed
            try:
                yield from effect  # pyright: ignore[reportReturnType] - checks a wrong effect
            except Exception:  # noqa: BLE001 - checks that no error reaches the guard
                return Service()
            finally:
                is_guard_closed = True
            raise AssertionError("unreachable")

        def handler(service: Service) -> Effectful[str, Effect[Any]]:
            yield from ()
            return "command_result"

        is_guard_closed = False
        engine = make_engine(handler, [GuardDef(service)])

        with pytest.raises(UnsupportedEffectError) as exc_info:
            engine.execute(TestCommand(), MESSAGE_CONTEXT).send(None)

        assert exc_info.value.args == (effect,)
        assert is_guard_closed

    def test_generates_same_guard_effect_ids_for_same_message_id(self) -> None:
        engine = make_engine(handle_service_with_three_effects, [GuardDef(make_service_with_two_effects)])

        effect_ids1 = collect_effect_ids(engine.execute(TestCommand(), MessageContext(message_id="test_message_id")))
        effect_ids2 = collect_effect_ids(engine.execute(TestCommand(), MessageContext(message_id="test_message_id")))

        assert effect_ids1 == effect_ids2
        assert len(set(effect_ids1)) == 5

    def test_does_not_shift_handler_effect_ids_when_guard_added(self) -> None:
        handler_effect_ids = collect_effect_ids(
            make_engine(handle_with_three_effects).execute(TestCommand(), MessageContext(message_id="test_message_id"))
        )
        engine = make_engine(handle_service_with_three_effects, [GuardDef(make_service_with_two_effects)])

        effect_ids = collect_effect_ids(engine.execute(TestCommand(), MessageContext(message_id="test_message_id")))

        assert effect_ids[2:] == handler_effect_ids

    def test_does_not_repeat_guard_effect_ids_in_message_whose_id_extends_other_message_id(self) -> None:
        engine = make_engine(
            handler=handle_service_with_three_effects,
            guard_defs=[GuardDef(make_service_with_two_effects)],
        )
        guard_effect_ids = collect_effect_ids(engine.execute(TestCommand(), MessageContext(message_id="m")))[:2]
        message_id = f"m::{Service.__module__}.{Service.__qualname__}"

        effect_ids = collect_effect_ids(engine.execute(TestCommand(), MessageContext(message_id=message_id)))

        assert set(effect_ids).isdisjoint(guard_effect_ids)

    def test_generates_different_guard_effect_ids_for_different_guards(self) -> None:
        def other() -> Effectful[OtherService, QueryEffect]:
            yield from TestInternalEffect()
            yield from TestExternalReadEffect()
            return OtherService()

        def handler(service: Service, other: OtherService) -> Effectful[str, Effect[Any]]:
            yield from ()
            return "command_result"

        engine = make_engine(
            handler=handler,
            guard_defs=[
                GuardDef(make_service_with_two_effects),
                GuardDef(other),
            ],
        )

        effect_ids = collect_effect_ids(engine.execute(TestCommand(), MessageContext(message_id="test_message_id")))

        assert len(set(effect_ids)) == 4


class TestMessageMiddlewares:
    def test_passes_message_context_and_guard_values_into_middleware_and_returns_its_result(self) -> None:
        def service() -> Effectful[Service, QueryEffect]:
            yield from ()
            return Service()

        def middleware(
            context: MessageContext,
            command: TestCommand,
            service: Service,
        ) -> MessageMiddlewareEffectful[str]:
            result = yield from Proceed()
            return f"{context.message_id} {command is message} {type(service).__name__} {result}"

        def handler() -> Effectful[str, Effect[Any]]:
            yield from ()
            return "command_result"

        message = TestCommand()
        engine = make_engine(
            handler=handler,
            guard_defs=[GuardDef(service)],
            middleware_defs=[MessageMiddlewareDef("test", middleware)],
        )

        assert run(engine.execute(message, MESSAGE_CONTEXT)) == "test_message_id True Service command_result"

    def test_yields_call_effect_handler_instruction_when_middleware_yields_write_effect(self) -> None:
        def middleware() -> MessageMiddlewareEffectful[object]:
            result = yield from Proceed()
            yield from effect
            return result

        def handler() -> Effectful[str, Effect[Any]]:
            yield from ()
            return "command_result"

        effect = TestDatabaseWriteEffect()
        command = TestCommand()
        engine = make_engine(handler, middleware_defs=[MessageMiddlewareDef("test", middleware)])

        assert_generator(
            generator=engine.execute(command, MessageContext(message_id="test_message_id")),
            steps=[YeildStep(make_expected_instruction(effect, command), "effect_result")],
            return_value="command_result",
        )

    def test_runs_guards_then_middlewares_from_outer_one_then_handler(self) -> None:
        def service() -> Effectful[Service, QueryEffect]:
            events.append("guard")
            yield from ()
            return Service()

        def outer() -> MessageMiddlewareEffectful[object]:
            events.append("outer before")
            result = yield from Proceed()
            events.append("outer after")
            return result

        def inner(service: Service) -> MessageMiddlewareEffectful[object]:
            events.append("inner before")
            result = yield from Proceed()
            events.append("inner after")
            return result

        def handler() -> Effectful[str, Effect[Any]]:
            events.append("handler")
            yield from ()
            return "command_result"

        events: MutableSequence[str] = []
        engine = make_engine(
            handler=handler,
            guard_defs=[GuardDef(service)],
            middleware_defs=[MessageMiddlewareDef("outer", outer), MessageMiddlewareDef("inner", inner)],
        )

        assert run(engine.execute(TestCommand(), MESSAGE_CONTEXT)) == "command_result"
        assert events == ["guard", "outer before", "inner before", "handler", "inner after", "outer after"]

    def test_does_not_call_handler_when_middleware_does_not_proceed(self) -> None:
        def middleware() -> MessageMiddlewareEffectful[str]:
            yield from ()
            return "cached_result"

        def handler() -> Effectful[str, Effect[Any]]:
            nonlocal is_handler_called
            is_handler_called = True
            yield from ()
            return "command_result"

        is_handler_called = False
        engine = make_engine(handler, middleware_defs=[MessageMiddlewareDef("test", middleware)])

        assert run(engine.execute(TestCommand(), MESSAGE_CONTEXT)) == "cached_result"
        assert not is_handler_called

    def test_runs_inner_part_anew_with_same_effect_ids_on_each_proceed_and_guards_once(self) -> None:
        def service() -> Effectful[Service, QueryEffect]:
            events.append("guard")
            yield from ()
            return Service()

        def middleware() -> MessageMiddlewareEffectful[object]:
            yield from Proceed()
            return (yield from Proceed())

        def handler(service: Service) -> Effectful[str, Effect[Any]]:
            events.append("handler")
            return (yield from handle_with_three_effects())

        events: MutableSequence[str] = []
        engine = make_engine(
            handler=handler,
            guard_defs=[GuardDef(service)],
            middleware_defs=[MessageMiddlewareDef("test", middleware)],
        )

        effect_ids = collect_effect_ids(engine.execute(TestCommand(), MESSAGE_CONTEXT))

        assert events == ["guard", "handler", "handler"]
        assert effect_ids[:3] == effect_ids[3:]

    def test_does_not_shift_handler_effect_ids_when_middleware_added(self) -> None:
        handler_effect_ids = collect_effect_ids(
            make_engine(handle_with_three_effects).execute(TestCommand(), MESSAGE_CONTEXT)
        )
        engine = make_engine(
            handler=handle_with_three_effects,
            middleware_defs=[MessageMiddlewareDef("test", proceed_with_three_effects)],
        )

        effect_ids = collect_effect_ids(engine.execute(TestCommand(), MESSAGE_CONTEXT))

        assert effect_ids[3:] == handler_effect_ids
        assert set(effect_ids[:3]).isdisjoint(handler_effect_ids)

    def test_generates_different_effect_ids_for_middlewares_with_different_names(self) -> None:
        engine = make_engine(
            handler=handle_with_three_effects,
            middleware_defs=[
                MessageMiddlewareDef("first", proceed_with_three_effects),
                MessageMiddlewareDef("second", proceed_with_three_effects),
            ],
        )

        effect_ids = collect_effect_ids(engine.execute(TestCommand(), MESSAGE_CONTEXT))

        assert len(set(effect_ids)) == 9

    def test_throws_inner_error_into_middleware_at_proceed(self) -> None:
        def middleware() -> MessageMiddlewareEffectful[str]:
            try:
                yield from Proceed()
            except ValueError as exc:
                return f"caught {exc}"
            return "unreachable"

        def handler() -> Effectful[str, Effect[Any]]:
            yield from TestInternalEffect()
            return "unreachable"

        engine = make_engine(handler, middleware_defs=[MessageMiddlewareDef("test", middleware)])
        execution_plan = engine.execute(TestCommand(), MESSAGE_CONTEXT)
        execution_plan.send(None)

        with pytest.raises(StopIteration) as exc_info:
            execution_plan.throw(ValueError("effect_error"))

        assert exc_info.value.value == "caught effect_error"

    def test_raises_unsupported_effect_error_of_inner_part_without_throwing_it_into_middleware(self) -> None:
        def middleware() -> MessageMiddlewareEffectful[str]:
            nonlocal is_middleware_closed
            try:
                yield from Proceed()
            except Exception:  # noqa: BLE001 - checks that no error reaches the middleware
                return "swallowed"
            finally:
                is_middleware_closed = True
            return "unreachable"

        def handler() -> Effectful[str, Effect[Any]]:
            yield from effect
            return "unreachable"

        effect = TestControlEffect()
        is_middleware_closed = False
        engine = make_engine(handler, middleware_defs=[MessageMiddlewareDef("test", middleware)])

        with pytest.raises(UnsupportedEffectError) as exc_info:
            engine.execute(TestCommand(), MESSAGE_CONTEXT).send(None)

        assert exc_info.value.args == (effect,)
        assert is_middleware_closed

    def test_throws_unsupported_effect_error_of_effect_handler_through_handler_into_middleware(self) -> None:
        def middleware() -> MessageMiddlewareEffectful[str]:
            try:
                yield from Proceed()
            except UnsupportedEffectError as exc:
                return f"caught {exc.args == (effect,)}"
            return "unreachable"

        def handler() -> Effectful[str, Effect[Any]]:
            yield from TestInternalEffect()
            return "unreachable"

        effect = TestControlEffect()
        engine = make_engine(handler, middleware_defs=[MessageMiddlewareDef("test", middleware)])
        execution_plan = engine.execute(TestCommand(), MESSAGE_CONTEXT)
        execution_plan.send(None)

        with pytest.raises(StopIteration) as exc_info:
            execution_plan.throw(UnsupportedEffectError(effect))

        assert exc_info.value.value == "caught True"

    @pytest.mark.parametrize("effect", [TestControlEffect(), TestEvent(), TestCommand()])
    def test_raises_unsupported_effect_error_when_middleware_yields_unsupported_effect(
        self,
        effect: Effect[Any],
    ) -> None:
        def middleware() -> MessageMiddlewareEffectful[str]:
            yield from effect  # pyright: ignore[reportReturnType] - checks a wrong effect
            return "unreachable"

        engine = make_engine(handle_with_three_effects, middleware_defs=[MessageMiddlewareDef("test", middleware)])

        with pytest.raises(UnsupportedEffectError) as exc_info:
            engine.execute(TestCommand(), MESSAGE_CONTEXT).send(None)

        assert exc_info.value.args == (effect,)

    def test_closes_all_generators_of_chain_when_execution_plan_closed(self) -> None:
        def middleware() -> MessageMiddlewareEffectful[object]:
            try:
                return (yield from Proceed())
            finally:
                events.append("middleware closed")

        def handler() -> Effectful[str, Effect[Any]]:
            try:
                yield from TestInternalEffect()
            finally:
                events.append("handler closed")
            return "command_result"

        events: MutableSequence[str] = []
        engine = make_engine(handler, middleware_defs=[MessageMiddlewareDef("test", middleware)])
        execution_plan = engine.execute(TestCommand(), MESSAGE_CONTEXT)
        execution_plan.send(None)

        execution_plan.close()

        assert events == ["handler closed", "middleware closed"]

    @pytest.mark.parametrize(
        argnames=("command_middleware_defs", "expected_result"),
        argvalues=[(None, "engine command_result"), ((), "command_result")],
    )
    def test_uses_engine_middlewares_unless_command_def_replaces_them(
        self,
        command_middleware_defs: Sequence[MessageMiddlewareDef] | None,
        expected_result: str,
    ) -> None:
        def engine_middleware() -> MessageMiddlewareEffectful[str]:
            return f"engine {(yield from Proceed())}"

        engine = make_engine(
            handler=handle_without_effects,
            middleware_defs=[MessageMiddlewareDef("engine", engine_middleware)],
            command_middleware_defs=command_middleware_defs,
        )

        assert run(engine.execute(TestCommand(), MESSAGE_CONTEXT)) == expected_result

    @pytest.mark.parametrize("is_command_list", [False, True])
    def test_raises_duplicate_message_middleware_def_error_when_middlewares_have_same_name(
        self,
        is_command_list: bool,
    ) -> None:
        middleware_defs = [MessageMiddlewareDef("test", proceed), MessageMiddlewareDef("test", proceed)]
        all_middleware_defs = [*middleware_defs, MessageMiddlewareDef("other", proceed)]

        with pytest.raises(DuplicateMessageMiddlewareDefError) as exc_info:
            make_engine(
                handler=handle_without_effects,
                middleware_defs=() if is_command_list else all_middleware_defs,
                command_middleware_defs=all_middleware_defs if is_command_list else None,
            )

        assert exc_info.value.args == (middleware_defs,)

    def test_raises_unsupported_function_error_when_middleware_is_not_generator_function(self) -> None:
        def middleware() -> str:
            return "middleware_result"

        with pytest.raises(UnsupportedFunctionError) as exc_info:
            make_engine(
                handler=handle_without_effects,
                middleware_defs=[MessageMiddlewareDef("test", middleware)],  # pyright: ignore[reportArgumentType] - checks a wrong middleware
            )

        assert exc_info.value.args == (middleware,)

    @pytest.mark.parametrize("command_def_type", [StagedCommandDef, AtomicCommandDef])
    def test_raises_dependency_not_found_error_when_middleware_parameter_has_no_guard(
        self,
        command_def_type: type[StagedCommandDef[Any]] | type[AtomicCommandDef[Any]],
    ) -> None:
        def middleware(service: Service) -> MessageMiddlewareEffectful[object]:
            return (yield from Proceed())

        with pytest.raises(DependencyNotFoundError) as exc_info:
            OperationEngineImpl(
                command_defs=[command_def_type("test", TestCommand, handle_without_effects)],  # pyright: ignore[reportArgumentType] - the handler is only checked, not run
                middleware_defs=[MessageMiddlewareDef("test", middleware)],
            )

        assert exc_info.value.args == (middleware, "service", Service)


class TestExecuteQuery:
    def test_passes_query_message_context_and_guard_values_into_handler_and_returns_its_result(self) -> None:
        def service() -> Effectful[Service, QueryEffect]:
            yield from ()
            return Service()

        def handler(context: MessageContext, query: TestQuery, service: Service) -> QueryEffectful[str]:
            calls.append((context, query, service))
            effect_result = yield from read
            return f"query_result {effect_result}"

        calls: MutableSequence[tuple[MessageContext, TestQuery, Service]] = []
        read = TestDatabaseReadEffect()
        query = TestQuery()
        engine = make_query_engine(handler, guard_defs=[GuardDef(service)])

        assert_generator(
            generator=engine.query(query, MESSAGE_CONTEXT),
            steps=[YeildStep(make_expected_instruction(read, query), "effect_result")],
            return_value="query_result effect_result",
        )
        assert len(calls) == 1
        assert calls[0][:2] == (MESSAGE_CONTEXT, query)

    @pytest.mark.parametrize(
        argnames="effect",
        argvalues=[
            TestDatabaseWriteEffect(),
            TestExternalWriteEffect(),
            TestControlEffect(),
            TestCommand(),
            TestQuery(),
            TestEvent(),
            Proceed(),
            write_transaction(),
        ],
    )
    def test_raises_unsupported_effect_error_when_handler_yields_unsupported_effect(self, effect: Effect[Any]) -> None:
        def handler() -> Effectful[str, Effect[Any]]:
            yield from effect
            return "unreachable"

        engine = make_query_engine(handler)

        with pytest.raises(UnsupportedEffectError) as exc_info:
            run_plan(engine.query(TestQuery(), MESSAGE_CONTEXT))

        assert exc_info.value.args == (effect,)

    @pytest.mark.parametrize(
        argnames=("query_middleware_defs", "expected_result"),
        argvalues=[(None, "engine query_result"), ((), "query_result")],
    )
    def test_uses_engine_middlewares_unless_query_def_replaces_them(
        self,
        query_middleware_defs: Sequence[MessageMiddlewareDef] | None,
        expected_result: str,
    ) -> None:
        def engine_middleware() -> MessageMiddlewareEffectful[str]:
            return f"engine {(yield from Proceed())}"

        def handler() -> QueryEffectful[str]:
            yield from ()
            return "query_result"

        engine = make_query_engine(
            handler=handler,
            middleware_defs=[MessageMiddlewareDef("engine", engine_middleware)],
            query_middleware_defs=query_middleware_defs,
        )

        assert run(engine.query(TestQuery(), MESSAGE_CONTEXT)) == expected_result

    def test_lets_middleware_yield_write_effects(self) -> None:
        def middleware() -> MessageMiddlewareEffectful[object]:
            result = yield from Proceed()
            yield from TestDatabaseWriteEffect()
            yield from write_transaction()
            return result

        def handler() -> QueryEffectful[str]:
            yield from ()
            return "query_result"

        effect_types: MutableSequence[type[Effect[Any]]] = []
        engine = make_query_engine(handler, middleware_defs=[MessageMiddlewareDef("cache", middleware)])

        result = run_plan(
            execution_plan=engine.query(TestQuery(), MESSAGE_CONTEXT),
            call_effect_handler=lambda instruction: effect_types.append(type(instruction.effect)),
        )

        assert result == "query_result"
        assert effect_types == [TestDatabaseWriteEffect, TestDatabaseWriteEffect]

    def test_raises_query_not_defined_error_when_query_not_defined(self) -> None:
        class OtherQuery(TestQuery):
            pass

        engine = make_query_engine(handle_query)

        with pytest.raises(QueryNotDefinedError) as exc_info:
            engine.query(OtherQuery(), MESSAGE_CONTEXT).send(None)

        assert exc_info.value.args == (OtherQuery,)

    def test_does_not_execute_query_as_command(self) -> None:
        engine = make_query_engine(handle_query)

        with pytest.raises(CommandNotDefinedError):
            engine.execute(TestQuery(), MESSAGE_CONTEXT).send(None)  # pyright: ignore[reportArgumentType] - checks a query passed as a command

    def test_raises_duplicate_query_def_error_when_define_queries_with_same_type(self) -> None:
        query_defs = [QueryDef("first", TestQuery, handle_query), QueryDef("second", TestQuery, handle_query)]

        with pytest.raises(DuplicateQueryDefError) as exc_info:
            OperationEngineImpl(query_defs=query_defs)

        assert exc_info.value.args == (query_defs,)


class TestTransactions:
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
        execution_plan = make_query_engine(handler).query(query, MESSAGE_CONTEXT)

        instruction = execution_plan.send(None)
        assert isinstance(instruction, RunTransactionInstruction)
        assert instruction.transaction is transaction
        assert_generator(
            generator=instruction.plan,
            steps=[
                YeildStep(make_expected_instruction(first_read, query), "first"),
                YeildStep(make_expected_instruction(second_read, query), "second"),
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

        run_plan(engine.execute(TestCommand(), MESSAGE_CONTEXT), on_transaction=transactions.append)

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
            execution_plan=make_engine(handler).execute(TestCommand(), MESSAGE_CONTEXT),
            call_effect_handler=lambda instruction: effect_types.append(type(instruction.effect)) or "effect_result",
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
            execution_plan=make_engine(handler).execute(TestCommand(), MESSAGE_CONTEXT),
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
            run_plan(make_engine(handler).execute(TestCommand(), MESSAGE_CONTEXT))

        assert exc_info.value.args == (effect,)

    def test_throws_transaction_error_into_staged_handler(self) -> None:
        def handler() -> Effectful[str, Effect[Any]]:
            try:
                yield from write_transaction()
            except ValueError as exc:
                return f"caught {exc}"
            return "unreachable"

        execution_plan = make_engine(handler).execute(TestCommand(), MESSAGE_CONTEXT)
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

        result = run_plan(make_engine(handler).execute(TestCommand(), MESSAGE_CONTEXT))

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
            make_engine(transaction_handler).execute(TestCommand(), MESSAGE_CONTEXT)
        )
        plain_effect_ids = collect_effect_ids(make_engine(plain_handler).execute(TestCommand(), MESSAGE_CONTEXT))

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


class OtherEvent(Event):
    pass


class UndefinedEvent(Event):
    pass


def handle_event() -> StagedEffectful[None]:
    yield from ()


@WriteTransaction
def save_nothing() -> WriteTransactionEffectful[None]:
    yield from ()


def handle_event_atomically() -> AtomicEffectful[None]:
    yield from ()
    return save_nothing()


EVENT_DEFS: Sequence[EventDef[Any]] = [
    EventDef(
        name="test.event",
        event_type=TestEvent,
        version=2,
        handler_defs=[
            StagedEventHandlerDef("first", handle_event),
            AtomicEventHandlerDef("second", handle_event_atomically),
        ],
    ),
    EventDef(name="test.other", event_type=OtherEvent),
]


@WriteTransaction
def publish_in_transaction(event: Event) -> WriteTransactionEffectful[str, Event]:
    yield from event
    return (yield from TestDatabaseWriteEffect())


def publish_other_event() -> StagedEffectful[None, OtherEvent]:
    yield from OtherEvent()


def publish_other_event_atomically() -> AtomicEffectful[str, Event]:
    yield from ()
    return publish_in_transaction(OtherEvent())


def publish_event_in_transaction() -> StagedEffectful[str, Event]:
    return (yield from publish_in_transaction(TestEvent()))


def publish_event_atomically() -> AtomicEffectful[str, Event]:
    yield from ()
    return publish_in_transaction(TestEvent())


def publish_undefined_event() -> StagedEffectful[str, Event]:
    try:
        yield from UndefinedEvent()
    except Exception:  # noqa: BLE001 - checks that no error reaches the handler
        return "swallowed"
    return "unreachable"


@WriteTransaction
def publish_undefined_event_in_transaction() -> WriteTransactionEffectful[str, Event]:
    try:
        return (yield from publish_in_transaction.function(UndefinedEvent()))
    except Exception:  # noqa: BLE001 - checks that no error reaches the transaction
        return "swallowed"


def publish_undefined_event_atomically() -> AtomicEffectful[str, Event]:
    yield from ()
    return publish_undefined_event_in_transaction()


class TestEvents:
    def test_writes_events_of_atomic_transaction_to_outbox_at_end_of_transaction(self) -> None:
        def handler() -> AtomicEffectful[str, Event]:
            yield from ()
            return publish_in_transaction(event)

        event = TestEvent()
        engine = make_engine(handler, command_def_type=AtomicCommandDef, event_defs=EVENT_DEFS)
        execution_plan = engine.execute(TestCommand(), MESSAGE_CONTEXT)

        instruction = execution_plan.send(None)
        assert isinstance(instruction, RunTransactionInstruction)
        write_instruction = instruction.plan.send(None)
        assert isinstance(write_instruction, CallEffectHandlerInstruction)
        assert isinstance(write_instruction.effect, TestDatabaseWriteEffect)
        outbox_instruction = instruction.plan.send("write_result")
        assert isinstance(outbox_instruction, CallEffectHandlerInstruction)
        assert isinstance(outbox_instruction.effect, WriteOutboxEffect)
        assert [outbox_event.event for outbox_event in outbox_instruction.effect.events] == [event]
        with pytest.raises(StopIteration) as exc_info:
            instruction.plan.send(None)
        assert exc_info.value.value == "write_result"

    def test_makes_outbox_event_with_its_definition_and_handlers(self) -> None:
        event = TestEvent()

        effects = collect_effects(plan_publishing(event))

        assert len(effects) == 1
        assert isinstance(effects[0].effect, WriteOutboxEffect)
        outbox_event = effects[0].effect.events[0]
        assert effects[0].effect.events == (
            OutboxEvent(
                event_id=outbox_event.event_id,
                source_message_id="test_message_id",
                name="test.event",
                version=2,
                handlers=("first", "second"),
                event=event,
            ),
        )
        assert outbox_event.handler_message_id("first") == f"{outbox_event.event_id}:first"

    def test_takes_event_id_from_effect_ids_in_place_of_event(self) -> None:
        def handler() -> StagedEffectful[None, TestEvent]:
            yield from TestInternalEffect()
            yield from TestEvent()
            yield from TestInternalEffect()

        def plain_handler() -> Effectful[None, Effect[Any]]:
            yield from TestInternalEffect()
            yield from TestInternalEffect()
            yield from TestInternalEffect()

        effects = collect_effects(make_engine(handler, event_defs=EVENT_DEFS).execute(TestCommand(), MESSAGE_CONTEXT))
        plain_effect_ids = collect_effect_ids(make_engine(plain_handler).execute(TestCommand(), MESSAGE_CONTEXT))

        outbox_effect = effects[2].effect
        assert isinstance(outbox_effect, WriteOutboxEffect)
        assert [effects[0].effect_context.effect_id, outbox_effect.events[0].event_id] == plain_effect_ids[:2]
        assert effects[1].effect_context.effect_id == plain_effect_ids[2]

    def test_generates_same_event_ids_for_same_message_id(self) -> None:
        first = collect_effects(plan_publishing(TestEvent()))
        second = collect_effects(plan_publishing(TestEvent()))

        assert [effect.effect_context for effect in first] == [effect.effect_context for effect in second]
        assert isinstance(first[0].effect, WriteOutboxEffect)
        assert isinstance(second[0].effect, WriteOutboxEffect)
        assert first[0].effect.events[0].event_id == second[0].effect.events[0].event_id

    def test_writes_events_of_transaction_in_one_effect_and_skips_events_without_handlers(self) -> None:
        @WriteTransaction
        def transaction() -> WriteTransactionEffectful[None, Event]:
            yield from first
            yield from OtherEvent()
            yield from second

        def handler() -> AtomicEffectful[None, Event]:
            yield from ()
            return transaction()

        first = TestEvent()
        second = TestEvent()
        engine = make_engine(handler, command_def_type=AtomicCommandDef, event_defs=EVENT_DEFS)

        effects = collect_effects(engine.execute(TestCommand(), MESSAGE_CONTEXT))

        assert len(effects) == 1
        assert isinstance(effects[0].effect, WriteOutboxEffect)
        assert [outbox_event.event for outbox_event in effects[0].effect.events] == [first, second]

    @pytest.mark.parametrize(
        argnames=("command_def_type", "handler"),
        argvalues=[(StagedCommandDef, publish_other_event), (AtomicCommandDef, publish_other_event_atomically)],
    )
    def test_does_not_write_outbox_when_no_event_has_handlers(
        self,
        command_def_type: type[StagedCommandDef[Any]] | type[AtomicCommandDef[Any]],
        handler: StagedHandler[Any, Any],
    ) -> None:
        engine = make_engine(handler, command_def_type=command_def_type, event_defs=EVENT_DEFS)

        effects = collect_effects(engine.execute(TestCommand(), MESSAGE_CONTEXT))

        assert not any(isinstance(effect.effect, WriteOutboxEffect) for effect in effects)

    def test_writes_events_of_staged_handler_after_it_returns_outside_its_transactions(self) -> None:
        def handler() -> StagedEffectful[str, Event]:
            transaction_result = yield from publish_in_transaction(first)
            published = yield from second
            return f"{transaction_result} {published}"

        first = TestEvent()
        second = TestEvent()
        execution_plan = make_engine(handler, event_defs=EVENT_DEFS).execute(TestCommand(), MESSAGE_CONTEXT)

        instruction = execution_plan.send(None)
        assert isinstance(instruction, RunTransactionInstruction)
        write_instruction = instruction.plan.send(None)
        assert isinstance(write_instruction, CallEffectHandlerInstruction)
        assert isinstance(write_instruction.effect, TestDatabaseWriteEffect)
        with pytest.raises(StopIteration):
            instruction.plan.send("write_result")
        outbox_instruction = execution_plan.send("write_result")
        assert isinstance(outbox_instruction, CallEffectHandlerInstruction)
        assert isinstance(outbox_instruction.effect, WriteOutboxEffect)
        assert [outbox_event.event for outbox_event in outbox_instruction.effect.events] == [first, second]
        with pytest.raises(StopIteration) as exc_info:
            execution_plan.send(None)
        assert exc_info.value.value == "write_result None"

    def test_drops_events_of_failed_run_of_staged_handler(self) -> None:
        def handler() -> StagedEffectful[None, TestEvent]:
            nonlocal runs
            runs += 1
            yield from failed_event if runs == 1 else event
            if runs == 1:
                raise ValueError("handler_error")

        runs = 0
        failed_event = TestEvent()
        event = TestEvent()
        engine = make_engine(
            handler=handler,
            middleware_defs=[MessageMiddlewareDef("retry", retry_once)],
            event_defs=EVENT_DEFS,
        )

        effects = collect_effects(engine.execute(TestCommand(), MESSAGE_CONTEXT))

        assert len(effects) == 1
        assert isinstance(effects[0].effect, WriteOutboxEffect)
        assert [outbox_event.event for outbox_event in effects[0].effect.events] == [event]

    @pytest.mark.parametrize(
        argnames=("command_def_type", "handler"),
        argvalues=[(StagedCommandDef, publish_event_in_transaction), (AtomicCommandDef, publish_event_atomically)],
    )
    def test_throws_outbox_error_into_middleware_so_that_it_can_retry(
        self,
        command_def_type: type[StagedCommandDef[Any]] | type[AtomicCommandDef[Any]],
        handler: StagedHandler[Any, Any],
    ) -> None:
        def write_outbox_once(instruction: CallEffectHandlerInstruction[Any]) -> object:
            if isinstance(instruction.effect, WriteOutboxEffect):
                outbox_effects.append(instruction.effect)
                if len(outbox_effects) == 1:
                    raise ValueError("outbox_error")
            return "effect_result"

        outbox_effects: MutableSequence[WriteOutboxEffect] = []
        engine = make_engine(
            handler=handler,
            middleware_defs=[MessageMiddlewareDef("retry", retry_once)],
            command_def_type=command_def_type,
            event_defs=EVENT_DEFS,
        )

        result = run_plan(engine.execute(TestCommand(), MESSAGE_CONTEXT), write_outbox_once)

        assert result == "effect_result"
        assert len(outbox_effects) == 2
        assert outbox_effects[0].events[0].event_id == outbox_effects[1].events[0].event_id

    @pytest.mark.parametrize(
        argnames=("command_def_type", "handler"),
        argvalues=[(StagedCommandDef, publish_undefined_event), (AtomicCommandDef, publish_undefined_event_atomically)],
    )
    def test_raises_event_not_defined_error_without_throwing_it_into_handler(
        self,
        command_def_type: type[StagedCommandDef[Any]] | type[AtomicCommandDef[Any]],
        handler: StagedHandler[Any, Any],
    ) -> None:
        engine = make_engine(handler, command_def_type=command_def_type, event_defs=EVENT_DEFS)

        with pytest.raises(EventNotDefinedError) as exc_info:
            run_plan(engine.execute(TestCommand(), MESSAGE_CONTEXT))

        assert exc_info.value.args == (UndefinedEvent,)

    def test_raises_unsupported_effect_error_when_transaction_of_middleware_publishes_event(self) -> None:
        def middleware() -> MessageMiddlewareEffectful[object]:
            yield from publish_in_transaction(event)  # pyright: ignore[reportReturnType] - checks what the middleware type forbids
            return (yield from Proceed())

        event = TestEvent()
        engine = make_engine(
            handler=handle_without_effects,
            middleware_defs=[MessageMiddlewareDef("test", middleware)],
            event_defs=EVENT_DEFS,
        )

        with pytest.raises(UnsupportedEffectError) as exc_info:
            run_plan(engine.execute(TestCommand(), MESSAGE_CONTEXT))

        assert exc_info.value.args == (event,)


class TestEventDefinition:
    @pytest.mark.parametrize("same", ["type", "name"])
    def test_raises_duplicate_event_def_error_when_events_have_same_type_or_name(self, same: str) -> None:
        event_defs = [
            EventDef("first", TestEvent),
            EventDef("first" if same == "name" else "second", OtherEvent if same == "name" else TestEvent),
        ]

        with pytest.raises(DuplicateEventDefError) as exc_info:
            OperationEngineImpl(event_defs=event_defs)

        assert exc_info.value.args == (event_defs,)

    def test_raises_duplicate_event_handler_def_error_when_handlers_of_event_have_same_name(self) -> None:
        handler_defs = [StagedEventHandlerDef("test", handle_event), StagedEventHandlerDef("test", handle_event)]

        with pytest.raises(DuplicateEventHandlerDefError) as exc_info:
            OperationEngineImpl(event_defs=[EventDef("test", TestEvent, handler_defs=handler_defs)])

        assert exc_info.value.args == (handler_defs,)

    def test_allows_same_handler_name_in_different_events(self) -> None:
        OperationEngineImpl(
            event_defs=[
                EventDef("test", TestEvent, handler_defs=[StagedEventHandlerDef("handler", handle_event)]),
                EventDef("other", OtherEvent, handler_defs=[StagedEventHandlerDef("handler", handle_event)]),
            ]
        )

    def test_raises_unsupported_function_error_when_event_handler_is_not_generator_function(self) -> None:
        def handler(event: TestEvent) -> None:
            return None

        with pytest.raises(UnsupportedFunctionError) as exc_info:
            OperationEngineImpl(
                event_defs=[
                    EventDef("test", TestEvent, handler_defs=[StagedEventHandlerDef("test", handler)]),  # pyright: ignore[reportArgumentType] - checks a wrong handler
                ]
            )

        assert exc_info.value.args == (handler,)

    def test_binds_parameters_of_event_handler_and_its_middlewares_to_guards_of_event(self) -> None:
        def service(event: TestEvent) -> Effectful[Service, QueryEffect]:
            yield from ()
            return Service()

        def middleware(other: OtherService) -> MessageMiddlewareEffectful[object]:
            return (yield from Proceed())

        def handler(event: TestEvent, service: Service) -> StagedEffectful[None]:
            yield from ()

        with pytest.raises(DependencyNotFoundError) as exc_info:
            OperationEngineImpl(
                event_defs=[
                    EventDef(
                        name="test",
                        event_type=TestEvent,
                        handler_defs=[
                            StagedEventHandlerDef("ok", handler),
                            StagedEventHandlerDef("test", handler, [MessageMiddlewareDef("test", middleware)]),
                        ],
                    ),
                ],
                guard_defs=[GuardDef(service)],
            )

        assert exc_info.value.args == (middleware, "other", OtherService)


def plan_publishing(event: Event) -> ExecutionPlan[Any]:
    def handler() -> StagedEffectful[None, Event]:
        yield from event

    return make_engine(handler, event_defs=EVENT_DEFS).execute(TestCommand(), MESSAGE_CONTEXT)


def collect_effects(execution_plan: ExecutionPlan[Any]) -> Sequence[CallEffectHandlerInstruction[Any]]:
    instructions: MutableSequence[CallEffectHandlerInstruction[Any]] = []
    run_plan(execution_plan, instructions.append)
    return instructions


def make_engine(
    handler: StagedHandler[Any, Any],
    guard_defs: Collection[GuardDef[Any]] = (),
    middleware_defs: Sequence[MessageMiddlewareDef] = (),
    command_middleware_defs: Sequence[MessageMiddlewareDef] | None = None,
    command_def_type: type[StagedCommandDef[Any]] | type[AtomicCommandDef[Any]] = StagedCommandDef,
    event_defs: Collection[EventDef[Any]] = (),
) -> OperationEngineImpl:
    return OperationEngineImpl(
        command_defs=[
            command_def_type(
                name="test",
                message_type=TestCommand,
                handler=handler,  # pyright: ignore[reportArgumentType] - atomic tests also check what the handler type forbids
                middleware_defs=command_middleware_defs,
            ),
        ],
        event_defs=event_defs,
        guard_defs=guard_defs,
        middleware_defs=middleware_defs,
    )


def make_query_engine(
    handler: StagedHandler[Any, Any],
    guard_defs: Collection[GuardDef[Any]] = (),
    middleware_defs: Sequence[MessageMiddlewareDef] = (),
    query_middleware_defs: Sequence[MessageMiddlewareDef] | None = None,
) -> OperationEngineImpl:
    return OperationEngineImpl(
        query_defs=[
            QueryDef(
                name="test",
                message_type=TestQuery,
                handler=handler,  # pyright: ignore[reportArgumentType] - query tests also check what the handler type forbids
                middleware_defs=query_middleware_defs,
            ),
        ],
        guard_defs=guard_defs,
        middleware_defs=middleware_defs,
    )


def proceed() -> MessageMiddlewareEffectful[object]:
    return (yield from Proceed())


def retry_once() -> MessageMiddlewareEffectful[object]:
    try:
        return (yield from Proceed())
    except ValueError:
        return (yield from Proceed())


def proceed_with_three_effects() -> MessageMiddlewareEffectful[object]:
    yield from handle_with_three_effects()
    return (yield from Proceed())


def handle_query() -> QueryEffectful[str]:
    yield from ()
    return "query_result"


def handle_without_effects() -> Effectful[str, Effect[Any]]:
    yield from ()
    return "command_result"


def handle_with_three_effects() -> Effectful[
    str, TestInternalEffect | TestExternalReadEffect | TestDatabaseWriteEffect
]:
    yield from TestInternalEffect()
    yield from TestExternalReadEffect()
    yield from TestDatabaseWriteEffect()
    return "command_result"


def handle_service_with_three_effects(service: Service) -> Effectful[str, Effect[Any]]:
    return (yield from handle_with_three_effects())


def make_service_with_two_effects() -> Effectful[Service, QueryEffect]:
    yield from TestInternalEffect()
    yield from TestExternalReadEffect()
    return Service()


def collect_effect_ids(execution_plan: ExecutionPlan[Any]) -> Sequence[str]:
    return [instruction.effect_context.effect_id for instruction in collect_effects(execution_plan)]


def run_plan(
    execution_plan: ExecutionPlan[Any],
    call_effect_handler: Callable[[CallEffectHandlerInstruction[Any]], object] = lambda instruction: None,
    on_transaction: Callable[[object], object] = lambda transaction: None,
) -> object:
    """Runs the plan like an environment, the functions play the effect handlers and the transactions."""
    try:
        instruction = execution_plan.send(None)
        while True:
            try:
                if isinstance(instruction, RunTransactionInstruction):
                    on_transaction(instruction.transaction)
                    result = run_plan(instruction.plan, call_effect_handler, on_transaction)
                else:
                    assert isinstance(instruction, CallEffectHandlerInstruction)
                    result = call_effect_handler(instruction)
            except Exception as exc:  # noqa: BLE001 - the plan decides how to handle the error
                instruction = execution_plan.throw(exc)
            else:
                instruction = execution_plan.send(result)
    except StopIteration as exc:
        return exc.value


def raise_effect_error(instruction: CallEffectHandlerInstruction[Any]) -> object:
    raise ValueError("effect_error")


def make_expected_instruction(effect: HandledEffect[Any], message: Message[Any]) -> CallEffectHandlerInstruction[Any]:
    return CallEffectHandlerInstruction(
        message_context=MessageContext(message_id="test_message_id"),
        message=message,
        effect_context=EffectContext(effect_id=ANY),
        effect=effect,
    )


MESSAGE_CONTEXT = MessageContext(message_id="test_message_id")
