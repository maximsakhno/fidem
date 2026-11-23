from collections.abc import Collection, MutableSequence, Sequence
from dataclasses import dataclass
from typing import Any
from unittest.mock import ANY

import pytest

from fidem.di.core import DependencyNotFoundError, UnsupportedFunctionError
from fidem.effects import DatabaseEffect, DatabaseReadEffect, Effect, EffectContext, Effectful
from fidem.guards import GuardDef, GuardEffect
from fidem.messages import Command, Event, MessageContext, StagedHandler, Transaction
from fidem.operation_engine import (
    AtomicCommandDef,
    CallEffectHandlerInstruction,
    CommandNotDefinedError,
    DuplicateCommandDefError,
    ExecutionMode,
    ExecutionPlan,
    HandledEffect,
    OperationEngineImpl,
    StagedCommandDef,
    UnsupportedEffectError,
)
from fidem.testing import YeildStep, assert_generator
from tests.effects import TestControlEffect, TestDatabaseWriteEffect, TestExternalReadEffect, TestInternalEffect
from tests.messages import TestCommand, TestEvent
from tests.stubs import run


class Service:
    pass


class OtherService:
    pass


@dataclass(frozen=True, slots=True)
class HandlerCall:
    context: MessageContext
    command: TestCommand


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

    @pytest.mark.parametrize("effect", [TestControlEffect(), TestEvent(), TestCommand()])
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

    def test_raises_not_implemented_error_when_command_is_atomic(self) -> None:
        def transaction() -> Effectful[str, DatabaseEffect[Any] | Event]:
            yield from ()
            return "command_result"

        def handler() -> Effectful[Transaction[str, Event], DatabaseReadEffect[Any]]:
            yield from ()
            return transaction

        engine = OperationEngineImpl(command_defs=[AtomicCommandDef("test", TestCommand, handler)])

        with pytest.raises(NotImplementedError):
            engine.execute(TestCommand(), MESSAGE_CONTEXT).send(None)


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
        def other(context: MessageContext) -> Effectful[OtherService, GuardEffect]:
            events.append(f"other {context.message_id}")
            assert (yield from internal_effect) == "internal_result"
            return OtherService()

        def service(other: OtherService) -> Effectful[Service, GuardEffect]:
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
        def service() -> Effectful[Service, GuardEffect]:
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
        def service() -> Effectful[Service, GuardEffect]:
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
        def service() -> Effectful[Service, GuardEffect]:
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

    @pytest.mark.parametrize("effect", [TestDatabaseWriteEffect(), TestControlEffect(), TestCommand()])
    def test_raises_unsupported_effect_error_when_guard_yields_not_guard_effect(
        self,
        effect: Effect[Any],
    ) -> None:
        def service() -> Effectful[Service, GuardEffect]:
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
        def other() -> Effectful[OtherService, GuardEffect]:
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


def make_engine(
    handler: StagedHandler[Any, Any],
    guard_defs: Collection[GuardDef[Any]] = (),
) -> OperationEngineImpl:
    return OperationEngineImpl(
        command_defs=[StagedCommandDef("test", TestCommand, handler)],
        guard_defs=guard_defs,
    )


def handle_with_three_effects() -> Effectful[str, Effect[Any]]:
    yield from TestInternalEffect()
    yield from TestExternalReadEffect()
    yield from TestDatabaseWriteEffect()
    return "command_result"


def handle_service_with_three_effects(service: Service) -> Effectful[str, Effect[Any]]:
    return (yield from handle_with_three_effects())


def make_service_with_two_effects() -> Effectful[Service, GuardEffect]:
    yield from TestInternalEffect()
    yield from TestExternalReadEffect()
    return Service()


def collect_effect_ids(execution_plan: ExecutionPlan[Any]) -> Sequence[str]:
    return [
        instruction.effect_context.effect_id
        for instruction in execution_plan
        if isinstance(instruction, CallEffectHandlerInstruction)
    ]


def make_expected_instruction(effect: HandledEffect[Any], command: TestCommand) -> CallEffectHandlerInstruction[Any]:
    return CallEffectHandlerInstruction(
        message_context=MessageContext(message_id="test_message_id"),
        message=command,
        effect_context=EffectContext(effect_id=ANY),
        effect=effect,
    )


MESSAGE_CONTEXT = MessageContext(message_id="test_message_id")
