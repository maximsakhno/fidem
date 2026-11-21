from collections.abc import MutableSequence, Sequence
from typing import Any
from unittest.mock import ANY

import pytest

from fidem.effects import Effect, EffectContext, Effectful
from fidem.messages import Command, MessageContext, StagedHandler
from fidem.operation_engine import (
    AtomicCommandDef,
    CallEffectHandlerInstruction,
    CommandNotDefinedError,
    DuplicateCommandDefError,
    ExecutionMode,
    HandledEffect,
    OperationEngineImpl,
    StagedCommandDef,
    UnsupportedEffectError,
)
from fidem.testing import YeildStep, assert_generator
from tests.effects import TestControlEffect, TestDatabaseWriteEffect, TestExternalReadEffect, TestInternalEffect
from tests.messages import TestCommand, TestEvent
from tests.stubs import GeneratorFunctionStub


class TestExecuteEphemeral:
    def test_returns_result_when_handler_returns_result(self) -> None:
        command = TestCommand()
        command_result = "command_result"
        handler_stub = GeneratorFunctionStub(
            args=(MessageContext(message_id=ANY), command), return_value=command_result
        )
        engine = self._make_engine(handler_stub)

        assert_generator(generator=engine.execute(command), return_value=command_result)

    @pytest.mark.parametrize(
        argnames="effect",
        argvalues=[TestInternalEffect(), TestExternalReadEffect(), TestDatabaseWriteEffect()],
    )
    def test_yields_call_effect_handler_instruction_when_handler_yields_effect(
        self,
        effect: HandledEffect[str],
    ) -> None:
        command = TestCommand()
        effect_result = "effect_result"
        command_result = "command_result"
        handler_stub = GeneratorFunctionStub(
            args=(MessageContext(message_id=ANY), command),
            steps=[YeildStep(effect, effect_result)],
            return_value=command_result,
        )
        engine = self._make_engine(handler_stub)

        assert_generator(
            generator=engine.execute(command),
            steps=[YeildStep(CallEffectHandlerInstruction(EffectContext(effect_id=ANY), effect), effect_result)],
            return_value=command_result,
        )

    @pytest.mark.parametrize("effect", [TestControlEffect(), TestEvent(), TestCommand()])
    def test_raises_unsupported_effect_error_when_handler_yields_unsupported_effect(self, effect: Effect[Any]) -> None:
        handler_stub = GeneratorFunctionStub(steps=[YeildStep(effect, None)])
        engine = self._make_engine(handler_stub)

        with pytest.raises(UnsupportedEffectError) as exc_info:
            engine.execute(TestCommand()).send(None)

        assert exc_info.value.args == (effect,)

    def test_does_not_throw_unsupported_effect_error_into_handler(self) -> None:
        def handler(context: MessageContext, command: TestCommand) -> Effectful[str, Effect[Any]]:
            nonlocal is_handler_closed
            try:
                yield from TestControlEffect()
            except Exception:  # noqa: BLE001 - checks that no error reaches the handler
                return "swallowed"
            finally:
                is_handler_closed = True
            return "unreachable"

        is_handler_closed = False
        engine = self._make_engine(handler)

        with pytest.raises(UnsupportedEffectError):
            engine.execute(TestCommand()).send(None)

        assert is_handler_closed

    def test_closes_handler_when_execution_plan_closed(self) -> None:
        def handler(context: MessageContext, command: TestCommand) -> Effectful[str, Effect[Any]]:
            nonlocal is_handler_closed
            try:
                yield from TestInternalEffect()
            finally:
                is_handler_closed = True
            return "command_result"

        is_handler_closed = False
        engine = self._make_engine(handler)
        execution_plan = engine.execute(TestCommand())
        execution_plan.send(None)

        execution_plan.close()

        assert is_handler_closed

    def test_passes_message_id_into_handler_context(self) -> None:
        command = TestCommand()
        message_id = "test_message_id"
        handler_stub = GeneratorFunctionStub(args=(MessageContext(message_id=message_id), command))
        engine = self._make_engine(handler_stub)

        assert_generator(generator=engine.execute(command, message_id))

    def test_generates_different_message_ids_when_message_id_not_passed(self) -> None:
        def handler(context: MessageContext, command: TestCommand) -> Effectful[str, Effect[Any]]:
            message_ids.append(context.message_id)
            yield from ()
            return "command_result"

        message_ids: MutableSequence[str] = []
        engine = self._make_engine(handler)

        assert_generator(generator=engine.execute(TestCommand()), return_value="command_result")
        assert_generator(generator=engine.execute(TestCommand()), return_value="command_result")

        assert len(set(message_ids)) == 2

    def test_generates_same_effect_ids_for_same_message_id(self) -> None:
        engine = self._make_engine_with_three_effects()

        effect_ids1 = self._collect_effect_ids(engine, "test_message_id")
        effect_ids2 = self._collect_effect_ids(engine, "test_message_id")

        assert effect_ids1 == effect_ids2

    def test_generates_unique_effect_ids_within_command(self) -> None:
        engine = self._make_engine_with_three_effects()

        effect_ids = self._collect_effect_ids(engine, "test_message_id")

        assert len(set(effect_ids)) == 3

    def test_generates_different_effect_ids_for_different_message_ids(self) -> None:
        engine = self._make_engine_with_three_effects()

        effect_ids1 = self._collect_effect_ids(engine, "test_message_id1")
        effect_ids2 = self._collect_effect_ids(engine, "test_message_id2")

        assert set(effect_ids1).isdisjoint(effect_ids2)

    def test_throws_instruction_error_into_handler(self) -> None:
        def handler(context: MessageContext, command: TestCommand) -> Effectful[str, Effect[Any]]:
            try:
                yield from TestInternalEffect()
            except ValueError as exc:
                return str(exc)
            return "unreachable"

        engine = self._make_engine(handler)
        execution_plan = engine.execute(TestCommand())
        execution_plan.send(None)

        with pytest.raises(StopIteration) as exc_info:
            execution_plan.throw(ValueError("effect_error"))

        assert exc_info.value.value == "effect_error"

    def test_raises_instruction_error_when_handler_does_not_handle_it(self) -> None:
        handler_stub = GeneratorFunctionStub(steps=[YeildStep(TestInternalEffect(), None)])
        engine = self._make_engine(handler_stub)
        execution_plan = engine.execute(TestCommand())
        execution_plan.send(None)
        effect_error = ValueError("effect_error")

        with pytest.raises(ValueError, match="effect_error") as exc_info:
            execution_plan.throw(effect_error)

        assert exc_info.value is effect_error

    def test_raises_command_not_defined_error_when_command_not_defined(self) -> None:
        engine = OperationEngineImpl(command_defs=[])

        with pytest.raises(CommandNotDefinedError) as exc_info:
            engine.execute(TestCommand()).send(None)

        assert exc_info.value.args == (TestCommand,)

    def test_raises_not_implemented_error_when_command_is_durable(self) -> None:
        command_def = StagedCommandDef(
            name="test",
            message_type=TestCommand,
            handler=GeneratorFunctionStub(),
            execution_mode=ExecutionMode.DURABLE,
        )
        engine = OperationEngineImpl(command_defs=[command_def])

        with pytest.raises(NotImplementedError):
            engine.execute(TestCommand()).send(None)

    def test_raises_not_implemented_error_when_command_is_atomic(self) -> None:
        engine = OperationEngineImpl(command_defs=[AtomicCommandDef("test", TestCommand, GeneratorFunctionStub())])

        with pytest.raises(NotImplementedError):
            engine.execute(TestCommand()).send(None)

    def _make_engine(self, handler: StagedHandler[Any, Any]) -> OperationEngineImpl:
        return OperationEngineImpl(command_defs=[StagedCommandDef("test", TestCommand, handler)])

    def _make_engine_with_three_effects(self) -> OperationEngineImpl:
        handler_stub = GeneratorFunctionStub(
            steps=[
                YeildStep(TestInternalEffect(), None),
                YeildStep(TestExternalReadEffect(), None),
                YeildStep(TestDatabaseWriteEffect(), None),
            ],
        )
        return self._make_engine(handler_stub)

    def _collect_effect_ids(self, engine: OperationEngineImpl, message_id: str) -> Sequence[str]:
        return [
            instruction.context.effect_id
            for instruction in engine.execute(TestCommand(), message_id)
            if isinstance(instruction, CallEffectHandlerInstruction)
        ]


class TestCommandDefinition:
    def test_raises_duplicate_command_def_error_when_define_commands_with_same_type(self) -> None:
        class OtherCommand(Command[str]):
            pass

        duplicate_command_defs = [
            StagedCommandDef("test1", TestCommand, GeneratorFunctionStub()),
            StagedCommandDef("test2", TestCommand, GeneratorFunctionStub()),
        ]
        other_command_defs = [
            StagedCommandDef("other", OtherCommand, GeneratorFunctionStub()),
        ]

        with pytest.raises(DuplicateCommandDefError) as exc_info:
            OperationEngineImpl(command_defs=duplicate_command_defs + other_command_defs)

        assert exc_info.value.args == (duplicate_command_defs,)
