from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from fidem.effects import Effect, EffectContext, Effectful, InternalEffect
from fidem.environment import DuplicateEffectDefError, EffectDef, EffectNotDefinedError, EnvironmentImpl
from fidem.messages import MessageContext
from fidem.operation_engine import (
    CallEffectHandlerInstruction,
    ExecutionPlan,
    Instruction,
    OperationEngine,
    OperationEngineImpl,
    StagedCommandDef,
)
from fidem.testing import YeildStep
from tests.effects import TestDatabaseWriteEffect, TestExternalReadEffect, TestInternalEffect
from tests.messages import TestCommand
from tests.stubs import GeneratorFunctionStub


class TestExecute:
    async def test_passes_command_and_message_id_into_operation_engine(self) -> None:
        command = TestCommand()
        message_id = "test_message_id"
        operation_engine_mock = self._make_operation_engine_mock(GeneratorFunctionStub())
        environment = EnvironmentImpl(operation_engine=operation_engine_mock)

        await environment.execute(command, message_id)

        operation_engine_mock.execute.assert_called_once_with(command, message_id)

    async def test_returns_execution_plan_result(self) -> None:
        execution_plan_result = "execution_plan_result"
        operation_engine_mock = self._make_operation_engine_mock(
            GeneratorFunctionStub(return_value=execution_plan_result)
        )
        environment = EnvironmentImpl(operation_engine=operation_engine_mock)

        result = await environment.execute(TestCommand())

        assert result == execution_plan_result

    async def test_calls_effect_handler_and_sends_its_result_into_execution_plan(self) -> None:
        instruction = self._make_instruction()
        effect_result = "effect_result"
        effect_handler_mock = AsyncMock(return_value=effect_result)
        operation_engine_mock = self._make_operation_engine_mock(
            GeneratorFunctionStub(steps=[YeildStep(instruction, effect_result)])
        )
        environment = EnvironmentImpl(
            operation_engine=operation_engine_mock,
            effect_defs=[EffectDef(TestInternalEffect, effect_handler_mock)],
        )

        await environment.execute(TestCommand())

        effect_handler_mock.assert_awaited_once_with(instruction.context, instruction.effect)

    async def test_throws_effect_handler_error_into_execution_plan(self) -> None:
        def execution_plan(command: TestCommand, message_id: str | None) -> ExecutionPlan[str]:
            try:
                yield self._make_instruction()
            except ValueError as exc:
                return str(exc)
            return "unreachable"

        operation_engine_mock = self._make_operation_engine_mock(execution_plan)
        environment = EnvironmentImpl(
            operation_engine=operation_engine_mock,
            effect_defs=[EffectDef(TestInternalEffect, AsyncMock(side_effect=ValueError("effect_error")))],
        )

        result = await environment.execute(TestCommand())

        assert result == "effect_error"

    async def test_raises_effect_handler_error_when_execution_plan_does_not_handle_it(self) -> None:
        effect_error = ValueError("effect_error")
        operation_engine_mock = self._make_operation_engine_mock(
            GeneratorFunctionStub(steps=[YeildStep(self._make_instruction(), None)])
        )
        environment = EnvironmentImpl(
            operation_engine=operation_engine_mock,
            effect_defs=[EffectDef(TestInternalEffect, AsyncMock(side_effect=effect_error))],
        )

        with pytest.raises(ValueError, match="effect_error") as exc_info:
            await environment.execute(TestCommand())

        assert exc_info.value is effect_error

    async def test_does_not_throw_effect_not_defined_error_into_execution_plan(self) -> None:
        def execution_plan(command: TestCommand, message_id: str | None) -> ExecutionPlan[str]:
            nonlocal is_execution_plan_closed
            try:
                yield self._make_instruction()
            except Exception:  # noqa: BLE001 - checks that no error reaches the execution plan
                return "swallowed"
            finally:
                is_execution_plan_closed = True
            return "unreachable"

        is_execution_plan_closed = False
        environment = EnvironmentImpl(operation_engine=self._make_operation_engine_mock(execution_plan))

        with pytest.raises(EffectNotDefinedError):
            await environment.execute(TestCommand())

        assert is_execution_plan_closed

    async def test_raises_effect_not_defined_error_when_effect_not_defined(self) -> None:
        operation_engine_mock = self._make_operation_engine_mock(
            GeneratorFunctionStub(steps=[YeildStep(self._make_instruction(), None)])
        )
        environment = EnvironmentImpl(operation_engine=operation_engine_mock)

        with pytest.raises(EffectNotDefinedError) as exc_info:
            await environment.execute(TestCommand())

        assert exc_info.value.args == (TestInternalEffect,)

    async def test_raises_not_implemented_error_when_instruction_not_supported(self) -> None:
        class UnknownInstruction(Instruction[None]):
            pass

        operation_engine_mock = self._make_operation_engine_mock(
            GeneratorFunctionStub(steps=[YeildStep(UnknownInstruction(), None)])
        )
        environment = EnvironmentImpl(operation_engine=operation_engine_mock)

        with pytest.raises(NotImplementedError):
            await environment.execute(TestCommand())

    def _make_operation_engine_mock(self, execution_plan: Callable[..., ExecutionPlan[Any]]) -> Mock:
        operation_engine_mock = Mock(spec=OperationEngine)
        operation_engine_mock.execute.side_effect = execution_plan
        return operation_engine_mock

    def _make_instruction(self) -> CallEffectHandlerInstruction[str]:
        return CallEffectHandlerInstruction(EffectContext(effect_id="test_effect_id"), TestInternalEffect())


class TestExecuteWithOperationEngine:
    async def test_executes_command_with_effects(self) -> None:
        def handler(context: MessageContext, command: TestCommand) -> Effectful[str, Effect[Any]]:
            internal_result = yield from TestInternalEffect()
            external_result = yield from TestExternalReadEffect()
            database_result = yield from TestDatabaseWriteEffect()
            return f"{internal_result}, {external_result}, {database_result}"

        environment = EnvironmentImpl(
            operation_engine=OperationEngineImpl(command_defs=[StagedCommandDef("test", TestCommand, handler)]),
            effect_defs=[
                EffectDef(TestInternalEffect, AsyncMock(return_value="internal")),
                EffectDef(TestExternalReadEffect, AsyncMock(return_value="external")),
                EffectDef(TestDatabaseWriteEffect, AsyncMock(return_value="database")),
            ],
        )

        result = await environment.execute(TestCommand())

        assert result == "internal, external, database"

    async def test_passes_same_effect_ids_when_command_replayed_with_same_message_id(self) -> None:
        def handler(context: MessageContext, command: TestCommand) -> Effectful[str, Effect[Any]]:
            yield from TestInternalEffect()
            yield from TestInternalEffect()
            return "command_result"

        effect_handler_mock = AsyncMock(return_value="effect_result")
        environment = EnvironmentImpl(
            operation_engine=OperationEngineImpl(command_defs=[StagedCommandDef("test", TestCommand, handler)]),
            effect_defs=[EffectDef(TestInternalEffect, effect_handler_mock)],
        )

        await environment.execute(TestCommand(), "test_message_id")
        await environment.execute(TestCommand(), "test_message_id")

        effect_ids = [call.args[0].effect_id for call in effect_handler_mock.await_args_list]
        assert effect_ids[:2] == effect_ids[2:]
        assert len(set(effect_ids)) == 2


class TestEffectDefinition:
    def test_raises_duplicate_effect_def_error_when_defines_effects_with_same_type(self) -> None:
        class OtherEffect(InternalEffect[str]):
            pass

        duplicate_effect_defs = [
            EffectDef(TestInternalEffect, AsyncMock()),
            EffectDef(TestInternalEffect, AsyncMock()),
        ]
        other_effect_defs = [
            EffectDef(OtherEffect, AsyncMock()),
        ]

        with pytest.raises(DuplicateEffectDefError) as exc_info:
            EnvironmentImpl(
                operation_engine=Mock(spec=OperationEngine),
                effect_defs=duplicate_effect_defs + other_effect_defs,
            )

        assert exc_info.value.args == (duplicate_effect_defs,)
