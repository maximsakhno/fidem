from collections.abc import MutableSequence
from dataclasses import dataclass
from typing import Any

import pytest

from fidem.core.effects import Effect, EffectContext, Effectful
from fidem.core.environment import EffectDef, EffectNotDefinedError, EnvironmentImpl
from fidem.core.guards import GuardDef
from fidem.core.messages import MessageContext, QueryEffect
from fidem.core.operation_engine.definitions import StagedCommandDef
from fidem.core.operation_engine.engine import OperationEngineImpl
from fidem.core.operation_engine.instructions import ExecutionPlan, Instruction
from fidem.core.testing import YieldStep
from tests.effects import TestDatabaseWriteEffect, TestExternalReadEffect, TestInternalEffect
from tests.messages import TestCommand
from tests.stubs import GeneratorFunctionStub
from tests.unit.environment.helpers import Config, make_failing_handler, make_instruction, make_operation_engine_mock


@dataclass(frozen=True, slots=True)
class EffectHandlerCall:
    context: EffectContext
    effect: Effect[Any]


class TestHandle:
    async def test_passes_command_message_context_and_inputs_into_operation_engine(self) -> None:
        command = TestCommand()
        operation_engine_mock = make_operation_engine_mock(GeneratorFunctionStub())

        async with EnvironmentImpl(operation_engine=operation_engine_mock) as environment:
            await environment.handle(command, "first_input", "second_input", message_id="test_message_id")

        operation_engine_mock.handle.assert_called_once_with(
            message=command,
            message_context=MessageContext(message_id="test_message_id"),
            inputs=("first_input", "second_input"),
        )

    async def test_passes_same_message_context_into_operation_engine_and_effect_handler(self) -> None:
        async def handler(message_context: MessageContext) -> None:
            message_contexts.append(message_context)

        message_contexts: MutableSequence[MessageContext] = []
        operation_engine_mock = make_operation_engine_mock(
            GeneratorFunctionStub(steps=[YieldStep(make_instruction(), None)])
        )
        environment = EnvironmentImpl(
            operation_engine=operation_engine_mock,
            effect_defs=[EffectDef(TestInternalEffect, handler)],
        )

        async with environment:
            await environment.handle(TestCommand())

        assert message_contexts == [operation_engine_mock.handle.call_args.kwargs["message_context"]]
        assert message_contexts[0] is operation_engine_mock.handle.call_args.kwargs["message_context"]

    async def test_generates_different_message_ids_when_message_id_not_passed(self) -> None:
        operation_engine_mock = make_operation_engine_mock(GeneratorFunctionStub())

        async with EnvironmentImpl(operation_engine=operation_engine_mock) as environment:
            await environment.handle(TestCommand())
            await environment.handle(TestCommand())

        message_ids = {
            call.kwargs["message_context"].message_id for call in operation_engine_mock.handle.call_args_list
        }
        assert len(message_ids) == 2

    async def test_returns_execution_plan_result(self) -> None:
        execution_plan_result = "execution_plan_result"
        operation_engine_mock = make_operation_engine_mock(GeneratorFunctionStub(return_value=execution_plan_result))

        async with EnvironmentImpl(operation_engine=operation_engine_mock) as environment:
            result = await environment.handle(TestCommand())

        assert result == execution_plan_result

    async def test_calls_effect_handler_and_sends_its_result_into_execution_plan(self) -> None:
        async def handler(context: EffectContext, effect: TestInternalEffect) -> str:
            calls.append(EffectHandlerCall(context=context, effect=effect))
            return effect_result

        calls: MutableSequence[EffectHandlerCall] = []
        instruction = make_instruction()
        effect_result = "effect_result"
        operation_engine_mock = make_operation_engine_mock(
            GeneratorFunctionStub(steps=[YieldStep(instruction, effect_result)])
        )
        environment = EnvironmentImpl(
            operation_engine=operation_engine_mock,
            effect_defs=[EffectDef(TestInternalEffect, handler)],
        )

        async with environment:
            await environment.handle(TestCommand())

        assert calls == [EffectHandlerCall(context=instruction.call.effect_context, effect=instruction.call.effect)]

    async def test_throws_effect_handler_error_into_execution_plan(self) -> None:
        def execution_plan(**_: object) -> ExecutionPlan[str]:
            try:
                yield make_instruction()
            except ValueError as exc:
                return str(exc)
            return "unreachable"

        environment = EnvironmentImpl(
            operation_engine=make_operation_engine_mock(execution_plan),
            effect_defs=[EffectDef(TestInternalEffect, make_failing_handler(ValueError("effect_error")))],
        )

        async with environment:
            result = await environment.handle(TestCommand())

        assert result == "effect_error"

    async def test_raises_effect_handler_error_when_execution_plan_does_not_handle_it(self) -> None:
        effect_error = ValueError("effect_error")
        environment = EnvironmentImpl(
            operation_engine=make_operation_engine_mock(
                GeneratorFunctionStub(steps=[YieldStep(make_instruction(), None)])
            ),
            effect_defs=[EffectDef(TestInternalEffect, make_failing_handler(effect_error))],
        )

        async with environment:
            with pytest.raises(ValueError, match="effect_error") as exc_info:
                await environment.handle(TestCommand())

        assert exc_info.value is effect_error

    async def test_does_not_throw_effect_not_defined_error_into_execution_plan(self) -> None:
        def execution_plan(**_: object) -> ExecutionPlan[str]:
            nonlocal is_execution_plan_closed
            try:
                yield make_instruction()
            except Exception:  # noqa: BLE001 - checks that no error reaches the execution plan
                return "swallowed"
            finally:
                is_execution_plan_closed = True
            return "unreachable"

        is_execution_plan_closed = False

        async with EnvironmentImpl(operation_engine=make_operation_engine_mock(execution_plan)) as environment:
            with pytest.raises(EffectNotDefinedError):
                await environment.handle(TestCommand())

        assert is_execution_plan_closed

    async def test_raises_effect_not_defined_error_when_effect_not_defined(self) -> None:
        operation_engine_mock = make_operation_engine_mock(
            GeneratorFunctionStub(steps=[YieldStep(make_instruction(), None)])
        )

        async with EnvironmentImpl(operation_engine=operation_engine_mock) as environment:
            with pytest.raises(EffectNotDefinedError) as exc_info:
                await environment.handle(TestCommand())

        assert exc_info.value.args == (TestInternalEffect,)

    async def test_raises_not_implemented_error_when_instruction_not_supported(self) -> None:
        class UnknownInstruction(Instruction[None]):
            pass

        operation_engine_mock = make_operation_engine_mock(
            GeneratorFunctionStub(steps=[YieldStep(UnknownInstruction(), None)])
        )

        async with EnvironmentImpl(operation_engine=operation_engine_mock) as environment:
            with pytest.raises(NotImplementedError):
                await environment.handle(TestCommand())


class TestExecuteWithOperationEngine:
    async def test_executes_command_with_effects(self) -> None:
        def handler(context: MessageContext, command: TestCommand) -> Effectful[str, Effect[Any]]:
            internal_result = yield from TestInternalEffect()
            external_result = yield from TestExternalReadEffect()
            database_result = yield from TestDatabaseWriteEffect()
            return f"{internal_result}, {external_result}, {database_result}"

        async def handle_internal(effect: TestInternalEffect) -> str:
            return "internal"

        async def handle_external(effect: TestExternalReadEffect) -> str:
            return "external"

        async def handle_database(effect: TestDatabaseWriteEffect) -> str:
            return "database"

        environment = EnvironmentImpl(
            operation_engine=OperationEngineImpl(command_defs=[StagedCommandDef("test", TestCommand, handler)]),
            effect_defs=[
                EffectDef(TestInternalEffect, handle_internal),
                EffectDef(TestExternalReadEffect, handle_external),
                EffectDef(TestDatabaseWriteEffect, handle_database),
            ],
        )

        async with environment:
            result = await environment.handle(TestCommand())

        assert result == "internal, external, database"

    async def test_passes_same_effect_ids_when_command_replayed_with_same_message_id(self) -> None:
        def handler(context: MessageContext, command: TestCommand) -> Effectful[str, Effect[Any]]:
            yield from TestInternalEffect()
            yield from TestInternalEffect()
            return "command_result"

        async def handle_internal(context: EffectContext) -> str:
            effect_ids.append(context.effect_id)
            return "effect_result"

        effect_ids: MutableSequence[str] = []
        environment = EnvironmentImpl(
            operation_engine=OperationEngineImpl(command_defs=[StagedCommandDef("test", TestCommand, handler)]),
            effect_defs=[EffectDef(TestInternalEffect, handle_internal)],
        )

        async with environment:
            await environment.handle(TestCommand(), message_id="test_message_id")
            await environment.handle(TestCommand(), message_id="test_message_id")

        assert effect_ids[:2] == effect_ids[2:]
        assert len(set(effect_ids)) == 2


class TestGuards:
    async def test_executes_guard_effects_before_handler(self) -> None:
        def config(context: MessageContext) -> Effectful[Config, QueryEffect]:
            return Config(f"{context.message_id} {(yield from TestInternalEffect())}")

        async def handle_internal(message_context: MessageContext) -> str:
            return "effect_result"

        def handler(config: Config) -> Effectful[str, Effect[Any]]:
            yield from ()
            return config.value

        environment = EnvironmentImpl(
            operation_engine=OperationEngineImpl(
                command_defs=[StagedCommandDef("test", TestCommand, handler)],
                guard_defs=[GuardDef(config)],
            ),
            effect_defs=[EffectDef(TestInternalEffect, handle_internal)],
        )

        async with environment:
            result = await environment.handle(TestCommand(), message_id="test_message_id")

        assert result == "test_message_id effect_result"
