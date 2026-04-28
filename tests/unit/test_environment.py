from unittest.mock import AsyncMock, Mock, call

import pytest

from fidem.environment import (
    DuplicateIntentDefError,
    EnvironmentImpl,
    IntentNotDefinedError,
    ReadIntentDef,
    WriteIntentDef,
)
from fidem.intents import IntentCompensationContext, IntentContext, ReadIntent
from fidem.operation_engine import (
    CallIntentCompensationHandlerInstruction,
    CallIntentHandlerInstruction,
    CommandContextIn,
    OperationEngine,
)
from fidem.testing import YeildStep
from tests.intents import TestReadIntent, TestWriteIntent
from tests.operations import TestCommand
from tests.stubs import GeneratorFunctionStub, intent_middleware_stub


class TestExecute:
    async def test_passes_context_command_into_opeation_engine(self) -> None:
        command, context = TestCommand(), CommandContextIn()
        operation_engine_mock = Mock(spec=OperationEngine)
        operation_engine_mock.execute = Mock(side_effect=GeneratorFunctionStub())
        environment = EnvironmentImpl(operation_engine=operation_engine_mock)

        await environment.execute(command, context)

        operation_engine_mock.execute.assert_called_once_with(command, context)

    async def test_returns_execution_plan_result(self) -> None:
        command, context = TestCommand(), CommandContextIn()
        execution_plan_result = "execution_plan_result"
        operation_engine_mock = Mock(spec=OperationEngine)
        operation_engine_mock.execute = Mock(side_effect=GeneratorFunctionStub(return_value=execution_plan_result))
        environment = EnvironmentImpl(operation_engine=operation_engine_mock)

        result = await environment.execute(command, context)

        assert result == execution_plan_result

    async def test_calls_intent_handler_when_call_intent_handler_instruction_yields(self) -> None:
        command, command_context = TestCommand(), CommandContextIn()
        intent_context = IntentContext(intent_id="test_intent_id")
        intent = TestReadIntent()
        intent_result = "intent_result"
        intent_handler_mock = AsyncMock(return_value=intent_result)
        operation_engine_mock = Mock(spec=OperationEngine)
        operation_engine_mock.execute = Mock(
            side_effect=GeneratorFunctionStub(
                steps=[YeildStep(CallIntentHandlerInstruction(intent_context, intent), intent_result)]
            )
        )
        environment = EnvironmentImpl(
            operation_engine=operation_engine_mock,
            intent_defs=[ReadIntentDef(TestReadIntent, intent_handler_mock)],
        )

        await environment.execute(command, command_context)

        intent_handler_mock.assert_awaited_once_with(intent_context, intent)

    async def test_raises_intent_not_defined_error_when_intent_not_defined(self) -> None:
        command, command_context = TestCommand(), CommandContextIn()
        intent_context = IntentContext(intent_id="test_intent_id")
        intent = TestReadIntent()
        intent_result = "intent_result"
        operation_engine_mock = Mock(spec=OperationEngine)
        operation_engine_mock.execute = Mock(
            side_effect=GeneratorFunctionStub(
                steps=[YeildStep(CallIntentHandlerInstruction(intent_context, intent), intent_result)]
            )
        )
        environment = EnvironmentImpl(operation_engine=operation_engine_mock)

        with pytest.raises(IntentNotDefinedError) as exc_info:
            await environment.execute(command, command_context)

        assert exc_info.value.args == (TestReadIntent,)

    async def test_calls_intent_compensation_handler_when_compensation_instruction_yeilds(self) -> None:
        command, command_context = TestCommand(), CommandContextIn()
        intent_context = IntentContext(intent_id="test_intent_id")
        intent = TestWriteIntent()
        intent_result = "intent_result"
        intent_compensation_context = IntentCompensationContext(
            intent_id="compensation_intent_id",
            original_context=intent_context,
            original_result=intent_result,
        )
        intent_handler_mock = AsyncMock(return_value=intent_result)
        intent_compensation_handler_mock = AsyncMock(return_value=None)
        operation_engine_mock = Mock(spec=OperationEngine)
        operation_engine_mock.execute = Mock(
            side_effect=GeneratorFunctionStub(
                steps=[YeildStep(CallIntentCompensationHandlerInstruction(intent_compensation_context, intent), None)]
            )
        )
        environment = EnvironmentImpl(
            operation_engine=operation_engine_mock,
            intent_defs=[WriteIntentDef(TestWriteIntent, intent_handler_mock, intent_compensation_handler_mock)],
        )

        await environment.execute(command, command_context)

        intent_handler_mock.assert_not_awaited()
        intent_compensation_handler_mock.assert_awaited_once_with(intent_compensation_context, intent)


class TestIntentDefinition:
    def test_raises_duplicate_intent_def_error_when_defines_intents_with_same_type(self) -> None:
        class OtherReadIntent(ReadIntent[str]):
            pass

        duplicate_intent_defs = [
            ReadIntentDef(TestReadIntent, AsyncMock()),
            ReadIntentDef(TestReadIntent, AsyncMock()),
        ]
        other_intent_defs = [
            ReadIntentDef(OtherReadIntent, AsyncMock()),
        ]
        with pytest.raises(DuplicateIntentDefError) as exc_info:
            EnvironmentImpl(
                operation_engine=Mock(spec=OperationEngine),
                intent_defs=duplicate_intent_defs + other_intent_defs,
            )

        assert exc_info.value.args == (duplicate_intent_defs,)


class TestIntentMiddleware:
    async def test_global_middlewares_calls_in_reverse_order(self) -> None:
        middlewares = Mock()
        middlewares.first = self._make_middleware_mock()
        middlewares.second = self._make_middleware_mock()
        middlewares.third = self._make_middleware_mock()
        intent_context = IntentContext(intent_id="test_intent_id")
        intent = TestReadIntent()
        intent_result = "intent_result"
        intent_handler_mock = AsyncMock(return_value=intent_result)
        operation_engine_mock = Mock(spec=OperationEngine)
        operation_engine_mock.execute = Mock(
            side_effect=GeneratorFunctionStub(
                steps=[YeildStep(CallIntentHandlerInstruction(intent_context, intent), intent_result)]
            )
        )

        EnvironmentImpl(
            operation_engine=operation_engine_mock,
            intent_defs=[ReadIntentDef(TestReadIntent, intent_handler_mock)],
            middlewares=[middlewares.first, middlewares.second, middlewares.third],
        )

        expected_calls = [
            call.third(intent_handler_mock),
            call.second(intent_handler_mock),
            call.first(intent_handler_mock),
        ]
        middlewares.assert_has_calls(expected_calls, any_order=False)

    async def test_local_middlewares_calls_in_reverse_order(self) -> None:
        middlewares = Mock()
        middlewares.first = self._make_middleware_mock()
        middlewares.second = self._make_middleware_mock()
        middlewares.third = self._make_middleware_mock()
        intent_context = IntentContext(intent_id="test_intent_id")
        intent = TestReadIntent()
        intent_result = "intent_result"
        intent_handler_mock = AsyncMock(return_value=intent_result)
        operation_engine_mock = Mock(spec=OperationEngine)
        operation_engine_mock.execute = Mock(
            side_effect=GeneratorFunctionStub(
                steps=[YeildStep(CallIntentHandlerInstruction(intent_context, intent), intent_result)]
            )
        )

        EnvironmentImpl(
            operation_engine=operation_engine_mock,
            intent_defs=[
                ReadIntentDef(
                    TestReadIntent,
                    intent_handler_mock,
                    middlewares=[middlewares.first, middlewares.second, middlewares.third],
                )
            ],
        )

        expected_calls = [
            call.third(intent_handler_mock),
            call.second(intent_handler_mock),
            call.first(intent_handler_mock),
        ]
        middlewares.assert_has_calls(expected_calls, any_order=False)

    async def test_local_middlewares_overrides_global_middlewares(self) -> None:
        global_middleware = self._make_middleware_mock()
        local_middleware = self._make_middleware_mock()
        intent_context = IntentContext(intent_id="test_intent_id")
        intent = TestReadIntent()
        intent_result = "intent_result"
        intent_handler_mock = AsyncMock(return_value=intent_result)
        operation_engine_mock = Mock(spec=OperationEngine)
        operation_engine_mock.execute = Mock(
            side_effect=GeneratorFunctionStub(
                steps=[YeildStep(CallIntentHandlerInstruction(intent_context, intent), intent_result)]
            )
        )

        EnvironmentImpl(
            operation_engine=operation_engine_mock,
            intent_defs=[
                ReadIntentDef(
                    TestReadIntent,
                    intent_handler_mock,
                    middlewares=[local_middleware],
                )
            ],
            middlewares=[global_middleware],
        )

        global_middleware.assert_not_called()
        local_middleware.assert_called_once_with(intent_handler_mock)

    def _make_middleware_mock(self) -> Mock:
        return Mock(side_effect=intent_middleware_stub)
