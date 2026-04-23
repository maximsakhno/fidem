from unittest.mock import ANY, Mock, call

import pytest

from fidem.intents import IntentCompensationContext, IntentContext
from fidem.operation_engine import (
    CallIntentCompensationHandlerInstruction,
    CallIntentHandlerInstruction,
    CommandContextIn,
    CommandDef,
    CommandNotDefinedError,
    DuplicateCommandDefError,
    OperationEngineImpl,
)
from fidem.operations import Command, CommandContext, CommandGenerator
from fidem.testing import RaiseStep, YeildStep, assert_generator
from tests.intents import TestReadIntent, TestWriteIntent
from tests.operations import TestCommand
from tests.stubs import GeneratorFunctionStub, command_middleware_stub


class TestExecuteEphemeral:
    def test_returns_result_when_command_returns_result(self) -> None:
        test_context, test_command = CommandContext(operation_id=ANY), TestCommand()
        command_result = "command_result"
        command_handler_stub = GeneratorFunctionStub(
            args=(test_context, test_command),
            steps=[],
            return_value=command_result,
        )
        engine = OperationEngineImpl(command_defs=[CommandDef(TestCommand, command_handler_stub)])

        assert_generator(
            generator=engine.execute(test_command),
            steps=[],
            return_value=command_result,
        )

    def test_returns_intent_result_when_command_returns_write_intent(self) -> None:
        test_context, test_command = CommandContext(operation_id=ANY), TestCommand()
        write_intent, write_intent_result = TestWriteIntent(), "write_intent_result"
        command_handler_stub = GeneratorFunctionStub(
            args=(test_context, test_command),
            steps=[],
            return_value=write_intent,
        )
        engine = OperationEngineImpl(command_defs=[CommandDef(TestCommand, command_handler_stub)])

        assert_generator(
            generator=engine.execute(test_command),
            steps=[YeildStep(CallIntentHandlerInstruction(IntentContext(ANY), write_intent), write_intent_result)],
            return_value=write_intent_result,
        )

    def test_yeilds_execute_intent_instruction_when_commnad_yields_read_intent(self) -> None:
        test_context, test_command = CommandContext(operation_id=ANY), TestCommand()
        intent, intent_result = TestReadIntent(), "intent_result1"
        command_result = "command_result"
        command_handler_stub = GeneratorFunctionStub(
            args=(test_context, test_command),
            steps=[YeildStep(intent, intent_result)],
            return_value=command_result,
        )
        engine = OperationEngineImpl(command_defs=[CommandDef(TestCommand, command_handler_stub)])

        assert_generator(
            generator=engine.execute(test_command),
            steps=[YeildStep(CallIntentHandlerInstruction(IntentContext(ANY), intent), intent_result)],
            return_value=command_result,
        )

    def test_generates_same_intent_ids_for_same_operation_id(self) -> None:
        operation_id = "test_operation_id"
        test_context, test_command = CommandContext(operation_id=ANY), TestCommand()
        read_intent_results = [
            "read_intent_result1",
            "read_intent_result2",
            "read_intent_result3",
        ]
        write_intent_result = "write_intent_result"
        command_handler_stub = GeneratorFunctionStub(
            args=(test_context, test_command),
            steps=[
                YeildStep(TestReadIntent(), read_intent_results[0]),
                YeildStep(TestReadIntent(), read_intent_results[1]),
                YeildStep(TestReadIntent(), read_intent_results[2]),
            ],
            return_value=TestWriteIntent(),
        )
        engine = OperationEngineImpl(command_defs=[CommandDef(TestCommand, command_handler_stub)])

        intent_ids: list[str] = []
        execute_gen1 = engine.execute(test_command, CommandContextIn(operation_id))

        instruction = execute_gen1.send(None)
        assert isinstance(instruction, CallIntentHandlerInstruction)
        intent_ids.append(instruction.context.intent_id)

        for intent_result in read_intent_results:
            instruction = execute_gen1.send(intent_result)
            assert isinstance(instruction, CallIntentHandlerInstruction)
            intent_ids.append(instruction.context.intent_id)

        with pytest.raises(StopIteration) as exc_info:
            execute_gen1.send(write_intent_result)

        assert exc_info.value.value == write_intent_result
        assert len(intent_ids) == 4

        execute_gen2 = engine.execute(test_command, CommandContextIn(operation_id))

        instruction = execute_gen2.send(None)
        assert isinstance(instruction, CallIntentHandlerInstruction)
        assert intent_ids[0] == instruction.context.intent_id
        for intent_result, intent_id in zip(read_intent_results, intent_ids[1:], strict=True):
            instruction = execute_gen2.send(intent_result)
            assert isinstance(instruction, CallIntentHandlerInstruction)
            assert intent_id == instruction.context.intent_id

        with pytest.raises(StopIteration) as exc_info:
            execute_gen2.send(write_intent_result)

        assert exc_info.value.value == write_intent_result

    def test_generates_different_intent_ids_for_different_operation_id(self) -> None:
        operation_id1 = "test_operation_id1"
        operation_id2 = "test_operation_id2"
        test_context, test_command = CommandContext(operation_id=ANY), TestCommand()
        read_intent_results = [
            "read_intent_result1",
            "read_intent_result2",
            "read_intent_result3",
        ]
        write_intent_result = "write_intent_result"
        command_handler_stub = GeneratorFunctionStub(
            args=(test_context, test_command),
            steps=[
                YeildStep(TestReadIntent(), read_intent_results[0]),
                YeildStep(TestReadIntent(), read_intent_results[1]),
                YeildStep(TestReadIntent(), read_intent_results[2]),
            ],
            return_value=TestWriteIntent(),
        )
        engine = OperationEngineImpl(command_defs=[CommandDef(TestCommand, command_handler_stub)])

        intent_ids: list[str] = []
        execute_gen1 = engine.execute(test_command, CommandContextIn(operation_id1))

        instruction = execute_gen1.send(None)
        assert isinstance(instruction, CallIntentHandlerInstruction)
        intent_ids.append(instruction.context.intent_id)

        for intent_result in read_intent_results:
            instruction = execute_gen1.send(intent_result)
            assert isinstance(instruction, CallIntentHandlerInstruction)
            intent_ids.append(instruction.context.intent_id)

        with pytest.raises(StopIteration) as exc_info:
            execute_gen1.send(write_intent_result)

        assert exc_info.value.value == write_intent_result
        assert len(intent_ids) == 4

        execute_gen2 = engine.execute(test_command, CommandContextIn(operation_id2))

        instruction = execute_gen2.send(None)
        assert isinstance(instruction, CallIntentHandlerInstruction)
        assert instruction.context.intent_id not in intent_ids
        for intent_result in read_intent_results:
            instruction = execute_gen2.send(intent_result)
            assert isinstance(instruction, CallIntentHandlerInstruction)
            assert instruction.context.intent_id not in intent_ids

        with pytest.raises(StopIteration) as exc_info:
            execute_gen2.send(write_intent_result)

        assert exc_info.value.value == write_intent_result

    def test_generates_same_operation_id_for_same_passed_operation_id(self) -> None:
        def test_command_handler(context: CommandContext, command: TestCommand) -> CommandGenerator[str]:
            nonlocal operation_id

            if operation_id is None:
                operation_id = context.operation_id
            else:
                assert operation_id == context.operation_id

            intent_result = yield intent
            assert intent_result == intent_result
            return command_result

        operation_id_in = "test_operation_id"
        operation_id: str | None = None
        intent, intent_result = TestReadIntent(), "intent_result1"
        command_result = "command_result"
        engine = OperationEngineImpl(command_defs=[CommandDef(TestCommand, test_command_handler)])

        assert_generator(
            generator=engine.execute(TestCommand(), CommandContextIn(operation_id_in)),
            steps=[YeildStep(CallIntentHandlerInstruction(IntentContext(ANY), intent), intent_result)],
            return_value=command_result,
        )
        assert_generator(
            generator=engine.execute(TestCommand(), CommandContextIn(operation_id_in)),
            steps=[YeildStep(CallIntentHandlerInstruction(IntentContext(ANY), intent), intent_result)],
            return_value=command_result,
        )

        assert operation_id is not None

    def test_generates_different_operation_id_for_different_passed_operation_id(self) -> None:
        def test_command_handler(context: CommandContext, command: TestCommand) -> CommandGenerator[str]:
            nonlocal operation_id

            if operation_id is None:
                operation_id = context.operation_id
            else:
                assert operation_id != context.operation_id

            intent_result = yield intent
            assert intent_result == intent_result
            return command_result

        operation_id_in1 = "test_operation_id1"
        operation_id_in2 = "test_operation_id2"
        operation_id: str | None = None
        intent, intent_result = TestReadIntent(), "intent_result1"
        command_result = "command_result"
        engine = OperationEngineImpl(command_defs=[CommandDef(TestCommand, test_command_handler)])

        assert_generator(
            generator=engine.execute(TestCommand(), CommandContextIn(operation_id_in1)),
            steps=[YeildStep(CallIntentHandlerInstruction(IntentContext(ANY), intent), intent_result)],
            return_value=command_result,
        )
        assert_generator(
            generator=engine.execute(TestCommand(), CommandContextIn(operation_id_in2)),
            steps=[YeildStep(CallIntentHandlerInstruction(IntentContext(ANY), intent), intent_result)],
            return_value=command_result,
        )

        assert operation_id is not None

    def test_generates_different_operation_id_when_operation_id_not_passed(self) -> None:
        def test_command_handler(context: CommandContext, command: TestCommand) -> CommandGenerator[str]:
            assert context.operation_id not in operation_ids
            operation_ids.append(context.operation_id)
            intent_result = yield intent
            assert intent_result == intent_result
            return command_result

        operation_ids: list[str] = []
        intent, intent_result = TestReadIntent(), "intent_result1"
        command_result = "command_result"
        engine = OperationEngineImpl(command_defs=[CommandDef(TestCommand, test_command_handler)])

        assert_generator(
            generator=engine.execute(TestCommand()),
            steps=[YeildStep(CallIntentHandlerInstruction(IntentContext(ANY), intent), intent_result)],
            return_value=command_result,
        )
        assert_generator(
            generator=engine.execute(TestCommand()),
            steps=[YeildStep(CallIntentHandlerInstruction(IntentContext(ANY), intent), intent_result)],
            return_value=command_result,
        )

        assert len(operation_ids) == 2

    def test_raises_command_not_defined_error_when_command_not_defined(self) -> None:
        engine = OperationEngineImpl(command_defs=[])
        with pytest.raises(CommandNotDefinedError) as exc_info:
            engine.execute(TestCommand()).send(None)

        assert exc_info.value.args == (TestCommand,)

    def test_yields_compensation_instructions_when_command_raises_error(self) -> None:
        test_context, test_command = CommandContext(operation_id=ANY), TestCommand()
        read_intent1, intent_result1 = TestReadIntent(), "intent_result1"
        write_intent1, intent_result2 = TestWriteIntent(), "intent_result2"
        read_intent2, intent_result3 = TestReadIntent(), "intent_result3"
        write_intent2, intent_result4 = TestWriteIntent(), "intent_result4"
        read_intent3, intent_result5 = TestReadIntent(), "intent_result5"
        command_result = "command_result"
        command_error = Exception("test_exception")
        command_handler_stub = GeneratorFunctionStub(
            args=(test_context, test_command),
            steps=[
                YeildStep(read_intent1, intent_result1),
                YeildStep(write_intent1, intent_result2),
                YeildStep(read_intent2, intent_result3),
                YeildStep(write_intent2, intent_result4),
                YeildStep(read_intent3, intent_result5),
                RaiseStep(command_error),
            ],
            return_value=command_result,
        )
        engine = OperationEngineImpl(command_defs=[CommandDef(TestCommand, command_handler_stub)])

        assert_generator(
            generator=engine.execute(test_command),
            steps=[
                YeildStep(CallIntentHandlerInstruction(IntentContext(ANY), read_intent1), intent_result1),
                YeildStep(CallIntentHandlerInstruction(IntentContext(ANY), write_intent1), intent_result2),
                YeildStep(CallIntentHandlerInstruction(IntentContext(ANY), read_intent2), intent_result3),
                YeildStep(CallIntentHandlerInstruction(IntentContext(ANY), write_intent2), intent_result4),
                YeildStep(CallIntentHandlerInstruction(IntentContext(ANY), read_intent3), intent_result5),
                YeildStep(
                    CallIntentCompensationHandlerInstruction(
                        IntentCompensationContext(ANY, IntentContext(ANY), intent_result4), write_intent2
                    ),
                    None,
                ),
                YeildStep(
                    CallIntentCompensationHandlerInstruction(
                        IntentCompensationContext(ANY, IntentContext(ANY), intent_result2), write_intent1
                    ),
                    None,
                ),
                RaiseStep(command_error),
            ],
        )

    def test_passes_original_context_and_result_into_compensation_instuction(self) -> None:
        test_context, test_command = CommandContext(operation_id=ANY), TestCommand()
        write_intent1, intent_result1 = TestWriteIntent(), "intent_result1"
        command_result = "command_result"
        command_error = Exception("test_exception")
        command_handler_stub = GeneratorFunctionStub(
            args=(test_context, test_command),
            steps=[
                YeildStep(write_intent1, intent_result1),
                RaiseStep(command_error),
            ],
            return_value=command_result,
        )
        engine = OperationEngineImpl(command_defs=[CommandDef(TestCommand, command_handler_stub)])

        execute_gen = engine.execute(test_command)
        instruction = execute_gen.send(None)
        assert isinstance(instruction, CallIntentHandlerInstruction)
        original_context = instruction.context
        instruction = execute_gen.send(intent_result1)
        assert isinstance(instruction, CallIntentCompensationHandlerInstruction)
        compensation_context = instruction.context
        assert compensation_context.original_context == original_context
        assert compensation_context.original_result == intent_result1
        try:
            execute_gen.send(None)
        except Exception as exc:
            assert exc == command_error


class TestComandDefinition:
    def test_raises_duplicate_command_def_error_when_define_commands_with_the_same_type(self) -> None:
        class OtherCommand(Command[str]):
            pass

        test_context, test_command = CommandContext(operation_id=ANY), TestCommand()
        duplicate_command_defs = [
            CommandDef(TestCommand, GeneratorFunctionStub(args=(test_context, test_command))),
            CommandDef(TestCommand, GeneratorFunctionStub(args=(test_context, test_command))),
        ]
        other_command_defs = [
            CommandDef(OtherCommand, GeneratorFunctionStub(args=(test_context, OtherCommand()))),
        ]

        with pytest.raises(DuplicateCommandDefError) as exc_info:
            OperationEngineImpl(command_defs=duplicate_command_defs + other_command_defs)

        assert exc_info.value.args == (duplicate_command_defs,)


class TestCommandMiddleware:
    def test_global_middlewares_calls_in_reverse_order(self) -> None:
        middlewares = Mock()
        middlewares.first = self._make_middleware_mock()
        middlewares.second = self._make_middleware_mock()
        middlewares.third = self._make_middleware_mock()
        test_context, test_command = CommandContext(operation_id=ANY), TestCommand()
        command_result = "command_result"
        command_handler_stub = GeneratorFunctionStub(
            args=(test_context, test_command),
            steps=[],
            return_value=command_result,
        )

        OperationEngineImpl(
            command_defs=[CommandDef(TestCommand, command_handler_stub)],
            command_middlewares=[middlewares.first, middlewares.second, middlewares.third],
        )

        expected_calls = [
            call.third(command_handler_stub),
            call.second(command_handler_stub),
            call.first(command_handler_stub),
        ]
        middlewares.assert_has_calls(expected_calls, any_order=False)

    def test_local_middlewares_calls_in_reverse_order(self) -> None:
        middlewares = Mock()
        middlewares.first = self._make_middleware_mock()
        middlewares.second = self._make_middleware_mock()
        middlewares.third = self._make_middleware_mock()
        test_context, test_command = CommandContext(operation_id=ANY), TestCommand()
        command_result = "command_result"
        command_handler_stub = GeneratorFunctionStub(
            args=(test_context, test_command),
            steps=[],
            return_value=command_result,
        )

        OperationEngineImpl(
            command_defs=[
                CommandDef(
                    TestCommand,
                    command_handler_stub,
                    middlewares=[middlewares.first, middlewares.second, middlewares.third],
                )
            ]
        )

        expected_calls = [
            call.third(command_handler_stub),
            call.second(command_handler_stub),
            call.first(command_handler_stub),
        ]
        middlewares.assert_has_calls(expected_calls, any_order=False)

    def test_local_middlewares_overrides_global_middlewares(self) -> None:
        global_middleware = self._make_middleware_mock()
        local_middleware = self._make_middleware_mock()
        test_context, test_command = CommandContext(operation_id=ANY), TestCommand()
        command_result = "command_result"
        command_handler_stub = GeneratorFunctionStub(
            args=(test_context, test_command),
            steps=[],
            return_value=command_result,
        )

        OperationEngineImpl(
            command_defs=[
                CommandDef(
                    TestCommand,
                    command_handler_stub,
                    middlewares=[local_middleware],
                )
            ],
            command_middlewares=[global_middleware],
        )

        global_middleware.assert_not_called()
        local_middleware.assert_called_once_with(command_handler_stub)

    def _make_middleware_mock(self) -> Mock:
        return Mock(side_effect=command_middleware_stub)
