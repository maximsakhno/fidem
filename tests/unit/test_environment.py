import asyncio
from collections.abc import AsyncGenerator, Callable, Collection, MutableSequence
from dataclasses import dataclass
from typing import Any, NamedTuple
from unittest.mock import ANY, Mock

import pytest

from fidem.di.core import MESSAGE_CONTEXT_KEY, DependencyNotFoundError, Scope, ScopeNotAvailableError
from fidem.di.effects import (
    AsyncFunctionEffectDependencyResolver,
    AsyncGeneratorFunctionEffectDependencyResolver,
    EffectDependencyDef,
    EffectDependencyLifetime,
)
from fidem.di.messages import (
    DependencyEffect,
    GeneratorFunctionMessageDependencyResolver,
    MessageDependencyDef,
    MessageDependencyLifetime,
)
from fidem.effects import Effect, EffectContext, Effectful, InternalEffect
from fidem.environment import (
    DuplicateEffectDefError,
    EffectDef,
    EffectNotDefinedError,
    EnvironmentAlreadyStartedError,
    EnvironmentImpl,
    EnvironmentNotStartedError,
)
from fidem.messages import Command, MessageContext
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


class Resource:
    pass


@dataclass(frozen=True, slots=True)
class Config:
    value: str


class EffectHandlerCall(NamedTuple):
    context: EffectContext
    effect: Effect[Any]


class BuiltinDependencies(NamedTuple):
    message: TestCommand
    message_context: MessageContext
    effect: Effect[Any]
    effect_context: EffectContext


class TestExecute:
    async def test_passes_command_and_message_id_into_operation_engine(self) -> None:
        command = TestCommand()
        message_id = "test_message_id"
        operation_engine_mock = make_operation_engine_mock(GeneratorFunctionStub())

        async with EnvironmentImpl(operation_engine=operation_engine_mock) as environment:
            await environment.execute(command, message_id)

        operation_engine_mock.execute.assert_called_once_with(
            command=command,
            message_id=message_id,
            application_scope=ANY,
        )

    async def test_passes_generated_message_id_into_operation_engine_and_message_context(self) -> None:
        async def handler(message_context: MessageContext) -> None:
            message_contexts.append(message_context)

        message_contexts: MutableSequence[MessageContext] = []
        operation_engine_mock = make_operation_engine_mock(
            GeneratorFunctionStub(steps=[YeildStep(make_instruction(), None)])
        )
        environment = EnvironmentImpl(
            operation_engine=operation_engine_mock,
            effect_defs=[EffectDef(TestInternalEffect, handler)],
        )

        async with environment:
            await environment.execute(TestCommand())

        message_id = operation_engine_mock.execute.call_args.kwargs["message_id"]
        assert message_id is not None
        assert message_contexts == [MessageContext(message_id=message_id)]

    async def test_returns_execution_plan_result(self) -> None:
        execution_plan_result = "execution_plan_result"
        operation_engine_mock = make_operation_engine_mock(GeneratorFunctionStub(return_value=execution_plan_result))

        async with EnvironmentImpl(operation_engine=operation_engine_mock) as environment:
            result = await environment.execute(TestCommand())

        assert result == execution_plan_result

    async def test_calls_effect_handler_and_sends_its_result_into_execution_plan(self) -> None:
        async def handler(context: EffectContext, effect: TestInternalEffect) -> str:
            calls.append(EffectHandlerCall(context=context, effect=effect))
            return effect_result

        calls: MutableSequence[EffectHandlerCall] = []
        instruction = make_instruction()
        effect_result = "effect_result"
        operation_engine_mock = make_operation_engine_mock(
            GeneratorFunctionStub(steps=[YeildStep(instruction, effect_result)])
        )
        environment = EnvironmentImpl(
            operation_engine=operation_engine_mock,
            effect_defs=[EffectDef(TestInternalEffect, handler)],
        )

        async with environment:
            await environment.execute(TestCommand())

        assert calls == [EffectHandlerCall(context=instruction.context, effect=instruction.effect)]

    async def test_throws_effect_handler_error_into_execution_plan(self) -> None:
        def execution_plan(
            command: TestCommand, application_scope: Scope, message_id: str | None
        ) -> ExecutionPlan[str]:
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
            result = await environment.execute(TestCommand())

        assert result == "effect_error"

    async def test_raises_effect_handler_error_when_execution_plan_does_not_handle_it(self) -> None:
        effect_error = ValueError("effect_error")
        environment = EnvironmentImpl(
            operation_engine=make_operation_engine_mock(
                GeneratorFunctionStub(steps=[YeildStep(make_instruction(), None)])
            ),
            effect_defs=[EffectDef(TestInternalEffect, make_failing_handler(effect_error))],
        )

        async with environment:
            with pytest.raises(ValueError, match="effect_error") as exc_info:
                await environment.execute(TestCommand())

        assert exc_info.value is effect_error

    async def test_does_not_throw_effect_not_defined_error_into_execution_plan(self) -> None:
        def execution_plan(
            command: TestCommand, application_scope: Scope, message_id: str | None
        ) -> ExecutionPlan[str]:
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
                await environment.execute(TestCommand())

        assert is_execution_plan_closed

    async def test_raises_effect_not_defined_error_when_effect_not_defined(self) -> None:
        operation_engine_mock = make_operation_engine_mock(
            GeneratorFunctionStub(steps=[YeildStep(make_instruction(), None)])
        )

        async with EnvironmentImpl(operation_engine=operation_engine_mock) as environment:
            with pytest.raises(EffectNotDefinedError) as exc_info:
                await environment.execute(TestCommand())

        assert exc_info.value.args == (TestInternalEffect,)

    async def test_raises_not_implemented_error_when_instruction_not_supported(self) -> None:
        class UnknownInstruction(Instruction[None]):
            pass

        operation_engine_mock = make_operation_engine_mock(
            GeneratorFunctionStub(steps=[YeildStep(UnknownInstruction(), None)])
        )

        async with EnvironmentImpl(operation_engine=operation_engine_mock) as environment:
            with pytest.raises(NotImplementedError):
                await environment.execute(TestCommand())


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
            result = await environment.execute(TestCommand())

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
            await environment.execute(TestCommand(), "test_message_id")
            await environment.execute(TestCommand(), "test_message_id")

        assert effect_ids[:2] == effect_ids[2:]
        assert len(set(effect_ids)) == 2


class TestEffectDefinition:
    def test_raises_duplicate_effect_def_error_when_defines_effects_with_same_type(self) -> None:
        class OtherEffect(InternalEffect[str]):
            pass

        handler = make_failing_handler(ValueError())
        duplicate_effect_defs = [EffectDef(TestInternalEffect, handler), EffectDef(TestInternalEffect, handler)]
        other_effect_defs = [EffectDef(OtherEffect, handler)]

        with pytest.raises(DuplicateEffectDefError) as exc_info:
            EnvironmentImpl(
                operation_engine=Mock(spec=OperationEngine),
                effect_defs=duplicate_effect_defs + other_effect_defs,
            )

        assert exc_info.value.args == (duplicate_effect_defs,)


class TestLifecycle:
    async def test_raises_environment_not_started_error_when_executes_before_start(self) -> None:
        environment = EnvironmentImpl(operation_engine=make_operation_engine_mock(GeneratorFunctionStub()))

        with pytest.raises(EnvironmentNotStartedError):
            await environment.execute(TestCommand())

    async def test_raises_environment_not_started_error_when_executes_after_stop(self) -> None:
        environment = EnvironmentImpl(operation_engine=make_operation_engine_mock(GeneratorFunctionStub()))
        async with environment:
            pass

        with pytest.raises(EnvironmentNotStartedError):
            await environment.execute(TestCommand())

    async def test_raises_environment_not_started_error_when_stops_before_start(self) -> None:
        environment = EnvironmentImpl(operation_engine=make_operation_engine_mock())

        with pytest.raises(EnvironmentNotStartedError):
            await environment.__aexit__(None, None, None)

    async def test_raises_environment_already_started_error_when_starts_twice(self) -> None:
        environment = EnvironmentImpl(operation_engine=make_operation_engine_mock())

        async with environment:
            with pytest.raises(EnvironmentAlreadyStartedError):
                await environment.__aenter__()

    async def test_resolves_application_dependencies_on_start_and_closes_them_on_stop(self) -> None:
        async def resource() -> AsyncGenerator[Resource]:
            events.append("start")
            yield Resource()
            events.append("stop")

        events: MutableSequence[str] = []
        environment = EnvironmentImpl(
            operation_engine=make_operation_engine_mock(),
            dependency_defs=[EffectDependencyDef(AsyncGeneratorFunctionEffectDependencyResolver(resource))],
        )

        async with environment:
            assert events == ["start"]

        assert events == ["start", "stop"]


class TestEffectHandlerDependencies:
    async def test_passes_builtin_dependencies_into_effect_handler(self) -> None:
        async def handler(
            message: TestCommand,
            message_context: MessageContext,
            effect: TestInternalEffect,
            effect_context: EffectContext,
        ) -> None:
            calls.append(BuiltinDependencies(message, message_context, effect, effect_context))

        calls: MutableSequence[BuiltinDependencies] = []
        command = TestCommand()
        instruction = make_instruction()
        environment = EnvironmentImpl(
            operation_engine=make_operation_engine_mock(GeneratorFunctionStub(steps=[YeildStep(instruction, None)])),
            effect_defs=[EffectDef(TestInternalEffect, handler)],
        )

        async with environment:
            await environment.execute(command, "test_message_id")

        assert calls == [
            BuiltinDependencies(
                command, MessageContext(message_id="test_message_id"), instruction.effect, instruction.context
            )
        ]

    async def test_resolves_message_dependency_once_per_message_and_closes_it_after_message(self) -> None:
        async def resource(message_context: MessageContext) -> AsyncGenerator[Resource]:
            events.append(f"start {message_context.message_id}")
            yield Resource()
            events.append(f"stop {message_context.message_id}")

        async def handler(resource: Resource) -> None:
            resources.append(resource)
            events.append("handler")

        events: MutableSequence[str] = []
        resources: MutableSequence[Resource] = []
        environment = self._make_environment(
            effect_count=2,
            handler=handler,
            dependency_defs=[
                EffectDependencyDef(
                    AsyncGeneratorFunctionEffectDependencyResolver(resource), lifetime=EffectDependencyLifetime.MESSAGE
                )
            ],
        )

        async with environment:
            await environment.execute(TestCommand(), "message1")
            await environment.execute(TestCommand(), "message2")

        assert events == [
            "start message1",
            "handler",
            "handler",
            "stop message1",
            "start message2",
            "handler",
            "handler",
            "stop message2",
        ]
        assert resources[0] is resources[1]
        assert resources[1] is not resources[2]

    async def test_does_not_resolve_message_dependency_when_no_effect_needs_it(self) -> None:
        async def resource() -> Resource:
            raise AssertionError("must not be resolved")

        environment = EnvironmentImpl(
            operation_engine=make_operation_engine_mock(GeneratorFunctionStub()),
            dependency_defs=[
                EffectDependencyDef(
                    AsyncFunctionEffectDependencyResolver(resource), lifetime=EffectDependencyLifetime.MESSAGE
                )
            ],
        )

        async with environment:
            await environment.execute(TestCommand())

    async def test_closes_message_dependency_with_error_that_failed_message(self) -> None:
        async def resource() -> AsyncGenerator[Resource]:
            try:
                yield Resource()
            except ValueError as exc:
                errors.append(exc)
                raise

        async def handler(resource: Resource) -> None:
            pass

        def execution_plan(
            command: TestCommand, application_scope: Scope, message_id: str | None
        ) -> ExecutionPlan[str]:
            yield make_instruction()
            raise message_error

        errors: MutableSequence[Exception] = []
        message_error = ValueError("message_error")
        environment = EnvironmentImpl(
            operation_engine=make_operation_engine_mock(execution_plan),
            effect_defs=[EffectDef(TestInternalEffect, handler)],
            dependency_defs=[
                EffectDependencyDef(
                    AsyncGeneratorFunctionEffectDependencyResolver(resource), lifetime=EffectDependencyLifetime.MESSAGE
                )
            ],
        )

        async with environment:
            with pytest.raises(ValueError, match="message_error"):
                await environment.execute(TestCommand())

        assert errors == [message_error]

    async def test_resolves_effect_dependency_for_each_effect_and_closes_it_after_handler(self) -> None:
        async def resource() -> AsyncGenerator[Resource]:
            events.append("start")
            yield Resource()
            events.append("stop")

        async def handler(resource: Resource) -> None:
            events.append("handler")

        events: MutableSequence[str] = []
        environment = self._make_environment(
            effect_count=2,
            handler=handler,
            dependency_defs=[
                EffectDependencyDef(
                    AsyncGeneratorFunctionEffectDependencyResolver(resource), lifetime=EffectDependencyLifetime.EFFECT
                )
            ],
        )

        async with environment:
            await environment.execute(TestCommand())

        assert events == ["start", "handler", "stop", "start", "handler", "stop"]

    async def test_closes_effect_dependency_with_effect_handler_error(self) -> None:
        async def resource() -> AsyncGenerator[Resource]:
            try:
                yield Resource()
            except ValueError as exc:
                errors.append(exc)
                raise

        errors: MutableSequence[Exception] = []
        effect_error = ValueError("effect_error")

        async def handler(resource: Resource) -> None:
            raise effect_error

        environment = self._make_environment(
            effect_count=1,
            handler=handler,
            dependency_defs=[
                EffectDependencyDef(
                    AsyncGeneratorFunctionEffectDependencyResolver(resource), lifetime=EffectDependencyLifetime.EFFECT
                )
            ],
        )

        async with environment:
            with pytest.raises(ValueError, match="effect_error") as exc_info:
                await environment.execute(TestCommand())

        assert exc_info.value is effect_error
        assert errors == [effect_error]

    async def test_throws_effect_dependency_closing_error_into_execution_plan(self) -> None:
        async def resource() -> AsyncGenerator[Resource]:
            yield Resource()
            raise ValueError("closing_error")

        async def handler(resource: Resource) -> str:
            return "effect_result"

        def execution_plan(
            command: TestCommand, application_scope: Scope, message_id: str | None
        ) -> ExecutionPlan[str]:
            try:
                yield make_instruction()
            except ValueError as exc:
                return str(exc)
            return "unreachable"

        environment = EnvironmentImpl(
            operation_engine=make_operation_engine_mock(execution_plan),
            effect_defs=[EffectDef(TestInternalEffect, handler)],
            dependency_defs=[
                EffectDependencyDef(
                    AsyncGeneratorFunctionEffectDependencyResolver(resource), lifetime=EffectDependencyLifetime.EFFECT
                )
            ],
        )

        async with environment:
            result = await environment.execute(TestCommand())

        assert result == "closing_error"

    async def test_raises_exception_group_when_effect_handler_and_dependency_closing_fail(self) -> None:
        closing_error = ValueError("closing_error")
        effect_error = ValueError("effect_error")

        async def resource() -> AsyncGenerator[Resource]:
            yield Resource()

        async def failing_resource() -> AsyncGenerator[Resource]:
            try:
                yield Resource()
            finally:
                raise closing_error

        async def handler(resource: Resource, failing_resource: Resource) -> None:
            raise effect_error

        environment = EnvironmentImpl(
            operation_engine=make_operation_engine_mock(
                GeneratorFunctionStub(steps=[YeildStep(make_instruction(), None)])
            ),
            effect_defs=[EffectDef(TestInternalEffect, handler)],
            dependency_defs=[
                EffectDependencyDef(
                    resolver=AsyncGeneratorFunctionEffectDependencyResolver(resource),
                    lifetime=EffectDependencyLifetime.EFFECT,
                    name="resource",
                ),
                EffectDependencyDef(
                    resolver=AsyncGeneratorFunctionEffectDependencyResolver(failing_resource),
                    lifetime=EffectDependencyLifetime.EFFECT,
                    name="failing_resource",
                ),
            ],
        )

        async with environment:
            with pytest.raises(ExceptionGroup) as exc_info:
                await environment.execute(TestCommand())

        assert exc_info.value.exceptions == (effect_error, closing_error)

    async def test_raises_timeout_error_when_cancelled_while_effect_dependency_closing_fails(self) -> None:
        async def resource() -> AsyncGenerator[Resource]:
            try:
                yield Resource()
            finally:
                raise ValueError("closing_error")

        async def handler(resource: Resource) -> None:
            await asyncio.Event().wait()

        environment = self._make_environment(
            effect_count=1,
            handler=handler,
            dependency_defs=[
                EffectDependencyDef(
                    AsyncGeneratorFunctionEffectDependencyResolver(resource), lifetime=EffectDependencyLifetime.EFFECT
                )
            ],
        )

        async with environment:
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.01):
                    await environment.execute(TestCommand())

    async def test_raises_dependency_not_found_error_when_message_has_other_type(self) -> None:
        class OtherCommand(Command[str]):
            pass

        async def handler(message: OtherCommand) -> None:
            pass

        environment = self._make_environment(effect_count=1, handler=handler)

        async with environment:
            with pytest.raises(DependencyNotFoundError) as exc_info:
                await environment.execute(TestCommand())

        assert exc_info.value.args == (handler, "message", OtherCommand)

    def _make_environment(
        self,
        effect_count: int,
        handler: Callable[..., Any],
        dependency_defs: Collection[EffectDependencyDef[Any]] = (),
    ) -> EnvironmentImpl:
        instructions = [
            CallEffectHandlerInstruction(
                context=EffectContext(effect_id=f"test_effect_id{index}"), effect=TestInternalEffect()
            )
            for index in range(effect_count)
        ]
        return EnvironmentImpl(
            operation_engine=make_operation_engine_mock(
                GeneratorFunctionStub(steps=[YeildStep(instruction, None) for instruction in instructions])
            ),
            effect_defs=[EffectDef(TestInternalEffect, handler)],
            dependency_defs=dependency_defs,
        )


class TestMessageDependencies:
    async def test_resolves_application_message_dependencies_with_effects_once_on_start(self) -> None:
        def config() -> Effectful[Config, DependencyEffect]:
            events.append("factory")
            return Config((yield from TestInternalEffect()))

        async def handle_internal(effect: TestInternalEffect) -> str:
            events.append("effect")
            return "config_value"

        def handler(config: Config) -> Effectful[str, Effect[Any]]:
            yield from ()
            return config.value

        events: MutableSequence[str] = []
        environment = EnvironmentImpl(
            operation_engine=OperationEngineImpl(
                command_defs=[StagedCommandDef("test", TestCommand, handler)],
                dependency_defs=[MessageDependencyDef(GeneratorFunctionMessageDependencyResolver(config))],
            ),
            effect_defs=[EffectDef(TestInternalEffect, handle_internal)],
        )

        async with environment:
            assert events == ["factory", "effect"]
            results = [await environment.execute(TestCommand()), await environment.execute(TestCommand())]

        assert results == ["config_value", "config_value"]
        assert events == ["factory", "effect"]

    async def test_executes_message_dependency_effects_before_handler(self) -> None:
        def config(context: MessageContext) -> Effectful[Config, DependencyEffect]:
            return Config(f"{context.message_id} {(yield from TestInternalEffect())}")

        async def handle_internal(message_context: MessageContext) -> str:
            return "effect_result"

        def handler(config: Config) -> Effectful[str, Effect[Any]]:
            yield from ()
            return config.value

        environment = EnvironmentImpl(
            operation_engine=OperationEngineImpl(
                command_defs=[StagedCommandDef("test", TestCommand, handler)],
                dependency_defs=[
                    MessageDependencyDef(
                        resolver=GeneratorFunctionMessageDependencyResolver(config),
                        lifetime=MessageDependencyLifetime.MESSAGE,
                    )
                ],
            ),
            effect_defs=[EffectDef(TestInternalEffect, handle_internal)],
        )

        async with environment:
            result = await environment.execute(TestCommand(), "test_message_id")

        assert result == "test_message_id effect_result"

    async def test_raises_scope_not_available_error_and_stops_when_start_effect_needs_message_scope(self) -> None:
        async def resource() -> AsyncGenerator[Resource]:
            events.append("start")
            try:
                yield Resource()
            finally:
                events.append("stop")

        def config() -> Effectful[Config, DependencyEffect]:
            return Config((yield from TestInternalEffect()))

        async def handle_internal(message_context: MessageContext) -> str:
            raise AssertionError("must not be called")

        events: MutableSequence[str] = []
        environment = EnvironmentImpl(
            operation_engine=OperationEngineImpl(
                dependency_defs=[MessageDependencyDef(GeneratorFunctionMessageDependencyResolver(config))],
            ),
            effect_defs=[EffectDef(TestInternalEffect, handle_internal)],
            dependency_defs=[EffectDependencyDef(AsyncGeneratorFunctionEffectDependencyResolver(resource))],
        )

        with pytest.raises(ScopeNotAvailableError) as exc_info:
            async with environment:
                pass

        assert exc_info.value.args == (EffectDependencyLifetime.MESSAGE, MESSAGE_CONTEXT_KEY)
        assert events == ["start", "stop"]


def make_operation_engine_mock(
    execution_plan: Callable[..., ExecutionPlan[Any]] | None = None,
    start_plan: Callable[[], ExecutionPlan[Scope]] | None = None,
) -> Mock:
    operation_engine_mock = Mock(spec=OperationEngine)
    operation_engine_mock.start.side_effect = start_plan or start_without_dependencies
    operation_engine_mock.execute.side_effect = execution_plan
    return operation_engine_mock


def start_without_dependencies() -> ExecutionPlan[Scope]:
    yield from ()
    return Scope()


def make_instruction() -> CallEffectHandlerInstruction[str]:
    return CallEffectHandlerInstruction(EffectContext(effect_id="test_effect_id"), TestInternalEffect())


def make_failing_handler(error: Exception) -> Callable[..., Any]:
    async def handler(effect: TestInternalEffect) -> str:
        raise error

    return handler
