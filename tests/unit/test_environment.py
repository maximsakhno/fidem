from asyncio import Barrier, Event, gather, sleep, timeout
from collections.abc import AsyncGenerator, Awaitable, Callable, Collection, MutableSequence, Sequence
from dataclasses import dataclass
from typing import Any
from unittest.mock import Mock

import pytest

from fidem.di.core import DependencyNotFoundError, UnsupportedFunctionError
from fidem.di.effects import (
    AsyncFunctionEffectDependencyResolver,
    AsyncGeneratorFunctionEffectDependencyResolver,
    EffectDependencyDef,
    EffectDependencyLifetime,
    ScopeClosedError,
)
from fidem.effects import Effect, EffectContext, Effectful, InternalEffect
from fidem.environment import (
    CallNext,
    DuplicateEffectDefError,
    EffectCall,
    EffectDef,
    EffectMiddlewareDef,
    EffectNotDefinedError,
    EnvironmentAlreadyStartedError,
    EnvironmentImpl,
    EnvironmentNotStartedError,
)
from fidem.guards import GuardDef, GuardEffect
from fidem.messages import Command, MessageContext
from fidem.operation_engine import (
    CallEffectHandlerInstruction,
    ExecutionPlan,
    HandledEffect,
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


@dataclass(frozen=True, slots=True)
class EffectHandlerCall:
    context: EffectContext
    effect: Effect[Any]


@dataclass(frozen=True, slots=True)
class BuiltinDependencies:
    message: TestCommand
    message_context: MessageContext
    effect: Effect[Any]
    effect_context: EffectContext


class TestExecute:
    async def test_passes_command_and_message_context_into_operation_engine(self) -> None:
        command = TestCommand()
        operation_engine_mock = make_operation_engine_mock(GeneratorFunctionStub())

        async with EnvironmentImpl(operation_engine=operation_engine_mock) as environment:
            await environment.execute(command, "test_message_id")

        operation_engine_mock.execute.assert_called_once_with(
            command=command,
            message_context=MessageContext(message_id="test_message_id"),
        )

    async def test_passes_same_message_context_into_operation_engine_and_effect_handler(self) -> None:
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

        assert message_contexts == [operation_engine_mock.execute.call_args.kwargs["message_context"]]
        assert message_contexts[0] is operation_engine_mock.execute.call_args.kwargs["message_context"]

    async def test_generates_different_message_ids_when_message_id_not_passed(self) -> None:
        operation_engine_mock = make_operation_engine_mock(GeneratorFunctionStub())

        async with EnvironmentImpl(operation_engine=operation_engine_mock) as environment:
            await environment.execute(TestCommand())
            await environment.execute(TestCommand())

        message_ids = {
            call.kwargs["message_context"].message_id for call in operation_engine_mock.execute.call_args_list
        }
        assert len(message_ids) == 2

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

        assert calls == [EffectHandlerCall(context=instruction.effect_context, effect=instruction.effect)]

    async def test_throws_effect_handler_error_into_execution_plan(self) -> None:
        def execution_plan(command: TestCommand, message_context: MessageContext) -> ExecutionPlan[str]:
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
        def execution_plan(command: TestCommand, message_context: MessageContext) -> ExecutionPlan[str]:
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
                message=command,
                message_context=MessageContext(message_id="test_message_id"),
                effect=instruction.effect,
                effect_context=instruction.effect_context,
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

        def execution_plan(command: TestCommand, message_context: MessageContext) -> ExecutionPlan[str]:
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

        def execution_plan(command: TestCommand, message_context: MessageContext) -> ExecutionPlan[str]:
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
            await Event().wait()

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
                async with timeout(0.01):
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
        instructions = [make_instruction(effect_id=f"test_effect_id{index}") for index in range(effect_count)]
        return EnvironmentImpl(
            operation_engine=make_operation_engine_mock(
                GeneratorFunctionStub(steps=[YeildStep(instruction, None) for instruction in instructions])
            ),
            effect_defs=[EffectDef(TestInternalEffect, handler)],
            dependency_defs=dependency_defs,
        )


class TestGuards:
    async def test_executes_guard_effects_before_handler(self) -> None:
        def config(context: MessageContext) -> Effectful[Config, GuardEffect]:
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
            result = await environment.execute(TestCommand(), "test_message_id")

        assert result == "test_message_id effect_result"


class TestEffectMiddlewares:
    async def test_calls_middlewares_in_order_with_first_as_outermost(self) -> None:
        async def handler() -> str:
            events.append("handler")
            return "effect_result"

        events: MutableSequence[str] = []
        environment = self._make_environment(
            handler=handler,
            effect_middleware_defs=[
                self._make_middleware_def("first", events),
                self._make_middleware_def("second", events),
            ],
        )

        async with environment:
            result = await environment.execute(TestCommand())

        assert result == "effect_result"
        assert events == ["first before", "second before", "handler", "second after", "first after"]

    async def test_passes_message_and_effect_from_instruction_into_middleware(self) -> None:
        async def middleware(effect_call: EffectCall, call_next: CallNext) -> object:
            effect_calls.append(effect_call)
            return await call_next()

        async def handler() -> None:
            pass

        effect_calls: MutableSequence[EffectCall] = []
        instruction = make_instruction()
        environment = self._make_environment(
            handler=handler,
            effect_middleware_defs=[EffectMiddlewareDef(middleware)],
            instruction=instruction,
        )

        async with environment:
            await environment.execute(TestCommand())

        assert effect_calls == [
            EffectCall(
                message_context=instruction.message_context,
                message=instruction.message,
                effect_context=instruction.effect_context,
                effect=instruction.effect,
            )
        ]

    async def test_uses_effect_middlewares_instead_of_environment_middlewares(self) -> None:
        async def handler() -> None:
            events.append("handler")

        events: MutableSequence[str] = []
        environment = self._make_environment(
            handler=handler,
            effect_middleware_defs=[self._make_middleware_def("environment", events)],
            middleware_defs=[self._make_middleware_def("effect", events)],
        )

        async with environment:
            await environment.execute(TestCommand())

        assert events == ["effect before", "handler", "effect after"]

    async def test_calls_no_middlewares_when_effect_middlewares_are_empty(self) -> None:
        async def handler() -> None:
            events.append("handler")

        events: MutableSequence[str] = []
        environment = self._make_environment(
            handler=handler,
            effect_middleware_defs=[self._make_middleware_def("environment", events)],
            middleware_defs=(),
        )

        async with environment:
            await environment.execute(TestCommand())

        assert events == ["handler"]

    async def test_returns_middleware_result_without_resolving_dependencies_and_calling_handler(self) -> None:
        async def middleware(effect_call: EffectCall, call_next: CallNext) -> object:
            return "middleware_result"

        async def resource() -> Resource:
            raise AssertionError("must not be resolved")

        async def handler(resource: Resource) -> str:
            raise AssertionError("must not be called")

        environment = self._make_environment(
            handler=handler,
            effect_middleware_defs=[EffectMiddlewareDef(middleware)],
            dependency_defs=[
                EffectDependencyDef(
                    AsyncFunctionEffectDependencyResolver(resource), lifetime=EffectDependencyLifetime.EFFECT
                )
            ],
        )

        async with environment:
            result = await environment.execute(TestCommand())

        assert result == "middleware_result"

    async def test_resolves_effect_dependencies_on_each_call_and_message_dependencies_once(self) -> None:
        async def middleware(effect_call: EffectCall, call_next: CallNext) -> object:
            await call_next()
            return await call_next()

        async def message_resource() -> AsyncGenerator[Resource]:
            events.append("start message")
            yield Resource()
            events.append("stop message")

        async def effect_resource(message_resource: Resource) -> AsyncGenerator[Config]:
            events.append("start effect")
            yield Config("effect")
            events.append("stop effect")

        async def handler(message_resource: Resource, effect_resource: Config) -> None:
            events.append("handler")

        events: MutableSequence[str] = []
        environment = self._make_environment(
            handler=handler,
            effect_middleware_defs=[EffectMiddlewareDef(middleware)],
            dependency_defs=[
                EffectDependencyDef(
                    resolver=AsyncGeneratorFunctionEffectDependencyResolver(message_resource),
                    lifetime=EffectDependencyLifetime.MESSAGE,
                ),
                EffectDependencyDef(
                    resolver=AsyncGeneratorFunctionEffectDependencyResolver(effect_resource),
                    lifetime=EffectDependencyLifetime.EFFECT,
                ),
            ],
        )

        async with environment:
            await environment.execute(TestCommand())

        assert events == [
            "start message",
            "start effect",
            "handler",
            "stop effect",
            "start effect",
            "handler",
            "stop effect",
            "stop message",
        ]

    async def test_passes_dependency_resolution_error_into_middleware(self) -> None:
        async def resource() -> Resource:
            raise ValueError("resolution_error")

        async def handler(resource: Resource) -> None:
            raise AssertionError("must not be called")

        environment = self._make_environment(
            handler=handler,
            effect_middleware_defs=[EffectMiddlewareDef(self._catch_value_error)],
            dependency_defs=[
                EffectDependencyDef(
                    AsyncFunctionEffectDependencyResolver(resource), lifetime=EffectDependencyLifetime.EFFECT
                )
            ],
        )

        async with environment:
            result = await environment.execute(TestCommand())

        assert result == "resolution_error"

    async def test_passes_effect_dependency_closing_error_into_middleware(self) -> None:
        async def resource() -> AsyncGenerator[Resource]:
            yield Resource()
            raise ValueError("closing_error")

        async def handler(resource: Resource) -> str:
            return "effect_result"

        environment = self._make_environment(
            handler=handler,
            effect_middleware_defs=[EffectMiddlewareDef(self._catch_value_error)],
            dependency_defs=[
                EffectDependencyDef(
                    AsyncGeneratorFunctionEffectDependencyResolver(resource), lifetime=EffectDependencyLifetime.EFFECT
                )
            ],
        )

        async with environment:
            result = await environment.execute(TestCommand())

        assert result == "closing_error"

    async def test_throws_middleware_error_into_execution_plan(self) -> None:
        async def middleware(effect_call: EffectCall, call_next: CallNext) -> object:
            raise ValueError("middleware_error")

        def execution_plan(command: TestCommand, message_context: MessageContext) -> ExecutionPlan[str]:
            try:
                yield make_instruction()
            except ValueError as exc:
                return str(exc)
            return "unreachable"

        async def handler() -> None:
            pass

        environment = EnvironmentImpl(
            operation_engine=make_operation_engine_mock(execution_plan),
            effect_defs=[EffectDef(TestInternalEffect, handler)],
            effect_middleware_defs=[EffectMiddlewareDef(middleware)],
        )

        async with environment:
            result = await environment.execute(TestCommand())

        assert result == "middleware_error"

    async def test_does_not_call_middlewares_when_effect_not_defined(self) -> None:
        async def handler() -> None:
            pass

        events: MutableSequence[str] = []
        environment = self._make_environment(
            handler=handler,
            effect_middleware_defs=[self._make_middleware_def("environment", events)],
            instruction=make_instruction(effect=TestExternalReadEffect()),
        )

        async with environment:
            with pytest.raises(EffectNotDefinedError):
                await environment.execute(TestCommand())

        assert events == []

    async def test_calls_middlewares_for_guard_effects(self) -> None:
        async def middleware(effect_call: EffectCall, call_next: CallNext) -> object:
            effects.append(effect_call.effect)
            return await call_next()

        def config() -> Effectful[Config, GuardEffect]:
            return Config((yield from TestInternalEffect()))

        async def handle_internal() -> str:
            return "config_value"

        def handler(config: Config) -> Effectful[str, Effect[Any]]:
            yield from ()
            return config.value

        effects: MutableSequence[Effect[Any]] = []
        environment = EnvironmentImpl(
            operation_engine=OperationEngineImpl(
                command_defs=[StagedCommandDef("test", TestCommand, handler)],
                guard_defs=[GuardDef(config)],
            ),
            effect_defs=[EffectDef(TestInternalEffect, handle_internal)],
            effect_middleware_defs=[EffectMiddlewareDef(middleware)],
        )

        async with environment:
            result = await environment.execute(TestCommand())

        assert result == "config_value"
        assert [type(effect) for effect in effects] == [TestInternalEffect]

    @pytest.mark.timeout(5)
    async def test_isolates_message_dependencies_of_concurrent_messages(self) -> None:
        async def middleware(effect_call: EffectCall, call_next: CallNext) -> object:
            await barrier.wait()
            return await call_next()

        async def resource(message_context: MessageContext) -> Config:
            return Config(message_context.message_id)

        async def handler(resource: Config) -> str:
            return resource.value

        barrier = Barrier(2)
        environment = self._make_environment(
            handler=handler,
            effect_middleware_defs=[EffectMiddlewareDef(middleware)],
            dependency_defs=[
                EffectDependencyDef(
                    AsyncFunctionEffectDependencyResolver(resource), lifetime=EffectDependencyLifetime.MESSAGE
                )
            ],
        )

        async with environment:
            results = await gather(
                environment.execute(TestCommand(), "message1"),
                environment.execute(TestCommand(), "message2"),
            )

        assert results == ["message1", "message2"]

    async def test_resolves_message_dependency_once_when_middleware_calls_handler_concurrently(self) -> None:
        async def middleware(effect_call: EffectCall, call_next: CallNext) -> object:
            results = await gather(call_next(), call_next())
            return results[0]

        async def resource() -> AsyncGenerator[Resource]:
            events.append("start")
            await sleep(0)
            yield Resource()
            events.append("stop")

        async def handler(resource: Resource) -> None:
            resources.append(resource)

        events: MutableSequence[str] = []
        resources: MutableSequence[Resource] = []
        environment = self._make_environment(
            handler=handler,
            effect_middleware_defs=[EffectMiddlewareDef(middleware)],
            dependency_defs=[
                EffectDependencyDef(
                    resolver=AsyncGeneratorFunctionEffectDependencyResolver(resource),
                    lifetime=EffectDependencyLifetime.MESSAGE,
                )
            ],
        )

        async with environment:
            await environment.execute(TestCommand())

        assert events == ["start", "stop"]
        assert resources[0] is resources[1]

    async def test_raises_scope_closed_error_and_closes_dependency_when_handler_called_after_message(self) -> None:
        async def middleware(effect_call: EffectCall, call_next: CallNext) -> object:
            calls_next.append(call_next)
            return None

        async def resource() -> AsyncGenerator[Resource]:
            events.append("start")
            try:
                yield Resource()
            finally:
                events.append("stop")

        async def handler(resource: Resource) -> None:
            raise AssertionError("must not be called")

        calls_next: MutableSequence[CallNext] = []
        events: MutableSequence[str] = []
        environment = self._make_environment(
            handler=handler,
            effect_middleware_defs=[EffectMiddlewareDef(middleware)],
            dependency_defs=[
                EffectDependencyDef(
                    resolver=AsyncGeneratorFunctionEffectDependencyResolver(resource),
                    lifetime=EffectDependencyLifetime.MESSAGE,
                )
            ],
        )

        async with environment:
            await environment.execute(TestCommand())
            with pytest.raises(ScopeClosedError):
                await calls_next[0]()

        assert events == ["start", "stop"]

    def test_raises_unsupported_function_error_when_middleware_is_not_async(self) -> None:
        def middleware(effect_call: EffectCall, call_next: CallNext) -> Awaitable[object]:
            return call_next()

        async def handler() -> None:
            pass

        with pytest.raises(UnsupportedFunctionError) as exc_info:
            self._make_environment(handler=handler, effect_middleware_defs=[EffectMiddlewareDef(middleware)])

        assert exc_info.value.args == (middleware,)

    def _make_environment(
        self,
        handler: Callable[..., Any],
        effect_middleware_defs: Sequence[EffectMiddlewareDef] = (),
        middleware_defs: Sequence[EffectMiddlewareDef] | None = None,
        dependency_defs: Collection[EffectDependencyDef[Any]] = (),
        instruction: CallEffectHandlerInstruction[Any] | None = None,
    ) -> EnvironmentImpl:
        """Makes an environment whose command yields one effect and returns its result."""

        def execution_plan(command: TestCommand, message_context: MessageContext) -> ExecutionPlan[object]:
            return (yield make_instruction() if instruction is None else instruction)

        return EnvironmentImpl(
            operation_engine=make_operation_engine_mock(execution_plan),
            effect_defs=[EffectDef(TestInternalEffect, handler, middleware_defs)],
            dependency_defs=dependency_defs,
            effect_middleware_defs=effect_middleware_defs,
        )

    def _make_middleware_def(self, name: str, events: MutableSequence[str]) -> EffectMiddlewareDef:
        async def middleware(effect_call: EffectCall, call_next: CallNext) -> object:
            events.append(f"{name} before")
            result = await call_next()
            events.append(f"{name} after")
            return result

        return EffectMiddlewareDef(middleware)

    @staticmethod
    async def _catch_value_error(effect_call: EffectCall, call_next: CallNext) -> object:
        try:
            return await call_next()
        except ValueError as exc:
            return str(exc)


def make_operation_engine_mock(execution_plan: Callable[..., ExecutionPlan[Any]] | None = None) -> Mock:
    operation_engine_mock = Mock(spec=OperationEngine)
    operation_engine_mock.execute.side_effect = execution_plan
    return operation_engine_mock


def make_instruction(
    effect: HandledEffect[str] | None = None,
    effect_id: str = "test_effect_id",
) -> CallEffectHandlerInstruction[str]:
    return CallEffectHandlerInstruction(
        message_context=MessageContext(message_id="test_message_id"),
        message=TestCommand(),
        effect_context=EffectContext(effect_id=effect_id),
        effect=TestInternalEffect() if effect is None else effect,
    )


def make_failing_handler(error: Exception) -> Callable[..., Any]:
    async def handler(effect: TestInternalEffect) -> str:
        raise error

    return handler
