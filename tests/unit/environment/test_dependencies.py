from asyncio import Event, timeout
from collections.abc import AsyncGenerator, Callable, Collection, MutableSequence
from dataclasses import dataclass
from typing import Any

import pytest

from fidem.core.di.core import DependencyNotFoundError
from fidem.core.di.effects import EffectDependencyDef, EffectDependencyLifetime
from fidem.core.effects import Effect, EffectContext
from fidem.core.environment import EffectDef, EnvironmentImpl
from fidem.core.messages import Command, MessageContext
from fidem.core.operation_engine.instructions import ExecutionPlan
from fidem.core.testing import YieldStep
from tests.effects import TestInternalEffect
from tests.messages import TestCommand
from tests.stubs import GeneratorFunctionStub
from tests.unit.environment.helpers import Resource, make_instruction, make_operation_engine_mock


@dataclass(frozen=True, slots=True)
class BuiltinDependencies:
    message: TestCommand
    message_context: MessageContext
    effect: Effect[Any]
    effect_context: EffectContext


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
            operation_engine=make_operation_engine_mock(GeneratorFunctionStub(steps=[YieldStep(instruction, None)])),
            effect_defs=[EffectDef(TestInternalEffect, handler)],
        )

        async with environment:
            await environment.handle(command, message_id="test_message_id")

        assert calls == [
            BuiltinDependencies(
                message=command,
                message_context=MessageContext(message_id="test_message_id"),
                effect=instruction.call.effect,
                effect_context=instruction.call.effect_context,
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
            dependency_defs=[EffectDependencyDef(resource, lifetime=EffectDependencyLifetime.MESSAGE)],
        )

        async with environment:
            await environment.handle(TestCommand(), message_id="message1")
            await environment.handle(TestCommand(), message_id="message2")

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
        async def resource() -> AsyncGenerator[Resource]:
            raise AssertionError("must not be resolved")
            yield Resource()  # pyright: ignore[reportUnreachable] - makes the factory an async generator

        environment = EnvironmentImpl(
            operation_engine=make_operation_engine_mock(GeneratorFunctionStub()),
            dependency_defs=[EffectDependencyDef(resource, lifetime=EffectDependencyLifetime.MESSAGE)],
        )

        async with environment:
            await environment.handle(TestCommand())

    async def test_closes_message_dependency_with_error_that_failed_message(self) -> None:
        async def resource() -> AsyncGenerator[Resource]:
            try:
                yield Resource()
            except ValueError as exc:
                errors.append(exc)
                raise

        async def handler(resource: Resource) -> None:
            pass

        def execution_plan(**_: object) -> ExecutionPlan[str]:
            yield make_instruction()
            raise message_error

        errors: MutableSequence[Exception] = []
        message_error = ValueError("message_error")
        environment = EnvironmentImpl(
            operation_engine=make_operation_engine_mock(execution_plan),
            effect_defs=[EffectDef(TestInternalEffect, handler)],
            dependency_defs=[EffectDependencyDef(resource, lifetime=EffectDependencyLifetime.MESSAGE)],
        )

        async with environment:
            with pytest.raises(ValueError, match="message_error"):
                await environment.handle(TestCommand())

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
            dependency_defs=[EffectDependencyDef(resource, lifetime=EffectDependencyLifetime.EFFECT)],
        )

        async with environment:
            await environment.handle(TestCommand())

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
            dependency_defs=[EffectDependencyDef(resource, lifetime=EffectDependencyLifetime.EFFECT)],
        )

        async with environment:
            with pytest.raises(ValueError, match="effect_error") as exc_info:
                await environment.handle(TestCommand())

        assert exc_info.value is effect_error
        assert errors == [effect_error]

    async def test_throws_effect_dependency_closing_error_into_execution_plan(self) -> None:
        async def resource() -> AsyncGenerator[Resource]:
            yield Resource()
            raise ValueError("closing_error")

        async def handler(resource: Resource) -> str:
            return "effect_result"

        def execution_plan(**_: object) -> ExecutionPlan[str]:
            try:
                yield make_instruction()
            except ValueError as exc:
                return str(exc)
            return "unreachable"

        environment = EnvironmentImpl(
            operation_engine=make_operation_engine_mock(execution_plan),
            effect_defs=[EffectDef(TestInternalEffect, handler)],
            dependency_defs=[EffectDependencyDef(resource, lifetime=EffectDependencyLifetime.EFFECT)],
        )

        async with environment:
            result = await environment.handle(TestCommand())

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
                GeneratorFunctionStub(steps=[YieldStep(make_instruction(), None)])
            ),
            effect_defs=[EffectDef(TestInternalEffect, handler)],
            dependency_defs=[
                EffectDependencyDef(
                    factory=resource,
                    lifetime=EffectDependencyLifetime.EFFECT,
                    name="resource",
                ),
                EffectDependencyDef(
                    factory=failing_resource,
                    lifetime=EffectDependencyLifetime.EFFECT,
                    name="failing_resource",
                ),
            ],
        )

        async with environment:
            with pytest.raises(ExceptionGroup) as exc_info:
                await environment.handle(TestCommand())

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
            dependency_defs=[EffectDependencyDef(resource, lifetime=EffectDependencyLifetime.EFFECT)],
        )

        async with environment:
            with pytest.raises(TimeoutError):
                async with timeout(0.01):
                    await environment.handle(TestCommand())

    async def test_raises_dependency_not_found_error_when_message_has_other_type(self) -> None:
        class OtherCommand(Command[str]):
            pass

        async def handler(message: OtherCommand) -> None:
            pass

        environment = self._make_environment(effect_count=1, handler=handler)

        async with environment:
            with pytest.raises(DependencyNotFoundError) as exc_info:
                await environment.handle(TestCommand())

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
                GeneratorFunctionStub(steps=[YieldStep(instruction, None) for instruction in instructions])
            ),
            effect_defs=[EffectDef(TestInternalEffect, handler)],
            dependency_defs=dependency_defs,
        )
