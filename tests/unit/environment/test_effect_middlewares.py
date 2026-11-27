from asyncio import Barrier, Event, Future, ensure_future, gather, sleep
from collections.abc import AsyncGenerator, Awaitable, Callable, Collection, MutableSequence, Sequence
from typing import Any

import pytest

from fidem.di.core import UnsupportedFunctionError
from fidem.di.effects import EffectDependencyDef, EffectDependencyLifetime, ScopeClosedError
from fidem.effects import Effect, Effectful
from fidem.environment import CallNext, EffectDef, EffectMiddlewareDef, EffectNotDefinedError, EnvironmentImpl
from fidem.guards import GuardDef
from fidem.messages import MessageContext, QueryEffect
from fidem.operation_engine.definitions import StagedCommandDef
from fidem.operation_engine.engine import OperationEngineImpl
from fidem.operation_engine.instructions import CallEffectHandlerInstruction, EffectCall, ExecutionPlan
from tests.effects import TestExternalReadEffect, TestInternalEffect
from tests.messages import TestCommand
from tests.unit.environment.helpers import Config, Resource, make_instruction, make_operation_engine_mock


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
                message_context=instruction.call.message_context,
                message=instruction.call.message,
                effect_context=instruction.call.effect_context,
                effect=instruction.call.effect,
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

        async def resource() -> AsyncGenerator[Resource]:
            raise AssertionError("must not be resolved")
            yield Resource()  # pyright: ignore[reportUnreachable] - makes the factory an async generator

        async def handler(resource: Resource) -> str:
            raise AssertionError("must not be called")

        environment = self._make_environment(
            handler=handler,
            effect_middleware_defs=[EffectMiddlewareDef(middleware)],
            dependency_defs=[EffectDependencyDef(resource, lifetime=EffectDependencyLifetime.EFFECT)],
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
                    factory=message_resource,
                    lifetime=EffectDependencyLifetime.MESSAGE,
                ),
                EffectDependencyDef(
                    factory=effect_resource,
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
        async def resource() -> AsyncGenerator[Resource]:
            raise ValueError("resolution_error")
            yield Resource()  # pyright: ignore[reportUnreachable] - makes the factory an async generator

        async def handler(resource: Resource) -> None:
            raise AssertionError("must not be called")

        environment = self._make_environment(
            handler=handler,
            effect_middleware_defs=[EffectMiddlewareDef(self._catch_value_error)],
            dependency_defs=[EffectDependencyDef(resource, lifetime=EffectDependencyLifetime.EFFECT)],
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
            dependency_defs=[EffectDependencyDef(resource, lifetime=EffectDependencyLifetime.EFFECT)],
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

        def config() -> Effectful[Config, QueryEffect]:
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

        async def resource(message_context: MessageContext) -> AsyncGenerator[Config]:
            yield Config(message_context.message_id)

        async def handler(resource: Config) -> str:
            return resource.value

        barrier = Barrier(2)
        environment = self._make_environment(
            handler=handler,
            effect_middleware_defs=[EffectMiddlewareDef(middleware)],
            dependency_defs=[EffectDependencyDef(resource, lifetime=EffectDependencyLifetime.MESSAGE)],
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
                    factory=resource,
                    lifetime=EffectDependencyLifetime.MESSAGE,
                )
            ],
        )

        async with environment:
            await environment.execute(TestCommand())

        assert events == ["start", "stop"]
        assert resources[0] is resources[1]

    async def test_raises_scope_closed_error_without_creating_dependency_when_handler_called_after_message(
        self,
    ) -> None:
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
                    factory=resource,
                    lifetime=EffectDependencyLifetime.MESSAGE,
                )
            ],
        )

        async with environment:
            await environment.execute(TestCommand())
            with pytest.raises(ScopeClosedError):
                await calls_next[0]()

        assert events == []

    async def test_raises_scope_closed_error_when_handler_called_again_after_message(self) -> None:
        async def middleware(effect_call: EffectCall, call_next: CallNext) -> object:
            calls_next.append(call_next)
            return await call_next()

        async def resource() -> AsyncGenerator[Resource]:
            events.append("start")
            try:
                yield Resource()
            finally:
                events.append("stop")

        async def handler(resource: Resource) -> None:
            events.append("handler")

        calls_next: MutableSequence[CallNext] = []
        events: MutableSequence[str] = []
        environment = self._make_environment(
            handler=handler,
            effect_middleware_defs=[EffectMiddlewareDef(middleware)],
            dependency_defs=[
                EffectDependencyDef(
                    factory=resource,
                    lifetime=EffectDependencyLifetime.MESSAGE,
                )
            ],
        )

        async with environment:
            await environment.execute(TestCommand())
            with pytest.raises(ScopeClosedError):
                await calls_next[0]()

        assert events == ["start", "handler", "stop"]

    async def test_raises_scope_closed_error_and_closes_dependency_created_while_message_finished(self) -> None:
        async def middleware(effect_call: EffectCall, call_next: CallNext) -> object:
            calls.append(ensure_future(call_next()))
            await resource_started.wait()
            return None

        async def resource() -> AsyncGenerator[Resource]:
            events.append("start")
            resource_started.set()
            await message_finished.wait()
            try:
                yield Resource()
            finally:
                events.append("stop")

        async def handler(resource: Resource) -> None:
            raise AssertionError("must not be called")

        calls: MutableSequence[Future[object]] = []
        events: MutableSequence[str] = []
        resource_started = Event()
        message_finished = Event()
        environment = self._make_environment(
            handler=handler,
            effect_middleware_defs=[EffectMiddlewareDef(middleware)],
            dependency_defs=[
                EffectDependencyDef(
                    factory=resource,
                    lifetime=EffectDependencyLifetime.MESSAGE,
                )
            ],
        )

        async with environment:
            await environment.execute(TestCommand())
            message_finished.set()
            with pytest.raises(ScopeClosedError):
                await calls[0]

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
