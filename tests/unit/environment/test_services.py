from collections.abc import AsyncGenerator, MutableSequence

import pytest

from fidem.core.di.core import DependencyKey
from fidem.core.di.effects import DependencyLifetimeError, EffectDependencyDef, EffectDependencyLifetime
from fidem.core.environment import EffectDef, Environment, EnvironmentImpl, EnvironmentNotStartedError
from fidem.core.messages import MessageContext
from fidem.core.operation_engine.definitions import EventDef
from fidem.core.testing import YieldStep
from tests.effects import TestInternalEffect
from tests.messages import TestCommand, TestEvent
from tests.stubs import GeneratorFunctionStub
from tests.unit.environment.helpers import Config, Resource, make_instruction, make_operation_engine_mock

SERVICE = EffectDependencyLifetime.SERVICE
MESSAGE = EffectDependencyLifetime.MESSAGE
ENVIRONMENT_KEY = DependencyKey(name="environment", type=Environment)


class Service:
    pass


class OtherService:
    pass


class TestServiceLifecycle:
    async def test_starts_services_after_application_dependencies_and_stops_them_before(self) -> None:
        async def resource() -> AsyncGenerator[Resource]:
            events.append("start resource")
            yield Resource()
            events.append("stop resource")

        async def service() -> AsyncGenerator[Service]:
            events.append("start service")
            yield Service()
            events.append("stop service")

        events: MutableSequence[str] = []
        # The service is declared first and depends on nothing, so only the scopes order it after the resource.
        environment = EnvironmentImpl(
            operation_engine=make_operation_engine_mock(),
            dependency_defs=[EffectDependencyDef(service, lifetime=SERVICE), EffectDependencyDef(resource)],
        )

        async with environment:
            assert events == ["start resource", "start service"]

        assert events == ["start resource", "start service", "stop service", "stop resource"]

    async def test_starts_services_in_order_of_declaration_and_stops_them_in_reverse_order(self) -> None:
        async def service() -> AsyncGenerator[Service]:
            events.append("start service")
            yield Service()
            events.append("stop service")

        async def other_service() -> AsyncGenerator[OtherService]:
            events.append("start other")
            yield OtherService()
            events.append("stop other")

        events: MutableSequence[str] = []
        environment = EnvironmentImpl(
            operation_engine=make_operation_engine_mock(),
            dependency_defs=[
                EffectDependencyDef(service, lifetime=SERVICE),
                EffectDependencyDef(other_service, lifetime=SERVICE),
            ],
        )

        async with environment:
            pass

        assert events == ["start service", "start other", "stop other", "stop service"]

    async def test_passes_application_dependencies_and_other_services_into_service(self) -> None:
        async def config() -> AsyncGenerator[Config]:
            yield Config("value")

        async def service() -> AsyncGenerator[Service]:
            yield Service()

        async def other_service(config: Config, service: Service) -> AsyncGenerator[OtherService]:
            arguments.append((config, service))
            yield OtherService()

        arguments: MutableSequence[tuple[Config, Service]] = []
        environment = EnvironmentImpl(
            operation_engine=make_operation_engine_mock(),
            dependency_defs=[
                EffectDependencyDef(other_service, lifetime=SERVICE),
                EffectDependencyDef(service, lifetime=SERVICE),
                EffectDependencyDef(config),
            ],
        )

        async with environment:
            pass

        assert len(arguments) == 1
        assert arguments[0][0] == Config("value")
        assert isinstance(arguments[0][1], Service)

    async def test_lets_service_run_messages_while_it_starts_and_stops(self) -> None:
        async def handler(effect: TestInternalEffect) -> str:
            return "effect_result"

        async def service(environment: Environment) -> AsyncGenerator[Service]:
            results.append(await environment.handle(TestCommand()))
            yield Service()
            results.append(await environment.handle(TestCommand()))

        results: MutableSequence[str] = []
        environment = EnvironmentImpl(
            operation_engine=make_operation_engine_mock(
                GeneratorFunctionStub(steps=[YieldStep(make_instruction(), "effect_result")], return_value="result")
            ),
            effect_defs=[EffectDef(TestInternalEffect, handler)],
            dependency_defs=[EffectDependencyDef(service, lifetime=SERVICE)],
        )

        async with environment:
            pass

        assert results == ["result", "result"]

    async def test_stops_started_services_and_application_dependencies_when_service_fails_to_start(self) -> None:
        # The start error reaches the factories at their yield.
        async def resource() -> AsyncGenerator[Resource]:
            try:
                yield Resource()
            finally:
                events.append("stop resource")

        async def service() -> AsyncGenerator[Service]:
            try:
                yield Service()
            finally:
                events.append("stop service")

        async def failing_service() -> AsyncGenerator[OtherService]:
            raise ValueError("service_error")
            yield OtherService()  # pyright: ignore[reportUnreachable] - makes the factory an async generator

        events: MutableSequence[str] = []
        environment = EnvironmentImpl(
            operation_engine=make_operation_engine_mock(GeneratorFunctionStub()),
            dependency_defs=[
                EffectDependencyDef(resource),
                EffectDependencyDef(service, lifetime=SERVICE),
                EffectDependencyDef(failing_service, lifetime=SERVICE),
            ],
        )

        with pytest.raises(ValueError, match="service_error"):
            await environment.__aenter__()

        assert events == ["stop service", "stop resource"]
        with pytest.raises(EnvironmentNotStartedError):
            await environment.handle(TestCommand())

    async def test_stops_application_dependencies_when_service_fails_to_stop(self) -> None:
        async def resource() -> AsyncGenerator[Resource]:
            try:
                yield Resource()
            finally:
                events.append("stop resource")

        async def service() -> AsyncGenerator[Service]:
            yield Service()
            raise ValueError("service_error")

        events: MutableSequence[str] = []
        environment = EnvironmentImpl(
            operation_engine=make_operation_engine_mock(GeneratorFunctionStub()),
            dependency_defs=[EffectDependencyDef(resource), EffectDependencyDef(service, lifetime=SERVICE)],
        )

        with pytest.raises(ValueError, match="service_error"):
            async with environment:
                pass

        assert events == ["stop resource"]


class TestServiceAccess:
    def test_raises_dependency_lifetime_error_when_effect_handler_requires_service(self) -> None:
        async def service() -> AsyncGenerator[Service]:
            yield Service()

        async def handler(service: Service) -> None:
            pass

        with pytest.raises(DependencyLifetimeError) as exc_info:
            EnvironmentImpl(
                operation_engine=make_operation_engine_mock(),
                effect_defs=[EffectDef(TestInternalEffect, handler)],
                dependency_defs=[EffectDependencyDef(service, lifetime=SERVICE)],
            )

        assert exc_info.value.args == (handler, DependencyKey(name="", type=Service))

    def test_raises_dependency_lifetime_error_when_effect_handler_requires_environment(self) -> None:
        async def handler(environment: Environment) -> None:
            pass

        with pytest.raises(DependencyLifetimeError) as exc_info:
            EnvironmentImpl(
                operation_engine=make_operation_engine_mock(),
                effect_defs=[EffectDef(TestInternalEffect, handler)],
            )

        assert exc_info.value.args == (handler, ENVIRONMENT_KEY)

    def test_raises_dependency_lifetime_error_when_message_dependency_requires_environment(self) -> None:
        async def resource(environment: Environment) -> AsyncGenerator[Resource]:
            yield Resource()

        with pytest.raises(DependencyLifetimeError) as exc_info:
            EnvironmentImpl(
                operation_engine=make_operation_engine_mock(),
                dependency_defs=[EffectDependencyDef(resource, lifetime=MESSAGE)],
            )

        assert exc_info.value.args[1] == ENVIRONMENT_KEY

    def test_raises_dependency_lifetime_error_when_application_dependency_requires_service(self) -> None:
        async def service() -> AsyncGenerator[Service]:
            yield Service()

        async def resource(service: Service) -> AsyncGenerator[Resource]:
            yield Resource()

        with pytest.raises(DependencyLifetimeError):
            EnvironmentImpl(
                operation_engine=make_operation_engine_mock(),
                dependency_defs=[EffectDependencyDef(service, lifetime=SERVICE), EffectDependencyDef(resource)],
            )


class TestDeliver:
    async def test_runs_delivery_plan_of_engine_with_message_id_of_delivery(self) -> None:
        async def handler(message_context: MessageContext, effect: TestInternalEffect) -> str:
            message_contexts.append(message_context)
            return "effect_result"

        message_contexts: MutableSequence[MessageContext] = []
        event = TestEvent()
        operation_engine = make_operation_engine_mock(
            deliver_plan=GeneratorFunctionStub(steps=[YieldStep(make_instruction(), "effect_result")])
        )
        environment = EnvironmentImpl(
            operation_engine=operation_engine,
            effect_defs=[EffectDef(TestInternalEffect, handler)],
        )

        async with environment:
            await environment.deliver(event, "handler", "event:handler")

        operation_engine.deliver.assert_called_once_with(
            event=event,
            handler="handler",
            message_context=MessageContext(message_id="event:handler"),
        )
        assert message_contexts == [MessageContext(message_id="event:handler")]

    def test_exposes_event_defs_of_engine(self) -> None:
        event_defs = [EventDef("test", TestEvent)]

        environment = EnvironmentImpl(operation_engine=make_operation_engine_mock(event_defs=event_defs))

        assert environment.event_defs is event_defs
