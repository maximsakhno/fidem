import asyncio
from asyncio import Barrier, CancelledError, sleep, timeout
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Collection, MutableSequence, Sequence
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

import pytest

from fidem.di.core import (
    AmbiguousDependencyError,
    DependencyCycleError,
    DependencyKey,
    DependencyNotFoundError,
    DuplicateDependencyError,
    MissingAnnotationError,
    UnresolvedAnnotationError,
    UnsupportedFunctionError,
    UnsupportedParameterError,
    UnsupportedReturnTypeError,
)
from fidem.di.effects import (
    EFFECT_CONTEXT_KEY,
    EFFECT_KEY,
    DependencyFactory,
    DependencyLifetimeError,
    EffectDependencyContainer,
    EffectDependencyDef,
    EffectDependencyLifetime,
    EffectProvider,
    Scope,
)
from fidem.effects import Effect, EffectContext
from fidem.messages import Event, Message, MessageContext
from tests.effects import TestExternalReadEffect, TestInternalEffect
from tests.messages import TestCommand


class BaseService:
    pass


class Service(BaseService):
    pass


class OtherService:
    pass


class Config:
    pass


class Generic[T]:
    pass


@dataclass(frozen=True, slots=True)
class BuiltinDependencies:
    message: Message[Any]
    message_context: MessageContext
    effect: Effect[Any]
    effect_context: EffectContext


async def make_service() -> AsyncGenerator[Service]:
    yield Service()


async def make_other_service() -> AsyncGenerator[OtherService]:
    yield OtherService()


class TestEffectProvider:
    def test_takes_parameters_and_key_from_factory(self) -> None:
        async def service(other: OtherService, *, base: BaseService) -> AsyncGenerator[Service]:
            yield Service()

        provider = EffectProvider.make(EffectDependencyDef(service, lifetime=EffectDependencyLifetime.MESSAGE))

        assert provider.parameters == {"other": OtherService, "base": BaseService}
        assert provider.key == DependencyKey(name="", type=Service)
        assert provider.lifetime == EffectDependencyLifetime.MESSAGE

    def test_takes_key_type_and_name_from_dependency_def_when_set(self) -> None:
        provider = EffectProvider.make(EffectDependencyDef(make_service, name="service", type=BaseService))

        assert provider.key == DependencyKey(name="service", type=BaseService)

    def test_takes_dependency_type_from_async_iterator_item_type(self) -> None:
        async def service() -> AsyncIterator[Service]:
            yield Service()

        assert EffectProvider.make(EffectDependencyDef(service)).key.type is Service

    async def test_enters_factory_up_to_yield_and_finishes_it_on_exit(self) -> None:
        async def service(other: OtherService) -> AsyncGenerator[Service]:
            events.append("start")
            yield result
            events.append("stop")

        events: MutableSequence[str] = []
        result = Service()
        context_manager = EffectProvider.make(EffectDependencyDef(service)).factory(other=OtherService())

        dependency = await context_manager.__aenter__()
        events_after_enter = list(events)
        await context_manager.__aexit__(None, None, None)

        assert dependency is result
        assert events_after_enter == ["start"]
        assert events == ["start", "stop"]

    def test_raises_unsupported_return_type_error_when_return_type_is_not_async_generator(self) -> None:
        async def service() -> Service:  # pyright: ignore[reportInvalidTypeForm] - checks a wrong annotation
            yield Service()  # pyright: ignore[reportReturnType] - checks a wrong annotation

        with pytest.raises(UnsupportedReturnTypeError) as exc_info:
            EffectProvider.make(EffectDependencyDef(service))  # pyright: ignore[reportArgumentType] - checks a wrong annotation

        assert exc_info.value.args == (service, Service)

    def test_raises_unsupported_function_error_when_factory_is_async_function(self) -> None:
        async def service() -> Service:
            return Service()

        with pytest.raises(UnsupportedFunctionError) as exc_info:
            EffectProvider.make(EffectDependencyDef(service))  # pyright: ignore[reportArgumentType] - checks a wrong factory

        assert exc_info.value.args == (service,)

    def test_raises_unsupported_function_error_when_factory_is_plain_function(self) -> None:
        def service() -> Service:
            return Service()

        with pytest.raises(UnsupportedFunctionError) as exc_info:
            EffectProvider.make(EffectDependencyDef(service))  # pyright: ignore[reportArgumentType] - checks a wrong factory

        assert exc_info.value.args == (service,)

    def test_raises_missing_annotation_error_when_return_type_is_not_annotated(self) -> None:
        async def service():
            yield Service()

        with pytest.raises(MissingAnnotationError) as exc_info:
            EffectProvider.make(EffectDependencyDef(service))

        assert exc_info.value.args == (service, "return")

    def test_raises_missing_annotation_error_when_parameter_is_not_annotated(self) -> None:
        async def service(other) -> AsyncGenerator[Service]:  # pyright: ignore[reportMissingParameterType, reportUnknownParameterType] - checks a missing annotation
            yield Service()

        with pytest.raises(MissingAnnotationError) as exc_info:
            EffectProvider.make(EffectDependencyDef(service))

        assert exc_info.value.args == (service, "other")

    def test_raises_unresolved_annotation_error_when_annotation_refers_to_unknown_name(self) -> None:
        async def service(other: "UnknownService") -> AsyncGenerator[Service]:  # noqa: F821 # pyright: ignore[reportUndefinedVariable, reportUnknownParameterType] - checks an unresolved annotation
            yield Service()

        with pytest.raises(UnresolvedAnnotationError) as exc_info:
            EffectProvider.make(EffectDependencyDef(service))

        assert exc_info.value.args == (service,)

    @pytest.mark.parametrize("parameter", ["args", "kwargs", "positional"])
    def test_raises_unsupported_parameter_error_when_parameter_is_not_passed_by_name(self, parameter: str) -> None:
        async def with_args(*args: OtherService) -> AsyncGenerator[Service]:
            yield Service()

        async def with_kwargs(**kwargs: OtherService) -> AsyncGenerator[Service]:
            yield Service()

        async def with_positional(positional: OtherService, /) -> AsyncGenerator[Service]:
            yield Service()

        factories: dict[str, DependencyFactory[Service]] = {
            "args": with_args,
            "kwargs": with_kwargs,
            "positional": with_positional,
        }

        with pytest.raises(UnsupportedParameterError) as exc_info:
            EffectProvider.make(EffectDependencyDef(factories[parameter]))

        assert exc_info.value.args == (factories[parameter], parameter)


class TestScope:
    async def test_closes_dependencies_in_reverse_order(self) -> None:
        events: MutableSequence[str] = []
        scope = Scope({})
        await add_dependency(scope, make_recording_factory(events, "first"), DependencyKey(name="first", type=Service))
        await add_dependency(
            scope, make_recording_factory(events, "second"), DependencyKey(name="second", type=Service)
        )

        await scope.close(None)

        assert events == ["stop second", "stop first"]

    async def test_raises_single_closing_error_as_is(self) -> None:
        error = ValueError("closing_error")
        scope = Scope({})
        await add_dependency(scope, make_failing_factory(error))

        with pytest.raises(ValueError, match="closing_error") as exc_info:
            await scope.close(None)

        assert exc_info.value is error

    async def test_closes_all_dependencies_and_raises_exception_group_when_several_closings_fail(self) -> None:
        first_error = ValueError("first")
        second_error = ValueError("second")
        scope = Scope({})
        await add_dependency(scope, make_failing_factory(first_error), DependencyKey(name="first", type=Service))
        await add_dependency(scope, make_failing_factory(second_error), DependencyKey(name="second", type=Service))

        with pytest.raises(ExceptionGroup) as exc_info:
            await scope.close(None)

        assert exc_info.value.exceptions == (second_error, first_error)

    async def test_throws_error_into_factory_on_exit(self) -> None:
        async def service() -> AsyncGenerator[Service]:
            try:
                yield Service()
            except ValueError as exc:
                errors.append(exc)
                raise

        errors: MutableSequence[Exception] = []
        error = ValueError("error")
        scope = Scope({})
        await add_dependency(scope, service)

        with pytest.raises(ValueError, match="error"):
            async with scope:
                raise error

        assert errors == [error]

    async def test_does_not_raise_when_factory_suppresses_scope_error(self) -> None:
        async def service() -> AsyncGenerator[Service]:
            with suppress(ValueError):
                yield Service()

        scope = Scope({})
        await add_dependency(scope, service)

        await scope.close(ValueError("scope_error"))

    async def test_raises_runtime_error_when_factory_yields_twice(self) -> None:
        async def service() -> AsyncGenerator[Service]:
            yield Service()
            yield Service()

        scope = Scope({})
        await add_dependency(scope, service)

        with pytest.raises(RuntimeError):
            await scope.close(None)

    async def test_adds_closing_errors_as_notes_when_scope_is_cancelled(self) -> None:
        async def service() -> AsyncGenerator[Service]:
            try:
                yield Service()
            finally:
                raise ValueError("closing_error")

        cancellation = CancelledError()
        scope = Scope({})
        await add_dependency(scope, service)

        await scope.close(cancellation)

        assert cancellation.__notes__ == ["Failed to close a dependency: ValueError('closing_error')"]


class TestDependencyMatching:
    async def test_matches_dependency_by_type_when_it_is_the_only_candidate(self) -> None:
        async def handler(any_name: Service) -> None:
            calls.append(any_name)

        calls: MutableSequence[Service] = []
        await run_handler(handler, [EffectDependencyDef(make_service, name="service")])

        assert [type(call) for call in calls] == [Service]

    async def test_matches_dependency_by_base_class(self) -> None:
        async def handler(service: BaseService) -> None:
            calls.append(service)

        calls: MutableSequence[BaseService] = []
        await run_handler(handler, [EffectDependencyDef(make_service)])

        assert [type(call) for call in calls] == [Service]

    async def test_matches_dependency_by_name_when_several_have_matching_type(self) -> None:
        async def first() -> AsyncGenerator[Service]:
            yield Service()

        async def second() -> AsyncGenerator[Service]:
            yield second_service

        async def handler(second: BaseService) -> None:
            calls.append(second)

        calls: MutableSequence[BaseService] = []
        second_service = Service()
        await run_handler(
            handler=handler,
            dependency_defs=[
                EffectDependencyDef(first, name="first"),
                EffectDependencyDef(second, name="second", type=BaseService),
            ],
        )

        assert calls == [second_service]

    def test_raises_ambiguous_dependency_error_when_name_does_not_choose_one_dependency(self) -> None:
        async def base_service() -> AsyncGenerator[BaseService]:
            yield BaseService()

        async def handler(service: BaseService) -> None:
            pass

        container = EffectDependencyContainer(
            dependency_defs=[
                EffectDependencyDef(make_service, name="first"),
                EffectDependencyDef(base_service, name="second"),
            ],
            effect_types=[TestInternalEffect],
        )

        with pytest.raises(AmbiguousDependencyError) as exc_info:
            container.prepare_effect_handler(TestInternalEffect, handler)

        assert exc_info.value.args == (
            handler,
            "service",
            [DependencyKey(name="first", type=Service), DependencyKey(name="second", type=BaseService)],
        )

    def test_raises_dependency_not_found_error_when_no_dependency_has_matching_type(self) -> None:
        async def handler(service: Service) -> None:
            pass

        container = EffectDependencyContainer(
            dependency_defs=[EffectDependencyDef(make_other_service, name="service")],
            effect_types=[TestInternalEffect],
        )

        with pytest.raises(DependencyNotFoundError) as exc_info:
            container.prepare_effect_handler(TestInternalEffect, handler)

        assert exc_info.value.args == (handler, "service", Service)

    def test_does_not_match_dependency_by_object(self) -> None:
        async def handler(service: object) -> None:
            pass

        container = EffectDependencyContainer(dependency_defs=[EffectDependencyDef(make_service)])

        with pytest.raises(DependencyNotFoundError):
            container.prepare_effect_handler(TestInternalEffect, handler)

    async def test_matches_generic_alias_exactly(self) -> None:
        async def int_generic() -> AsyncGenerator[Generic[int]]:
            yield int_value

        async def str_generic() -> AsyncGenerator[Generic[str]]:
            yield Generic[str]()

        async def handler(generic: Generic[int]) -> None:
            calls.append(generic)

        calls: MutableSequence[Generic[int]] = []
        int_value = Generic[int]()
        await run_handler(
            handler=handler,
            dependency_defs=[EffectDependencyDef(int_generic), EffectDependencyDef(str_generic)],
        )

        assert calls == [int_value]

    def test_raises_dependency_not_found_error_when_generic_alias_does_not_match(self) -> None:
        async def int_generic() -> AsyncGenerator[Generic[int]]:
            yield Generic[int]()

        async def handler(generic: Generic[str]) -> None:
            pass

        container = EffectDependencyContainer(dependency_defs=[EffectDependencyDef(int_generic)])

        with pytest.raises(DependencyNotFoundError):
            container.prepare_effect_handler(TestInternalEffect, handler)

    async def test_matches_generic_parameterized_with_any_as_generic_class(self) -> None:
        async def int_generic() -> AsyncGenerator[Generic[int]]:
            yield int_value

        async def handler(generic: Generic[Any]) -> None:
            calls.append(generic)

        calls: MutableSequence[Generic[Any]] = []
        int_value = Generic[int]()
        await run_handler(handler, [EffectDependencyDef(int_generic, type=Generic)])

        assert calls == [int_value]


class TestBuiltinDependencies:
    async def test_matches_effect_by_its_base_class(self) -> None:
        async def handler(effect: Effect[Any]) -> None:
            calls.append(effect)

        calls: MutableSequence[Effect[Any]] = []
        effect = TestInternalEffect()
        await run_handler(handler, effect=effect)

        assert calls == [effect]

    async def test_does_not_match_message_by_its_base_class(self) -> None:
        async def handler(value: Effect[Any]) -> None:
            calls.append(value)

        calls: MutableSequence[Effect[Any]] = []
        effect = TestInternalEffect()
        await run_handler(handler, effect=effect)

        assert calls == [effect]

    async def test_matches_effect_message_by_its_type_as_effect(self) -> None:
        class EffectEvent(Event):
            pass

        async def handler(event: EffectEvent) -> None:
            calls.append(event)

        calls: MutableSequence[EffectEvent] = []
        effect = EffectEvent()
        await run_handler(handler, effect=effect)

        assert calls == [effect]

    async def test_matches_message_by_its_subclass(self) -> None:
        async def handler(command: TestCommand) -> None:
            calls.append(command)

        calls: MutableSequence[TestCommand] = []
        await run_handler(handler)

        assert calls == [MESSAGE]

    async def test_passes_builtin_dependencies_into_factories(self) -> None:
        async def service(
            message: Message[Any],
            message_context: MessageContext,
            effect: Effect[Any],
            effect_context: EffectContext,
        ) -> AsyncGenerator[Service]:
            calls.append(BuiltinDependencies(message, message_context, effect, effect_context))
            yield Service()

        async def handler(service: Service) -> None:
            pass

        calls: MutableSequence[BuiltinDependencies] = []
        effect = TestInternalEffect()
        await run_handler(
            handler=handler,
            dependency_defs=[EffectDependencyDef(service, lifetime=EffectDependencyLifetime.EFFECT)],
            effect=effect,
        )

        assert calls == [BuiltinDependencies(MESSAGE, MESSAGE_CONTEXT, effect, EFFECT_CONTEXT)]

    def test_raises_duplicate_dependency_error_when_dependency_has_builtin_key(self) -> None:
        async def effect_context() -> AsyncGenerator[EffectContext]:
            yield EffectContext(effect_id="test")

        with pytest.raises(DuplicateDependencyError) as exc_info:
            EffectDependencyContainer(dependency_defs=[EffectDependencyDef(effect_context, name="effect_context")])

        assert exc_info.value.args == (EFFECT_CONTEXT_KEY,)

    def test_raises_dependency_not_found_error_when_handler_effect_does_not_match_factory_effect(self) -> None:
        async def service(effect: TestExternalReadEffect) -> AsyncGenerator[Service]:
            yield Service()

        async def handler(service: Service) -> None:
            pass

        container = EffectDependencyContainer(
            dependency_defs=[EffectDependencyDef(service, lifetime=EffectDependencyLifetime.EFFECT)],
            effect_types=[TestInternalEffect, TestExternalReadEffect],
        )

        with pytest.raises(DependencyNotFoundError) as exc_info:
            container.prepare_effect_handler(TestInternalEffect, handler)

        assert exc_info.value.args == (
            DependencyKey(name="", type=Service),
            "effect",
            TestExternalReadEffect,
        )


class TestDependencyValidation:
    def test_raises_duplicate_dependency_error_when_dependencies_have_same_key(self) -> None:
        with pytest.raises(DuplicateDependencyError) as exc_info:
            EffectDependencyContainer(
                dependency_defs=[
                    EffectDependencyDef(make_service, name="service"),
                    EffectDependencyDef(make_service, name="service"),
                ],
            )

        assert exc_info.value.args == (DependencyKey(name="service", type=Service),)

    def test_raises_dependency_cycle_error_when_dependencies_depend_on_each_other(self) -> None:
        async def service(other: OtherService) -> AsyncGenerator[Service]:
            yield Service()

        async def other_service(service: Service) -> AsyncGenerator[OtherService]:
            yield OtherService()

        with pytest.raises(DependencyCycleError):
            EffectDependencyContainer(
                dependency_defs=[EffectDependencyDef(service), EffectDependencyDef(other_service)],
            )

    @pytest.mark.parametrize(
        argnames=("lifetime", "dependency_lifetime"),
        argvalues=[
            (EffectDependencyLifetime.APPLICATION, EffectDependencyLifetime.MESSAGE),
            (EffectDependencyLifetime.APPLICATION, EffectDependencyLifetime.EFFECT),
            (EffectDependencyLifetime.MESSAGE, EffectDependencyLifetime.EFFECT),
        ],
    )
    def test_raises_dependency_lifetime_error_when_dependency_depends_on_narrower_one(
        self,
        lifetime: EffectDependencyLifetime,
        dependency_lifetime: EffectDependencyLifetime,
    ) -> None:
        async def service(other: OtherService) -> AsyncGenerator[Service]:
            yield Service()

        with pytest.raises(DependencyLifetimeError) as exc_info:
            EffectDependencyContainer(
                dependency_defs=[
                    EffectDependencyDef(service, lifetime=lifetime),
                    EffectDependencyDef(make_other_service, lifetime=dependency_lifetime),
                ],
            )

        assert exc_info.value.args == (
            DependencyKey(name="", type=Service),
            DependencyKey(name="", type=OtherService),
        )

    def test_raises_dependency_lifetime_error_when_application_dependency_depends_on_effect(self) -> None:
        async def service(effect: TestInternalEffect) -> AsyncGenerator[Service]:
            yield Service()

        with pytest.raises(DependencyLifetimeError):
            EffectDependencyContainer(
                dependency_defs=[EffectDependencyDef(service)],
                effect_types=[TestInternalEffect],
            )

    def test_raises_dependency_lifetime_error_when_application_dependency_depends_on_message(self) -> None:
        async def service(message_context: MessageContext) -> AsyncGenerator[Service]:
            yield Service()

        with pytest.raises(DependencyLifetimeError):
            EffectDependencyContainer(dependency_defs=[EffectDependencyDef(service)])

    def test_validates_dependencies_not_used_by_effect_handlers(self) -> None:
        async def service(other: OtherService) -> AsyncGenerator[Service]:
            yield Service()

        with pytest.raises(DependencyNotFoundError):
            EffectDependencyContainer(
                dependency_defs=[EffectDependencyDef(service, lifetime=EffectDependencyLifetime.EFFECT)],
            )

    def test_accepts_unused_dependency_on_effect_when_some_effect_type_matches(self) -> None:
        async def service(effect: TestExternalReadEffect) -> AsyncGenerator[Service]:
            yield Service()

        EffectDependencyContainer(
            dependency_defs=[EffectDependencyDef(service, lifetime=EffectDependencyLifetime.EFFECT)],
            effect_types=[TestInternalEffect, TestExternalReadEffect],
        )

    def test_raises_dependency_not_found_error_when_unused_dependency_needs_unknown_effect(self) -> None:
        async def service(effect: TestExternalReadEffect) -> AsyncGenerator[Service]:
            yield Service()

        with pytest.raises(DependencyNotFoundError):
            EffectDependencyContainer(
                dependency_defs=[EffectDependencyDef(service, lifetime=EffectDependencyLifetime.EFFECT)],
                effect_types=[TestInternalEffect],
            )

    def test_raises_unsupported_function_error_when_effect_handler_is_not_async(self) -> None:
        def handler() -> None:
            pass

        with pytest.raises(UnsupportedFunctionError):
            EffectDependencyContainer().prepare_effect_handler(TestInternalEffect, handler)  # pyright: ignore[reportArgumentType] - checks a wrong handler


class TestEffectHandlerResolution:
    async def test_resolves_dependencies_in_topological_order(self) -> None:
        async def base() -> AsyncGenerator[Config]:
            events.append("base")
            yield Config()

        async def other(base: Config) -> AsyncGenerator[OtherService]:
            events.append("other")
            yield OtherService()

        async def service(other: OtherService, base: Config) -> AsyncGenerator[Service]:
            events.append("service")
            yield Service()

        async def handler(service: Service, base: Config) -> None:
            events.append("handler")

        events: MutableSequence[str] = []
        await run_handler(
            handler=handler,
            dependency_defs=[
                EffectDependencyDef(service, lifetime=EffectDependencyLifetime.EFFECT),
                EffectDependencyDef(other, lifetime=EffectDependencyLifetime.MESSAGE),
                EffectDependencyDef(base, lifetime=EffectDependencyLifetime.MESSAGE),
            ],
        )

        assert events == ["base", "other", "service", "handler"]

    async def test_does_not_resolve_message_dependency_again_when_it_is_in_scope(self) -> None:
        async def service() -> AsyncGenerator[Service]:
            events.append("service")
            yield Service()

        async def handler(service: Service) -> None:
            pass

        events: MutableSequence[str] = []
        container = EffectDependencyContainer(
            dependency_defs=[EffectDependencyDef(service, lifetime=EffectDependencyLifetime.MESSAGE)],
        )
        prepared_handler = container.prepare_effect_handler(TestInternalEffect, handler)
        scopes = make_scopes(TestInternalEffect())

        await prepared_handler.resolve(scopes)
        await prepared_handler.resolve([scopes[0], scopes[1], make_scopes(TestInternalEffect())[2]])

        assert events == ["service"]

    async def test_does_not_resolve_application_dependency_for_handler(self) -> None:
        async def handler(service: Service) -> None:
            pass

        container = EffectDependencyContainer(dependency_defs=[EffectDependencyDef(make_service)])

        prepared_handler = container.prepare_effect_handler(TestInternalEffect, handler)

        assert prepared_handler.steps == ()


class TestApplicationLifecycle:
    @pytest.mark.timeout(5)
    async def test_resolves_independent_dependencies_concurrently(self) -> None:
        barrier = Barrier(2)

        async def service() -> AsyncGenerator[Service]:
            await barrier.wait()
            yield Service()

        async def other_service() -> AsyncGenerator[OtherService]:
            await barrier.wait()
            yield OtherService()

        container = EffectDependencyContainer(
            dependency_defs=[EffectDependencyDef(service), EffectDependencyDef(other_service)],
        )

        async with timeout(1):
            scope = await container.start()

        assert DependencyKey(name="", type=Service) in scope.dependencies
        assert DependencyKey(name="", type=OtherService) in scope.dependencies

    async def test_resolves_dependency_after_its_dependencies_and_closes_it_before_them(self) -> None:
        async def base() -> AsyncGenerator[Config]:
            events.append("start base")
            await sleep(0)
            yield Config()
            events.append("stop base")

        async def other(base: Config) -> AsyncGenerator[OtherService]:
            events.append("start other")
            yield OtherService()
            await sleep(0)
            events.append("stop other")

        async def service(other: OtherService, base: Config) -> AsyncGenerator[Service]:
            events.append("start service")
            yield Service()
            events.append("stop service")

        events: MutableSequence[str] = []
        container = EffectDependencyContainer(
            dependency_defs=[EffectDependencyDef(service), EffectDependencyDef(other), EffectDependencyDef(base)],
        )

        scope = await container.start()
        await container.stop(scope, None)

        assert events == [
            "start base",
            "start other",
            "start service",
            "stop service",
            "stop other",
            "stop base",
        ]

    @pytest.mark.timeout(5)
    async def test_closes_independent_dependencies_concurrently(self) -> None:
        barrier = Barrier(2)

        async def service() -> AsyncGenerator[Service]:
            yield Service()
            await barrier.wait()

        async def other_service() -> AsyncGenerator[OtherService]:
            yield OtherService()
            await barrier.wait()

        container = EffectDependencyContainer(
            dependency_defs=[EffectDependencyDef(service), EffectDependencyDef(other_service)],
        )
        scope = await container.start()

        async with timeout(1):
            await container.stop(scope, None)

    async def test_closes_resolved_dependencies_and_raises_exception_group_when_resolution_fails(self) -> None:
        async def base() -> AsyncGenerator[Config]:
            try:
                yield Config()
            finally:
                events.append("stop base")

        async def service(base: Config) -> AsyncGenerator[Service]:
            raise error
            yield Service()  # pyright: ignore[reportUnreachable] - makes the factory an async generator

        async def other_service() -> AsyncGenerator[OtherService]:
            await asyncio.Event().wait()
            raise AssertionError("must be cancelled")
            yield OtherService()  # pyright: ignore[reportUnreachable] - makes the factory an async generator

        events: MutableSequence[str] = []
        error = ValueError("error")
        container = EffectDependencyContainer(
            dependency_defs=[
                EffectDependencyDef(base),
                EffectDependencyDef(service),
                EffectDependencyDef(other_service),
            ],
        )

        with pytest.raises(ExceptionGroup) as exc_info:
            await container.start()

        assert exc_info.value.exceptions == (error,)
        assert events == ["stop base"]

    async def test_passes_error_into_dependencies_on_stop(self) -> None:
        async def service() -> AsyncGenerator[Service]:
            try:
                yield Service()
            except ValueError as exc:
                errors.append(exc)
                raise

        errors: MutableSequence[Exception] = []
        error = ValueError("error")
        container = EffectDependencyContainer(dependency_defs=[EffectDependencyDef(service)])
        scope = await container.start()

        await container.stop(scope, error)

        assert errors == [error]

    async def test_closes_all_dependencies_and_raises_exception_group_when_several_closings_fail(self) -> None:
        first_error = ValueError("first")
        second_error = ValueError("second")

        async def service() -> AsyncGenerator[Service]:
            yield Service()
            raise first_error

        async def other_service() -> AsyncGenerator[OtherService]:
            yield OtherService()
            raise second_error

        container = EffectDependencyContainer(
            dependency_defs=[EffectDependencyDef(service), EffectDependencyDef(other_service)],
        )
        scope = await container.start()

        with pytest.raises(ExceptionGroup) as exc_info:
            await container.stop(scope, None)

        assert set(exc_info.value.exceptions) == {first_error, second_error}


MESSAGE = TestCommand()
MESSAGE_CONTEXT = MessageContext(message_id="test_message_id")
EFFECT_CONTEXT = EffectContext(effect_id="test_effect_id")


def make_scopes(effect: Effect[Any], application_scope: Scope | None = None) -> Sequence[Scope]:
    return (
        application_scope or Scope({}),
        EffectDependencyContainer.make_message_scope(MESSAGE, MESSAGE_CONTEXT),
        EffectDependencyContainer.make_effect_scope(effect, EFFECT_CONTEXT),
    )


async def run_handler(
    handler: Callable[..., Awaitable[object]],
    dependency_defs: Collection[EffectDependencyDef[Any]] = (),
    effect: Effect[Any] | None = None,
) -> object:
    effect = effect or TestInternalEffect()
    container = EffectDependencyContainer(dependency_defs=dependency_defs, effect_types=[type(effect)])
    prepared_handler = container.prepare_effect_handler(type(effect), handler)
    application_scope = await container.start()
    application_scope, message_scope, effect_scope = make_scopes(effect, application_scope)
    async with message_scope, effect_scope:
        scopes = (application_scope, message_scope, effect_scope)
        await prepared_handler.resolve(scopes)
        result = await prepared_handler.call(scopes)
    await container.stop(application_scope, None)
    return result


async def add_dependency(
    scope: Scope,
    factory: DependencyFactory[Service],
    key: DependencyKey = EFFECT_KEY,
) -> None:
    context_manager = EffectProvider.make(EffectDependencyDef(factory)).factory()
    await scope.add(key, await context_manager.__aenter__(), context_manager)


def make_recording_factory(events: MutableSequence[str], name: str) -> DependencyFactory[Service]:
    async def service() -> AsyncGenerator[Service]:
        yield Service()
        events.append(f"stop {name}")

    return service


def make_failing_factory(error: Exception) -> DependencyFactory[Service]:
    async def service() -> AsyncGenerator[Service]:
        yield Service()
        raise error

    return service
