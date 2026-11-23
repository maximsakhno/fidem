from collections.abc import AsyncGenerator, Callable, Generator, Mapping, MutableSequence, Sequence
from typing import Any, override

import pytest

from fidem.di.core import (
    MESSAGE_CONTEXT_KEY,
    DependencyKey,
    DependencyLifetimeError,
    DependencyNotFoundError,
    DuplicateDependencyError,
    MissingAnnotationError,
    Scope,
    UnsupportedFunctionError,
    UnsupportedLifetimeError,
    UnsupportedReturnTypeError,
)
from fidem.di.messages import (
    DependencyEffect,
    FunctionMessageDependencyResolver,
    GeneratorFunctionMessageDependencyResolver,
    InstanceMessageDependencyResolver,
    MessageDependencyContainer,
    MessageDependencyDef,
    MessageDependencyLifetime,
    MessageDependencyResolver,
    PreparedMessageHandler,
)
from fidem.effects import Effect, Effectful
from fidem.messages import Command, Message, MessageContext
from fidem.testing import YeildStep, assert_generator
from tests.effects import TestExternalReadEffect
from tests.messages import TestCommand
from tests.stubs import make_application_dependency_def, make_message_dependency_def, run


class Service:
    pass


class OtherService:
    pass


class OtherCommand(Command[str]):
    pass


def make_generator_service() -> Effectful[Service, DependencyEffect]:
    yield from ()
    return Service()


async def make_async_service() -> Service:
    return Service()


async def make_async_generator_service() -> AsyncGenerator[Service]:
    yield Service()


class TestInstanceMessageDependencyResolver:
    def test_resolves_instance_without_effects(self) -> None:
        service = Service()
        resolver = InstanceMessageDependencyResolver(service)

        assert_generator(generator=resolver.resolve({}), return_value=service)
        assert resolver.dependency_type is Service
        assert resolver.parameters == {}

    def test_supports_all_lifetimes(self) -> None:
        resolver = InstanceMessageDependencyResolver(Service())

        assert tuple(resolver.lifetimes) == tuple(MessageDependencyLifetime)


class TestFunctionMessageDependencyResolver:
    def test_resolves_function_result_with_arguments_without_effects(self) -> None:
        def service(other: OtherService) -> Service:
            calls.append(other)
            return result

        calls: MutableSequence[OtherService] = []
        result = Service()
        other = OtherService()
        resolver = FunctionMessageDependencyResolver(service)

        assert_generator(generator=resolver.resolve({"other": other}), return_value=result)
        assert calls == [other]
        assert resolver.parameters == {"other": OtherService}
        assert resolver.dependency_type is Service
        assert tuple(resolver.lifetimes) == tuple(MessageDependencyLifetime)

    def test_resolves_method_result(self) -> None:
        class Factory:
            def service(self) -> Service:
                return result

        result = Service()
        resolver = FunctionMessageDependencyResolver(Factory().service)

        assert_generator(generator=resolver.resolve({}), return_value=result)

    @pytest.mark.parametrize(
        "function",
        [
            pytest.param(make_generator_service, id="generator"),
            pytest.param(make_async_service, id="async"),
            pytest.param(make_async_generator_service, id="async_generator"),
            pytest.param(Service, id="class"),
        ],
    )
    def test_raises_unsupported_function_error_when_function_is_not_plain(self, function: Callable[..., Any]) -> None:
        with pytest.raises(UnsupportedFunctionError) as exc_info:
            FunctionMessageDependencyResolver(function)

        assert exc_info.value.args == (function,)

    def test_raises_missing_annotation_error_when_return_type_is_not_annotated(self) -> None:
        def service():
            return Service()

        with pytest.raises(MissingAnnotationError) as exc_info:
            FunctionMessageDependencyResolver(service)

        assert exc_info.value.args == (service, "return")


class TestGeneratorFunctionMessageDependencyResolver:
    def test_resolves_factory_result_with_arguments_and_effects(self) -> None:
        def service(other: OtherService) -> Effectful[Service, DependencyEffect]:
            calls.append(other)
            effect_result = yield from effect
            assert effect_result == "effect_result"
            return result

        calls: MutableSequence[OtherService] = []
        effect = TestExternalReadEffect()
        result = Service()
        other = OtherService()
        resolver = GeneratorFunctionMessageDependencyResolver(service)

        assert_generator(
            generator=resolver.resolve({"other": other}),
            steps=[YeildStep(effect, "effect_result")],
            return_value=result,
        )
        assert calls == [other]
        assert resolver.parameters == {"other": OtherService}
        assert resolver.dependency_type is Service

    def test_raises_unsupported_function_error_when_function_is_not_generator(self) -> None:
        def service() -> Service:
            return Service()

        with pytest.raises(UnsupportedFunctionError):
            GeneratorFunctionMessageDependencyResolver(service)  # pyright: ignore[reportArgumentType] - checks a wrong function

    def test_raises_unsupported_return_type_error_when_return_type_is_not_effectful(self) -> None:
        def service() -> Generator[DependencyEffect, Any, Service]:
            yield from ()
            return Service()

        with pytest.raises(UnsupportedReturnTypeError) as exc_info:
            GeneratorFunctionMessageDependencyResolver(service)

        assert exc_info.value.args == (service, Generator[DependencyEffect, Any, Service])

    def test_raises_missing_annotation_error_when_return_type_is_not_annotated(self) -> None:
        def service():  # pyright: ignore[reportUnknownParameterType] - checks a missing annotation
            yield from ()
            return Service()

        with pytest.raises(MissingAnnotationError) as exc_info:
            GeneratorFunctionMessageDependencyResolver(service)

        assert exc_info.value.args == (service, "return")

    def test_raises_unsupported_lifetime_error_when_lifetime_is_application(self) -> None:
        dependency_def = MessageDependencyDef(
            resolver=GeneratorFunctionMessageDependencyResolver(make_generator_service),
            lifetime=MessageDependencyLifetime.APPLICATION,
        )

        with pytest.raises(UnsupportedLifetimeError) as exc_info:
            MessageDependencyContainer(dependency_defs=[dependency_def])

        assert exc_info.value.args == (dependency_def,)


class TestMessageDependencyDef:
    def test_takes_key_type_from_resolver_and_lifetime_is_message_by_default(self) -> None:
        dependency_def = MessageDependencyDef(InstanceMessageDependencyResolver(Service()))

        assert dependency_def.key == DependencyKey(name="", type=Service)
        assert dependency_def.lifetime == MessageDependencyLifetime.MESSAGE


class TestMessageHandlerPreparation:
    def test_passes_message_and_message_context_into_handler(self) -> None:
        def handler(context: MessageContext, command: TestCommand) -> Effectful[str, Effect[Any]]:
            yield from ()
            return f"{context.message_id} {command is MESSAGE}"

        prepared_handler = MessageDependencyContainer(message_types=[TestCommand]).prepare_message_handler(
            TestCommand, handler
        )

        assert_generator(generator=prepared_handler.call(make_scopes()), return_value="test_message_id True")

    def test_matches_message_by_its_base_class(self) -> None:
        def service(message: Message[Any]) -> Effectful[Service, DependencyEffect]:
            messages.append(message)
            yield from ()
            return Service()

        def handler(service: Service) -> Effectful[str, Effect[Any]]:
            yield from ()
            return "command_result"

        messages: MutableSequence[Message[Any]] = []
        container = MessageDependencyContainer(
            dependency_defs=[make_message_dependency_def(service)],
            message_types=[TestCommand],
        )
        prepared_handler = container.prepare_message_handler(TestCommand, handler)

        run_steps(prepared_handler, make_scopes())

        assert messages == [MESSAGE]

    def test_raises_dependency_not_found_error_when_factory_needs_other_message_type(self) -> None:
        def service(command: OtherCommand) -> Effectful[Service, DependencyEffect]:
            yield from ()
            return Service()

        def handler(service: Service) -> Effectful[str, Effect[Any]]:
            yield from ()
            return "command_result"

        container = MessageDependencyContainer(
            dependency_defs=[make_message_dependency_def(service)],
            message_types=[TestCommand, OtherCommand],
        )

        with pytest.raises(DependencyNotFoundError) as exc_info:
            container.prepare_message_handler(TestCommand, handler)

        assert exc_info.value.args == (DependencyKey(name="", type=Service), "command", OtherCommand)

    def test_collects_message_dependencies_in_topological_order(self) -> None:
        def other(context: MessageContext) -> Effectful[OtherService, DependencyEffect]:
            yield from ()
            return OtherService()

        def service(other: OtherService) -> Effectful[Service, DependencyEffect]:
            yield from ()
            return Service()

        def handler(service: Service) -> Effectful[str, Effect[Any]]:
            yield from ()
            return "command_result"

        container = MessageDependencyContainer(
            dependency_defs=[make_message_dependency_def(service), make_message_dependency_def(other)],
            message_types=[TestCommand],
        )

        prepared_handler = container.prepare_message_handler(TestCommand, handler)

        assert [step.key.type for step in prepared_handler.steps] == [OtherService, Service]

    def test_does_not_collect_application_dependencies(self) -> None:
        def handler(service: Service) -> Effectful[str, Effect[Any]]:
            yield from ()
            return "command_result"

        container = MessageDependencyContainer(
            dependency_defs=[make_application_dependency_def(InstanceMessageDependencyResolver(Service()))],
            message_types=[TestCommand],
        )

        prepared_handler = container.prepare_message_handler(TestCommand, handler)

        assert prepared_handler.steps == ()

    def test_raises_unsupported_function_error_when_handler_is_not_generator(self) -> None:
        def handler() -> str:
            return "command_result"

        with pytest.raises(UnsupportedFunctionError):
            MessageDependencyContainer().prepare_message_handler(TestCommand, handler)  # pyright: ignore[reportArgumentType] - checks a wrong handler


class TestMessageDependencyValidation:
    def test_orders_application_dependencies_topologically(self) -> None:
        def service(other: OtherService) -> Service:
            return Service()

        container = MessageDependencyContainer(
            dependency_defs=[
                make_application_dependency_def(FunctionMessageDependencyResolver(service)),
                make_application_dependency_def(InstanceMessageDependencyResolver(OtherService())),
            ],
        )

        assert [step.key.type for step in container.application_steps] == [OtherService, Service]

    def test_raises_dependency_lifetime_error_when_application_dependency_needs_message_context(self) -> None:
        def service(context: MessageContext) -> Service:
            return Service()

        with pytest.raises(DependencyLifetimeError) as exc_info:
            MessageDependencyContainer(
                dependency_defs=[make_application_dependency_def(FunctionMessageDependencyResolver(service))],
            )

        assert exc_info.value.args == (DependencyKey(name="", type=Service), MESSAGE_CONTEXT_KEY)

    def test_raises_dependency_not_found_error_when_unused_dependency_needs_unknown_message(self) -> None:
        def service(command: OtherCommand) -> Effectful[Service, DependencyEffect]:
            yield from ()
            return Service()

        with pytest.raises(DependencyNotFoundError):
            MessageDependencyContainer(
                dependency_defs=[make_message_dependency_def(service)], message_types=[TestCommand]
            )

    def test_raises_duplicate_dependency_error_when_dependency_has_builtin_key(self) -> None:
        dependency_def = MessageDependencyDef(
            resolver=InstanceMessageDependencyResolver(MessageContext(message_id="test")),
            name="message_context",
        )

        with pytest.raises(DuplicateDependencyError) as exc_info:
            MessageDependencyContainer(dependency_defs=[dependency_def])

        assert exc_info.value.args == (MESSAGE_CONTEXT_KEY,)

    def test_raises_unsupported_lifetime_error_when_effectful_resolver_declares_application_lifetime(self) -> None:
        class EffectfulResolver(MessageDependencyResolver[Service]):
            def __init__(self) -> None:
                super().__init__(parameters={}, dependency_type=Service, lifetimes=tuple(MessageDependencyLifetime))

            @override
            def resolve(self, arguments: Mapping[str, object]) -> Effectful[Service, DependencyEffect]:
                yield from ()
                return Service()

        dependency_def = make_application_dependency_def(EffectfulResolver())

        with pytest.raises(UnsupportedLifetimeError) as exc_info:
            MessageDependencyContainer(dependency_defs=[dependency_def])

        assert exc_info.value.args == (dependency_def,)


MESSAGE = TestCommand()
MESSAGE_CONTEXT = MessageContext(message_id="test_message_id")


def make_scopes() -> Sequence[Scope]:
    return (Scope(), MessageDependencyContainer.make_message_scope(MESSAGE, MESSAGE_CONTEXT))


def run_steps(prepared_handler: PreparedMessageHandler, scopes: Sequence[Scope]) -> None:
    for step in prepared_handler.steps:
        run(step.run(scopes))
