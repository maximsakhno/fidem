from collections.abc import AsyncGenerator, Generator, MutableSequence
from typing import Any

import pytest

from fidem.di.core import (
    MESSAGE_CONTEXT_KEY,
    DependencyKey,
    DependencyNotFoundError,
    DuplicateDependencyError,
    MissingAnnotationError,
    Scope,
    UnsupportedFunctionError,
    UnsupportedReturnTypeError,
)
from fidem.effects import Effect, Effectful
from fidem.guards import GuardContainer, GuardDef, GuardEffect, GuardResolver, PreparedMessageHandler
from fidem.messages import Command, Message, MessageContext
from fidem.testing import assert_generator
from tests.messages import TestCommand
from tests.stubs import run


class Service:
    pass


class OtherService:
    pass


class OtherCommand(Command[str]):
    pass


def make_service() -> Effectful[Service, GuardEffect]:
    yield from ()
    return Service()


class TestGuardResolver:
    def test_takes_parameters_and_value_type_from_guard_signature(self) -> None:
        def service(other: OtherService) -> Effectful[Service, GuardEffect]:
            yield from ()
            return Service()

        resolver = GuardResolver(service)

        assert resolver.parameters == {"other": OtherService}
        assert resolver.dependency_type is Service
        assert resolver.guard is service

    def test_raises_unsupported_function_error_when_guard_is_plain_function(self) -> None:
        def service() -> Service:
            return Service()

        with pytest.raises(UnsupportedFunctionError) as exc_info:
            GuardResolver(service)  # pyright: ignore[reportArgumentType] - checks a wrong guard

        assert exc_info.value.args == (service,)

    def test_raises_unsupported_function_error_when_guard_is_class(self) -> None:
        with pytest.raises(UnsupportedFunctionError) as exc_info:
            GuardResolver(Service)  # pyright: ignore[reportArgumentType] - checks a wrong guard

        assert exc_info.value.args == (Service,)

    def test_raises_unsupported_function_error_when_guard_is_async_function(self) -> None:
        async def service() -> Service:
            return Service()

        with pytest.raises(UnsupportedFunctionError) as exc_info:
            GuardResolver(service)  # pyright: ignore[reportArgumentType] - checks a wrong guard

        assert exc_info.value.args == (service,)

    def test_raises_unsupported_function_error_when_guard_is_async_generator_function(self) -> None:
        async def service() -> AsyncGenerator[Service]:
            yield Service()

        with pytest.raises(UnsupportedFunctionError) as exc_info:
            GuardResolver(service)  # pyright: ignore[reportArgumentType] - checks a wrong guard

        assert exc_info.value.args == (service,)

    def test_raises_unsupported_return_type_error_when_return_type_is_not_effectful(self) -> None:
        def service() -> Generator[GuardEffect, Any, Service]:
            yield from ()
            return Service()

        with pytest.raises(UnsupportedReturnTypeError) as exc_info:
            GuardResolver(service)

        assert exc_info.value.args == (service, Generator[GuardEffect, Any, Service])

    def test_raises_missing_annotation_error_when_return_type_is_not_annotated(self) -> None:
        def service():  # pyright: ignore[reportUnknownParameterType] - checks a missing annotation
            yield from ()
            return Service()

        with pytest.raises(MissingAnnotationError) as exc_info:
            GuardResolver(service)

        assert exc_info.value.args == (service, "return")


class TestMessageHandlerPreparation:
    def test_passes_message_and_message_context_into_handler(self) -> None:
        def handler(context: MessageContext, command: TestCommand) -> Effectful[str, Effect[Any]]:
            yield from ()
            return f"{context.message_id} {command is MESSAGE}"

        prepared_handler = GuardContainer(message_types=[TestCommand]).prepare_message_handler(TestCommand, handler)

        assert_generator(generator=prepared_handler.call(make_scope()), return_value="test_message_id True")

    def test_passes_guard_values_into_handler(self) -> None:
        def service(context: MessageContext) -> Effectful[Service, GuardEffect]:
            yield from ()
            return result

        def handler(service: Service) -> Effectful[Service, Effect[Any]]:
            yield from ()
            return service

        result = Service()
        container = GuardContainer(guard_defs=[GuardDef(service)], message_types=[TestCommand])
        prepared_handler = container.prepare_message_handler(TestCommand, handler)
        scope = make_scope()

        run_steps(prepared_handler, scope)

        assert run(prepared_handler.call(scope)) is result

    def test_matches_message_by_its_base_class(self) -> None:
        def service(message: Message[Any]) -> Effectful[Service, GuardEffect]:
            messages.append(message)
            yield from ()
            return Service()

        def handler(service: Service) -> Effectful[str, Effect[Any]]:
            yield from ()
            return "command_result"

        messages: MutableSequence[Message[Any]] = []
        container = GuardContainer(guard_defs=[GuardDef(service)], message_types=[TestCommand])
        prepared_handler = container.prepare_message_handler(TestCommand, handler)

        run_steps(prepared_handler, make_scope())

        assert messages == [MESSAGE]

    def test_matches_guard_by_type_from_guard_def(self) -> None:
        def handler(other: OtherService) -> Effectful[str, Effect[Any]]:
            yield from ()
            return "command_result"

        container = GuardContainer(
            guard_defs=[GuardDef(make_service, type=OtherService)],
            message_types=[TestCommand],
        )

        prepared_handler = container.prepare_message_handler(TestCommand, handler)

        assert [step.key for step in prepared_handler.steps] == [DependencyKey(name="", type=OtherService)]

    def test_matches_guard_by_name_from_guard_def_when_types_are_same(self) -> None:
        def handler(second: Service) -> Effectful[str, Effect[Any]]:
            yield from ()
            return "command_result"

        container = GuardContainer(
            guard_defs=[
                GuardDef(make_service, name="first"),
                GuardDef(make_service, name="second"),
            ],
            message_types=[TestCommand],
        )

        prepared_handler = container.prepare_message_handler(TestCommand, handler)

        assert [step.key for step in prepared_handler.steps] == [DependencyKey(name="second", type=Service)]

    def test_raises_dependency_not_found_error_when_guard_needs_other_message_type(self) -> None:
        def service(command: OtherCommand) -> Effectful[Service, GuardEffect]:
            yield from ()
            return Service()

        def handler(service: Service) -> Effectful[str, Effect[Any]]:
            yield from ()
            return "command_result"

        container = GuardContainer(guard_defs=[GuardDef(service)], message_types=[TestCommand, OtherCommand])

        with pytest.raises(DependencyNotFoundError) as exc_info:
            container.prepare_message_handler(TestCommand, handler)

        assert exc_info.value.args == (DependencyKey(name="", type=Service), "command", OtherCommand)

    def test_collects_guards_in_topological_order(self) -> None:
        def other(context: MessageContext) -> Effectful[OtherService, GuardEffect]:
            yield from ()
            return OtherService()

        def service(other: OtherService) -> Effectful[Service, GuardEffect]:
            yield from ()
            return Service()

        def handler(service: Service) -> Effectful[str, Effect[Any]]:
            yield from ()
            return "command_result"

        container = GuardContainer(guard_defs=[GuardDef(service), GuardDef(other)], message_types=[TestCommand])

        prepared_handler = container.prepare_message_handler(TestCommand, handler)

        assert [step.key.type for step in prepared_handler.steps] == [OtherService, Service]

    def test_does_not_collect_guards_handler_does_not_need(self) -> None:
        def handler() -> Effectful[str, Effect[Any]]:
            yield from ()
            return "command_result"

        container = GuardContainer(guard_defs=[GuardDef(make_service)], message_types=[TestCommand])

        prepared_handler = container.prepare_message_handler(TestCommand, handler)

        assert prepared_handler.steps == ()

    def test_raises_unsupported_function_error_when_handler_is_not_generator(self) -> None:
        def handler() -> str:
            return "command_result"

        with pytest.raises(UnsupportedFunctionError):
            GuardContainer().prepare_message_handler(TestCommand, handler)  # pyright: ignore[reportArgumentType] - checks a wrong handler


class TestGuardValidation:
    def test_raises_unsupported_function_error_when_guard_is_not_generator_function(self) -> None:
        def service() -> Service:
            return Service()

        with pytest.raises(UnsupportedFunctionError) as exc_info:
            GuardContainer(guard_defs=[GuardDef(service)])  # pyright: ignore[reportArgumentType] - checks a wrong guard

        assert exc_info.value.args == (service,)

    def test_raises_dependency_not_found_error_when_unused_guard_needs_unknown_message(self) -> None:
        def service(command: OtherCommand) -> Effectful[Service, GuardEffect]:
            yield from ()
            return Service()

        with pytest.raises(DependencyNotFoundError):
            GuardContainer(guard_defs=[GuardDef(service)], message_types=[TestCommand])

    def test_raises_duplicate_dependency_error_when_guard_has_builtin_key(self) -> None:
        def message_context() -> Effectful[MessageContext, GuardEffect]:
            yield from ()
            return MessageContext(message_id="test")

        with pytest.raises(DuplicateDependencyError) as exc_info:
            GuardContainer(guard_defs=[GuardDef(message_context, name="message_context")])

        assert exc_info.value.args == (MESSAGE_CONTEXT_KEY,)


MESSAGE = TestCommand()
MESSAGE_CONTEXT = MessageContext(message_id="test_message_id")


def make_scope() -> Scope:
    return GuardContainer.make_message_scope(MESSAGE, MESSAGE_CONTEXT)


def run_steps(prepared_handler: PreparedMessageHandler, scope: Scope) -> None:
    for step in prepared_handler.steps:
        run(step.run(scope))
