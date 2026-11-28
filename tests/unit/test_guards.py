from collections.abc import AsyncGenerator, Generator, MutableSequence
from typing import Any, NewType, Protocol

import pytest

from fidem.core.di.core import (
    MESSAGE_CONTEXT_KEY,
    DependencyKey,
    DependencyNotFoundError,
    DuplicateDependencyError,
    MissingAnnotationError,
    UnsupportedFunctionError,
    UnsupportedReturnTypeError,
    make_message_dependencies,
)
from fidem.core.effects import Effect, Effectful
from fidem.core.guards import (
    GuardContainer,
    GuardDef,
    GuardInputDef,
    GuardProvider,
    GuardValues,
    PreparedGuards,
    UnsupportedGuardInputTypeError,
)
from fidem.core.messages import Command, Message, MessageContext, QueryEffect
from fidem.core.testing import assert_generator
from tests.messages import TestCommand
from tests.stubs import run


class Service:
    pass


class OtherService:
    pass


class OtherCommand(Command[str]):
    pass


def make_service() -> Effectful[Service, QueryEffect]:
    yield from ()
    return Service()


class SpecialService(Service):
    pass


def make_special_service() -> Effectful[SpecialService, QueryEffect]:
    yield from ()
    return SpecialService()


class UncheckableInput(Protocol):
    pass


class TestGuardProvider:
    def test_takes_parameters_and_key_from_guard_signature(self) -> None:
        def service(other: OtherService) -> Effectful[Service, QueryEffect]:
            yield from ()
            return Service()

        provider = GuardProvider.make(GuardDef(service))

        assert provider.parameters == {"other": OtherService}
        assert provider.key == DependencyKey(name="", type=Service)
        assert provider.guard is service

    def test_raises_unsupported_function_error_when_guard_is_plain_function(self) -> None:
        def service() -> Service:
            return Service()

        with pytest.raises(UnsupportedFunctionError) as exc_info:
            GuardProvider.make(GuardDef(service))  # pyright: ignore[reportArgumentType] - checks a wrong guard

        assert exc_info.value.args == (service,)

    def test_raises_unsupported_function_error_when_guard_is_class(self) -> None:
        with pytest.raises(UnsupportedFunctionError) as exc_info:
            GuardProvider.make(GuardDef(Service))  # pyright: ignore[reportArgumentType] - checks a wrong guard

        assert exc_info.value.args == (Service,)

    def test_raises_unsupported_function_error_when_guard_is_async_function(self) -> None:
        async def service() -> Service:
            return Service()

        with pytest.raises(UnsupportedFunctionError) as exc_info:
            GuardProvider.make(GuardDef(service))  # pyright: ignore[reportArgumentType] - checks a wrong guard

        assert exc_info.value.args == (service,)

    def test_raises_unsupported_function_error_when_guard_is_async_generator_function(self) -> None:
        async def service() -> AsyncGenerator[Service]:
            yield Service()

        with pytest.raises(UnsupportedFunctionError) as exc_info:
            GuardProvider.make(GuardDef(service))  # pyright: ignore[reportArgumentType] - checks a wrong guard

        assert exc_info.value.args == (service,)

    def test_raises_unsupported_return_type_error_when_return_type_is_not_effectful(self) -> None:
        def service() -> Generator[QueryEffect, Any, Service]:
            yield from ()
            return Service()

        with pytest.raises(UnsupportedReturnTypeError) as exc_info:
            GuardProvider.make(GuardDef(service))

        assert exc_info.value.args == (service, Generator[QueryEffect, Any, Service])

    def test_raises_missing_annotation_error_when_return_type_is_not_annotated(self) -> None:
        def service():  # pyright: ignore[reportUnknownParameterType] - checks a missing annotation
            yield from ()
            return Service()

        with pytest.raises(MissingAnnotationError) as exc_info:
            GuardProvider.make(GuardDef(service))

        assert exc_info.value.args == (service, "return")


class TestMessageHandlerPreparation:
    def test_passes_message_and_message_context_into_handler(self) -> None:
        def handler(context: MessageContext, command: TestCommand) -> Effectful[str, Effect[Any]]:
            yield from ()
            return f"{context.message_id} {command is MESSAGE}"

        prepared = GuardContainer(message_types=[TestCommand]).prepare(TestCommand, [handler])

        assert_generator(generator=prepared.functions[0].call(make_values()), return_value="test_message_id True")

    def test_passes_guard_values_into_handler(self) -> None:
        def service(context: MessageContext) -> Effectful[Service, QueryEffect]:
            yield from ()
            return result

        def handler(service: Service) -> Effectful[Service, Effect[Any]]:
            yield from ()
            return service

        result = Service()
        container = GuardContainer(guard_defs=[GuardDef(service)], message_types=[TestCommand])
        prepared = container.prepare(TestCommand, [handler])
        values = make_values()

        run_steps(prepared, values)

        assert run(prepared.functions[0].call(values)) is result

    def test_matches_message_by_its_base_class(self) -> None:
        def service(message: Message[Any]) -> Effectful[Service, QueryEffect]:
            messages.append(message)
            yield from ()
            return Service()

        def handler(service: Service) -> Effectful[str, Effect[Any]]:
            yield from ()
            return "command_result"

        messages: MutableSequence[Message[Any]] = []
        container = GuardContainer(guard_defs=[GuardDef(service)], message_types=[TestCommand])
        prepared = container.prepare(TestCommand, [handler])

        run_steps(prepared, make_values())

        assert messages == [MESSAGE]

    def test_matches_guard_by_type_from_guard_def(self) -> None:
        def handler(other: OtherService) -> Effectful[str, Effect[Any]]:
            yield from ()
            return "command_result"

        container = GuardContainer(
            guard_defs=[GuardDef(make_service, type=OtherService)],
            message_types=[TestCommand],
        )

        prepared = container.prepare(TestCommand, [handler])

        assert [step.provider.key for step in prepared.steps] == [DependencyKey(name="", type=OtherService)]

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

        prepared = container.prepare(TestCommand, [handler])

        assert [step.provider.key for step in prepared.steps] == [DependencyKey(name="second", type=Service)]

    def test_raises_dependency_not_found_error_when_guard_needs_other_message_type(self) -> None:
        def service(command: OtherCommand) -> Effectful[Service, QueryEffect]:
            yield from ()
            return Service()

        def handler(service: Service) -> Effectful[str, Effect[Any]]:
            yield from ()
            return "command_result"

        container = GuardContainer(guard_defs=[GuardDef(service)], message_types=[TestCommand, OtherCommand])

        with pytest.raises(DependencyNotFoundError) as exc_info:
            container.prepare(TestCommand, [handler])

        assert exc_info.value.args == (DependencyKey(name="", type=Service), "command", OtherCommand)

    def test_collects_guards_in_topological_order(self) -> None:
        def other(context: MessageContext) -> Effectful[OtherService, QueryEffect]:
            yield from ()
            return OtherService()

        def service(other: OtherService) -> Effectful[Service, QueryEffect]:
            yield from ()
            return Service()

        def handler(service: Service) -> Effectful[str, Effect[Any]]:
            yield from ()
            return "command_result"

        container = GuardContainer(guard_defs=[GuardDef(service), GuardDef(other)], message_types=[TestCommand])

        prepared = container.prepare(TestCommand, [handler])

        assert [step.provider.key.type for step in prepared.steps] == [OtherService, Service]

    def test_does_not_collect_guards_handler_does_not_need(self) -> None:
        def handler() -> Effectful[str, Effect[Any]]:
            yield from ()
            return "command_result"

        container = GuardContainer(guard_defs=[GuardDef(make_service)], message_types=[TestCommand])

        prepared = container.prepare(TestCommand, [handler])

        assert prepared.steps == ()

    def test_collects_guards_of_all_functions_once_in_topological_order(self) -> None:
        def other(context: MessageContext) -> Effectful[OtherService, QueryEffect]:
            yield from ()
            return OtherService()

        def service(other: OtherService) -> Effectful[Service, QueryEffect]:
            yield from ()
            return Service()

        def middleware(service: Service) -> Effectful[str, Effect[Any]]:
            yield from ()
            return "middleware_result"

        def handler(other: OtherService, service: Service) -> Effectful[str, Effect[Any]]:
            yield from ()
            return "command_result"

        container = GuardContainer(guard_defs=[GuardDef(service), GuardDef(other)], message_types=[TestCommand])

        prepared = container.prepare(TestCommand, [middleware, handler])

        assert [step.provider.key.type for step in prepared.steps] == [OtherService, Service]
        assert [function.function for function in prepared.functions] == [middleware, handler]

    def test_raises_unsupported_function_error_when_handler_is_not_generator(self) -> None:
        def handler() -> str:
            return "command_result"

        with pytest.raises(UnsupportedFunctionError):
            GuardContainer().prepare(TestCommand, [handler])  # pyright: ignore[reportArgumentType] - checks a wrong handler


class TestGuardValidation:
    def test_raises_unsupported_function_error_when_guard_is_not_generator_function(self) -> None:
        def service() -> Service:
            return Service()

        with pytest.raises(UnsupportedFunctionError) as exc_info:
            GuardContainer(guard_defs=[GuardDef(service)])  # pyright: ignore[reportArgumentType] - checks a wrong guard

        assert exc_info.value.args == (service,)

    def test_raises_dependency_not_found_error_when_unused_guard_needs_unknown_message(self) -> None:
        def service(command: OtherCommand) -> Effectful[Service, QueryEffect]:
            yield from ()
            return Service()

        with pytest.raises(DependencyNotFoundError):
            GuardContainer(guard_defs=[GuardDef(service)], message_types=[TestCommand])

    def test_raises_duplicate_dependency_error_when_guard_has_builtin_key(self) -> None:
        def message_context() -> Effectful[MessageContext, QueryEffect]:
            yield from ()
            return MessageContext(message_id="test")

        with pytest.raises(DuplicateDependencyError) as exc_info:
            GuardContainer(guard_defs=[GuardDef(message_context, name="message_context")])

        assert exc_info.value.args == (MESSAGE_CONTEXT_KEY,)


class TestGuardInputValidation:
    def test_raises_unsupported_guard_input_type_error_when_input_type_is_not_class(self) -> None:
        token_type = NewType("token_type", str)

        with pytest.raises(UnsupportedGuardInputTypeError) as exc_info:
            GuardContainer(guard_defs=[GuardInputDef(token_type)])  # pyright: ignore[reportArgumentType] - checks a wrong input type

        assert exc_info.value.args == (token_type,)

    @pytest.mark.parametrize(
        argnames="input_type",
        argvalues=[UncheckableInput, Any],
        ids=["protocol", "any"],
    )
    def test_raises_unsupported_guard_input_type_error_when_isinstance_cannot_check_input_type(
        self,
        input_type: type[Any],
    ) -> None:
        with pytest.raises(UnsupportedGuardInputTypeError) as exc_info:
            GuardContainer(guard_defs=[GuardInputDef(input_type)])

        assert exc_info.value.args == (input_type,)

    @pytest.mark.parametrize(
        argnames="guard_def",
        argvalues=[GuardDef(make_service, name="service"), GuardDef(make_special_service)],
        ids=["named_guard_of_input_type", "guard_of_input_subclass"],
    )
    def test_raises_duplicate_dependency_error_when_guard_value_could_be_taken_for_input(
        self,
        guard_def: GuardDef[Any],
    ) -> None:
        with pytest.raises(DuplicateDependencyError):
            GuardContainer(guard_defs=[GuardInputDef(Service), guard_def])

    def test_raises_duplicate_dependency_error_when_input_is_declared_twice(self) -> None:
        with pytest.raises(DuplicateDependencyError) as exc_info:
            GuardContainer(guard_defs=[GuardInputDef(Service), GuardInputDef(Service)])

        assert exc_info.value.args == (DependencyKey(name="", type=Service),)

    def test_raises_duplicate_dependency_error_when_guard_returns_input_type(self) -> None:
        with pytest.raises(DuplicateDependencyError) as exc_info:
            GuardContainer(guard_defs=[GuardInputDef(Service), GuardDef(make_service)])

        assert exc_info.value.args == (DependencyKey(name="", type=Service),)

    def test_collects_inputs_guards_need(self) -> None:
        def service(other: OtherService) -> Effectful[Service, QueryEffect]:
            yield from ()
            return Service()

        def handler(service: Service) -> Effectful[str, Effect[Any]]:
            yield from ()
            return "result"

        container = GuardContainer(
            guard_defs=[GuardDef(service), GuardInputDef(OtherService), GuardInputDef(OtherCommand)],
            message_types=[TestCommand],
        )

        assert container.prepare(TestCommand, [handler]).input_keys == {DependencyKey(name="", type=OtherService)}


MESSAGE = TestCommand()
MESSAGE_CONTEXT = MessageContext(message_id="test_message_id")


def make_values() -> GuardValues:
    return make_message_dependencies(MESSAGE, MESSAGE_CONTEXT)


def run_steps(prepared: PreparedGuards, values: GuardValues) -> None:
    for step in prepared.steps:
        run(step.run(values))
