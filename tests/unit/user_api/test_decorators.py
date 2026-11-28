from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import Any

import pytest

from fidem import (
    AtomicEffectful,
    Command,
    EffectContext,
    EffectDependencyLifetime,
    Effectful,
    Event,
    ExecutionMode,
    InternalEffect,
    MessageMiddlewareEffectful,
    Proceed,
    Query,
    QueryEffect,
    QueryEffectful,
    StagedEffectful,
    command,
    command_handler,
    dependency,
    effect,
    effect_handler,
    effect_middleware,
    event,
    event_handler,
    guard,
    guard_input,
    middleware,
    query_handler,
)
from fidem._decorators import declared_definition, effect_middleware_def, is_declared, message_middleware_def
from fidem.core import (
    AtomicCommandDef,
    AtomicEventHandlerDef,
    EffectDef,
    EffectDependencyDef,
    EffectMiddlewareDef,
    EventDef,
    GuardDef,
    GuardInputDef,
    MessageMiddlewareDef,
    QueryDef,
    StagedCommandDef,
    StagedEventHandlerDef,
    UnsupportedFunctionError,
    UnsupportedReturnTypeError,
)
from fidem.core.environment import CallNext
from fidem.core.operation_engine.instructions import EffectCall
from fidem.errors import AlreadyDeclaredError, HandledTypeError, UndeclaredClassError


@command("save", version=2)
@dataclass(frozen=True, slots=True)
class SaveCommand(Command[str]):
    value: str


@event("saved", version=3)
@dataclass(frozen=True, slots=True)
class SavedEvent(Event):
    value: str


@dataclass(frozen=True, slots=True)
class GetQuery(Query[str]):
    pass


@dataclass(frozen=True, slots=True)
class UndeclaredCommand(Command[str]):
    pass


@effect("load", version=4)
@dataclass(frozen=True, slots=True)
class LoadEffect(InternalEffect[str]):
    pass


type SaveEffectful = AtomicEffectful[str]


def logging_middleware() -> MessageMiddlewareEffectful[object]:
    return (yield from Proceed())


async def pass_effect(effect_call: EffectCall, call_next: CallNext) -> object:
    return await call_next()


# The decorator reads no annotations, so the handler may refer to a class declared below it.
@command_handler
def save_later(command: "LaterCommand") -> StagedEffectful[str]:
    yield from ()
    raise NotImplementedError


@command("later")
@dataclass(frozen=True, slots=True)
class LaterCommand(Command[str]):
    pass


class TestCommandHandler:
    def test_defines_staged_command_with_name_and_version_of_its_class(self) -> None:
        @command_handler
        def save(command: SaveCommand) -> StagedEffectful[str]:
            yield from ()
            return command.value

        assert declared_definition(save) == StagedCommandDef(
            name="save",
            message_type=SaveCommand,
            handler=save,
            version=2,
        )

    def test_defines_atomic_command_by_return_type(self) -> None:
        @command_handler
        def save(command: SaveCommand) -> AtomicEffectful[str]:
            yield from ()
            raise NotImplementedError

        assert isinstance(declared_definition(save), AtomicCommandDef)

    def test_defines_atomic_command_by_alias_of_its_return_type(self) -> None:
        @command_handler
        def save(command: SaveCommand) -> SaveEffectful:
            yield from ()
            raise NotImplementedError

        assert isinstance(declared_definition(save), AtomicCommandDef)

    def test_passes_options(self) -> None:
        middleware_def = MessageMiddlewareDef(name="named", middleware=logging_middleware)

        @command_handler(middlewares=[logging_middleware, middleware_def], mode=ExecutionMode.DURABLE)
        def save(command: SaveCommand) -> StagedEffectful[str]:
            yield from ()
            raise NotImplementedError

        definition = declared_definition(save)

        assert isinstance(definition, StagedCommandDef)
        assert definition.middleware_defs == (
            MessageMiddlewareDef(name="logging_middleware", middleware=logging_middleware),
            middleware_def,
        )
        assert definition.execution_mode == ExecutionMode.DURABLE

    def test_keeps_no_middlewares(self) -> None:
        @command_handler(middlewares=())
        def save(command: SaveCommand) -> StagedEffectful[str]:
            yield from ()
            raise NotImplementedError

        definition = declared_definition(save)

        assert isinstance(definition, StagedCommandDef)
        assert definition.middleware_defs == ()

    def test_reads_annotations_of_classes_declared_after_handler(self) -> None:
        definition = declared_definition(save_later)

        assert isinstance(definition, StagedCommandDef)
        assert definition.message_type is LaterCommand

    def test_returns_function_itself(self) -> None:
        def save(command: SaveCommand) -> StagedEffectful[str]:
            yield from ()
            return command.value

        assert command_handler(save) is save

    def test_raises_unsupported_return_type_error_when_return_type_is_neither_atomic_nor_staged(self) -> None:
        @command_handler
        def save(command: SaveCommand) -> Effectful[str, Any]:
            yield from ()
            raise NotImplementedError

        with pytest.raises(UnsupportedReturnTypeError):
            declared_definition(save)

    def test_raises_handled_type_error_when_no_parameter_has_command_type(self) -> None:
        @command_handler
        def save() -> StagedEffectful[str]:
            yield from ()
            raise NotImplementedError

        with pytest.raises(HandledTypeError) as exc_info:
            declared_definition(save)

        assert exc_info.value.args == (save, [])

    def test_raises_handled_type_error_when_parameters_have_several_command_types(self) -> None:
        @command_handler
        def save(first: SaveCommand, second: UndeclaredCommand) -> StagedEffectful[str]:
            yield from ()
            raise NotImplementedError

        with pytest.raises(HandledTypeError) as exc_info:
            declared_definition(save)

        assert exc_info.value.args == (save, [SaveCommand, UndeclaredCommand])

    def test_raises_undeclared_class_error_when_command_class_has_no_command_decorator(self) -> None:
        @command_handler
        def save(command: UndeclaredCommand) -> StagedEffectful[str]:
            yield from ()
            raise NotImplementedError

        with pytest.raises(UndeclaredClassError) as exc_info:
            declared_definition(save)

        assert exc_info.value.args == (save, UndeclaredCommand)

    def test_raises_undeclared_class_error_when_only_base_of_command_class_is_declared(self) -> None:
        @dataclass(frozen=True, slots=True)
        class SubCommand(SaveCommand):
            pass

        @command_handler
        def save(command: SubCommand) -> StagedEffectful[str]:
            yield from ()
            raise NotImplementedError

        with pytest.raises(UndeclaredClassError):
            declared_definition(save)


class TestQueryHandler:
    def test_defines_query(self) -> None:
        @query_handler(middlewares=[logging_middleware])
        def get(query: GetQuery) -> QueryEffectful[str]:
            yield from ()
            raise NotImplementedError

        assert declared_definition(get) == QueryDef(
            message_type=GetQuery,
            handler=get,
            middleware_defs=(MessageMiddlewareDef(name="logging_middleware", middleware=logging_middleware),),
        )


class TestEventHandler:
    def test_defines_event_with_handler_named_after_function(self) -> None:
        @event_handler
        def record(event: SavedEvent) -> StagedEffectful[None]:
            yield from ()
            raise NotImplementedError

        assert declared_definition(record) == EventDef(
            name="saved",
            event_type=SavedEvent,
            version=3,
            handler_defs=(StagedEventHandlerDef(name="record", handler=record),),
        )

    def test_defines_atomic_handler_with_name(self) -> None:
        @event_handler(name="audit", mode=ExecutionMode.DURABLE)
        def record(event: SavedEvent) -> AtomicEffectful[None]:
            yield from ()
            raise NotImplementedError

        definition = declared_definition(record)

        assert isinstance(definition, EventDef)
        assert definition.handler_defs == (
            AtomicEventHandlerDef(name="audit", handler=record, execution_mode=ExecutionMode.DURABLE),
        )

    def test_raises_undeclared_class_error_when_event_class_has_no_event_decorator(self) -> None:
        @dataclass(frozen=True, slots=True)
        class UndeclaredEvent(Event):
            pass

        @event_handler
        def record(event: UndeclaredEvent) -> StagedEffectful[None]:
            yield from ()
            raise NotImplementedError

        with pytest.raises(UndeclaredClassError):
            declared_definition(record)


class TestGuard:
    def test_defines_guard(self) -> None:
        @guard(name="user", type=str)
        def current_user() -> Effectful[str, QueryEffect]:
            yield from ()
            raise NotImplementedError

        assert declared_definition(current_user) == GuardDef(guard=current_user, name="user", type=str)


class TestEffectHandler:
    def test_defines_effect_handler_by_effect_parameter_but_not_message_one(self) -> None:
        @effect_handler(middlewares=[pass_effect])
        async def load(command: SaveCommand, effect: LoadEffect, context: EffectContext) -> str:
            raise NotImplementedError

        assert declared_definition(load) == EffectDef(
            name="load",
            effect_type=LoadEffect,
            handler=load,
            version=4,
            middleware_defs=(EffectMiddlewareDef(pass_effect),),
        )

    def test_keeps_middlewares_of_environment_without_middlewares(self) -> None:
        @effect_handler
        async def load(effect: LoadEffect) -> str:
            raise NotImplementedError

        assert declared_definition(load) == EffectDef(name="load", effect_type=LoadEffect, handler=load, version=4)

    def test_raises_undeclared_class_error_when_effect_class_has_no_effect_decorator(self) -> None:
        @dataclass(frozen=True, slots=True)
        class UndeclaredEffect(InternalEffect[str]):
            pass

        @effect_handler
        async def load(effect: UndeclaredEffect) -> str:
            raise NotImplementedError

        with pytest.raises(UndeclaredClassError) as exc_info:
            declared_definition(load)

        assert exc_info.value.args == (load, UndeclaredEffect)


class TestDependency:
    def test_defines_dependency(self) -> None:
        @dependency(lifetime=EffectDependencyLifetime.SERVICE, name="named", type=object)
        async def resource() -> AsyncGenerator[str]:
            yield "resource"

        assert declared_definition(resource) == EffectDependencyDef(
            factory=resource,
            lifetime=EffectDependencyLifetime.SERVICE,
            name="named",
            type=object,
        )

    def test_defines_application_dependency_by_default(self) -> None:
        @dependency
        async def resource() -> AsyncGenerator[str]:
            yield "resource"

        assert declared_definition(resource) == EffectDependencyDef(factory=resource)


class TestMiddleware:
    def test_names_middleware_after_function(self) -> None:
        @middleware
        def log() -> MessageMiddlewareEffectful[object]:
            return (yield from Proceed())

        assert message_middleware_def(log) == MessageMiddlewareDef(name="log", middleware=log)

    def test_names_middleware(self) -> None:
        @middleware(name="logging")
        def log() -> MessageMiddlewareEffectful[object]:
            return (yield from Proceed())

        assert message_middleware_def(log) == MessageMiddlewareDef(name="logging", middleware=log)

    def test_declares_no_definition(self) -> None:
        @middleware
        def log() -> MessageMiddlewareEffectful[object]:
            return (yield from Proceed())

        assert not is_declared(log)
        assert declared_definition(log) is None


class TestEffectMiddleware:
    def test_defines_middleware_of_effects(self) -> None:
        @effect_middleware
        async def log(effect_call: EffectCall, call_next: CallNext) -> object:
            return await call_next()

        assert effect_middleware_def(log) == EffectMiddlewareDef(log)
        assert not is_declared(log)


class TestMarks:
    def test_raises_already_declared_error_when_function_is_declared_twice(self) -> None:
        @guard
        def current_user(command: SaveCommand) -> Effectful[str, QueryEffect]:
            yield from ()
            raise NotImplementedError

        with pytest.raises(AlreadyDeclaredError) as exc_info:
            command_handler(current_user)

        assert exc_info.value.args == (current_user,)

    def test_raises_unsupported_function_error_when_function_keeps_no_attributes(self) -> None:
        class Mailer:
            async def send(self, effect: LoadEffect) -> str:
                raise NotImplementedError

        send = Mailer().send

        with pytest.raises(UnsupportedFunctionError):
            effect_handler(send)

    def test_raises_already_declared_error_when_class_is_declared_twice(self) -> None:
        @event("token")
        class Token(Event):
            pass

        with pytest.raises(AlreadyDeclaredError) as exc_info:
            guard_input(Token)

        assert exc_info.value.args == (Token,)

    def test_declares_nothing_for_plain_function(self) -> None:
        def plain() -> None:
            pass

        assert not is_declared(plain)
        assert declared_definition(plain) is None


class TestClasses:
    def test_returns_event_of_class_replaced_by_dataclass_with_slots(self) -> None:
        @dataclass(frozen=True, slots=True)
        @event("replaced")
        class ReplacedEvent(Event):
            value: str

        assert declared_definition(ReplacedEvent) == EventDef(name="replaced", event_type=ReplacedEvent)

    def test_returns_guard_input(self) -> None:
        @guard_input
        class Token:
            pass

        assert declared_definition(Token) == GuardInputDef(Token)

    def test_declares_no_definition_for_command_class(self) -> None:
        assert not is_declared(SaveCommand)
        assert declared_definition(SaveCommand) is None

    def test_declares_no_definition_for_subclass_of_declared_event(self) -> None:
        class SubEvent(SavedEvent):
            pass

        assert declared_definition(SubEvent) is None
