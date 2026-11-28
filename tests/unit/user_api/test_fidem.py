from collections.abc import MutableSequence
from dataclasses import dataclass, field
from datetime import datetime

import pytest

from fidem import (
    Bundle,
    Command,
    Effectful,
    EffectNotDefinedError,
    Event,
    Fidem,
    InternalEffect,
    MessageContext,
    MessageMiddlewareEffectful,
    NowEffect,
    Proceed,
    Query,
    QueryEffect,
    QueryEffectful,
    StagedEffectful,
    command,
    command_handler,
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
from fidem.core import (
    DuplicateEventDefError,
    EffectDef,
    EventDef,
    MessageMiddlewareDef,
    OutboxDispatcherOptions,
    StagedEventHandlerDef,
)
from fidem.core.environment import CallNext
from fidem.core.operation_engine.instructions import EffectCall
from fidem.errors import UnsupportedDefinitionError
from fidem.memory import MemoryOutbox
from tests.unit.user_api.packages import app
from tests.unit.user_api.packages.app.messages import ChargeCommand


@command("greet")
@dataclass(frozen=True, slots=True)
class GreetCommand(Command[str]):
    name: str


@dataclass(frozen=True, slots=True)
class GreetQuery(Query[str]):
    name: str


@event("greeted")
@dataclass(frozen=True, slots=True)
class GreetedEvent(Event):
    name: str


@effect("greeting")
@dataclass(frozen=True, slots=True)
class GreetingEffect(InternalEffect[str]):
    name: str


@guard_input
@dataclass(frozen=True, slots=True)
class Token:
    user: str


@dataclass(frozen=True, slots=True)
class User:
    name: str


@effect_handler
async def handle_greeting(effect: GreetingEffect) -> str:
    return f"hello, {effect.name}"


@command_handler
def greet(command: GreetCommand, context: MessageContext) -> StagedEffectful[str]:
    greeting = yield from GreetingEffect(command.name)
    return f"{greeting} in {context.message_id}"


@query_handler
def get_greeting(query: GreetQuery) -> QueryEffectful[str]:
    return (yield from GreetingEffect(query.name))


@dataclass(slots=True)
class Calls:
    names: MutableSequence[str] = field(default_factory=list)


def make_outbox() -> MemoryOutbox:
    return MemoryOutbox(OutboxDispatcherOptions())


class TestHandle:
    async def test_runs_command_with_message_id(self) -> None:
        async with Fidem(definitions=[greet, handle_greeting]) as fidem:
            result = await fidem.handle(GreetCommand("Alice"), message_id="request-1")

        assert result == "hello, Alice in request-1"

    async def test_runs_query(self) -> None:
        async with Fidem(definitions=[get_greeting, handle_greeting]) as fidem:
            result = await fidem.handle(GreetQuery("Alice"))

        assert result == "hello, Alice"

    async def test_passes_inputs_to_guards(self) -> None:
        @guard
        def current_user(token: Token) -> Effectful[User, QueryEffect]:
            yield from ()
            return User(token.user)

        @command_handler
        def greet_user(command: GreetCommand, user: User) -> StagedEffectful[str]:
            yield from ()
            return f"{user.name} greets {command.name}"

        async with Fidem(definitions=[greet_user, current_user, Token]) as fidem:
            result = await fidem.handle(GreetCommand("Bob"), Token("Alice"))

        assert result == "Alice greets Bob"

    def test_hides_methods_of_environment_for_framework_only(self) -> None:
        fidem = Fidem()

        assert not hasattr(fidem, "deliver")
        assert not hasattr(fidem, "event_defs")


class TestStdlib:
    async def test_handles_effects_of_stdlib_by_default(self) -> None:
        @command_handler
        def now(command: GreetCommand) -> StagedEffectful[datetime]:
            return (yield from NowEffect())

        async with Fidem(definitions=[now]) as fidem:
            result = await fidem.handle(GreetCommand("Alice"))

        assert isinstance(result, datetime)

    async def test_does_not_handle_effects_of_stdlib_without_them(self) -> None:
        @command_handler
        def now(command: GreetCommand) -> StagedEffectful[datetime]:
            return (yield from NowEffect())

        async with Fidem(definitions=[now], stdlib=False) as fidem:
            with pytest.raises(EffectNotDefinedError):
                await fidem.handle(GreetCommand("Alice"))


class TestDefinitions:
    async def test_accepts_definitions_of_core(self) -> None:
        async def greet_in_core(effect: GreetingEffect) -> str:
            return f"hi, {effect.name}"

        async with Fidem(definitions=[greet, EffectDef("greeting", GreetingEffect, greet_in_core)]) as fidem:
            result = await fidem.handle(GreetCommand("Alice"), message_id="request-1")

        assert result == "hi, Alice in request-1"

    async def test_counts_declaration_given_twice_once(self) -> None:
        async with Fidem(definitions=[greet, greet, handle_greeting]) as fidem:
            result = await fidem.handle(GreetCommand("Alice"), message_id="request-1")

        assert result == "hello, Alice in request-1"

    async def test_merges_handlers_of_one_event(self) -> None:
        calls = Calls()

        @command_handler
        def publish(command: GreetCommand) -> StagedEffectful[str, GreetedEvent]:
            yield from GreetedEvent(command.name)
            return command.name

        @event_handler
        def first(event: GreetedEvent) -> StagedEffectful[None]:
            yield from ()
            calls.names.append("first")

        @event_handler
        def second(event: GreetedEvent) -> StagedEffectful[None]:
            yield from ()
            calls.names.append("second")

        outbox = make_outbox()
        definitions = [publish, first, second, outbox]
        async with Fidem(definitions=definitions) as fidem:
            await fidem.handle(GreetCommand("Alice"))
            await outbox.drain()

        assert sorted(calls.names) == ["first", "second"]

    async def test_declares_event_of_class_without_handlers(self) -> None:
        @command_handler
        def publish(command: GreetCommand) -> StagedEffectful[str, GreetedEvent]:
            yield from GreetedEvent(command.name)
            return command.name

        outbox = make_outbox()
        definitions = [publish, GreetedEvent, outbox]
        async with Fidem(definitions=definitions) as fidem:
            result = await fidem.handle(GreetCommand("Alice"))

        assert result == "Alice"
        assert outbox.rows == ()

    def test_raises_duplicate_event_def_error_when_definitions_of_one_event_differ(self) -> None:
        def record(event: GreetedEvent) -> StagedEffectful[None]:
            yield from ()
            raise NotImplementedError

        other_def = EventDef("other", GreetedEvent, handler_defs=[StagedEventHandlerDef("record", record)])

        with pytest.raises(DuplicateEventDefError):
            Fidem(definitions=[GreetedEvent, other_def])

    def test_raises_unsupported_definition_error_for_class_without_definition(self) -> None:
        with pytest.raises(UnsupportedDefinitionError) as exc_info:
            Fidem(definitions=[GreetCommand])

        assert exc_info.value.args == (GreetCommand,)


class TestBundle:
    async def test_adds_contents_of_bundles_nested_in_each_other(self) -> None:
        async with Fidem(definitions=[Bundle(greet, Bundle(handle_greeting))]) as fidem:
            result = await fidem.handle(GreetCommand("Alice"), message_id="request-1")

        assert result == "hello, Alice in request-1"

    async def test_counts_definition_given_in_bundle_and_by_itself_once(self) -> None:
        async with Fidem(definitions=[Bundle(greet, handle_greeting), greet]) as fidem:
            result = await fidem.handle(GreetCommand("Alice"), message_id="request-1")

        assert result == "hello, Alice in request-1"


class TestModules:
    async def test_runs_discovered_handler_once_though_it_is_found_twice(self) -> None:
        async with Fidem(modules=[app]) as fidem:
            result = await fidem.handle(ChargeCommand(10))

        assert result == "charged 10"

    async def test_adds_discovered_definitions_to_given_ones(self) -> None:
        async with Fidem(modules=[app], definitions=[greet, handle_greeting]) as fidem:
            charged = await fidem.handle(ChargeCommand(10))
            greeted = await fidem.handle(GreetCommand("Alice"), message_id="request-1")

        assert (charged, greeted) == ("charged 10", "hello, Alice in request-1")


class TestMiddlewares:
    async def test_runs_middlewares_in_order_named_after_functions(self) -> None:
        calls = Calls()

        def outer() -> MessageMiddlewareEffectful[object]:
            calls.names.append("outer")
            return (yield from Proceed())

        def inner() -> MessageMiddlewareEffectful[object]:
            calls.names.append("inner")
            return (yield from Proceed())

        async with Fidem(definitions=[greet, handle_greeting], middlewares=[outer, inner]) as fidem:
            await fidem.handle(GreetCommand("Alice"))

        assert calls.names == ["outer", "inner"]

    async def test_accepts_definition_of_middleware(self) -> None:
        def cached() -> MessageMiddlewareEffectful[object]:
            yield from ()
            return "cached"

        middleware_def = MessageMiddlewareDef(name="cache", middleware=cached)
        async with Fidem(definitions=[greet], middlewares=[middleware_def]) as fidem:
            result = await fidem.handle(GreetCommand("Alice"))

        assert result == "cached"

    async def test_accepts_declared_middleware(self) -> None:
        @middleware(name="cache")
        def cached() -> MessageMiddlewareEffectful[object]:
            yield from ()
            return "cached"

        async with Fidem(definitions=[greet], middlewares=[cached]) as fidem:
            result = await fidem.handle(GreetCommand("Alice"))

        assert result == "cached"

    def test_does_not_take_middleware_for_definition(self) -> None:
        @middleware
        def cached() -> MessageMiddlewareEffectful[object]:
            yield from ()
            return "cached"

        with pytest.raises(UnsupportedDefinitionError):
            Fidem(definitions=[cached])

    async def test_runs_effect_middlewares(self) -> None:
        calls = Calls()

        @effect_middleware
        async def record(effect_call: EffectCall, call_next: CallNext) -> object:
            calls.names.append(type(effect_call.effect).__name__)
            return await call_next()

        async with Fidem(definitions=[greet, handle_greeting], effect_middlewares=[record]) as fidem:
            await fidem.handle(GreetCommand("Alice"))

        assert calls.names == ["GreetingEffect"]

    async def test_lets_handler_replace_middlewares(self) -> None:
        def failing() -> MessageMiddlewareEffectful[object]:
            yield from ()
            raise AssertionError

        @command_handler(middlewares=())
        def greet_without_middlewares(command: GreetCommand) -> StagedEffectful[str]:
            yield from ()
            return command.name

        async with Fidem(definitions=[greet_without_middlewares], middlewares=[failing]) as fidem:
            result = await fidem.handle(GreetCommand("Alice"))

        assert result == "Alice"
