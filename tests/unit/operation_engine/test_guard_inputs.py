from collections.abc import Callable, MutableSequence, Sequence
from dataclasses import dataclass
from typing import Any

import pytest

from fidem.di.core import DependencyNotFoundError
from fidem.effects import Effect, Effectful
from fidem.guards import (
    AmbiguousGuardInputError,
    Guard,
    GuardDef,
    GuardInputDef,
    GuardInputNotDefinedError,
    MissingGuardInputError,
)
from fidem.messages import QueryEffect
from fidem.operation_engine.definitions import (
    EventDef,
    MessageMiddlewareDef,
    MessageMiddlewareEffectful,
    Proceed,
    StagedEventHandlerDef,
)
from fidem.operation_engine.engine import OperationEngineImpl
from fidem.testing import YieldStep, assert_generator
from tests.effects import TestInternalEffect
from tests.messages import TestCommand, TestEvent, TestQuery
from tests.stubs import run
from tests.unit.operation_engine.helpers import (
    MESSAGE_CONTEXT,
    handle_without_effects,
    make_engine,
    make_expected_instruction,
    make_query_engine,
)


@dataclass(frozen=True, slots=True)
class AccessToken:
    raw: str


@dataclass(frozen=True, slots=True)
class BearerToken(AccessToken):
    pass


@dataclass(frozen=True, slots=True)
class Locale:
    code: str


@dataclass(frozen=True, slots=True)
class User:
    name: str


def authenticate(token: AccessToken) -> Effectful[User, QueryEffect]:
    yield from ()
    return User(name=token.raw)


def greet(user: User) -> Effectful[str, Effect[Any]]:
    yield from ()
    return f"hello {user.name}"


def handle_token(token: AccessToken) -> Effectful[str, Effect[Any]]:
    yield from ()
    return token.raw


def take_token(token: AccessToken) -> MessageMiddlewareEffectful[object]:
    return (yield from Proceed())


TOKEN_GUARD_DEFS = [GuardDef(authenticate), GuardInputDef(AccessToken)]


def make_counting_authenticate(calls: MutableSequence[str]) -> Guard[User]:
    def counting_authenticate(token: AccessToken) -> Effectful[User, QueryEffect]:
        calls.append(token.raw)
        return (yield from authenticate(token))

    return counting_authenticate


class TestGuardInputs:
    @pytest.mark.parametrize(
        argnames=("make", "message"),
        argvalues=[(make_engine, TestCommand()), (make_query_engine, TestQuery())],
        ids=["command", "query"],
    )
    def test_passes_input_into_guard_and_guard_value_into_handler(
        self,
        make: Callable[..., OperationEngineImpl],
        message: TestCommand | TestQuery,
    ) -> None:
        engine = make(greet, TOKEN_GUARD_DEFS)

        assert run(engine.handle(message, MESSAGE_CONTEXT, [AccessToken("alice")])) == "hello alice"

    def test_matches_input_of_subclass_of_declared_type(self) -> None:
        engine = make_engine(greet, TOKEN_GUARD_DEFS)

        assert run(engine.handle(TestCommand(), MESSAGE_CONTEXT, [BearerToken("alice")])) == "hello alice"

    def test_ignores_declared_input_operation_does_not_need(self) -> None:
        engine = make_engine(handle_without_effects, [*TOKEN_GUARD_DEFS, GuardInputDef(Locale)])

        assert run(engine.handle(TestCommand(), MESSAGE_CONTEXT, [AccessToken("alice"), Locale("en")])) == (
            "command_result"
        )

    def test_does_not_pass_inputs_into_effect_calls(self) -> None:
        def handler(user: User) -> Effectful[str, Effect[Any]]:
            yield from effect
            return user.name

        effect = TestInternalEffect()
        command = TestCommand()
        engine = make_engine(handler, TOKEN_GUARD_DEFS)

        assert_generator(
            generator=engine.handle(command, MESSAGE_CONTEXT, [AccessToken("alice")]),
            steps=[YieldStep(make_expected_instruction(effect, command), None)],
            return_value="alice",
        )

    def test_does_not_run_guards_again_when_middleware_proceeds_twice(self) -> None:
        def retry(user: User) -> MessageMiddlewareEffectful[object]:
            yield from Proceed()
            return (yield from Proceed())

        calls: MutableSequence[str] = []
        engine = make_engine(
            handler=greet,
            guard_defs=[GuardDef(make_counting_authenticate(calls)), GuardInputDef(AccessToken)],
            middleware_defs=[MessageMiddlewareDef("retry", retry)],
        )

        assert run(engine.handle(TestCommand(), MESSAGE_CONTEXT, [AccessToken("alice")])) == "hello alice"
        assert calls == ["alice"]


class TestGuardInputErrors:
    def test_raises_guard_input_not_defined_error_for_undeclared_input(self) -> None:
        engine = make_engine(greet, TOKEN_GUARD_DEFS)

        with pytest.raises(GuardInputNotDefinedError) as exc_info:
            run(engine.handle(TestCommand(), MESSAGE_CONTEXT, [AccessToken("alice"), Locale("en")]))

        assert exc_info.value.args == (Locale,)

    def test_raises_ambiguous_guard_input_error_when_two_inputs_match_one_declaration(self) -> None:
        engine = make_engine(greet, TOKEN_GUARD_DEFS)

        with pytest.raises(AmbiguousGuardInputError) as exc_info:
            run(engine.handle(TestCommand(), MESSAGE_CONTEXT, [AccessToken("alice"), BearerToken("bob")]))

        assert exc_info.value.args == ([AccessToken, BearerToken],)

    def test_raises_ambiguous_guard_input_error_when_input_matches_two_declarations(self) -> None:
        def authenticate_bearer(token: BearerToken) -> Effectful[User, QueryEffect]:
            return (yield from authenticate(token))

        engine = make_engine(
            handler=greet,
            guard_defs=[GuardDef(authenticate_bearer), GuardInputDef(AccessToken), GuardInputDef(BearerToken)],
        )

        with pytest.raises(AmbiguousGuardInputError) as exc_info:
            run(engine.handle(TestCommand(), MESSAGE_CONTEXT, [BearerToken("alice")]))

        assert exc_info.value.args == ([AccessToken, BearerToken],)

    def test_raises_missing_guard_input_error_and_does_not_run_guards(self) -> None:
        calls: MutableSequence[str] = []
        engine = make_engine(greet, [GuardDef(make_counting_authenticate(calls)), GuardInputDef(AccessToken)])

        with pytest.raises(MissingGuardInputError) as exc_info:
            run(engine.handle(TestCommand(), MESSAGE_CONTEXT))

        assert exc_info.value.args == ([AccessToken],)
        assert calls == []

    def test_raises_dependency_not_found_error_when_event_handler_guard_needs_input(self) -> None:
        def handle_event(user: User) -> Effectful[None, Effect[Any]]:
            yield from ()

        with pytest.raises(DependencyNotFoundError) as exc_info:
            make_engine(
                handler=handle_without_effects,
                guard_defs=TOKEN_GUARD_DEFS,
                event_defs=[EventDef("test", TestEvent, handler_defs=[StagedEventHandlerDef("greet", handle_event)])],
            )

        assert exc_info.value.args[1:] == ("token", AccessToken)

    @pytest.mark.parametrize(
        argnames="middleware_defs",
        argvalues=[(), [MessageMiddlewareDef("take_token", take_token)]],
        ids=["handler", "middleware"],
    )
    def test_raises_dependency_not_found_error_when_function_other_than_guard_needs_input(
        self,
        middleware_defs: Sequence[MessageMiddlewareDef],
    ) -> None:
        handler = handle_without_effects if middleware_defs else handle_token

        with pytest.raises(DependencyNotFoundError) as exc_info:
            make_engine(handler, TOKEN_GUARD_DEFS, middleware_defs=middleware_defs)

        assert exc_info.value.args[1:] == ("token", AccessToken)
