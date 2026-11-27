from collections.abc import MutableSequence
from typing import Any

import pytest

from fidem.effects import Effect, Effectful
from fidem.guards import GuardDef
from fidem.messages import MessageContext, QueryEffect
from fidem.operation_engine.definitions import Proceed
from fidem.operation_engine.errors import UnsupportedEffectError
from fidem.testing import YieldStep, assert_generator
from tests.effects import TestControlEffect, TestDatabaseWriteEffect, TestExternalReadEffect, TestInternalEffect
from tests.ids import type_name
from tests.messages import TestCommand, TestEvent
from tests.stubs import run
from tests.transactions import write_transaction
from tests.unit.operation_engine.helpers import (
    MESSAGE_CONTEXT,
    OtherService,
    Service,
    collect_effect_ids,
    handle_with_three_effects,
    make_engine,
    make_expected_instruction,
)


def handle_service_with_three_effects(service: Service) -> Effectful[str, Effect[Any]]:
    return (yield from handle_with_three_effects())


def make_service_with_two_effects() -> Effectful[Service, QueryEffect]:
    yield from TestInternalEffect()
    yield from TestExternalReadEffect()
    return Service()


class TestGuards:
    def test_runs_guards_with_their_effects_before_handler(self) -> None:
        def other(context: MessageContext) -> Effectful[OtherService, QueryEffect]:
            events.append(f"other {context.message_id}")
            assert (yield from internal_effect) == "internal_result"
            return OtherService()

        def service(other: OtherService) -> Effectful[Service, QueryEffect]:
            events.append("service")
            assert (yield from external_effect) == "external_result"
            return Service()

        def handler(service: Service, other: OtherService) -> Effectful[str, Effect[Any]]:
            events.append("handler")
            yield from ()
            return f"{type(service).__name__} {type(other).__name__}"

        events: MutableSequence[str] = []
        internal_effect = TestInternalEffect()
        external_effect = TestExternalReadEffect()
        command = TestCommand()
        engine = make_engine(handler, [GuardDef(service), GuardDef(other)])

        assert_generator(
            generator=engine.execute(command, MessageContext(message_id="test_message_id")),
            steps=[
                YieldStep(make_expected_instruction(internal_effect, command), "internal_result"),
                YieldStep(make_expected_instruction(external_effect, command), "external_result"),
            ],
            return_value="Service OtherService",
        )
        assert events == ["other test_message_id", "service", "handler"]

    def test_runs_guards_for_each_message(self) -> None:
        def service() -> Effectful[Service, QueryEffect]:
            yield from ()
            return Service()

        def handler(service: Service) -> Effectful[Service, Effect[Any]]:
            yield from ()
            return service

        engine = make_engine(handler, [GuardDef(service)])

        first = run(engine.execute(TestCommand(), MESSAGE_CONTEXT))
        second = run(engine.execute(TestCommand(), MESSAGE_CONTEXT))

        assert first is not second

    def test_throws_effect_error_into_guard(self) -> None:
        def service() -> Effectful[Service, QueryEffect]:
            try:
                yield from TestInternalEffect()
            except ValueError:
                return Service()
            raise AssertionError("unreachable")

        def handler(service: Service) -> Effectful[str, Effect[Any]]:
            yield from ()
            return type(service).__name__

        engine = make_engine(handler, [GuardDef(service)])
        execution_plan = engine.execute(TestCommand(), MESSAGE_CONTEXT)
        execution_plan.send(None)

        with pytest.raises(StopIteration) as exc_info:
            execution_plan.throw(ValueError("effect_error"))

        assert exc_info.value.value == "Service"

    def test_raises_guard_error_and_does_not_call_handler(self) -> None:
        def service() -> Effectful[Service, QueryEffect]:
            yield from ()
            raise PermissionError("forbidden")

        def handler(service: Service) -> Effectful[str, Effect[Any]]:
            nonlocal is_handler_called
            is_handler_called = True
            yield from ()
            return "command_result"

        is_handler_called = False
        engine = make_engine(handler, [GuardDef(service)])

        with pytest.raises(PermissionError, match="forbidden"):
            engine.execute(TestCommand(), MESSAGE_CONTEXT).send(None)

        assert not is_handler_called

    @pytest.mark.parametrize(
        argnames="effect",
        argvalues=[
            TestDatabaseWriteEffect(),
            TestControlEffect(),
            TestCommand(),
            TestEvent(),
            Proceed(),
            write_transaction(),
        ],
        ids=type_name,
    )
    def test_raises_unsupported_effect_error_when_guard_yields_not_guard_effect(
        self,
        effect: Effect[Any],
    ) -> None:
        def service() -> Effectful[Service, QueryEffect]:
            nonlocal is_guard_closed
            try:
                yield from effect  # pyright: ignore[reportReturnType] - checks a wrong effect
            except Exception:  # noqa: BLE001 - checks that no error reaches the guard
                return Service()
            finally:
                is_guard_closed = True
            raise AssertionError("unreachable")

        def handler(service: Service) -> Effectful[str, Effect[Any]]:
            yield from ()
            return "command_result"

        is_guard_closed = False
        engine = make_engine(handler, [GuardDef(service)])

        with pytest.raises(UnsupportedEffectError) as exc_info:
            engine.execute(TestCommand(), MESSAGE_CONTEXT).send(None)

        assert exc_info.value.args == (effect,)
        assert is_guard_closed

    def test_generates_same_guard_effect_ids_for_same_message_id(self) -> None:
        engine = make_engine(handle_service_with_three_effects, [GuardDef(make_service_with_two_effects)])

        effect_ids1 = collect_effect_ids(engine.execute(TestCommand(), MessageContext(message_id="test_message_id")))
        effect_ids2 = collect_effect_ids(engine.execute(TestCommand(), MessageContext(message_id="test_message_id")))

        assert effect_ids1 == effect_ids2
        assert len(set(effect_ids1)) == 5

    def test_does_not_shift_handler_effect_ids_when_guard_added(self) -> None:
        handler_effect_ids = collect_effect_ids(
            make_engine(handle_with_three_effects).execute(TestCommand(), MessageContext(message_id="test_message_id"))
        )
        engine = make_engine(handle_service_with_three_effects, [GuardDef(make_service_with_two_effects)])

        effect_ids = collect_effect_ids(engine.execute(TestCommand(), MessageContext(message_id="test_message_id")))

        assert effect_ids[2:] == handler_effect_ids

    def test_does_not_repeat_guard_effect_ids_in_message_whose_id_extends_other_message_id(self) -> None:
        engine = make_engine(
            handler=handle_service_with_three_effects,
            guard_defs=[GuardDef(make_service_with_two_effects)],
        )
        guard_effect_ids = collect_effect_ids(engine.execute(TestCommand(), MessageContext(message_id="m")))[:2]
        message_id = f"m::{Service.__module__}.{Service.__qualname__}"

        effect_ids = collect_effect_ids(engine.execute(TestCommand(), MessageContext(message_id=message_id)))

        assert set(effect_ids).isdisjoint(guard_effect_ids)

    def test_generates_different_guard_effect_ids_for_different_guards(self) -> None:
        def other() -> Effectful[OtherService, QueryEffect]:
            yield from TestInternalEffect()
            yield from TestExternalReadEffect()
            return OtherService()

        def handler(service: Service, other: OtherService) -> Effectful[str, Effect[Any]]:
            yield from ()
            return "command_result"

        engine = make_engine(
            handler=handler,
            guard_defs=[
                GuardDef(make_service_with_two_effects),
                GuardDef(other),
            ],
        )

        effect_ids = collect_effect_ids(engine.execute(TestCommand(), MessageContext(message_id="test_message_id")))

        assert len(set(effect_ids)) == 4
