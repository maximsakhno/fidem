from collections.abc import MutableSequence, Sequence
from typing import Any

import pytest

from fidem.di.core import DependencyNotFoundError, UnsupportedFunctionError
from fidem.effects import Effect, Effectful
from fidem.guards import GuardDef
from fidem.messages import AtomicEffectful, Event, QueryEffect, StagedEffectful, StagedHandler
from fidem.operation_engine.definitions import (
    AtomicCommandDef,
    AtomicEventHandlerDef,
    EventDef,
    MessageMiddlewareDef,
    MessageMiddlewareEffectful,
    Proceed,
    StagedCommandDef,
    StagedEventHandlerDef,
)
from fidem.operation_engine.engine import OperationEngineImpl
from fidem.operation_engine.errors import (
    DuplicateEventDefError,
    DuplicateEventHandlerDefError,
    EventNotDefinedError,
    UnsupportedEffectError,
)
from fidem.operation_engine.instructions import CallEffectHandlerInstruction, ExecutionPlan, RunTransactionInstruction
from fidem.outbox import OutboxEvent, WriteOutboxEffect
from fidem.transactions import IMPLICIT_WRITE_TRANSACTION, WriteTransaction, WriteTransactionEffectful
from tests.effects import TestDatabaseWriteEffect, TestInternalEffect
from tests.messages import TestCommand, TestEvent
from tests.unit.operation_engine.helpers import (
    MESSAGE_CONTEXT,
    OtherService,
    Service,
    collect_effect_ids,
    collect_effects,
    handle_without_effects,
    make_engine,
    run_plan,
)


class OtherEvent(Event):
    pass


class UndefinedEvent(Event):
    pass


def handle_event() -> StagedEffectful[None]:
    yield from ()


@WriteTransaction
def save_nothing() -> WriteTransactionEffectful[None]:
    yield from ()


def handle_event_atomically() -> AtomicEffectful[None]:
    yield from ()
    return save_nothing()


EVENT_DEFS: Sequence[EventDef[Any]] = [
    EventDef(
        name="test.event",
        event_type=TestEvent,
        version=2,
        handler_defs=[
            StagedEventHandlerDef("first", handle_event),
            AtomicEventHandlerDef("second", handle_event_atomically),
        ],
    ),
    EventDef(name="test.other", event_type=OtherEvent),
]


@WriteTransaction
def publish_in_transaction(event: Event) -> WriteTransactionEffectful[str, Event]:
    yield from event
    return (yield from TestDatabaseWriteEffect())


def publish_other_event() -> StagedEffectful[None, OtherEvent]:
    yield from OtherEvent()


def publish_other_event_atomically() -> AtomicEffectful[str, Event]:
    yield from ()
    return publish_in_transaction(OtherEvent())


def publish_event_in_transaction() -> StagedEffectful[str, Event]:
    return (yield from publish_in_transaction(TestEvent()))


def publish_event_atomically() -> AtomicEffectful[str, Event]:
    yield from ()
    return publish_in_transaction(TestEvent())


def publish_undefined_event() -> StagedEffectful[str, Event]:
    try:
        yield from UndefinedEvent()
    except Exception:  # noqa: BLE001 - checks that no error reaches the handler
        return "swallowed"
    return "unreachable"


@WriteTransaction
def publish_undefined_event_in_transaction() -> WriteTransactionEffectful[str, Event]:
    try:
        return (yield from publish_in_transaction.function(UndefinedEvent()))
    except Exception:  # noqa: BLE001 - checks that no error reaches the transaction
        return "swallowed"


def publish_undefined_event_atomically() -> AtomicEffectful[str, Event]:
    yield from ()
    return publish_undefined_event_in_transaction()


def plan_publishing(event: Event) -> ExecutionPlan[Any]:
    def handler() -> StagedEffectful[None, Event]:
        yield from event

    return make_engine(handler, event_defs=EVENT_DEFS).execute(TestCommand(), MESSAGE_CONTEXT)


def retry_once() -> MessageMiddlewareEffectful[object]:
    try:
        return (yield from Proceed())
    except ValueError:
        return (yield from Proceed())


class TestEvents:
    def test_writes_events_of_atomic_transaction_to_outbox_at_end_of_transaction(self) -> None:
        def handler() -> AtomicEffectful[str, Event]:
            yield from ()
            return publish_in_transaction(event)

        event = TestEvent()
        engine = make_engine(handler, command_def_type=AtomicCommandDef, event_defs=EVENT_DEFS)
        execution_plan = engine.execute(TestCommand(), MESSAGE_CONTEXT)

        instruction = execution_plan.send(None)
        assert isinstance(instruction, RunTransactionInstruction)
        write_instruction = instruction.plan.send(None)
        assert isinstance(write_instruction, CallEffectHandlerInstruction)
        assert isinstance(write_instruction.call.effect, TestDatabaseWriteEffect)
        outbox_instruction = instruction.plan.send("write_result")
        assert isinstance(outbox_instruction, CallEffectHandlerInstruction)
        assert isinstance(outbox_instruction.call.effect, WriteOutboxEffect)
        assert [outbox_event.event for outbox_event in outbox_instruction.call.effect.events] == [event]
        with pytest.raises(StopIteration) as exc_info:
            instruction.plan.send(None)
        assert exc_info.value.value == "write_result"

    def test_makes_outbox_event_with_its_definition_and_handlers(self) -> None:
        event = TestEvent()

        effects = collect_effects(plan_publishing(event))

        assert len(effects) == 1
        assert isinstance(effects[0].call.effect, WriteOutboxEffect)
        outbox_event = effects[0].call.effect.events[0]
        assert effects[0].call.effect.events == (
            OutboxEvent(
                event_id=outbox_event.event_id,
                source_message_id="test_message_id",
                name="test.event",
                version=2,
                handlers=("first", "second"),
                event=event,
            ),
        )
        assert outbox_event.handler_message_id("first") == f"{outbox_event.event_id}:first"

    def test_takes_event_id_from_effect_ids_in_place_of_event(self) -> None:
        def handler() -> StagedEffectful[None, TestEvent]:
            yield from TestInternalEffect()
            yield from TestEvent()
            yield from TestInternalEffect()

        def plain_handler() -> Effectful[None, Effect[Any]]:
            yield from TestInternalEffect()
            yield from TestInternalEffect()
            yield from TestInternalEffect()

        effects = collect_effects(make_engine(handler, event_defs=EVENT_DEFS).execute(TestCommand(), MESSAGE_CONTEXT))
        plain_effect_ids = collect_effect_ids(make_engine(plain_handler).execute(TestCommand(), MESSAGE_CONTEXT))

        outbox_effect = effects[2].call.effect
        assert isinstance(outbox_effect, WriteOutboxEffect)
        assert [effects[0].call.effect_context.effect_id, outbox_effect.events[0].event_id] == plain_effect_ids[:2]
        assert effects[1].call.effect_context.effect_id == plain_effect_ids[2]

    def test_generates_same_event_ids_for_same_message_id(self) -> None:
        first = collect_effects(plan_publishing(TestEvent()))
        second = collect_effects(plan_publishing(TestEvent()))

        assert [effect.call.effect_context for effect in first] == [effect.call.effect_context for effect in second]
        assert isinstance(first[0].call.effect, WriteOutboxEffect)
        assert isinstance(second[0].call.effect, WriteOutboxEffect)
        assert first[0].call.effect.events[0].event_id == second[0].call.effect.events[0].event_id

    def test_writes_events_of_transaction_in_one_effect_and_skips_events_without_handlers(self) -> None:
        @WriteTransaction
        def transaction() -> WriteTransactionEffectful[None, Event]:
            yield from first
            yield from OtherEvent()
            yield from second

        def handler() -> AtomicEffectful[None, Event]:
            yield from ()
            return transaction()

        first = TestEvent()
        second = TestEvent()
        engine = make_engine(handler, command_def_type=AtomicCommandDef, event_defs=EVENT_DEFS)

        effects = collect_effects(engine.execute(TestCommand(), MESSAGE_CONTEXT))

        assert len(effects) == 1
        assert isinstance(effects[0].call.effect, WriteOutboxEffect)
        assert [outbox_event.event for outbox_event in effects[0].call.effect.events] == [first, second]

    @pytest.mark.parametrize(
        argnames=("command_def_type", "handler"),
        argvalues=[(StagedCommandDef, publish_other_event), (AtomicCommandDef, publish_other_event_atomically)],
    )
    def test_does_not_write_outbox_when_no_event_has_handlers(
        self,
        command_def_type: type[StagedCommandDef[Any]] | type[AtomicCommandDef[Any]],
        handler: StagedHandler[Any, Any],
    ) -> None:
        engine = make_engine(handler, command_def_type=command_def_type, event_defs=EVENT_DEFS)

        effects = collect_effects(engine.execute(TestCommand(), MESSAGE_CONTEXT))

        assert not any(isinstance(effect.call.effect, WriteOutboxEffect) for effect in effects)

    def test_writes_events_of_staged_handler_after_it_returns_outside_its_transactions(self) -> None:
        def handler() -> StagedEffectful[str, Event]:
            transaction_result = yield from publish_in_transaction(first)
            published = yield from second
            return f"{transaction_result} {published}"

        first = TestEvent()
        second = TestEvent()
        execution_plan = make_engine(handler, event_defs=EVENT_DEFS).execute(TestCommand(), MESSAGE_CONTEXT)

        instruction = execution_plan.send(None)
        assert isinstance(instruction, RunTransactionInstruction)
        write_instruction = instruction.plan.send(None)
        assert isinstance(write_instruction, CallEffectHandlerInstruction)
        assert isinstance(write_instruction.call.effect, TestDatabaseWriteEffect)
        with pytest.raises(StopIteration):
            instruction.plan.send("write_result")
        outbox_transaction_instruction = execution_plan.send("write_result")
        assert isinstance(outbox_transaction_instruction, RunTransactionInstruction)
        assert outbox_transaction_instruction.transaction is IMPLICIT_WRITE_TRANSACTION
        outbox_instruction = outbox_transaction_instruction.plan.send(None)
        assert isinstance(outbox_instruction, CallEffectHandlerInstruction)
        assert isinstance(outbox_instruction.call.effect, WriteOutboxEffect)
        assert [outbox_event.event for outbox_event in outbox_instruction.call.effect.events] == [first, second]
        with pytest.raises(StopIteration):
            outbox_transaction_instruction.plan.send(None)
        with pytest.raises(StopIteration) as exc_info:
            execution_plan.send(None)
        assert exc_info.value.value == "write_result None"

    def test_drops_events_of_failed_run_of_staged_handler(self) -> None:
        def handler() -> StagedEffectful[None, TestEvent]:
            nonlocal runs
            runs += 1
            yield from failed_event if runs == 1 else event
            if runs == 1:
                raise ValueError("handler_error")

        runs = 0
        failed_event = TestEvent()
        event = TestEvent()
        engine = make_engine(
            handler=handler,
            middleware_defs=[MessageMiddlewareDef("retry", retry_once)],
            event_defs=EVENT_DEFS,
        )

        effects = collect_effects(engine.execute(TestCommand(), MESSAGE_CONTEXT))

        assert len(effects) == 1
        assert isinstance(effects[0].call.effect, WriteOutboxEffect)
        assert [outbox_event.event for outbox_event in effects[0].call.effect.events] == [event]

    @pytest.mark.parametrize(
        argnames=("command_def_type", "handler"),
        argvalues=[(StagedCommandDef, publish_event_in_transaction), (AtomicCommandDef, publish_event_atomically)],
    )
    def test_throws_outbox_error_into_middleware_so_that_it_can_retry(
        self,
        command_def_type: type[StagedCommandDef[Any]] | type[AtomicCommandDef[Any]],
        handler: StagedHandler[Any, Any],
    ) -> None:
        def write_outbox_once(instruction: CallEffectHandlerInstruction[Any]) -> object:
            if isinstance(instruction.call.effect, WriteOutboxEffect):
                outbox_effects.append(instruction.call.effect)
                if len(outbox_effects) == 1:
                    raise ValueError("outbox_error")
            return "effect_result"

        outbox_effects: MutableSequence[WriteOutboxEffect] = []
        engine = make_engine(
            handler=handler,
            middleware_defs=[MessageMiddlewareDef("retry", retry_once)],
            command_def_type=command_def_type,
            event_defs=EVENT_DEFS,
        )

        result = run_plan(engine.execute(TestCommand(), MESSAGE_CONTEXT), write_outbox_once)

        assert result == "effect_result"
        assert len(outbox_effects) == 2
        assert outbox_effects[0].events[0].event_id == outbox_effects[1].events[0].event_id

    @pytest.mark.parametrize(
        argnames=("command_def_type", "handler"),
        argvalues=[(StagedCommandDef, publish_undefined_event), (AtomicCommandDef, publish_undefined_event_atomically)],
    )
    def test_raises_event_not_defined_error_without_throwing_it_into_handler(
        self,
        command_def_type: type[StagedCommandDef[Any]] | type[AtomicCommandDef[Any]],
        handler: StagedHandler[Any, Any],
    ) -> None:
        engine = make_engine(handler, command_def_type=command_def_type, event_defs=EVENT_DEFS)

        with pytest.raises(EventNotDefinedError) as exc_info:
            run_plan(engine.execute(TestCommand(), MESSAGE_CONTEXT))

        assert exc_info.value.args == (UndefinedEvent,)

    def test_raises_unsupported_effect_error_when_transaction_of_middleware_publishes_event(self) -> None:
        def middleware() -> MessageMiddlewareEffectful[object]:
            yield from publish_in_transaction(event)  # pyright: ignore[reportReturnType] - checks what the middleware type forbids
            return (yield from Proceed())

        event = TestEvent()
        engine = make_engine(
            handler=handle_without_effects,
            middleware_defs=[MessageMiddlewareDef("test", middleware)],
            event_defs=EVENT_DEFS,
        )

        with pytest.raises(UnsupportedEffectError) as exc_info:
            run_plan(engine.execute(TestCommand(), MESSAGE_CONTEXT))

        assert exc_info.value.args == (event,)


class TestEventDefinition:
    @pytest.mark.parametrize("same", ["type", "name"])
    def test_raises_duplicate_event_def_error_when_events_have_same_type_or_name(self, same: str) -> None:
        event_defs = [
            EventDef("first", TestEvent),
            EventDef("first" if same == "name" else "second", OtherEvent if same == "name" else TestEvent),
        ]

        with pytest.raises(DuplicateEventDefError) as exc_info:
            OperationEngineImpl(event_defs=event_defs)

        assert exc_info.value.args == (event_defs,)

    def test_raises_duplicate_event_handler_def_error_when_handlers_of_event_have_same_name(self) -> None:
        handler_defs = [StagedEventHandlerDef("test", handle_event), StagedEventHandlerDef("test", handle_event)]

        with pytest.raises(DuplicateEventHandlerDefError) as exc_info:
            OperationEngineImpl(event_defs=[EventDef("test", TestEvent, handler_defs=handler_defs)])

        assert exc_info.value.args == (handler_defs,)

    def test_allows_same_handler_name_in_different_events(self) -> None:
        OperationEngineImpl(
            event_defs=[
                EventDef("test", TestEvent, handler_defs=[StagedEventHandlerDef("handler", handle_event)]),
                EventDef("other", OtherEvent, handler_defs=[StagedEventHandlerDef("handler", handle_event)]),
            ]
        )

    def test_raises_unsupported_function_error_when_event_handler_is_not_generator_function(self) -> None:
        def handler(event: TestEvent) -> None:
            return None

        with pytest.raises(UnsupportedFunctionError) as exc_info:
            OperationEngineImpl(
                event_defs=[
                    EventDef("test", TestEvent, handler_defs=[StagedEventHandlerDef("test", handler)]),  # pyright: ignore[reportArgumentType] - checks a wrong handler
                ]
            )

        assert exc_info.value.args == (handler,)

    def test_binds_parameters_of_event_handler_and_its_middlewares_to_guards_of_event(self) -> None:
        def service(event: TestEvent) -> Effectful[Service, QueryEffect]:
            yield from ()
            return Service()

        def middleware(other: OtherService) -> MessageMiddlewareEffectful[object]:
            return (yield from Proceed())

        def handler(event: TestEvent, service: Service) -> StagedEffectful[None]:
            yield from ()

        with pytest.raises(DependencyNotFoundError) as exc_info:
            OperationEngineImpl(
                event_defs=[
                    EventDef(
                        name="test",
                        event_type=TestEvent,
                        handler_defs=[
                            StagedEventHandlerDef("ok", handler),
                            StagedEventHandlerDef("test", handler, [MessageMiddlewareDef("test", middleware)]),
                        ],
                    ),
                ],
                guard_defs=[GuardDef(service)],
            )

        assert exc_info.value.args == (middleware, "other", OtherService)
