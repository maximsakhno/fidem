from asyncio import Event as AsyncioEvent, get_running_loop, sleep, timeout
from collections.abc import Callable, Collection, MutableSequence, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest

from fidem.effects import InternalEffect
from fidem.environment import CallNext, EffectDef, EffectMiddlewareDef, EnvironmentImpl
from fidem.memory import MemoryOutbox, MemoryOutboxRow, OutboxStatus
from fidem.messages import Event, MessageContext, StagedEffectful
from fidem.operation_engine.definitions import EventDef, StagedCommandDef, StagedEventHandlerDef
from fidem.operation_engine.engine import OperationEngineImpl
from fidem.operation_engine.instructions import EffectCall
from fidem.outbox import OutboxDispatcherOptions, RetryPolicy, WriteOutboxEffect
from tests.messages import TestCommand


@dataclass(frozen=True, slots=True)
class Created(Event):
    value: str


@dataclass(frozen=True, slots=True)
class Followed(Event):
    value: str


@dataclass(frozen=True, slots=True)
class WorkEffect(InternalEffect[None]):
    value: str


def work(value: str) -> StagedEffectful[None]:
    yield from WorkEffect(value)


def handle_created(event: Created) -> StagedEffectful[None]:
    yield from work(event.value)


def handle_created_and_follow(event: Created) -> StagedEffectful[None, Followed]:
    yield from work(event.value)
    yield from Followed(f"after {event.value}")


def handle_followed(event: Followed) -> StagedEffectful[None]:
    yield from work(event.value)


def publish(values: Sequence[str]) -> Callable[[], StagedEffectful[str, Created]]:
    def handler() -> StagedEffectful[str, Created]:
        for value in values:
            yield from Created(value)
        return "published"

    return handler


IMMEDIATE_RETRIES = RetryPolicy(max_attempts=3, initial_delay=timedelta(0))


class Deliveries:
    """Records the work of event handlers, a failure or a gate can be set for it."""

    def __init__(self) -> None:
        self.calls: MutableSequence[tuple[str, str]] = []
        self.failures: int = 0
        self.gate: AsyncioEvent | None = None
        self.started: AsyncioEvent = AsyncioEvent()
        self.running: int = 0
        self.max_running: int = 0

    async def handle(self, effect: WorkEffect, message_context: MessageContext) -> None:
        self.calls.append((effect.value, message_context.message_id))
        self.running += 1
        self.max_running = max(self.max_running, self.running)
        self.started.set()
        try:
            if self.gate is not None:
                await self.gate.wait()
            if self.failures:
                self.failures -= 1
                raise ValueError("work_error")
        finally:
            self.running -= 1


def make_environment(
    outbox: MemoryOutbox,
    deliveries: Deliveries,
    command_handler: Callable[[], StagedEffectful[str, Created]],
    created_handlers: Sequence[Callable[..., StagedEffectful[None, Any]]] = (handle_created,),
    effect_middleware_defs: Collection[EffectMiddlewareDef] = (),
) -> EnvironmentImpl:
    return EnvironmentImpl(
        operation_engine=OperationEngineImpl(
            command_defs=[StagedCommandDef("test", TestCommand, command_handler)],
            event_defs=[
                EventDef(
                    name="created",
                    event_type=Created,
                    handler_defs=[
                        StagedEventHandlerDef(f"handler{index}", handler)
                        for index, handler in enumerate(created_handlers)
                    ],
                ),
                EventDef("followed", Followed, handler_defs=[StagedEventHandlerDef("followed", handle_followed)]),
            ],
        ),
        effect_defs=[EffectDef(WorkEffect, deliveries.handle), *outbox.effect_defs],
        dependency_defs=outbox.dependency_defs,
        effect_middleware_defs=tuple(effect_middleware_defs),
    )


def statuses(rows: Sequence[MemoryOutboxRow]) -> Sequence[tuple[str, OutboxStatus, int]]:
    return sorted((row.handler, row.status, row.attempts) for row in rows)


@pytest.mark.timeout(5)
class TestMemoryOutbox:
    async def test_delivers_event_to_each_handler_with_its_message_id_and_marks_rows_done(self) -> None:
        outbox = MemoryOutbox()
        deliveries = Deliveries()
        environment = make_environment(outbox, deliveries, publish(["a"]), created_handlers=[handle_created] * 2)

        async with environment:
            await environment.handle(TestCommand(), "command")
            await outbox.drain()

        assert statuses(outbox.rows) == [("handler0", OutboxStatus.DONE, 1), ("handler1", OutboxStatus.DONE, 1)]
        assert sorted(deliveries.calls) == sorted(("a", row.message_id) for row in outbox.rows)
        assert all(row.message_id.endswith(f":{row.handler}") for row in outbox.rows)

    async def test_retries_failed_delivery_and_marks_it_failed_with_traceback_after_last_attempt(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        outbox = MemoryOutbox(OutboxDispatcherOptions(retry_policy=IMMEDIATE_RETRIES))
        deliveries = Deliveries()
        deliveries.failures = 3
        environment = make_environment(outbox, deliveries, publish(["a"]))

        async with environment:
            await environment.handle(TestCommand())
            await outbox.drain()

        [row] = outbox.rows
        assert (row.status, row.attempts) == (OutboxStatus.FAILED, 3)
        assert row.last_error is not None
        assert row.last_error.startswith("Traceback")
        assert "ValueError: work_error" in row.last_error
        assert len(deliveries.calls) == 3
        assert [record.levelname for record in caplog.records] == ["WARNING", "WARNING", "ERROR"]

    async def test_completes_delivery_that_succeeds_on_retry_and_keeps_its_last_error(self) -> None:
        outbox = MemoryOutbox(OutboxDispatcherOptions(retry_policy=IMMEDIATE_RETRIES))
        deliveries = Deliveries()
        deliveries.failures = 1
        environment = make_environment(outbox, deliveries, publish(["a"]))

        async with environment:
            await environment.handle(TestCommand())
            await outbox.drain()

        [row] = outbox.rows
        assert (row.status, row.attempts) == (OutboxStatus.DONE, 2)
        assert row.last_error is not None

    async def test_runs_no_more_deliveries_at_once_than_concurrency(self) -> None:
        outbox = MemoryOutbox(OutboxDispatcherOptions(concurrency=2))
        deliveries = Deliveries()
        deliveries.gate = AsyncioEvent()
        environment = make_environment(outbox, deliveries, publish(["a", "b", "c", "d"]))

        async with environment:
            await environment.handle(TestCommand())
            await deliveries.started.wait()
            deliveries.gate.set()
            await outbox.drain()

        assert deliveries.max_running == 2
        assert len(deliveries.calls) == 4

    async def test_skips_rows_already_written_when_operation_is_repeated(self) -> None:
        outbox = MemoryOutbox()
        deliveries = Deliveries()
        environment = make_environment(outbox, deliveries, publish(["a"]))

        async with environment:
            await environment.handle(TestCommand(), "command")
            await outbox.drain()
            await environment.handle(TestCommand(), "command")
            await outbox.drain()

        assert len(outbox.rows) == 1
        assert len(deliveries.calls) == 1

    async def test_waits_for_running_delivery_on_stop(self) -> None:
        outbox = MemoryOutbox()
        deliveries = Deliveries()
        deliveries.gate = AsyncioEvent()
        environment = make_environment(outbox, deliveries, publish(["a"]))

        async with environment:
            await environment.handle(TestCommand())
            await deliveries.started.wait()
            # Opens the gate only after the stop has begun to wait.
            get_running_loop().call_later(0.05, deliveries.gate.set)

        assert statuses(outbox.rows) == [("handler0", OutboxStatus.DONE, 1)]

    async def test_cancels_running_delivery_after_stop_timeout_without_counting_its_attempt(self) -> None:
        outbox = MemoryOutbox(OutboxDispatcherOptions(stop_timeout=timedelta(0)))
        deliveries = Deliveries()
        deliveries.gate = AsyncioEvent()
        environment = make_environment(outbox, deliveries, publish(["a"]))

        async with environment:
            await environment.handle(TestCommand())
            await deliveries.started.wait()

        assert statuses(outbox.rows) == [("handler0", OutboxStatus.PENDING, 0)]
        assert deliveries.running == 0

    async def test_delivers_rows_left_pending_when_environment_starts_again(self) -> None:
        outbox = MemoryOutbox(OutboxDispatcherOptions(stop_timeout=timedelta(0)))
        deliveries = Deliveries()
        deliveries.gate = AsyncioEvent()
        environment = make_environment(outbox, deliveries, publish(["a"]))
        async with environment:
            await environment.handle(TestCommand())
            await deliveries.started.wait()
        deliveries.gate = None

        async with environment:
            await outbox.drain()

        assert statuses(outbox.rows) == [("handler0", OutboxStatus.DONE, 1)]

    async def test_does_not_take_again_delivery_that_it_still_runs_after_its_lease_ends(self) -> None:
        outbox = MemoryOutbox(OutboxDispatcherOptions(lease=timedelta(milliseconds=20)))
        deliveries = Deliveries()
        deliveries.gate = AsyncioEvent()
        environment = make_environment(outbox, deliveries, publish(["a"]))

        async with environment:
            await environment.handle(TestCommand())
            await deliveries.started.wait()
            await sleep(0.1)
            deliveries.gate.set()
            await outbox.drain()

        assert len(deliveries.calls) == 1
        assert statuses(outbox.rows) == [("handler0", OutboxStatus.DONE, 1)]

    async def test_marks_failed_delivery_whose_last_attempt_ended_its_lease_without_result(self) -> None:
        options = OutboxDispatcherOptions(
            retry_policy=RetryPolicy(max_attempts=1),
            lease=timedelta(milliseconds=20),
            stop_timeout=timedelta(0),
        )
        outbox = MemoryOutbox(options)
        hung = Deliveries()
        hung.gate = AsyncioEvent()
        other = Deliveries()
        # The other environment plays a dispatcher of another process that sees the lease end.
        hung_environment = make_environment(outbox, hung, publish(["a"]))
        other_environment = make_environment(outbox, other, publish(["a"]))

        async with hung_environment:
            await hung_environment.handle(TestCommand())
            await hung.started.wait()
            await sleep(0.05)
            async with other_environment:
                await outbox.drain()

        [row] = outbox.rows
        assert (row.status, row.attempts) == (OutboxStatus.FAILED, 1)
        assert row.last_error == "The lease of attempt 1 ended without a result"
        assert other.calls == []

    async def test_drops_events_of_failed_transaction(self) -> None:
        async def fail_after_outbox(effect_call: EffectCall, call_next: CallNext) -> object:
            result = await call_next()
            if not isinstance(effect_call.effect, WriteOutboxEffect):
                return result
            raise ValueError("transaction_error")

        outbox = MemoryOutbox()
        deliveries = Deliveries()
        environment = make_environment(
            outbox=outbox,
            deliveries=deliveries,
            command_handler=publish(["a"]),
            effect_middleware_defs=[EffectMiddlewareDef(fail_after_outbox)],
        )

        async with environment:
            with pytest.raises(ValueError, match="transaction_error"):
                await environment.handle(TestCommand())
            await outbox.drain()

        assert outbox.rows == ()

    async def test_drain_waits_for_deliveries_of_events_that_handlers_publish(self) -> None:
        outbox = MemoryOutbox()
        deliveries = Deliveries()
        environment = make_environment(outbox, deliveries, publish(["a"]), created_handlers=[handle_created_and_follow])

        async with environment:
            await environment.handle(TestCommand())
            async with timeout(1):
                await outbox.drain()

        assert statuses(outbox.rows) == [("followed", OutboxStatus.DONE, 1), ("handler0", OutboxStatus.DONE, 1)]
        assert sorted(value for value, _ in deliveries.calls) == ["a", "after a"]
