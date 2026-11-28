from asyncio import Event as AsyncioEvent, gather, get_running_loop, sleep, timeout
from collections.abc import AsyncGenerator, Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from uuid import uuid4

import pytest
from asyncpg import Pool, Record

from fidem.asyncpg import (
    AsyncpgConnectionDef,
    AsyncpgOutboxDispatcherDef,
    AsyncpgOutboxEffectDef,
    AsyncpgOutboxStore,
)
from fidem.di.effects import EffectDependencyDef
from fidem.effects import InternalEffect
from fidem.environment import EffectDef, EnvironmentImpl
from fidem.messages import Event, StagedEffectful
from fidem.operation_engine.definitions import EventDef, StagedCommandDef, StagedEventHandlerDef
from fidem.operation_engine.engine import OperationEngineImpl
from fidem.outbox import OutboxDispatcherOptions, RetryPolicy
from fidem.pydantic import PydanticSerializer
from fidem.serialization import Serializer
from tests.messages import TestCommand

pytestmark = [
    pytest.mark.asyncio(loop_scope="session"),
    # The first test also waits for the container to start, possibly after pulling its image.
    pytest.mark.timeout(60),
]

LONG_LEASE = timedelta(hours=1)
MAX_ATTEMPTS = 10
SHORT_LEASE = timedelta(milliseconds=50)

INSERT_SQL = """
INSERT INTO fidem.outbox (id, event_id, source_message_id, handler, event_name, event_version, payload)
VALUES ($1, 'event', 'message', $2, $3, $4, '{"value": "stored"}')
"""


@dataclass(frozen=True, slots=True)
class Noticed(Event):
    value: str


@dataclass(frozen=True, slots=True)
class WorkEffect(InternalEffect[None]):
    value: str


def handle_noticed(event: Noticed) -> StagedEffectful[None]:
    yield from WorkEffect(event.value)


class Work:
    """The effect handler of event handlers, which records their work, raises the errors in turn or blocks."""

    def __init__(self, errors: Sequence[Exception] = (), gate: AsyncioEvent | None = None) -> None:
        self.values: list[str] = []
        self.started: AsyncioEvent = AsyncioEvent()
        self._errors: list[Exception] = list(errors)
        self._gate: AsyncioEvent | None = gate

    async def handle(self, effect: WorkEffect) -> None:
        self.values.append(effect.value)
        self.started.set()
        if self._gate is not None:
            await self._gate.wait()
        if self._errors:
            raise self._errors.pop(0)


def make_event_def() -> EventDef[Noticed]:
    """Each test has its own event name, so dispatchers of other tests do not know its rows."""
    return EventDef(
        name=f"test.noticed.{uuid4().hex}",
        event_type=Noticed,
        handler_defs=[StagedEventHandlerDef("handler", handle_noticed)],
    )


def publish() -> StagedEffectful[str, Noticed]:
    yield from Noticed("published")
    return "published"


def make_environment(
    pool: Pool,
    work: Work,
    options: OutboxDispatcherOptions,
    *,
    notify: bool = True,
) -> EnvironmentImpl:
    # The pool outlives the environment because starting it for each test is slower.
    async def shared_pool() -> AsyncGenerator[Pool]:
        yield pool

    async def serializer() -> AsyncGenerator[Serializer]:
        yield PydanticSerializer()

    return EnvironmentImpl(
        operation_engine=OperationEngineImpl(
            command_defs=[StagedCommandDef("test", TestCommand, publish)],
            event_defs=[make_event_def()],
        ),
        effect_defs=[EffectDef(WorkEffect, work.handle), AsyncpgOutboxEffectDef(notify=notify)],
        dependency_defs=[
            EffectDependencyDef(shared_pool),
            EffectDependencyDef(serializer),
            AsyncpgConnectionDef(),
            AsyncpgOutboxDispatcherDef(options),
        ],
    )


async def fetch_delivery(pool: Pool, source_message_id: str) -> Record:
    row = await pool.fetchrow(
        """
        SELECT status::text, attempts, last_error, available_at <= now() AS available_now, processed_at
        FROM fidem.outbox WHERE source_message_id = $1
        """,
        source_message_id,
    )
    assert row is not None
    return row


async def wait_for_delivery(pool: Pool, source_message_id: str, until: Callable[[Record], bool]) -> Record:
    async with timeout(5):
        while not until(row := await fetch_delivery(pool, source_message_id)):  # noqa: ASYNC110 - polls the table, which sends no events
            await sleep(0.01)
    return row


async def wait_for_status(pool: Pool, source_message_id: str, status: str) -> Record:
    return await wait_for_delivery(pool, source_message_id, lambda row: row["status"] == status)


class TestAsyncpgOutboxDispatcher:
    async def test_delivers_event_on_notification_without_waiting_for_poll(self, outbox_pool: Pool) -> None:
        work = Work()
        environment = make_environment(outbox_pool, work, OutboxDispatcherOptions(poll_interval=timedelta(minutes=1)))
        message_id = uuid4().hex

        async with environment:
            # The first take of the dispatcher would find the row without a notification.
            await sleep(0.2)
            await environment.handle(TestCommand(), message_id)
            row = await wait_for_status(outbox_pool, message_id, "done")

        assert work.values == ["published"]
        assert (row["attempts"], row["last_error"]) == (1, None)
        assert row["processed_at"] is not None

    async def test_delivers_event_by_polling_when_notifications_are_off(self, outbox_pool: Pool) -> None:
        work = Work()
        options = OutboxDispatcherOptions(poll_interval=timedelta(milliseconds=50))
        environment = make_environment(outbox_pool, work, options, notify=False)
        message_id = uuid4().hex

        async with environment:
            await environment.handle(TestCommand(), message_id)
            await wait_for_status(outbox_pool, message_id, "done")

        assert work.values == ["published"]

    async def test_records_failed_attempt_and_retries_after_delay(self, outbox_pool: Pool) -> None:
        work = Work(errors=[ValueError("work_error")])
        options = OutboxDispatcherOptions(retry_policy=RetryPolicy(initial_delay=timedelta(hours=1)))
        environment = make_environment(outbox_pool, work, options)
        message_id = uuid4().hex

        async with environment:
            await environment.handle(TestCommand(), message_id)
            row = await wait_for_delivery(outbox_pool, message_id, lambda row: row["last_error"] is not None)

        assert (row["status"], row["attempts"], row["available_now"]) == ("pending", 1, False)
        assert "ValueError: work_error" in row["last_error"]

    async def test_retries_when_retry_is_due_without_waiting_for_poll(self, outbox_pool: Pool) -> None:
        work = Work(errors=[ValueError("work_error")])
        options = OutboxDispatcherOptions(
            retry_policy=RetryPolicy(initial_delay=timedelta(milliseconds=100)),
            poll_interval=timedelta(minutes=1),
        )
        environment = make_environment(outbox_pool, work, options)
        message_id = uuid4().hex

        async with environment:
            # The first take of the dispatcher would find the row without a notification.
            await sleep(0.2)
            await environment.handle(TestCommand(), message_id)
            row = await wait_for_status(outbox_pool, message_id, "done")

        assert row["attempts"] == 2
        assert work.values == ["published", "published"]

    async def test_marks_failed_delivery_whose_last_attempt_ended_its_lease_without_result(
        self,
        outbox_pool: Pool,
    ) -> None:
        work = Work()
        environment = make_environment(
            pool=outbox_pool,
            work=work,
            options=OutboxDispatcherOptions(retry_policy=RetryPolicy(max_attempts=1)),
        )
        [event_def] = environment.event_defs
        [row_id] = await insert_rows(outbox_pool, event_def)
        source_message_id = uuid4().hex
        # The last attempt took the row and its process crashed before the result.
        await outbox_pool.execute(
            "UPDATE fidem.outbox SET attempts = 1, source_message_id = $2 WHERE id = $1",
            row_id,
            source_message_id,
        )

        async with environment:
            row = await wait_for_status(outbox_pool, source_message_id, "failed")

        assert (row["attempts"], row["last_error"]) == (1, "The lease of attempt 1 ended without a result")
        assert work.values == []

    async def test_marks_delivery_failed_after_last_attempt(self, outbox_pool: Pool) -> None:
        work = Work(errors=[ValueError("work_error")])
        environment = make_environment(
            pool=outbox_pool,
            work=work,
            options=OutboxDispatcherOptions(retry_policy=RetryPolicy(max_attempts=1)),
        )
        message_id = uuid4().hex

        async with environment:
            await environment.handle(TestCommand(), message_id)
            row = await wait_for_status(outbox_pool, message_id, "failed")

        assert row["attempts"] == 1
        assert "ValueError: work_error" in row["last_error"]

    async def test_releases_delivery_cancelled_on_stop_without_counting_its_attempt(self, outbox_pool: Pool) -> None:
        work = Work(gate=AsyncioEvent())
        environment = make_environment(outbox_pool, work, OutboxDispatcherOptions(stop_timeout=timedelta(0)))
        message_id = uuid4().hex

        async with environment:
            await environment.handle(TestCommand(), message_id)
            async with timeout(5):
                await work.started.wait()

        row = await fetch_delivery(outbox_pool, message_id)
        assert (row["status"], row["attempts"], row["available_now"]) == ("pending", 0, True)


class TestAsyncpgOutboxStore:
    async def test_takes_each_delivery_once_when_stores_take_concurrently(self, outbox_pool: Pool) -> None:
        event_def = make_event_def()
        row_ids = await insert_rows(outbox_pool, event_def, count=10)
        first = make_store(outbox_pool, event_def)
        second = make_store(outbox_pool, event_def)

        first_deliveries, second_deliveries = await gather(
            first.take(limit=10, lease=LONG_LEASE, max_attempts=MAX_ATTEMPTS, running=()),
            second.take(limit=10, lease=LONG_LEASE, max_attempts=MAX_ATTEMPTS, running=()),
        )

        first_ids = {delivery.message_id for delivery in first_deliveries}
        second_ids = {delivery.message_id for delivery in second_deliveries}
        assert first_ids.isdisjoint(second_ids)
        assert first_ids | second_ids == set(row_ids)

    async def test_takes_delivery_again_when_its_lease_ends(self, outbox_pool: Pool) -> None:
        event_def = make_event_def()
        [row_id] = await insert_rows(outbox_pool, event_def)
        store = make_store(outbox_pool, event_def)
        await store.take(limit=1, lease=SHORT_LEASE, max_attempts=MAX_ATTEMPTS, running=())
        assert await store.take(limit=1, lease=SHORT_LEASE, max_attempts=MAX_ATTEMPTS, running=()) == ()

        await sleep(SHORT_LEASE.total_seconds() * 2)
        [delivery] = await store.take(limit=1, lease=LONG_LEASE, max_attempts=MAX_ATTEMPTS, running=())

        assert (delivery.message_id, delivery.attempts) == (row_id, 2)

    async def test_ignores_result_of_attempt_whose_lease_ended(self, outbox_pool: Pool) -> None:
        event_def = make_event_def()
        [row_id] = await insert_rows(outbox_pool, event_def)
        store = make_store(outbox_pool, event_def)
        [late] = await store.take(limit=1, lease=SHORT_LEASE, max_attempts=MAX_ATTEMPTS, running=())
        await sleep(SHORT_LEASE.total_seconds() * 2)
        [current] = await store.take(limit=1, lease=LONG_LEASE, max_attempts=MAX_ATTEMPTS, running=())

        await store.fail(late, error="late_error", retry_delay=None)
        await store.complete(late)
        assert await fetch_row(outbox_pool, row_id) == ("pending", 2, None)

        await store.complete(current)
        assert await fetch_row(outbox_pool, row_id) == ("done", 2, None)

    async def test_releases_delivery_available_now_without_counting_its_attempt(self, outbox_pool: Pool) -> None:
        event_def = make_event_def()
        await insert_rows(outbox_pool, event_def)
        store = make_store(outbox_pool, event_def)
        [delivery] = await store.take(limit=1, lease=LONG_LEASE, max_attempts=MAX_ATTEMPTS, running=())

        await store.release(delivery)
        [again] = await store.take(limit=1, lease=LONG_LEASE, max_attempts=MAX_ATTEMPTS, running=())

        assert again.attempts == 1

    async def test_takes_only_deliveries_known_to_code(self, outbox_pool: Pool) -> None:
        event_def = make_event_def()
        await outbox_pool.execute(INSERT_SQL, uuid4().hex, "unknown", event_def.name, event_def.version)
        await outbox_pool.execute(INSERT_SQL, uuid4().hex, "handler", event_def.name, event_def.version + 1)
        store = make_store(outbox_pool, event_def)

        assert await store.take(limit=10, lease=LONG_LEASE, max_attempts=MAX_ATTEMPTS, running=()) == ()

    async def test_loads_stored_event(self, outbox_pool: Pool) -> None:
        event_def = make_event_def()
        await insert_rows(outbox_pool, event_def)
        store = make_store(outbox_pool, event_def)
        [delivery] = await store.take(limit=1, lease=LONG_LEASE, max_attempts=MAX_ATTEMPTS, running=())

        assert store.load_event(delivery) == Noticed("stored")

    async def test_does_not_take_deliveries_that_dispatcher_runs(self, outbox_pool: Pool) -> None:
        event_def = make_event_def()
        [row_id] = await insert_rows(outbox_pool, event_def)
        store = make_store(outbox_pool, event_def)

        deliveries = await store.take(limit=1, lease=LONG_LEASE, max_attempts=MAX_ATTEMPTS, running=[row_id])

        assert deliveries == ()

    async def test_does_not_take_deliveries_that_used_up_attempts(self, outbox_pool: Pool) -> None:
        event_def = make_event_def()
        [row_id] = await insert_rows(outbox_pool, event_def)
        await outbox_pool.execute("UPDATE fidem.outbox SET attempts = 2 WHERE id = $1", row_id)
        store = make_store(outbox_pool, event_def)

        assert await store.take(limit=1, lease=LONG_LEASE, max_attempts=2, running=()) == ()
        [exhausted] = await store.fail_exhausted(max_attempts=2, error="lease_error", running=())

        assert exhausted.message_id == row_id
        assert await fetch_row(outbox_pool, row_id) == ("failed", 2, "lease_error")

    async def test_wait_does_not_spin_on_delivery_locked_by_other_take(self, outbox_pool: Pool) -> None:
        event_def = make_event_def()
        [row_id] = await insert_rows(outbox_pool, event_def)
        store = make_store(outbox_pool, event_def)
        max_wait = timedelta(milliseconds=300)

        async with outbox_pool.acquire() as connection, connection.transaction(), store:
            await connection.execute("SELECT id FROM fidem.outbox WHERE id = $1 FOR UPDATE", row_id)
            assert await store.take(limit=1, lease=LONG_LEASE, max_attempts=MAX_ATTEMPTS, running=()) == ()
            # The first wait after listening returns at once, because notifications before it are lost.
            await store.wait(max_wait=max_wait)
            started = get_running_loop().time()
            await store.wait(max_wait=max_wait)

            assert get_running_loop().time() - started >= max_wait.total_seconds() / 2

    async def test_wait_returns_when_retry_is_due(self, outbox_pool: Pool) -> None:
        event_def = make_event_def()
        await insert_rows(outbox_pool, event_def)
        store = make_store(outbox_pool, event_def)
        [delivery] = await store.take(limit=1, lease=LONG_LEASE, max_attempts=MAX_ATTEMPTS, running=())
        await store.fail(delivery, error="error", retry_delay=timedelta(milliseconds=100))

        async with store:
            await store.wait(max_wait=timedelta(minutes=1))
            async with timeout(5):
                await store.wait(max_wait=timedelta(minutes=1))

        [again] = await store.take(limit=1, lease=LONG_LEASE, max_attempts=MAX_ATTEMPTS, running=())
        assert again.attempts == 2


def make_store(pool: Pool, event_def: EventDef[Any]) -> AsyncpgOutboxStore:
    return AsyncpgOutboxStore(pool=pool, serializer=PydanticSerializer(), event_defs=[event_def])


async def insert_rows(pool: Pool, event_def: EventDef[Any], count: int = 1) -> Sequence[str]:
    row_ids = [uuid4().hex for _ in range(count)]
    await pool.executemany(INSERT_SQL, [(row_id, "handler", event_def.name, event_def.version) for row_id in row_ids])
    return row_ids


async def fetch_row(pool: Pool, row_id: str) -> tuple[str, int, str | None]:
    row = await pool.fetchrow("SELECT status::text, attempts, last_error FROM fidem.outbox WHERE id = $1", row_id)
    assert row is not None
    return (row["status"], row["attempts"], row["last_error"])
