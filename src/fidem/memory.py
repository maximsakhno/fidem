from asyncio import Event as AsyncioEvent, get_running_loop
from collections.abc import AsyncIterator, Collection, Iterable, Iterator, MutableMapping, MutableSequence, Sequence
from dataclasses import dataclass, field, replace
from datetime import timedelta
from enum import Enum
from heapq import nsmallest
from typing import Final

from fidem.core.di.effects import EffectDependencyDef, EffectDependencyLifetime
from fidem.core.environment import EffectDef, EffectMiddlewareDef, Environment
from fidem.core.messages import Event, EventHandlerName, MessageId, MessageName
from fidem.core.outbox import (
    OutboxDelivery,
    OutboxDispatcher,
    OutboxDispatcherOptions,
    OutboxEvent,
    OutboxStore,
    WriteOutboxEffect,
    wait_for_change,
)

__all__ = [
    "MemoryOutbox",
    "MemoryOutboxRow",
    "OutboxStatus",
]


class OutboxStatus(Enum):
    PENDING = "pending"
    DONE = "done"
    FAILED = "failed"


@dataclass(frozen=True, slots=True, kw_only=True)
class MemoryOutboxRow:
    """A delivery of an event to one of its handlers."""

    message_id: MessageId
    event_name: MessageName
    event: Event
    handler: EventHandlerName
    status: OutboxStatus = OutboxStatus.PENDING
    attempts: int = 0
    last_error: str | None = None


class _MemoryOutboxStore(OutboxStore[Event]):
    """Keeps the rows in memory, every row is known to the code because the same process writes it."""

    def __init__(self) -> None:
        self._rows: MutableMapping[MessageId, MemoryOutboxRow] = {}
        self._available_at: MutableMapping[MessageId, float] = {}
        """When each pending row is available, in the time of the event loop."""
        self._changed: AsyncioEvent = AsyncioEvent()
        self._drained: AsyncioEvent = AsyncioEvent()
        self._drained.set()

    @property
    def rows(self) -> Sequence[MemoryOutboxRow]:
        return tuple(self._rows.values())

    def add(self, events: Iterable[OutboxEvent]) -> None:
        """Adds a row for each handler of each event and skips the rows already added."""
        now = get_running_loop().time()
        for event in events:
            for handler in event.handlers:
                message_id = event.handler_message_id(handler)
                if message_id not in self._rows:
                    self._rows[message_id] = MemoryOutboxRow(
                        message_id=message_id,
                        event_name=event.name,
                        event=event.event,
                        handler=handler,
                    )
                    self._make_available(message_id, now)

    async def drain(self) -> None:
        await self._drained.wait()

    async def take(
        self,
        limit: int,
        lease: timedelta,
        max_attempts: int,
        running: Collection[MessageId],
    ) -> Sequence[OutboxDelivery[Event]]:
        now = get_running_loop().time()
        available = (
            (available_at, message_id)
            for available_at, message_id in self._available(now, running)
            if self._rows[message_id].attempts < max_attempts
        )
        deliveries: MutableSequence[OutboxDelivery[Event]] = []
        for _, message_id in nsmallest(n=limit, iterable=available):
            self._rows[message_id] = replace(self._rows[message_id], attempts=self._rows[message_id].attempts + 1)
            self._available_at[message_id] = now + lease.total_seconds()
            deliveries.append(self._delivery(message_id))
        return deliveries

    async def fail_exhausted(
        self,
        max_attempts: int,
        error: str,
        running: Collection[MessageId],
    ) -> Sequence[OutboxDelivery[Event]]:
        exhausted = [
            message_id
            for _, message_id in self._available(get_running_loop().time(), running)
            if self._rows[message_id].attempts >= max_attempts
        ]
        for message_id in exhausted:
            self._rows[message_id] = replace(self._rows[message_id], last_error=error)
            self._finish(message_id, OutboxStatus.FAILED)
        return [self._delivery(message_id) for message_id in exhausted]

    def load_event(self, delivery: OutboxDelivery[Event]) -> Event:
        return delivery.payload

    async def complete(self, delivery: OutboxDelivery[Event]) -> None:
        if self._is_taken(delivery):
            self._finish(delivery.message_id, OutboxStatus.DONE)

    async def fail(self, delivery: OutboxDelivery[Event], error: str, retry_delay: timedelta | None) -> None:
        if not self._is_taken(delivery):
            return
        self._rows[delivery.message_id] = replace(self._rows[delivery.message_id], last_error=error)
        if retry_delay is None:
            self._finish(delivery.message_id, OutboxStatus.FAILED)
        else:
            self._make_available(delivery.message_id, get_running_loop().time() + retry_delay.total_seconds())

    async def release(self, delivery: OutboxDelivery[Event]) -> None:
        if self._is_taken(delivery):
            row = self._rows[delivery.message_id]
            self._rows[delivery.message_id] = replace(row, attempts=row.attempts - 1)
            self._make_available(delivery.message_id, get_running_loop().time())

    async def wait(self, max_wait: timedelta) -> None:
        # A row available now is one that the dispatcher still runs, so only later rows can wake it up.
        now = get_running_loop().time()
        next_available = min(
            (available_at for available_at in self._available_at.values() if available_at > now), default=None
        )
        if next_available is not None:
            max_wait = min(max_wait, timedelta(seconds=next_available - now))
        await wait_for_change(self._changed, max_wait)

    def _available(self, now: float, running: Collection[MessageId]) -> Iterator[tuple[float, MessageId]]:
        for message_id, available_at in self._available_at.items():
            if available_at <= now and message_id not in running:
                yield available_at, message_id

    def _delivery(self, message_id: MessageId) -> OutboxDelivery[Event]:
        row = self._rows[message_id]
        return OutboxDelivery(
            message_id=message_id,
            event_name=row.event_name,
            handler=row.handler,
            attempts=row.attempts,
            payload=row.event,
        )

    def _is_taken(self, delivery: OutboxDelivery[Event]) -> bool:
        row = self._rows[delivery.message_id]
        return row.status == OutboxStatus.PENDING and row.attempts == delivery.attempts

    def _make_available(self, message_id: MessageId, available_at: float) -> None:
        self._available_at[message_id] = available_at
        self._drained.clear()
        self._changed.set()

    def _finish(self, message_id: MessageId, status: OutboxStatus) -> None:
        self._rows[message_id] = replace(self._rows[message_id], status=status)
        del self._available_at[message_id]
        if not self._available_at:
            self._drained.set()


@dataclass(slots=True)
class _TransactionEvents:
    """Events written in one transaction, the outbox gets them only after the transaction succeeds."""

    events: MutableSequence[OutboxEvent] = field(default_factory=list)


class MemoryOutbox:
    """The outbox in the memory of the process: its rows do not survive the process and its events are not serialized.

    The environment gets `effect_defs` and `dependency_defs`, its dispatcher runs as a service.
    """

    def __init__(
        self,
        options: OutboxDispatcherOptions = OutboxDispatcherOptions(),
        *,
        middleware_defs: Sequence[EffectMiddlewareDef] | None = None,
    ) -> None:
        """Middlewares replace the environment middlewares for the writes, `None` keeps them."""
        self._store: _MemoryOutboxStore = _MemoryOutboxStore()
        self._options: OutboxDispatcherOptions = options
        self.effect_defs: Final[Sequence[EffectDef[WriteOutboxEffect]]] = (
            EffectDef(effect_type=WriteOutboxEffect, handler=self._write, middleware_defs=middleware_defs),
        )
        self.dependency_defs: Final[Sequence[EffectDependencyDef[object]]] = (
            EffectDependencyDef(self._transaction_events, lifetime=EffectDependencyLifetime.TRANSACTION),
            EffectDependencyDef(self._dispatcher, lifetime=EffectDependencyLifetime.SERVICE),
        )

    @property
    def rows(self) -> Sequence[MemoryOutboxRow]:
        return self._store.rows

    async def drain(self) -> None:
        """Waits until no delivery is pending, including retries and deliveries of the events of handlers."""
        await self._store.drain()

    @staticmethod
    async def _write(transaction_events: _TransactionEvents, effect: WriteOutboxEffect) -> None:
        transaction_events.events.extend(effect.events)

    async def _transaction_events(self) -> AsyncIterator[_TransactionEvents]:
        transaction_events = _TransactionEvents()
        # An error of the transaction is raised here and discards its events.
        yield transaction_events
        self._store.add(transaction_events.events)

    async def _dispatcher(self, environment: Environment) -> AsyncIterator[OutboxDispatcher]:
        async with OutboxDispatcher(store=self._store, environment=environment, options=self._options) as dispatcher:
            yield dispatcher
