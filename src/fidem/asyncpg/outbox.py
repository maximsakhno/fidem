from asyncio import Event as AsyncioEvent
from collections.abc import AsyncIterator, Collection, Iterable, Mapping, MutableSequence, Sequence
from datetime import timedelta
from importlib.resources import files
from types import TracebackType
from typing import Any, Final, NamedTuple, Self, cast

from asyncpg import Connection, Pool, Record

from fidem.asyncpg.migrations import MigrationSection, load_section
from fidem.di.effects import EffectDependencyDef, EffectDependencyLifetime
from fidem.effects import EffectId
from fidem.environment import EffectDef, EffectMiddlewareDef, Environment
from fidem.messages import Event, EventHandlerName, EventVersion, MessageId, MessageName
from fidem.operation_engine.definitions import EventDef
from fidem.outbox import (
    OutboxDelivery,
    OutboxDispatcher,
    OutboxDispatcherOptions,
    OutboxStore,
    WriteOutboxEffect,
    wait_for_change,
)
from fidem.serialization import Codec, Serializer

__all__ = [
    "OUTBOX_SECTION",
    "AsyncpgOutbox",
    "AsyncpgOutboxStore",
]

OUTBOX_SECTION: Final[MigrationSection] = load_section(
    name="fidem.outbox", directory=files(__package__) / "sql" / "outbox"
)
"""The table that events are written to in the transaction that publishes them."""

_CHANNEL: Final = "fidem_outbox"
"""The channel of `NOTIFY` that wakes dispatchers up when a transaction writes events."""


# A tuple because asyncpg takes each row as the sequence of the query parameters.
class _Row(NamedTuple):
    id: MessageId
    event_id: EffectId
    source_message_id: MessageId
    handler: EventHandlerName
    event_name: MessageName
    event_version: EventVersion
    payload: str


_INSERT_SQL: Final = """
    INSERT INTO fidem.outbox (id, event_id, source_message_id, handler, event_name, event_version, payload)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (id) DO NOTHING
"""
# PostgreSQL delivers a notification on commit and folds the same ones of a transaction into one.
_NOTIFY_SQL: Final = f"SELECT pg_notify('{_CHANNEL}', '')"


async def _write(connection: Connection, serializer: Serializer, effect: WriteOutboxEffect) -> None:
    rows: MutableSequence[_Row] = []
    for event in effect.events:
        # asyncpg expects jsonb as text.
        payload = serializer.codec(type(event.event)).to_json(event.event).decode()
        rows.extend(
            _Row(
                id=event.handler_message_id(handler),
                event_id=event.event_id,
                source_message_id=event.source_message_id,
                handler=handler,
                event_name=event.name,
                event_version=event.version,
                payload=payload,
            )
            for handler in event.handlers
        )
    await connection.executemany(_INSERT_SQL, rows)


async def _write_and_notify(connection: Connection, serializer: Serializer, effect: WriteOutboxEffect) -> None:
    await _write(connection=connection, serializer=serializer, effect=effect)
    await connection.execute(_NOTIFY_SQL)


class _StoredEvent(NamedTuple):
    version: EventVersion
    payload: str


# Only the deliveries that the code knows, so that a replica leaves the ones of other versions of the code alone.
_TAKE_SQL: Final = """
    WITH known (event_name, event_version, handler) AS (
        SELECT * FROM unnest($1::text[], $2::integer[], $3::text[])
    )
    UPDATE fidem.outbox AS outbox
    SET attempts = outbox.attempts + 1, available_at = now() + $4::interval
    WHERE outbox.id IN (
        SELECT pending.id
        FROM fidem.outbox AS pending
        JOIN known USING (event_name, event_version, handler)
        WHERE pending.status = 'pending'
            AND pending.available_at <= now()
            AND pending.attempts < $6
            AND pending.id <> ALL($7::text[])
        ORDER BY pending.available_at
        LIMIT $5
        FOR UPDATE OF pending SKIP LOCKED
    )
    RETURNING outbox.id, outbox.event_name, outbox.event_version, outbox.handler, outbox.attempts, outbox.payload
"""
_FAIL_EXHAUSTED_SQL: Final = """
    WITH known (event_name, event_version, handler) AS (
        SELECT * FROM unnest($1::text[], $2::integer[], $3::text[])
    )
    UPDATE fidem.outbox AS outbox
    SET status = 'failed', last_error = $5
    WHERE outbox.id IN (
        SELECT pending.id
        FROM fidem.outbox AS pending
        JOIN known USING (event_name, event_version, handler)
        WHERE pending.status = 'pending'
            AND pending.available_at <= now()
            AND pending.attempts >= $4
            AND pending.id <> ALL($6::text[])
        FOR UPDATE OF pending SKIP LOCKED
    )
    RETURNING outbox.id, outbox.event_name, outbox.event_version, outbox.handler, outbox.attempts, outbox.payload
"""
# Walks the index of pending rows from the earliest later one and stops at the first known one. A row available now
# is locked by another take or run by this dispatcher, so waiting for it would spin until the lock goes.
_NEXT_AVAILABLE_SQL: Final = """
    WITH known (event_name, event_version, handler) AS (
        SELECT * FROM unnest($1::text[], $2::integer[], $3::text[])
    )
    SELECT pending.available_at - now()
    FROM fidem.outbox AS pending
    WHERE pending.status = 'pending'
        AND pending.available_at > now()
        AND (pending.event_name, pending.event_version, pending.handler) IN (SELECT * FROM known)
    ORDER BY pending.available_at
    LIMIT 1
"""
# A result is recorded only while the delivery stays taken by the same attempt.
_COMPLETE_SQL: Final = """
    UPDATE fidem.outbox SET status = 'done', processed_at = now()
    WHERE id = $1 AND status = 'pending' AND attempts = $2
"""
_RETRY_SQL: Final = """
    UPDATE fidem.outbox SET available_at = now() + $3::interval, last_error = $4
    WHERE id = $1 AND status = 'pending' AND attempts = $2
"""
_FAIL_SQL: Final = """
    UPDATE fidem.outbox SET status = 'failed', last_error = $3
    WHERE id = $1 AND status = 'pending' AND attempts = $2
"""
_RELEASE_SQL: Final = """
    UPDATE fidem.outbox SET attempts = attempts - 1, available_at = now()
    WHERE id = $1 AND status = 'pending' AND attempts = $2
"""


class AsyncpgOutboxStore(OutboxStore[_StoredEvent]):
    """Rows of the table of `OUTBOX_SECTION`, a connection of the pool listens to notifications while entered."""

    def __init__(self, pool: Pool, serializer: Serializer, event_defs: Collection[EventDef[Any]]) -> None:
        self._pool: Pool = pool
        # Codecs are prepared here so that an unsupported event fails the start.
        self._codecs: Mapping[tuple[MessageName, EventVersion], Codec[Any]] = {
            (event_def.name, event_def.version): serializer.codec(event_def.event_type) for event_def in event_defs
        }
        names: MutableSequence[MessageName] = []
        versions: MutableSequence[EventVersion] = []
        handlers: MutableSequence[EventHandlerName] = []
        for event_def in event_defs:
            for handler_def in event_def.handler_defs:
                names.append(event_def.name)
                versions.append(event_def.version)
                handlers.append(handler_def.name)
        # The columns of the deliveries that the code knows, as the arrays that the queries unnest.
        self._known_args: Sequence[Sequence[object]] = (names, versions, handlers)
        self._listener: Connection | None = None
        self._notified: AsyncioEvent = AsyncioEvent()

    async def __aenter__(self) -> Self:
        await self._listen()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self._stop_listening()

    async def _listen(self) -> None:
        # The pool proxy forwards everything to the connection, and asyncpg documents it as one.
        listener = cast("Connection", await self._pool.acquire())
        try:
            await listener.add_listener(_CHANNEL, self._on_notification)
        except BaseException:
            await self._pool.release(listener)
            raise
        self._listener = listener
        # Notifications sent while nobody listened are lost, so a take after listening must not wait.
        self._notified.set()

    async def _stop_listening(self) -> None:
        listener = self._listener
        self._listener = None
        if listener is None:
            return
        # A broken connection goes back to the pool as is, and the pool replaces it.
        try:
            if not listener.is_closed():
                await listener.remove_listener(_CHANNEL, self._on_notification)
        finally:
            await self._pool.release(listener)

    def _on_notification(self, _connection: object, _pid: int, _channel: str, _payload: object) -> None:
        self._notified.set()

    async def take(
        self,
        limit: int,
        lease: timedelta,
        max_attempts: int,
        running: Collection[MessageId],
    ) -> Sequence[OutboxDelivery[_StoredEvent]]:
        records = await self._pool.fetch(_TAKE_SQL, *self._known_args, lease, limit, max_attempts, list(running))
        return self._deliveries(records)

    async def fail_exhausted(
        self,
        max_attempts: int,
        error: str,
        running: Collection[MessageId],
    ) -> Sequence[OutboxDelivery[_StoredEvent]]:
        records = await self._pool.fetch(_FAIL_EXHAUSTED_SQL, *self._known_args, max_attempts, error, list(running))
        return self._deliveries(records)

    @staticmethod
    def _deliveries(records: Iterable[Record]) -> Sequence[OutboxDelivery[_StoredEvent]]:
        return tuple(
            OutboxDelivery(
                message_id=record["id"],
                event_name=record["event_name"],
                handler=record["handler"],
                attempts=record["attempts"],
                payload=_StoredEvent(version=record["event_version"], payload=record["payload"]),
            )
            for record in records
        )

    def load_event(self, delivery: OutboxDelivery[_StoredEvent]) -> Event:
        stored = delivery.payload
        return self._codecs[delivery.event_name, stored.version].from_json(stored.payload)

    async def complete(self, delivery: OutboxDelivery[_StoredEvent]) -> None:
        await self._pool.execute(_COMPLETE_SQL, delivery.message_id, delivery.attempts)

    async def fail(self, delivery: OutboxDelivery[_StoredEvent], error: str, retry_delay: timedelta | None) -> None:
        if retry_delay is None:
            await self._pool.execute(_FAIL_SQL, delivery.message_id, delivery.attempts, error)
        else:
            await self._pool.execute(_RETRY_SQL, delivery.message_id, delivery.attempts, retry_delay, error)
            # The wait of the dispatcher may end after the retry is due, so the retry wakes it up.
            self._notified.set()

    async def release(self, delivery: OutboxDelivery[_StoredEvent]) -> None:
        await self._pool.execute(_RELEASE_SQL, delivery.message_id, delivery.attempts)
        self._notified.set()

    async def wait(self, max_wait: timedelta) -> None:
        if self._listener is None or self._listener.is_closed():
            await self._stop_listening()
            await self._listen()
        next_available: timedelta | None = await self._pool.fetchval(_NEXT_AVAILABLE_SQL, *self._known_args)
        if next_available is not None:
            max_wait = min(max_wait, max(timedelta(0), next_available))
        await wait_for_change(self._notified, max_wait)


class AsyncpgOutbox:
    """The outbox in the table of `OUTBOX_SECTION` with the `Pool`, `Connection` and `Serializer` dependencies.

    The environment gets `effect_defs`, which write events in the transaction that publishes them, and
    `dependency_defs`, whose service dispatches them. A process that only writes events gets only `effect_defs`.

    `notify` wakes dispatchers up on commit, it costs a lock that queues up the commits that notify.
    The dispatcher holds a connection of the pool to listen to notifications, so the pool needs at least one more
    connection for the queries. A pooler in transaction mode does not pass notifications, so the pool should connect
    to PostgreSQL directly.
    """

    def __init__(
        self,
        options: OutboxDispatcherOptions = OutboxDispatcherOptions(),
        *,
        notify: bool = True,
        middleware_defs: Sequence[EffectMiddlewareDef] | None = None,
    ) -> None:
        """Middlewares replace the environment middlewares for the writes, `None` keeps them."""
        self._options: OutboxDispatcherOptions = options
        self.effect_defs: Final[Sequence[EffectDef[WriteOutboxEffect]]] = (
            EffectDef(
                effect_type=WriteOutboxEffect,
                handler=_write_and_notify if notify else _write,
                middleware_defs=middleware_defs,
            ),
        )
        self.dependency_defs: Final[Sequence[EffectDependencyDef[object]]] = (
            EffectDependencyDef(self._dispatcher, lifetime=EffectDependencyLifetime.SERVICE),
        )

    async def _dispatcher(
        self,
        environment: Environment,
        pool: Pool,
        serializer: Serializer,
    ) -> AsyncIterator[OutboxDispatcher]:
        store = AsyncpgOutboxStore(pool=pool, serializer=serializer, event_defs=environment.event_defs)
        async with store, OutboxDispatcher(store=store, environment=environment, options=self._options) as running:
            yield running
