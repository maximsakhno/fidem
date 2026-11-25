from collections.abc import MutableSequence, Sequence
from importlib.resources import files
from typing import ClassVar, Final, NamedTuple

from asyncpg import Connection

from fidem.asyncpg.migrations import MigrationSection, load_section
from fidem.effects import EffectId
from fidem.environment import EffectDef, EffectMiddlewareDef
from fidem.messages import EventHandlerName, EventVersion, MessageId, MessageName
from fidem.outbox import WriteOutboxEffect
from fidem.serialization import Serializer

__all__ = [
    "OUTBOX_SECTION",
    "AsyncpgOutboxEffectDef",
]

OUTBOX_SECTION: Final[MigrationSection] = load_section(
    name="fidem.outbox", directory=files(__package__) / "sql" / "outbox"
)
"""The table that events are written to in the transaction that publishes them."""


class AsyncpgOutboxEffectDef(EffectDef[WriteOutboxEffect]):
    """Writes events to the table of `OUTBOX_SECTION` with the `Connection` and `Serializer` dependencies."""

    # A tuple because asyncpg takes each row as the sequence of the query parameters.
    class _Row(NamedTuple):
        id: MessageId
        event_id: EffectId
        source_message_id: MessageId
        handler: EventHandlerName
        event_name: MessageName
        event_version: EventVersion
        payload: str

    _INSERT_SQL: ClassVar[str] = """
        INSERT INTO fidem.outbox (id, event_id, source_message_id, handler, event_name, event_version, payload)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO NOTHING
    """

    def __init__(self, middleware_defs: Sequence[EffectMiddlewareDef] | None = None) -> None:
        super().__init__(
            effect_type=WriteOutboxEffect,
            handler=self._write,
            middleware_defs=middleware_defs,
        )

    @classmethod
    async def _write(cls, connection: Connection, serializer: Serializer, effect: WriteOutboxEffect) -> None:
        rows: MutableSequence[AsyncpgOutboxEffectDef._Row] = []
        for event in effect.events:
            # asyncpg expects jsonb as text.
            payload = serializer.codec(type(event.event)).to_json(event.event).decode()
            rows.extend(
                cls._Row(
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
        await connection.executemany(cls._INSERT_SQL, rows)
