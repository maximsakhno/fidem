from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final

from fidem.core.effects import DatabaseWriteEffect, EffectId, EffectName
from fidem.core.messages import Event, EventHandlerName, MessageId, MessageName, MessageVersion

__all__ = [
    "WRITE_OUTBOX_EFFECT_NAME",
    "OutboxEvent",
    "WriteOutboxEffect",
]


@dataclass(frozen=True, slots=True)
class OutboxEvent:
    """An event to deliver to each of its handlers."""

    event_id: EffectId
    source_message_id: MessageId
    name: MessageName
    version: MessageVersion
    handlers: Sequence[EventHandlerName]
    event: Event

    def handler_message_id(self, handler: EventHandlerName) -> MessageId:
        """The message id of the operation of the handler, which also identifies the delivery."""
        return f"{self.event_id}:{handler}"


@dataclass(frozen=True, slots=True)
class WriteOutboxEffect(DatabaseWriteEffect[None]):
    """Writes a delivery of each event to each of its handlers and skips the deliveries already written."""

    events: Sequence[OutboxEvent]


WRITE_OUTBOX_EFFECT_NAME: Final[EffectName] = "fidem.write_outbox"
"""The name of the effect each driver of the outbox handles."""
