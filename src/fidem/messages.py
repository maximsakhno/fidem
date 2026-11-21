from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from fidem.effects import (
    DatabaseEffect,
    DatabaseReadEffect,
    Effect,
    Effectful,
    ExternalReadEffect,
    InternalEffect,
    ReadEffect,
)


class Message[ResultT](Effect[ResultT]):
    pass


type MessageId = str


@dataclass(slots=True, frozen=True)
class MessageContext:
    message_id: MessageId


class Command[ResultT](Message[ResultT]):
    pass


class Query[ResultT](Message[ResultT]):
    pass


class Event(Message[None]):
    pass


type MessageHandler[ResultT, EffectT: Effect[Any]] = Callable[..., Effectful[ResultT, EffectT]]
type Transaction[ResultT, EventT: Event] = Callable[[], Effectful[ResultT, DatabaseEffect[Any] | EventT]]
type AtomicHandler[ResultT, EventT: Event] = MessageHandler[
    Transaction[ResultT, EventT],
    InternalEffect[Any] | ExternalReadEffect[Any] | DatabaseReadEffect[Any],
]
type StagedHandler[ResultT, EventT: Event] = MessageHandler[ResultT, Effect[Any] | EventT]
type QueryHandler[ResultT] = MessageHandler[ResultT, ReadEffect[Any]]
