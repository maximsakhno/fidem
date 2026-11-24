from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Never

from fidem.effects import (
    DatabaseEffect,
    DatabaseReadEffect,
    Effect,
    Effectful,
    ExternalReadEffect,
    ExternalWriteEffect,
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
type TransactionEffectful[ResultT, EventT: Event = Never] = Effectful[ResultT, DatabaseEffect[Any] | EventT]
"""The generator of a transaction of an atomic handler."""
type Transaction[ResultT, EventT: Event = Never] = Callable[[], TransactionEffectful[ResultT, EventT]]
type AtomicReadEffect = InternalEffect[Any] | ExternalReadEffect[Any] | DatabaseReadEffect[Any]
"""Effects an atomic handler may yield before it returns the write."""
type AtomicEffectful[ResultT, EventT: Event = Never] = Effectful[
    Transaction[ResultT, EventT] | ExternalWriteEffect[ResultT],
    AtomicReadEffect,
]
"""The generator of an atomic handler."""
type AtomicHandler[ResultT, EventT: Event = Never] = Callable[..., AtomicEffectful[ResultT, EventT]]
type StagedHandler[ResultT, EventT: Event = Never] = MessageHandler[ResultT, Effect[Any] | EventT]
type QueryHandler[ResultT] = MessageHandler[ResultT, ReadEffect[Any]]
