from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Final, Never

from fidem.effects import (
    DatabaseReadEffect,
    Effect,
    Effectful,
    ExternalReadEffect,
    ExternalWriteEffect,
    InternalEffect,
)
from fidem.transactions import ReadTransactionEffect, WriteTransactionEffect

__all__ = [
    "QUERY_EFFECT_TYPES",
    "AtomicEffectful",
    "AtomicHandler",
    "Command",
    "Event",
    "Message",
    "MessageContext",
    "MessageId",
    "MessageName",
    "Query",
    "QueryEffect",
    "QueryEffectful",
    "QueryHandler",
    "StagedEffectful",
    "StagedHandler",
]


class Message[ResultT](Effect[ResultT]):
    pass


type MessageId = str
type MessageName = str


@dataclass(slots=True, frozen=True)
class MessageContext:
    message_id: MessageId


class Command[ResultT](Message[ResultT]):
    pass


class Query[ResultT](Message[ResultT]):
    pass


class Event(Message[None]):
    pass


type QueryEffect = InternalEffect[Any] | ExternalReadEffect[Any] | DatabaseReadEffect[Any] | ReadTransactionEffect[Any]
"""Effects that only read, which a query handler, a guard and the first phase of an atomic handler may yield."""
QUERY_EFFECT_TYPES: Final[tuple[type[Effect[Any]], ...]] = (
    InternalEffect,
    ExternalReadEffect,
    DatabaseReadEffect,
    ReadTransactionEffect,
)
"""Runtime counterpart of `QueryEffect`."""
type QueryEffectful[ResultT] = Effectful[ResultT, QueryEffect]
"""The generator of a query handler."""
type QueryHandler[ResultT] = Callable[..., QueryEffectful[ResultT]]
type AtomicEffectful[ResultT, EventT: Event = Never] = Effectful[
    WriteTransactionEffect[ResultT, EventT] | ExternalWriteEffect[ResultT],
    QueryEffect,
]
"""The generator of an atomic handler."""
type AtomicHandler[ResultT, EventT: Event = Never] = Callable[..., AtomicEffectful[ResultT, EventT]]
type StagedEffectful[ResultT, EventT: Event = Never] = Effectful[ResultT, Effect[Any] | EventT]
"""The generator of a staged handler, `EventT` are the events it publishes."""
type StagedHandler[ResultT, EventT: Event = Never] = Callable[..., StagedEffectful[ResultT, EventT]]
