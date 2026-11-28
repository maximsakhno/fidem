from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any

from fidem.core.effects import ControlEffect, Effectful, HandledEffect
from fidem.core.messages import (
    AtomicHandler,
    Command,
    Event,
    EventHandlerName,
    EventVersion,
    MessageName,
    Query,
    QueryHandler,
    StagedHandler,
)
from fidem.core.transactions import ReadTransactionEffect, WriteTransactionEffect

__all__ = [
    "AnyEventHandlerDef",
    "AtomicCommandDef",
    "AtomicEventHandlerDef",
    "CommandDef",
    "EventDef",
    "EventHandlerDef",
    "ExecutionMode",
    "MessageMiddleware",
    "MessageMiddlewareDef",
    "MessageMiddlewareEffect",
    "MessageMiddlewareEffectful",
    "MiddlewareName",
    "Proceed",
    "QueryDef",
    "StagedCommandDef",
    "StagedEventHandlerDef",
]


class ExecutionMode(Enum):
    EPHEMERAL = auto()
    DURABLE = auto()


@dataclass(frozen=True, slots=True)
class Proceed(ControlEffect[Any]):
    """Runs the inner part of the middleware chain and returns its result."""


type MessageMiddlewareEffect = HandledEffect[Any] | ReadTransactionEffect[Any] | WriteTransactionEffect[Any] | Proceed
"""Effects a message middleware may yield, its write transactions publish no events."""
type MessageMiddlewareEffectful[ResultT] = Effectful[ResultT, MessageMiddlewareEffect]
"""The generator of a message middleware."""
type MessageMiddleware = Callable[..., MessageMiddlewareEffectful[object]]
type MiddlewareName = str


@dataclass(frozen=True, slots=True)
class MessageMiddlewareDef:
    name: MiddlewareName
    """A stable name the effect ids of the middleware depend on."""
    middleware: MessageMiddleware


@dataclass(frozen=True, slots=True)
class _CommandDef[ResultT, CommandHandlerT: Callable[..., Effectful[Any, Any]]]:
    name: MessageName
    """A stable name for serialization and storage."""
    message_type: type[Command[ResultT]]
    handler: CommandHandlerT
    middleware_defs: Sequence[MessageMiddlewareDef] | None = None
    """Replace the engine middlewares, `None` keeps them."""
    execution_mode: ExecutionMode = ExecutionMode.EPHEMERAL


@dataclass(frozen=True, slots=True)
class AtomicCommandDef[ResultT](_CommandDef[ResultT, AtomicHandler[ResultT, Event]]):
    pass


@dataclass(frozen=True, slots=True)
class StagedCommandDef[ResultT](_CommandDef[ResultT, StagedHandler[ResultT, Event]]):
    pass


type CommandDef[ResultT] = StagedCommandDef[ResultT] | AtomicCommandDef[ResultT]


@dataclass(frozen=True, slots=True)
class QueryDef[ResultT]:
    """A query has no name: it is never serialized, and its type tells it from the others."""

    message_type: type[Query[ResultT]]
    handler: QueryHandler[ResultT]
    middleware_defs: Sequence[MessageMiddlewareDef] | None = None
    """Replace the engine middlewares, `None` keeps them."""


@dataclass(frozen=True, slots=True)
class EventHandlerDef[EventHandlerT: Callable[..., Effectful[Any, Any]]]:
    name: EventHandlerName
    """A stable name unique among the handlers of the event, the message id of its operation depends on it."""
    handler: EventHandlerT
    middleware_defs: Sequence[MessageMiddlewareDef] | None = None
    """Replace the engine middlewares, `None` keeps them."""
    execution_mode: ExecutionMode = ExecutionMode.EPHEMERAL


@dataclass(frozen=True, slots=True)
class AtomicEventHandlerDef(EventHandlerDef[AtomicHandler[None, Event]]):
    pass


@dataclass(frozen=True, slots=True)
class StagedEventHandlerDef(EventHandlerDef[StagedHandler[None, Event]]):
    pass


type AnyEventHandlerDef = AtomicEventHandlerDef | StagedEventHandlerDef


@dataclass(frozen=True, slots=True)
class EventDef[EventT: Event]:
    name: MessageName
    event_type: type[EventT]
    version: EventVersion = 1
    handler_defs: Sequence[AnyEventHandlerDef] = ()
