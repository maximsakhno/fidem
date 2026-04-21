from collections.abc import Callable, Collection, Generator, Hashable
from dataclasses import dataclass
from typing import Any

from fidem.intents import UserIntent
from fidem.serialization import Serializable

type OperationId = Hashable


@dataclass(frozen=True)
class OperationContext:
    operation_id: OperationId


@dataclass(frozen=True)
class CommandContext(OperationContext):
    pass


@dataclass(frozen=True)
class EventContext(OperationContext):
    pass


class Operation[ResultT](Serializable):
    pass


class Command[ResultT](Operation[ResultT]):
    pass


class Event(Operation[None]):
    pass


type OperationGenerator[
    ResultT = Any,
    EventOutT: Event | None = None,
] = Generator[
    UserIntent[Any],
    Any,
    ResultT | Collection[EventOutT] | tuple[ResultT, Collection[EventOutT]],
]
type OperationHandler[
    OperationContextT: OperationContext = Any,
    OperationT: Operation[Any] = Any,
    ResultT = Any,
    EventOutT: Event | None = None,
] = Callable[
    [OperationContextT, OperationT],
    OperationGenerator[ResultT, EventOutT],
]
type OperationMiddleware[
    OperationContextT: OperationContext = Any,
    OperationT: Operation[Any] = Any,
    ResultT = Any,
    EventOutT: Event | None = None,
] = Callable[
    [OperationHandler[OperationContextT, OperationT, ResultT, EventOutT]],
    OperationHandler[OperationContextT, OperationT, ResultT, EventOutT],
]
type CommandGenerator[
    ResultT = Any,
    EventOutT: Event | None = None,
] = OperationGenerator[ResultT, EventOutT]
type CommandHandler[
    CommandT: Command[Any] = Any,
    ResultT = Any,
    EventOutT: Event | None = None,
] = OperationHandler[
    CommandContext,
    CommandT,
    ResultT,
    EventOutT,
]
type CommandMiddleware[
    CommandT: Command[Any] = Any,
    ResultT = Any,
    EventOutT: Event | None = None,
] = OperationMiddleware[
    CommandContext,
    CommandT,
    ResultT,
    EventOutT,
]
type EventGenerator[
    EventOutT: Event | None = None,
] = OperationGenerator[
    None,
    EventOutT,
]
type EventHandler[
    EventT: Event = Any,
    EventOutT: Event | None = None,
] = OperationHandler[
    EventContext,
    EventT,
    None,
    EventOutT,
]
type EventMiddleware[
    EventT: Event = Any,
    EventOutT: Event | None = None,
] = OperationMiddleware[
    EventContext,
    EventT,
    None,
    EventOutT,
]
