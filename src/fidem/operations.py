from collections.abc import Callable, Collection, Generator
from dataclasses import dataclass
from typing import Any

from fidem.intents import DatabaseWriteIntent, ReadIntent, WriteIntent

type OperationId = str


@dataclass(frozen=True, slots=True)
class OperationContext:
    operation_id: OperationId


@dataclass(frozen=True, slots=True)
class CommandContext(OperationContext):
    pass


@dataclass(frozen=True, slots=True)
class EventContext(OperationContext):
    pass


class Operation[ResultT]:
    pass


class Command[ResultT](Operation[ResultT]):
    pass


class Event(Operation[None]):
    pass


type OperationResult[
    ResultT,
    EventOutT: Event | None = None,
] = (
    ResultT
    | WriteIntent[ResultT]
    | Collection[EventOutT]
    | tuple[ResultT, Collection[EventOutT]]
    | tuple[DatabaseWriteIntent[ResultT], Collection[EventOutT] | Callable[[ResultT], Collection[EventOutT]]]
)
type OperationGenerator[
    IntentT: ReadIntent[Any] | WriteIntent[Any] = Any,
    ResultT = Any,
    EventOutT: Event | None = None,
] = Generator[
    IntentT,
    Any,
    OperationResult[ResultT, EventOutT],
]
type OperationHandler[
    OperationContextT: OperationContext = Any,
    OperationT: Operation[Any] = Any,
    IntentT: ReadIntent[Any] | WriteIntent[Any] = Any,
    ResultT = Any,
    EventOutT: Event | None = None,
] = Callable[
    [OperationContextT, OperationT],
    OperationGenerator[IntentT, ResultT, EventOutT],
]
type OperationMiddleware[
    OperationContextT: OperationContext = Any,
    OperationT: Operation[Any] = Any,
    IntentT: ReadIntent[Any] | WriteIntent[Any] = Any,
    ResultT = Any,
    EventOutT: Event | None = None,
] = Callable[
    [OperationHandler[OperationContextT, OperationT, IntentT, ResultT, EventOutT]],
    OperationHandler[OperationContextT, OperationT, IntentT, ResultT, EventOutT],
]


type CommandGenerator[
    ResultT = Any,
    EventOutT: Event | None = None,
] = OperationGenerator[
    ReadIntent[Any] | WriteIntent[Any],
    ResultT,
    EventOutT,
]
type CommandHandler[
    CommandT: Command[Any] = Any,
    ResultT = Any,
    EventOutT: Event | None = None,
] = OperationHandler[
    CommandContext,
    CommandT,
    ReadIntent[Any] | WriteIntent[Any],
    ResultT,
    EventOutT,
]
type ConsistentEphemeralCommandGenerator[
    ResultT = Any,
    EventOutT: Event | None = None,
] = OperationGenerator[
    ReadIntent[Any],
    ResultT,
    EventOutT,
]
type ConsistentEphemeralCommandHandler[
    CommandT: Command[Any] = Any,
    ResultT = Any,
    EventOutT: Event | None = None,
] = OperationHandler[
    CommandContext,
    CommandT,
    ReadIntent[Any],
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
    ReadIntent[Any] | WriteIntent[Any],
    ResultT,
    EventOutT,
]


type EventGenerator[
    EventOutT: Event | None = None,
] = OperationGenerator[
    ReadIntent[Any] | WriteIntent[Any],
    None,
    EventOutT,
]
type EventHandler[
    EventT: Event = Any,
    EventOutT: Event | None = None,
] = OperationHandler[
    EventContext,
    EventT,
    ReadIntent[Any] | WriteIntent[Any],
    None,
    EventOutT,
]
type EventMiddleware[
    EventT: Event = Any,
    EventOutT: Event | None = None,
] = OperationMiddleware[
    EventContext,
    EventT,
    ReadIntent[Any] | WriteIntent[Any],
    None,
    EventOutT,
]
