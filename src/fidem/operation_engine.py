from abc import ABC, abstractmethod
from collections.abc import Collection, Generator, Sequence
from dataclasses import dataclass
from typing import Any

from fidem.intents import Intent
from fidem.operations import (
    Command,
    CommandHandler,
    CommandMiddleware,
    Event,
    EventHandler,
    EventMiddleware,
    OperationId,
)


@dataclass(frozen=True)
class OperationContextIn:
    operation_id: OperationId | None = None


@dataclass(frozen=True)
class CommandContextIn(OperationContextIn):
    pass


@dataclass(frozen=True)
class EventContextIn(OperationContextIn):
    pass


@dataclass(frozen=True)
class CommandDef[CommandT: Command[Any] = Any, ResultT = Any, EventOutT: Event | None = None]:
    command_type: type[Command[ResultT]]
    handler: CommandHandler[CommandT, ResultT, EventOutT]
    middlewares: Sequence[CommandMiddleware[CommandT, ResultT, EventOutT]] | None = None
    name: str | None = None
    version: int = 1


@dataclass(frozen=True)
class EventDef[EventT: Event = Any, EventOutT: Event | None = None]:
    event_type: type[EventT]
    handler: EventHandler[EventT, EventOutT]
    middlewares: Sequence[EventMiddleware[EventT, EventOutT]] | None = None
    name: str | None = None
    version: int = 1


@dataclass(frozen=True)
class ScheduledEventDef[EventT: Event = Any]:
    name: str
    cron: str
    event: EventT


type ExecutionPlan[
    ResultT,
] = Generator[
    Intent[Any],
    Any,
    ResultT,
]


class OperationEngine(ABC):
    @abstractmethod
    def __init__(
        self,
        command_defs: Collection[CommandDef] = (),
        event_defs: Collection[EventDef] = (),
        scheduled_event_defs: Collection[ScheduledEventDef] = (),
        command_middlewares: Sequence[CommandMiddleware] = (),
        event_middlewares: Sequence[EventMiddleware] = (),
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def execute[ResultT](
        self,
        command: Command[ResultT],
        context: CommandContextIn | None = None,
    ) -> ExecutionPlan[ResultT]:
        raise NotImplementedError

    @abstractmethod
    def publish(
        self,
        event: Event,
        context: EventContextIn | None = None,
    ) -> ExecutionPlan[None]:
        raise NotImplementedError

    @abstractmethod
    def schedule(
        self,
        name: str,
        cron: str,
        event: Event,
        context: EventContextIn | None = None,
    ) -> ExecutionPlan[None]:
        raise NotImplementedError

    @abstractmethod
    def unschedule(
        self,
        name: str,
    ) -> ExecutionPlan[None]:
        raise NotImplementedError

    @abstractmethod
    def run_outbox_dispatcher(self) -> ExecutionPlan[None]:
        raise NotImplementedError

    @abstractmethod
    def run_scheduler(self) -> ExecutionPlan[None]:
        raise NotImplementedError
