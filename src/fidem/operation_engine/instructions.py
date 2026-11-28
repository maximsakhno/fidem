from abc import ABC, abstractmethod
from collections.abc import Collection
from dataclasses import dataclass
from typing import Any

from fidem.effects import Effect, EffectContext, Effectful, HandledEffect
from fidem.messages import Command, Event, EventHandlerName, Message, MessageContext, Query
from fidem.operation_engine.definitions import EventDef
from fidem.transactions import AnyTransaction

__all__ = [
    "CallEffectHandlerInstruction",
    "EffectCall",
    "ExecutionPlan",
    "HandledEffect",
    "Instruction",
    "OperationEngine",
    "RunTransactionInstruction",
]


class Instruction[ResultT](Effect[ResultT]):
    pass


type ExecutionPlan[ResultT] = Effectful[ResultT, Instruction[Any]]


@dataclass(frozen=True, slots=True)
class EffectCall:
    """A call of the handler of an effect that a message yielded."""

    message_context: MessageContext
    message: Message[Any]
    effect_context: EffectContext
    effect: HandledEffect[Any]


@dataclass(frozen=True, slots=True)
class CallEffectHandlerInstruction[ResultT](Instruction[ResultT]):
    call: EffectCall


@dataclass(frozen=True, slots=True)
class RunTransactionInstruction[ResultT](Instruction[ResultT]):
    """Runs the plan in one database transaction and returns its result."""

    transaction: AnyTransaction
    plan: ExecutionPlan[ResultT]


class OperationEngine(ABC):
    @property
    @abstractmethod
    def event_defs(self) -> Collection[EventDef[Any]]:
        """Declared events, by which a delivery finds the type of its event and its handlers."""
        raise NotImplementedError

    @abstractmethod
    def handle[ResultT](
        self,
        message: Command[ResultT] | Query[ResultT],
        message_context: MessageContext,
        inputs: Collection[object] = (),
    ) -> ExecutionPlan[ResultT]:
        """Plans the operation of the command or the query, the guards get the inputs."""
        raise NotImplementedError

    @abstractmethod
    def deliver(
        self,
        event: Event,
        handler: EventHandlerName,
        message_context: MessageContext,
    ) -> ExecutionPlan[None]:
        """Plans the operation of one handler of the event."""
        raise NotImplementedError
