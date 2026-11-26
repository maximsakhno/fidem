from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from fidem.effects import DatabaseEffect, Effect, EffectContext, Effectful, ExternalEffect, InternalEffect
from fidem.messages import Command, Message, MessageContext, Query
from fidem.transactions import AnyTransaction

__all__ = [
    "CallEffectHandlerInstruction",
    "ExecutionPlan",
    "HandledEffect",
    "Instruction",
    "OperationEngine",
    "RunTransactionInstruction",
]


type HandledEffect[ResultT] = InternalEffect[ResultT] | ExternalEffect[ResultT] | DatabaseEffect[ResultT]


class Instruction[ResultT](Effect[ResultT]):
    pass


type ExecutionPlan[ResultT] = Effectful[ResultT, Instruction[Any]]


@dataclass(frozen=True, slots=True)
class CallEffectHandlerInstruction[ResultT](Instruction[ResultT]):
    message_context: MessageContext
    message: Message[Any]
    effect_context: EffectContext
    effect: HandledEffect[ResultT]


@dataclass(frozen=True, slots=True)
class RunTransactionInstruction[ResultT](Instruction[ResultT]):
    """Runs the plan in one database transaction and returns its result."""

    transaction: AnyTransaction
    plan: ExecutionPlan[ResultT]


class OperationEngine(ABC):
    @abstractmethod
    def execute[ResultT](
        self,
        command: Command[ResultT],
        message_context: MessageContext,
    ) -> ExecutionPlan[ResultT]:
        raise NotImplementedError

    @abstractmethod
    def query[ResultT](
        self,
        query: Query[ResultT],
        message_context: MessageContext,
    ) -> ExecutionPlan[ResultT]:
        raise NotImplementedError
