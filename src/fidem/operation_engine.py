from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Collection, Mapping, MutableMapping, MutableSequence
from dataclasses import dataclass
from enum import Enum, auto
from random import Random
from typing import Any, Final
from uuid import uuid4

from fidem.effects import DatabaseEffect, Effect, EffectContext, Effectful, EffectId, ExternalEffect, InternalEffect
from fidem.messages import (
    AtomicHandler,
    Command,
    Event,
    Message,
    MessageContext,
    MessageHandler,
    MessageId,
    StagedHandler,
)


class ExecutionMode(Enum):
    EPHEMERAL = auto()
    DURABLE = auto()


@dataclass(frozen=True, slots=True)
class MessageDef[MessageT: Message[Any], MessageHandlerT: MessageHandler[Any, Any]]:
    name: str
    message_type: type[MessageT]
    handler: MessageHandlerT
    execution_mode: ExecutionMode = ExecutionMode.EPHEMERAL


@dataclass(frozen=True, slots=True)
class AtomicCommandDef[ResultT](MessageDef[Command[ResultT], AtomicHandler[ResultT, Event]]):
    pass


@dataclass(frozen=True, slots=True)
class StagedCommandDef[ResultT](MessageDef[Command[ResultT], StagedHandler[ResultT, Event]]):
    pass


type CommandDef[ResultT] = StagedCommandDef[ResultT] | AtomicCommandDef[ResultT]

type HandledEffect[ResultT] = InternalEffect[ResultT] | ExternalEffect[ResultT] | DatabaseEffect[ResultT]


class Instruction[ResultT](Effect[ResultT]):
    pass


type ExecutionPlan[ResultT] = Effectful[ResultT, Instruction[Any]]


@dataclass(frozen=True, slots=True)
class CallEffectHandlerInstruction[ResultT](Instruction[ResultT]):
    context: EffectContext
    effect: HandledEffect[ResultT]


class OperationEngine(ABC):
    @abstractmethod
    def execute[ResultT](
        self,
        command: Command[ResultT],
        message_id: MessageId | None = None,
    ) -> ExecutionPlan[ResultT]:
        raise NotImplementedError


class DuplicateCommandDefError(Exception):
    def __init__(self, command_defs: Collection[CommandDef[Any]]) -> None:
        super().__init__(command_defs)


class CommandNotDefinedError(Exception):
    def __init__(self, command_type: type[Command[Any]]) -> None:
        super().__init__(command_type)


class UnsupportedEffectError(Exception):
    def __init__(self, effect: Effect[Any]) -> None:
        super().__init__(effect)


class OperationEngineImpl(OperationEngine):
    _HANDLED_EFFECT_TYPES: Final = (InternalEffect, ExternalEffect, DatabaseEffect)

    def __init__(
        self,
        command_defs: Collection[CommandDef[Any]] = (),
    ) -> None:
        self._type_to_command_def: Mapping[type[Command[Any]], CommandDef[Any]] = self._make_type_to_command_def(
            command_defs
        )

    @classmethod
    def _make_type_to_command_def(
        cls,
        command_defs: Collection[CommandDef[Any]],
    ) -> Mapping[type[Command[Any]], CommandDef[Any]]:
        type_to_command_defs: MutableMapping[type[Command[Any]], MutableSequence[CommandDef[Any]]] = defaultdict(list)
        for command_def in command_defs:
            type_to_command_defs[command_def.message_type].append(command_def)

        type_to_command_def: MutableMapping[type[Command[Any]], CommandDef[Any]] = {}
        for command_type, defs in type_to_command_defs.items():
            if len(defs) > 1:
                raise DuplicateCommandDefError(defs)
            type_to_command_def[command_type] = defs[0]

        return type_to_command_def

    def execute[ResultT](
        self,
        command: Command[ResultT],
        message_id: MessageId | None = None,
    ) -> ExecutionPlan[ResultT]:
        command_def = self._get_command_def(type(command))
        execution_mode = command_def.execution_mode

        if execution_mode != ExecutionMode.EPHEMERAL:
            raise NotImplementedError(f"Execution mode {execution_mode} is not implemented yet")

        if isinstance(command_def, AtomicCommandDef):
            raise NotImplementedError("Atomic command handlers are not implemented yet")

        if message_id is None:
            message_id = uuid4().hex

        # Effect ids are derived from the message id so that replaying a command reproduces them.
        random = Random(message_id)  # noqa: S311 - determinism is required, not cryptographic strength
        context = MessageContext(message_id=message_id)
        effectful = command_def.handler(context, command)
        return (yield from self._execute_ephemeral(random, effectful))

    def _get_command_def(self, command_type: type[Command[Any]]) -> CommandDef[Any]:
        command_def = self._type_to_command_def.get(command_type)
        if command_def is None:
            raise CommandNotDefinedError(command_type)
        return command_def

    def _execute_ephemeral[ResultT](
        self,
        random: Random,
        effectful: Effectful[ResultT, Effect[Any]],
    ) -> ExecutionPlan[ResultT]:
        try:
            effect = effectful.send(None)
            while True:
                instruction = self._make_instruction(random, effect)
                try:
                    effect_result = yield instruction
                except Exception as exc:  # noqa: BLE001 - the handler decides how to handle the error
                    effect = effectful.throw(exc)
                else:
                    effect = effectful.send(effect_result)
        except StopIteration as exc:
            return exc.value
        finally:
            effectful.close()

    def _make_instruction(self, random: Random, effect: Effect[Any]) -> Instruction[Any]:
        if isinstance(effect, self._HANDLED_EFFECT_TYPES):
            context = EffectContext(effect_id=self._generate_effect_id(random))
            return CallEffectHandlerInstruction(context=context, effect=effect)
        raise UnsupportedEffectError(effect)

    def _generate_effect_id(self, random: Random) -> EffectId:
        return random.randbytes(16).hex()
