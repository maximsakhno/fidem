from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Collection, Mapping, MutableMapping, MutableSequence, Sequence
from dataclasses import dataclass
from enum import Enum, auto
from random import Random
from typing import Any, Final

from fidem.di.core import DependencyKey
from fidem.effects import (
    DatabaseEffect,
    Effect,
    EffectContext,
    Effectful,
    EffectId,
    ExternalEffect,
    InternalEffect,
)
from fidem.guards import GUARD_EFFECT_TYPES, GuardContainer, GuardDef, PreparedMessageHandler
from fidem.messages import (
    AtomicHandler,
    Command,
    Event,
    Message,
    MessageContext,
    MessageHandler,
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
    message_context: MessageContext
    message: Message[Any]
    effect_context: EffectContext
    effect: HandledEffect[ResultT]


class OperationEngine(ABC):
    @abstractmethod
    def execute[ResultT](
        self,
        command: Command[ResultT],
        message_context: MessageContext,
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


class _EffectIdGenerator:
    """Generates deterministic effect ids and seeds lazily because seeding is costly and many guards yield nothing."""

    def __init__(self, seed: str) -> None:
        self._seed: str = seed
        self._random: Random | None = None

    def __call__(self) -> EffectId:
        if self._random is None:
            self._random = Random(self._seed)  # noqa: S311 - determinism is required, not cryptographic strength
        return self._random.randbytes(16).hex()


@dataclass(frozen=True, slots=True)
class _PreparedCommand:
    command_def: CommandDef[Any]
    handler: PreparedMessageHandler
    seeds: Sequence[str]
    """Identify the guards in the seeds of the ids of their effects, in the order of the steps."""


class OperationEngineImpl(OperationEngine):
    _HANDLED_EFFECT_TYPES: Final = (InternalEffect, ExternalEffect, DatabaseEffect)
    _ANY_EFFECT_TYPES: Final[tuple[type[Effect[Any]], ...]] = (Effect,)

    def __init__(
        self,
        command_defs: Collection[CommandDef[Any]] = (),
        guard_defs: Collection[GuardDef[Any]] = (),
    ) -> None:
        type_to_command_def = self._make_type_to_command_def(command_defs)
        self._guard_container: GuardContainer = GuardContainer(
            guard_defs=guard_defs,
            message_types=tuple(type_to_command_def),
        )
        self._type_to_command: Mapping[type[Command[Any]], _PreparedCommand] = {
            command_type: self._prepare_command(command_def)
            for command_type, command_def in type_to_command_def.items()
        }

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

    def _prepare_command(self, command_def: CommandDef[Any]) -> _PreparedCommand:
        handler = self._guard_container.prepare_message_handler(command_def.message_type, command_def.handler)
        return _PreparedCommand(
            command_def=command_def,
            handler=handler,
            seeds=tuple(self._make_seed(step.key) for step in handler.steps),
        )

    @classmethod
    def _make_seed(cls, key: DependencyKey) -> str:
        dependency_type = key.type
        if isinstance(dependency_type, type):
            return f":{key.name}:{dependency_type.__module__}.{dependency_type.__qualname__}"
        return f":{key.name}:{dependency_type!r}"

    def execute[ResultT](
        self,
        command: Command[ResultT],
        message_context: MessageContext,
    ) -> ExecutionPlan[ResultT]:
        prepared_command = self._get_command(type(command))
        execution_mode = prepared_command.command_def.execution_mode

        if execution_mode != ExecutionMode.EPHEMERAL:
            raise NotImplementedError(f"Execution mode {execution_mode} is not implemented yet")

        if isinstance(prepared_command.command_def, AtomicCommandDef):
            raise NotImplementedError("Atomic command handlers are not implemented yet")

        message_id = message_context.message_id
        scope = self._guard_container.make_message_scope(command, message_context)
        # The length prefix keeps the seeds of different messages apart whatever their ids are.
        message_seed = f"{len(message_id)}:{message_id}"
        for step, seed in zip(prepared_command.handler.steps, prepared_command.seeds, strict=True):
            # Each guard has its own ids, so adding a guard does not shift the ids of the handler effects.
            yield from self._run(
                effectful=step.run(scope),
                allowed_effect_types=GUARD_EFFECT_TYPES,
                generate_effect_id=_EffectIdGenerator(message_seed + seed),
                message_context=message_context,
                message=command,
            )

        # Effect ids are derived from the message id so that replaying a command reproduces them.
        return (
            yield from self._run(
                effectful=prepared_command.handler.call(scope),
                allowed_effect_types=self._ANY_EFFECT_TYPES,
                generate_effect_id=_EffectIdGenerator(message_seed),
                message_context=message_context,
                message=command,
            )
        )

    def _get_command(self, command_type: type[Command[Any]]) -> _PreparedCommand:
        prepared_command = self._type_to_command.get(command_type)
        if prepared_command is None:
            raise CommandNotDefinedError(command_type)
        return prepared_command

    @classmethod
    def _run[ResultT](
        cls,
        effectful: Effectful[ResultT, Effect[Any]],
        allowed_effect_types: tuple[type[Effect[Any]], ...],
        generate_effect_id: Callable[[], EffectId],
        message_context: MessageContext,
        message: Message[Any],
    ) -> ExecutionPlan[ResultT]:
        try:
            effect = effectful.send(None)
            while True:
                instruction = cls._make_instruction(
                    effect=effect,
                    allowed_effect_types=allowed_effect_types,
                    generate_effect_id=generate_effect_id,
                    message_context=message_context,
                    message=message,
                )
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

    @classmethod
    def _make_instruction(
        cls,
        effect: Effect[Any],
        allowed_effect_types: tuple[type[Effect[Any]], ...],
        generate_effect_id: Callable[[], EffectId],
        message_context: MessageContext,
        message: Message[Any],
    ) -> Instruction[Any]:
        if not isinstance(effect, allowed_effect_types):
            raise UnsupportedEffectError(effect)
        if isinstance(effect, cls._HANDLED_EFFECT_TYPES):
            return CallEffectHandlerInstruction(
                message_context=message_context,
                message=message,
                effect_context=EffectContext(effect_id=generate_effect_id()),
                effect=effect,
            )
        raise UnsupportedEffectError(effect)
