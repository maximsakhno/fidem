from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Collection, Iterable, Mapping, MutableMapping, MutableSequence, Sequence
from dataclasses import dataclass
from enum import Enum, auto
from functools import partial
from random import Random
from typing import Any, Final

from fidem.di.core import DependencyKey, make_message_dependencies
from fidem.effects import (
    ControlEffect,
    DatabaseEffect,
    Effect,
    EffectContext,
    Effectful,
    EffectId,
    ExternalEffect,
    InternalEffect,
)
from fidem.guards import GUARD_EFFECT_TYPES, GuardContainer, GuardDef, GuardedFunction, GuardResolutionStep, GuardValues
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
class Proceed(ControlEffect[Any]):
    """Runs the inner part of the middleware chain and returns its result."""


type MessageMiddleware = Callable[..., Effectful[object, Effect[Any]]]


@dataclass(frozen=True, slots=True)
class MessageMiddlewareDef:
    name: str
    """A stable name the effect ids of the middleware depend on."""
    middleware: MessageMiddleware


@dataclass(frozen=True, slots=True)
class MessageDef[MessageT: Message[Any], MessageHandlerT: MessageHandler[Any, Any]]:
    name: str
    message_type: type[MessageT]
    handler: MessageHandlerT
    execution_mode: ExecutionMode = ExecutionMode.EPHEMERAL
    middleware_defs: Sequence[MessageMiddlewareDef] | None = None
    """Replace the engine middlewares, `None` keeps them."""


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


class DuplicateMessageMiddlewareDefError(Exception):
    def __init__(self, middleware_defs: Collection[MessageMiddlewareDef]) -> None:
        super().__init__(middleware_defs)


class CommandNotDefinedError(Exception):
    def __init__(self, command_type: type[Command[Any]]) -> None:
        super().__init__(command_type)


class UnsupportedEffectError(Exception):
    def __init__(self, effect: Effect[Any]) -> None:
        super().__init__(effect)


class _EngineError(Exception):
    """Carries an error of the engine past the handlers and the middlewares to the caller."""

    def __init__(self, error: Exception) -> None:
        super().__init__(error)
        self.error: Exception = error


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
class _PreparedGuard:
    step: GuardResolutionStep
    seed: str
    """Identifies the guard in the seed of the ids of its effects."""


@dataclass(frozen=True, slots=True)
class _PreparedMiddleware:
    middleware: GuardedFunction
    seed: str
    """Identifies the middleware in the seed of the ids of its effects."""


@dataclass(frozen=True, slots=True)
class _PreparedCommand:
    command_def: CommandDef[Any]
    guards: Sequence[_PreparedGuard]
    """Guards of the middlewares and the handler, each one after its guards."""
    middlewares: Sequence[_PreparedMiddleware]
    """Middlewares from the outer one."""
    handler: GuardedFunction


@dataclass(frozen=True, slots=True)
class _Execution:
    message: Command[Any]
    message_context: MessageContext
    values: GuardValues
    seed: str


type _PlanEffect = Callable[[Effect[Any]], Iterable[Instruction[Any]]]


class OperationEngineImpl(OperationEngine):
    _HANDLED_EFFECT_TYPES: Final = (InternalEffect, ExternalEffect, DatabaseEffect)
    _ANY_EFFECT_TYPES: Final[tuple[type[Effect[Any]], ...]] = (Effect,)

    def __init__(
        self,
        command_defs: Collection[CommandDef[Any]] = (),
        guard_defs: Collection[GuardDef[Any]] = (),
        middleware_defs: Sequence[MessageMiddlewareDef] = (),
    ) -> None:
        self._check_middleware_defs(middleware_defs)
        type_to_command_def = self._make_type_to_command_def(command_defs)
        self._guard_container: GuardContainer = GuardContainer(
            guard_defs=guard_defs,
            message_types=tuple(type_to_command_def),
        )
        self._type_to_command: Mapping[type[Command[Any]], _PreparedCommand] = {
            command_type: self._prepare_command(command_def, middleware_defs)
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

    @classmethod
    def _check_middleware_defs(cls, middleware_defs: Collection[MessageMiddlewareDef]) -> None:
        name_to_middleware_defs: MutableMapping[str, MutableSequence[MessageMiddlewareDef]] = defaultdict(list)
        for middleware_def in middleware_defs:
            name_to_middleware_defs[middleware_def.name].append(middleware_def)
        for defs in name_to_middleware_defs.values():
            if len(defs) > 1:
                raise DuplicateMessageMiddlewareDefError(defs)

    def _prepare_command(
        self,
        command_def: CommandDef[Any],
        middleware_defs: Sequence[MessageMiddlewareDef],
    ) -> _PreparedCommand:
        if command_def.middleware_defs is not None:
            self._check_middleware_defs(command_def.middleware_defs)
            middleware_defs = command_def.middleware_defs
        guards = self._guard_container.prepare(
            message_type=command_def.message_type,
            functions=(*(middleware_def.middleware for middleware_def in middleware_defs), command_def.handler),
        )
        return _PreparedCommand(
            command_def=command_def,
            guards=tuple(
                _PreparedGuard(step=step, seed=self._make_guard_seed(step.provider.key)) for step in guards.steps
            ),
            middlewares=tuple(
                _PreparedMiddleware(middleware=middleware, seed=f"#middleware:{middleware_def.name}")
                for middleware_def, middleware in zip(middleware_defs, guards.functions[:-1], strict=True)
            ),
            handler=guards.functions[-1],
        )

    @classmethod
    def _make_guard_seed(cls, key: DependencyKey) -> str:
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

        try:
            return (yield from self._execute(prepared_command, command, message_context))
        except _EngineError as exc:
            raise exc.error.with_traceback(exc.__traceback__) from None

    def _execute(
        self,
        prepared_command: _PreparedCommand,
        command: Command[Any],
        message_context: MessageContext,
    ) -> ExecutionPlan[Any]:
        message_id = message_context.message_id
        execution = _Execution(
            message=command,
            message_context=message_context,
            values=make_message_dependencies(command, message_context),
            # The length prefix keeps the seeds of different messages apart whatever their ids are.
            seed=f"{len(message_id)}:{message_id}",
        )
        for guard in prepared_command.guards:
            # Each guard has its own ids, so adding a guard does not shift the ids of the handler effects.
            yield from self._run(
                effectful=guard.step.run(execution.values),
                plan_effect=self._make_plan_effect(execution, GUARD_EFFECT_TYPES, guard.seed),
            )

        run_inner: Callable[[], ExecutionPlan[Any]] = partial(self._run_handler, execution, prepared_command.handler)
        for middleware in reversed(prepared_command.middlewares):
            run_inner = partial(
                self._run_middleware,
                execution=execution,
                middleware=middleware,
                run_inner=run_inner,
            )
        return (yield from run_inner())

    def _run_handler(self, execution: _Execution, handler: GuardedFunction) -> ExecutionPlan[Any]:
        # Effect ids are derived from the message id so that replaying a command reproduces them.
        return (
            yield from self._run(
                effectful=handler.call(execution.values),
                plan_effect=self._make_plan_effect(execution, self._ANY_EFFECT_TYPES, ""),
            )
        )

    def _run_middleware(
        self,
        execution: _Execution,
        middleware: _PreparedMiddleware,
        run_inner: Callable[[], ExecutionPlan[Any]],
    ) -> ExecutionPlan[Any]:
        return (
            yield from self._run(
                effectful=middleware.middleware.call(execution.values),
                plan_effect=partial(
                    self._plan_middleware_effect,
                    plan_effect=self._make_plan_effect(execution, self._ANY_EFFECT_TYPES, middleware.seed),
                    run_inner=run_inner,
                ),
            )
        )

    @classmethod
    def _plan_middleware_effect(
        cls,
        effect: Effect[Any],
        plan_effect: _PlanEffect,
        run_inner: Callable[[], ExecutionPlan[Any]],
    ) -> Iterable[Instruction[Any]]:
        if isinstance(effect, Proceed):
            return run_inner()
        return plan_effect(effect)

    def _get_command(self, command_type: type[Command[Any]]) -> _PreparedCommand:
        prepared_command = self._type_to_command.get(command_type)
        if prepared_command is None:
            raise CommandNotDefinedError(command_type)
        return prepared_command

    @classmethod
    def _run[ResultT](
        cls, effectful: Effectful[ResultT, Effect[Any]], plan_effect: _PlanEffect
    ) -> ExecutionPlan[ResultT]:
        try:
            effect = effectful.send(None)
            while True:
                effect_plan = plan_effect(effect)
                try:
                    effect_result = yield from effect_plan
                except _EngineError:
                    raise
                except Exception as exc:  # noqa: BLE001 - the handler decides how to handle the error
                    effect = effectful.throw(exc)
                else:
                    effect = effectful.send(effect_result)
        except StopIteration as exc:
            return exc.value
        finally:
            effectful.close()

    @classmethod
    def _make_plan_effect(
        cls,
        execution: _Execution,
        allowed_effect_types: tuple[type[Effect[Any]], ...],
        seed: str,
    ) -> _PlanEffect:
        return partial(
            cls._make_instruction,
            allowed_effect_types=allowed_effect_types,
            generate_effect_id=_EffectIdGenerator(execution.seed + seed),
            execution=execution,
        )

    @classmethod
    def _make_instruction(
        cls,
        effect: Effect[Any],
        allowed_effect_types: tuple[type[Effect[Any]], ...],
        generate_effect_id: Callable[[], EffectId],
        execution: _Execution,
    ) -> Instruction[Any]:
        if not isinstance(effect, allowed_effect_types):
            raise _EngineError(UnsupportedEffectError(effect))
        if isinstance(effect, cls._HANDLED_EFFECT_TYPES):
            return CallEffectHandlerInstruction(
                message_context=execution.message_context,
                message=execution.message,
                effect_context=EffectContext(effect_id=generate_effect_id()),
                effect=effect,
            )
        raise _EngineError(UnsupportedEffectError(effect))
