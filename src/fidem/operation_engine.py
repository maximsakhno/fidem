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
    ExternalWriteEffect,
    InternalEffect,
)
from fidem.guards import GuardContainer, GuardDef, GuardedFunction, GuardResolutionStep, GuardValues
from fidem.messages import (
    QUERY_EFFECT_TYPES,
    AtomicHandler,
    Command,
    Event,
    Message,
    MessageContext,
    Query,
    QueryHandler,
    StagedHandler,
)
from fidem.transactions import AnyTransaction, ReadTransactionEffect, WriteTransactionEffect

__all__ = [
    "AtomicCommandDef",
    "CallEffectHandlerInstruction",
    "CommandDef",
    "CommandNotDefinedError",
    "DuplicateCommandDefError",
    "DuplicateMessageMiddlewareDefError",
    "DuplicateQueryDefError",
    "ExecutionMode",
    "ExecutionPlan",
    "HandledEffect",
    "Instruction",
    "MessageDef",
    "MessageMiddleware",
    "MessageMiddlewareDef",
    "OperationEngine",
    "OperationEngineImpl",
    "Proceed",
    "QueryDef",
    "QueryNotDefinedError",
    "RunTransactionInstruction",
    "StagedCommandDef",
    "UnsupportedAtomicResultError",
    "UnsupportedEffectError",
]


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
class MessageDef[MessageT: Message[Any], MessageHandlerT: Callable[..., Effectful[Any, Any]]]:
    name: str
    message_type: type[MessageT]
    handler: MessageHandlerT
    middleware_defs: Sequence[MessageMiddlewareDef] | None = None
    """Replace the engine middlewares, `None` keeps them."""


@dataclass(frozen=True, slots=True)
class AtomicCommandDef[ResultT](MessageDef[Command[ResultT], AtomicHandler[ResultT, Event]]):
    execution_mode: ExecutionMode = ExecutionMode.EPHEMERAL


@dataclass(frozen=True, slots=True)
class StagedCommandDef[ResultT](MessageDef[Command[ResultT], StagedHandler[ResultT, Event]]):
    execution_mode: ExecutionMode = ExecutionMode.EPHEMERAL


type CommandDef[ResultT] = StagedCommandDef[ResultT] | AtomicCommandDef[ResultT]


@dataclass(frozen=True, slots=True)
class QueryDef[ResultT](MessageDef[Query[ResultT], QueryHandler[ResultT]]):
    pass


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


class DuplicateCommandDefError(Exception):
    def __init__(self, command_defs: Collection[CommandDef[Any]]) -> None:
        super().__init__(command_defs)


class DuplicateQueryDefError(Exception):
    def __init__(self, query_defs: Collection[QueryDef[Any]]) -> None:
        super().__init__(query_defs)


class DuplicateMessageMiddlewareDefError(Exception):
    def __init__(self, middleware_defs: Collection[MessageMiddlewareDef]) -> None:
        super().__init__(middleware_defs)


class CommandNotDefinedError(Exception):
    def __init__(self, command_type: type[Command[Any]]) -> None:
        super().__init__(command_type)


class QueryNotDefinedError(Exception):
    def __init__(self, query_type: type[Query[Any]]) -> None:
        super().__init__(query_type)


class UnsupportedEffectError(Exception):
    def __init__(self, effect: Effect[Any]) -> None:
        super().__init__(effect)


class UnsupportedAtomicResultError(Exception):
    """An atomic handler returned neither a write transaction effect nor an external write effect."""

    def __init__(self, result: object) -> None:
        super().__init__(result)


class EngineError(Exception):
    """Carries an error of the engine past the handlers and the middlewares to the caller."""

    def __init__(self, error: Exception) -> None:
        super().__init__(error)
        self.error: Exception = error


class EffectIdGenerator:
    """Generates deterministic effect ids and seeds lazily because seeding is costly and many guards yield nothing."""

    def __init__(self, seed: str) -> None:
        self._seed: str = seed
        self._random: Random | None = None

    def __call__(self) -> EffectId:
        if self._random is None:
            self._random = Random(self._seed)  # noqa: S311 - determinism is required, not cryptographic strength
        return self._random.randbytes(16).hex()


@dataclass(frozen=True, slots=True)
class PreparedGuard:
    step: GuardResolutionStep
    seed: str
    """Identifies the guard in the seed of the ids of its effects."""


@dataclass(frozen=True, slots=True)
class PreparedMiddleware:
    middleware: GuardedFunction
    seed: str
    """Identifies the middleware in the seed of the ids of its effects."""


type AnyMessageDef = CommandDef[Any] | QueryDef[Any]


@dataclass(frozen=True, slots=True)
class PreparedMessage[MessageDefT: AnyMessageDef]:
    message_def: MessageDefT
    guards: Sequence[PreparedGuard]
    """Guards of the middlewares and the handler, each one after its guards."""
    middlewares: Sequence[PreparedMiddleware]
    """Middlewares from the outer one."""
    handler: GuardedFunction


@dataclass(frozen=True, slots=True)
class Execution:
    message: Message[Any]
    message_context: MessageContext
    values: GuardValues
    seed: str


type PlanEffect = Callable[[Effect[Any]], Iterable[Instruction[Any]]]


class OperationEngineImpl(OperationEngine):
    _HANDLED_EFFECT_TYPES: Final = (InternalEffect, ExternalEffect, DatabaseEffect)
    _ANY_EFFECT_TYPES: Final[tuple[type[Effect[Any]], ...]] = (Effect,)

    def __init__(
        self,
        command_defs: Collection[CommandDef[Any]] = (),
        query_defs: Collection[QueryDef[Any]] = (),
        guard_defs: Collection[GuardDef[Any]] = (),
        middleware_defs: Sequence[MessageMiddlewareDef] = (),
    ) -> None:
        self._check_middleware_defs(middleware_defs)
        type_to_command_def = self._make_type_to_def(command_defs, DuplicateCommandDefError)
        type_to_query_def = self._make_type_to_def(query_defs, DuplicateQueryDefError)
        self._guard_container: GuardContainer = GuardContainer(
            guard_defs=guard_defs,
            message_types=(*type_to_command_def, *type_to_query_def),
        )
        self._type_to_command: Mapping[type[Message[Any]], PreparedMessage[CommandDef[Any]]] = {
            command_type: self._prepare_message(command_def, middleware_defs)
            for command_type, command_def in type_to_command_def.items()
        }
        self._type_to_query: Mapping[type[Message[Any]], PreparedMessage[QueryDef[Any]]] = {
            query_type: self._prepare_message(query_def, middleware_defs)
            for query_type, query_def in type_to_query_def.items()
        }

    @classmethod
    def _make_type_to_def[MessageDefT: AnyMessageDef](
        cls,
        message_defs: Collection[MessageDefT],
        duplicate_error_type: Callable[[Collection[MessageDefT]], Exception],
    ) -> Mapping[type[Message[Any]], MessageDefT]:
        type_to_message_defs: MutableMapping[type[Message[Any]], MutableSequence[MessageDefT]] = defaultdict(list)
        for message_def in message_defs:
            type_to_message_defs[message_def.message_type].append(message_def)

        type_to_message_def: MutableMapping[type[Message[Any]], MessageDefT] = {}
        for message_type, defs in type_to_message_defs.items():
            if len(defs) > 1:
                raise duplicate_error_type(defs)
            type_to_message_def[message_type] = defs[0]

        return type_to_message_def

    @classmethod
    def _check_middleware_defs(cls, middleware_defs: Collection[MessageMiddlewareDef]) -> None:
        name_to_middleware_defs: MutableMapping[str, MutableSequence[MessageMiddlewareDef]] = defaultdict(list)
        for middleware_def in middleware_defs:
            name_to_middleware_defs[middleware_def.name].append(middleware_def)
        for defs in name_to_middleware_defs.values():
            if len(defs) > 1:
                raise DuplicateMessageMiddlewareDefError(defs)

    def _prepare_message[MessageDefT: AnyMessageDef](
        self,
        message_def: MessageDefT,
        middleware_defs: Sequence[MessageMiddlewareDef],
    ) -> PreparedMessage[MessageDefT]:
        if message_def.middleware_defs is not None:
            self._check_middleware_defs(message_def.middleware_defs)
            middleware_defs = message_def.middleware_defs
        guards = self._guard_container.prepare(
            message_type=message_def.message_type,
            functions=(*(middleware_def.middleware for middleware_def in middleware_defs), message_def.handler),
        )
        return PreparedMessage(
            message_def=message_def,
            guards=tuple(
                PreparedGuard(step=step, seed=self._make_guard_seed(step.provider.key)) for step in guards.steps
            ),
            middlewares=tuple(
                PreparedMiddleware(middleware=middleware, seed=f"#middleware:{middleware_def.name}")
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
        prepared_command = self._type_to_command.get(type(command))
        if prepared_command is None:
            raise CommandNotDefinedError(type(command))
        execution_mode = prepared_command.message_def.execution_mode
        if execution_mode != ExecutionMode.EPHEMERAL:
            raise NotImplementedError(f"Execution mode {execution_mode} is not implemented yet")
        return (yield from self._execute(prepared_command, command, message_context))

    def query[ResultT](
        self,
        query: Query[ResultT],
        message_context: MessageContext,
    ) -> ExecutionPlan[ResultT]:
        prepared_query = self._type_to_query.get(type(query))
        if prepared_query is None:
            raise QueryNotDefinedError(type(query))
        return (yield from self._execute(prepared_query, query, message_context))

    def _execute[MessageDefT: AnyMessageDef](
        self,
        prepared_message: PreparedMessage[MessageDefT],
        message: Message[Any],
        message_context: MessageContext,
    ) -> ExecutionPlan[Any]:
        try:
            return (yield from self._run_chain(prepared_message, message, message_context))
        except EngineError as exc:
            raise exc.error.with_traceback(exc.__traceback__) from None

    def _run_chain[MessageDefT: AnyMessageDef](
        self,
        prepared_message: PreparedMessage[MessageDefT],
        message: Message[Any],
        message_context: MessageContext,
    ) -> ExecutionPlan[Any]:
        message_id = message_context.message_id
        execution = Execution(
            message=message,
            message_context=message_context,
            values=make_message_dependencies(message, message_context),
            # The length prefix keeps the seeds of different messages apart whatever their ids are.
            seed=f"{len(message_id)}:{message_id}",
        )
        for guard in prepared_message.guards:
            # Each guard has its own ids, so adding a guard does not shift the ids of the handler effects.
            yield from self._run(
                effectful=guard.step.run(execution.values),
                plan_effect=self._make_plan_effect(
                    execution=execution,
                    allowed_effect_types=QUERY_EFFECT_TYPES,
                    generate_effect_id=EffectIdGenerator(execution.seed + guard.seed),
                ),
            )

        run_inner: Callable[[], ExecutionPlan[Any]]
        match prepared_message.message_def:
            case AtomicCommandDef():
                run_inner = partial(self._run_atomic_handler, execution=execution, handler=prepared_message.handler)
            case StagedCommandDef():
                run_inner = partial(
                    self._run_handler,
                    execution=execution,
                    handler=prepared_message.handler,
                    allowed_effect_types=self._ANY_EFFECT_TYPES,
                )
            case QueryDef():
                run_inner = partial(
                    self._run_handler,
                    execution=execution,
                    handler=prepared_message.handler,
                    allowed_effect_types=QUERY_EFFECT_TYPES,
                )
        for middleware in reversed(prepared_message.middlewares):
            run_inner = partial(
                self._run_middleware,
                execution=execution,
                middleware=middleware,
                run_inner=run_inner,
            )
        return (yield from run_inner())

    def _run_handler(
        self,
        execution: Execution,
        handler: GuardedFunction,
        allowed_effect_types: tuple[type[Effect[Any]], ...],
    ) -> ExecutionPlan[Any]:
        # Effect ids are derived from the message id so that replaying a message reproduces them.
        return (
            yield from self._run(
                effectful=handler.call(execution.values),
                plan_effect=self._make_plan_effect(
                    execution=execution,
                    allowed_effect_types=allowed_effect_types,
                    generate_effect_id=EffectIdGenerator(execution.seed),
                ),
            )
        )

    def _run_atomic_handler(self, execution: Execution, handler: GuardedFunction) -> ExecutionPlan[Any]:
        # The write continues the ids of the reads, as if the handler yielded it.
        make_plan_effect = partial(
            self._make_plan_effect,
            execution=execution,
            generate_effect_id=EffectIdGenerator(execution.seed),
        )
        write = yield from self._run(
            effectful=handler.call(execution.values),
            plan_effect=make_plan_effect(allowed_effect_types=QUERY_EFFECT_TYPES),
        )
        if not isinstance(write, ExternalWriteEffect | WriteTransactionEffect):
            raise EngineError(UnsupportedAtomicResultError(write))
        return (yield from make_plan_effect(allowed_effect_types=self._ANY_EFFECT_TYPES)(write))

    def _run_middleware(
        self,
        execution: Execution,
        middleware: PreparedMiddleware,
        run_inner: Callable[[], ExecutionPlan[Any]],
    ) -> ExecutionPlan[Any]:
        return (
            yield from self._run(
                effectful=middleware.middleware.call(execution.values),
                plan_effect=partial(
                    self._plan_middleware_effect,
                    plan_effect=self._make_plan_effect(
                        execution=execution,
                        allowed_effect_types=self._ANY_EFFECT_TYPES,
                        generate_effect_id=EffectIdGenerator(execution.seed + middleware.seed),
                    ),
                    run_inner=run_inner,
                ),
            )
        )

    @classmethod
    def _plan_middleware_effect(
        cls,
        effect: Effect[Any],
        plan_effect: PlanEffect,
        run_inner: Callable[[], ExecutionPlan[Any]],
    ) -> Iterable[Instruction[Any]]:
        if isinstance(effect, Proceed):
            return run_inner()
        return plan_effect(effect)

    @classmethod
    def _run[ResultT](
        cls, effectful: Effectful[ResultT, Effect[Any]], plan_effect: PlanEffect
    ) -> ExecutionPlan[ResultT]:
        try:
            effect = effectful.send(None)
            while True:
                effect_plan = plan_effect(effect)
                try:
                    effect_result = yield from effect_plan
                except EngineError:
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
        execution: Execution,
        allowed_effect_types: tuple[type[Effect[Any]], ...],
        generate_effect_id: Callable[[], EffectId],
    ) -> PlanEffect:
        return partial(
            cls._make_instruction,
            allowed_effect_types=allowed_effect_types,
            generate_effect_id=generate_effect_id,
            execution=execution,
        )

    @classmethod
    def _make_instruction(
        cls,
        effect: Effect[Any],
        allowed_effect_types: tuple[type[Effect[Any]], ...],
        generate_effect_id: Callable[[], EffectId],
        execution: Execution,
    ) -> Instruction[Any]:
        if not isinstance(effect, allowed_effect_types):
            raise EngineError(UnsupportedEffectError(effect))
        if isinstance(effect, cls._HANDLED_EFFECT_TYPES):
            return CallEffectHandlerInstruction(
                message_context=execution.message_context,
                message=execution.message,
                effect_context=EffectContext(effect_id=generate_effect_id()),
                effect=effect,
            )
        if isinstance(effect, ReadTransactionEffect | WriteTransactionEffect):
            # The effects of the transaction continue the ids of the effects around it.
            return RunTransactionInstruction(
                transaction=effect.transaction,
                plan=cls._run_transaction(
                    effect=effect,
                    plan_effect=cls._make_plan_effect(
                        execution=execution,
                        allowed_effect_types=effect.transaction.effect_types,
                        generate_effect_id=generate_effect_id,
                    ),
                ),
            )
        raise EngineError(UnsupportedEffectError(effect))

    @classmethod
    def _run_transaction(
        cls,
        effect: ReadTransactionEffect[Any] | WriteTransactionEffect[Any, Any],
        plan_effect: PlanEffect,
    ) -> ExecutionPlan[Any]:
        # Called in the plan so that an error of the call reaches the handler like other errors of the transaction.
        effectful = effect.transaction.function(*effect.args, **effect.kwargs)
        return (yield from cls._run(effectful, plan_effect))
