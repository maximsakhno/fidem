from collections.abc import Callable, Collection, Iterable, Mapping, MutableSequence, Sequence
from dataclasses import dataclass, replace
from functools import partial
from random import Random
from typing import Any, Final, Self

from fidem._validation import check_unique
from fidem.di.core import DependencyKey, make_message_dependencies
from fidem.effects import (
    DatabaseEffect,
    DatabaseReadEffect,
    Effect,
    EffectContext,
    Effectful,
    EffectId,
    ExternalEffect,
    ExternalWriteEffect,
    InternalEffect,
)
from fidem.guards import GuardContainer, GuardDef, GuardedFunction, GuardResolutionStep, GuardValues
from fidem.messages import QUERY_EFFECT_TYPES, Command, Event, Message, MessageContext, Query
from fidem.operation_engine.definitions import (
    AnyEventHandlerDef,
    AtomicCommandDef,
    AtomicEventHandlerDef,
    CommandDef,
    EventDef,
    ExecutionMode,
    MessageMiddlewareDef,
    Proceed,
    QueryDef,
    StagedCommandDef,
    StagedEventHandlerDef,
)
from fidem.operation_engine.errors import (
    CommandNotDefinedError,
    DuplicateCommandDefError,
    DuplicateEventDefError,
    DuplicateEventHandlerDefError,
    DuplicateMessageMiddlewareDefError,
    DuplicateQueryDefError,
    EventNotDefinedError,
    QueryNotDefinedError,
    UnsupportedAtomicResultError,
    UnsupportedEffectError,
)
from fidem.operation_engine.instructions import (
    CallEffectHandlerInstruction,
    EffectCall,
    ExecutionPlan,
    Instruction,
    OperationEngine,
    RunTransactionInstruction,
)
from fidem.outbox import OutboxEvent, WriteOutboxEffect
from fidem.transactions import (
    IMPLICIT_READ_TRANSACTION,
    IMPLICIT_WRITE_TRANSACTION,
    AnyTransaction,
    ReadTransactionEffect,
    WriteTransactionEffect,
)

__all__ = [
    "OperationEngineImpl",
]


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


type _AnyMessageDef = CommandDef[Any] | QueryDef[Any] | AnyEventHandlerDef


@dataclass(frozen=True, slots=True)
class _PreparedMessage[MessageDefT: _AnyMessageDef]:
    message_def: MessageDefT
    guards: Sequence[_PreparedGuard]
    """Guards of the middlewares and the handler, each one after its guards."""
    middlewares: Sequence[_PreparedMiddleware]
    """Middlewares from the outer one."""
    handler: GuardedFunction


@dataclass(frozen=True, slots=True)
class _PreparedEvent:
    event_def: EventDef[Any]
    handlers: Sequence[_PreparedMessage[AnyEventHandlerDef]]


@dataclass(frozen=True, slots=True)
class _Execution:
    """One run of a message."""

    message: Message[Any]
    message_context: MessageContext
    values: GuardValues
    seed: str
    declared_events: Mapping[type[Message[Any]], _PreparedEvent]


type _EventBuffer = MutableSequence[OutboxEvent]
"""Events published so far by a transaction or a staged handler."""

_HANDLED_EFFECT_TYPES: Final = (InternalEffect, ExternalEffect, DatabaseEffect)
_ANY_EFFECT_TYPES: Final[tuple[type[Effect[Any]], ...]] = (Effect,)


@dataclass(frozen=True, slots=True)
class _EffectPlanner:
    """Turns each effect of an effectful into the instructions that run it."""

    execution: _Execution
    allowed_effect_types: tuple[type[Effect[Any]], ...]
    generate_effect_id: Callable[[], EffectId]
    events: _EventBuffer | None = None
    """Where the published events go, `None` forbids publishing."""
    in_transaction: bool = False
    proceed: Callable[[], ExecutionPlan[Any]] | None = None
    """Runs the inner part of the middleware chain, `None` outside a middleware."""

    def plan(self, effect: Effect[Any]) -> Iterable[Instruction[Any]]:
        if isinstance(effect, Proceed) and self.proceed is not None:
            return self.proceed()
        if isinstance(effect, Event) and self.events is not None:
            self._publish(effect, self.events)
            # A buffered event needs no instruction, so the effectful gets `None` at once.
            return ()
        if not isinstance(effect, self.allowed_effect_types):
            raise _EngineError(UnsupportedEffectError(effect))
        if isinstance(effect, DatabaseEffect) and not self.in_transaction:
            # A database effect outside a transaction runs as if the effectful called a transaction of this effect.
            return self.plan(_call_in_implicit_transaction(effect))
        if isinstance(effect, _HANDLED_EFFECT_TYPES):
            effect_call = EffectCall(
                message_context=self.execution.message_context,
                message=self.execution.message,
                effect_context=EffectContext(effect_id=self.generate_effect_id()),
                effect=effect,
            )
            return CallEffectHandlerInstruction(call=effect_call)
        if isinstance(effect, ReadTransactionEffect | WriteTransactionEffect):
            return RunTransactionInstruction(
                transaction=effect.transaction,
                plan=_run_transaction(effect, self.for_transaction(effect.transaction)),
            )
        raise _EngineError(UnsupportedEffectError(effect))

    def for_transaction(self, transaction: AnyTransaction) -> Self:
        """Plans the effects of the transaction, their ids continue the ids of the effects around it."""
        return replace(
            self,
            allowed_effect_types=transaction.effect_types,
            # Events of a write transaction go where the events around it go.
            events=None if transaction.read_only else self.events,
            in_transaction=True,
            proceed=None,
        )

    def _publish(self, event: Event, events: _EventBuffer) -> None:
        prepared_event = self.execution.declared_events.get(type(event))
        if prepared_event is None:
            raise _EngineError(EventNotDefinedError(type(event)))
        event_def = prepared_event.event_def
        event_id = self.generate_effect_id()
        if event_def.handler_defs:
            events.append(
                OutboxEvent(
                    event_id=event_id,
                    source_message_id=self.execution.message_context.message_id,
                    name=event_def.name,
                    version=event_def.version,
                    handlers=tuple(handler_def.name for handler_def in event_def.handler_defs),
                    event=event,
                )
            )


def _call_in_implicit_transaction(
    effect: DatabaseEffect[Any],
) -> ReadTransactionEffect[Any] | WriteTransactionEffect[Any]:
    if isinstance(effect, DatabaseReadEffect):
        return IMPLICIT_READ_TRANSACTION(effect)
    return IMPLICIT_WRITE_TRANSACTION(effect)


class OperationEngineImpl(OperationEngine):
    def __init__(
        self,
        command_defs: Collection[CommandDef[Any]] = (),
        query_defs: Collection[QueryDef[Any]] = (),
        event_defs: Collection[EventDef[Any]] = (),
        guard_defs: Collection[GuardDef[Any]] = (),
        middleware_defs: Sequence[MessageMiddlewareDef] = (),
    ) -> None:
        self._check_middleware_defs(middleware_defs)
        type_to_command_def = self._make_type_to_def(command_defs, DuplicateCommandDefError)
        type_to_query_def = self._make_type_to_def(query_defs, DuplicateQueryDefError)
        self._check_event_defs(event_defs)
        self._guard_container: GuardContainer = GuardContainer(
            guard_defs=guard_defs,
            message_types=(
                *type_to_command_def,
                *type_to_query_def,
                *(event_def.event_type for event_def in event_defs),
            ),
        )
        self._type_to_command: Mapping[type[Message[Any]], _PreparedMessage[CommandDef[Any]]] = {
            command_type: self._prepare_message(command_type, command_def, middleware_defs)
            for command_type, command_def in type_to_command_def.items()
        }
        self._type_to_query: Mapping[type[Message[Any]], _PreparedMessage[QueryDef[Any]]] = {
            query_type: self._prepare_message(query_type, query_def, middleware_defs)
            for query_type, query_def in type_to_query_def.items()
        }
        self._type_to_event: Mapping[type[Message[Any]], _PreparedEvent] = {
            event_def.event_type: self._prepare_event(event_def, middleware_defs) for event_def in event_defs
        }

    @staticmethod
    def _make_type_to_def[MessageDefT: CommandDef[Any] | QueryDef[Any]](
        message_defs: Collection[MessageDefT],
        duplicate_error_type: Callable[[Sequence[MessageDefT]], Exception],
    ) -> Mapping[type[Message[Any]], MessageDefT]:
        check_unique(message_defs, lambda message_def: message_def.message_type, duplicate_error_type)
        return {message_def.message_type: message_def for message_def in message_defs}

    @staticmethod
    def _check_middleware_defs(middleware_defs: Collection[MessageMiddlewareDef]) -> None:
        check_unique(middleware_defs, lambda middleware_def: middleware_def.name, DuplicateMessageMiddlewareDefError)

    @staticmethod
    def _check_event_defs(event_defs: Collection[EventDef[Any]]) -> None:
        # The outbox keeps the name of the event, so the name has to find the type when the event is read back.
        check_unique(event_defs, lambda event_def: event_def.event_type, DuplicateEventDefError)
        check_unique(event_defs, lambda event_def: event_def.name, DuplicateEventDefError)
        for event_def in event_defs:
            check_unique(
                defs=event_def.handler_defs,
                get_key=lambda handler_def: handler_def.name,
                duplicate_error_type=DuplicateEventHandlerDefError,
            )

    def _prepare_message[MessageDefT: _AnyMessageDef](
        self,
        message_type: type[Message[Any]],
        message_def: MessageDefT,
        middleware_defs: Sequence[MessageMiddlewareDef],
    ) -> _PreparedMessage[MessageDefT]:
        if message_def.middleware_defs is not None:
            self._check_middleware_defs(message_def.middleware_defs)
            middleware_defs = message_def.middleware_defs
        guards = self._guard_container.prepare(
            message_type=message_type,
            functions=(*(middleware_def.middleware for middleware_def in middleware_defs), message_def.handler),
        )
        return _PreparedMessage(
            message_def=message_def,
            guards=tuple(
                _PreparedGuard(step=step, seed=self._make_guard_seed(step.provider.key)) for step in guards.steps
            ),
            middlewares=tuple(
                _PreparedMiddleware(middleware=middleware, seed=f"#middleware:{middleware_def.name}")
                for middleware_def, middleware in zip(middleware_defs, guards.functions[:-1], strict=True)
            ),
            handler=guards.functions[-1],
        )

    def _prepare_event(
        self,
        event_def: EventDef[Any],
        middleware_defs: Sequence[MessageMiddlewareDef],
    ) -> _PreparedEvent:
        # Handlers are prepared ahead of their execution so that errors in their definitions show up on start.
        return _PreparedEvent(
            event_def=event_def,
            handlers=tuple(
                self._prepare_message(event_def.event_type, handler_def, middleware_defs)
                for handler_def in event_def.handler_defs
            ),
        )

    @staticmethod
    def _make_guard_seed(key: DependencyKey) -> str:
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

    def _execute[MessageDefT: _AnyMessageDef](
        self,
        prepared_message: _PreparedMessage[MessageDefT],
        message: Message[Any],
        message_context: MessageContext,
    ) -> ExecutionPlan[Any]:
        try:
            return (yield from _run_chain(prepared_message, self._make_execution(message, message_context)))
        except _EngineError as exc:
            raise exc.error.with_traceback(exc.__traceback__) from None

    def _make_execution(self, message: Message[Any], message_context: MessageContext) -> _Execution:
        message_id = message_context.message_id
        return _Execution(
            message=message,
            message_context=message_context,
            values=make_message_dependencies(message, message_context),
            # The length prefix keeps the seeds of different messages apart whatever their ids are.
            seed=f"{len(message_id)}:{message_id}",
            declared_events=self._type_to_event,
        )


def _run_chain[MessageDefT: _AnyMessageDef](
    prepared_message: _PreparedMessage[MessageDefT],
    execution: _Execution,
) -> ExecutionPlan[Any]:
    """Runs the guards, then the middlewares from the outer one, then the handler."""
    yield from _run_guards(prepared_message.guards, execution)
    run_inner: Callable[[], ExecutionPlan[Any]] = partial(_run_handler, prepared_message, execution)
    for middleware in reversed(prepared_message.middlewares):
        run_inner = partial(_run_middleware, middleware, execution, run_inner)
    return (yield from run_inner())


def _run_guards(guards: Sequence[_PreparedGuard], execution: _Execution) -> ExecutionPlan[None]:
    for guard in guards:
        # Each guard has its own ids, so adding a guard does not shift the ids of the handler effects.
        planner = _EffectPlanner(
            execution=execution,
            allowed_effect_types=QUERY_EFFECT_TYPES,
            generate_effect_id=_EffectIdGenerator(execution.seed + guard.seed),
        )
        yield from _run(guard.step.run(execution.values), planner)


def _run_middleware(
    middleware: _PreparedMiddleware,
    execution: _Execution,
    run_inner: Callable[[], ExecutionPlan[Any]],
) -> ExecutionPlan[Any]:
    planner = _EffectPlanner(
        execution=execution,
        allowed_effect_types=_ANY_EFFECT_TYPES,
        generate_effect_id=_EffectIdGenerator(execution.seed + middleware.seed),
        proceed=run_inner,
    )
    return (yield from _run(middleware.middleware.call(execution.values), planner))


def _run_handler[MessageDefT: _AnyMessageDef](
    prepared_message: _PreparedMessage[MessageDefT],
    execution: _Execution,
) -> ExecutionPlan[Any]:
    handler = prepared_message.handler
    match prepared_message.message_def:
        case AtomicCommandDef() | AtomicEventHandlerDef():
            return _run_atomic_handler(handler, execution)
        case StagedCommandDef() | StagedEventHandlerDef():
            return _run_staged_handler(handler, execution)
        case QueryDef():
            return _run_query_handler(handler, execution)


def _run_query_handler(handler: GuardedFunction, execution: _Execution) -> ExecutionPlan[Any]:
    # Effect ids are derived from the message id so that replaying a message reproduces them.
    planner = _EffectPlanner(
        execution=execution,
        allowed_effect_types=QUERY_EFFECT_TYPES,
        generate_effect_id=_EffectIdGenerator(execution.seed),
    )
    return (yield from _run(handler.call(execution.values), planner))


def _run_staged_handler(handler: GuardedFunction, execution: _Execution) -> ExecutionPlan[Any]:
    planner = _EffectPlanner(
        execution=execution,
        allowed_effect_types=_ANY_EFFECT_TYPES,
        generate_effect_id=_EffectIdGenerator(execution.seed),
    )
    # Each run buffers its own events, so a failed run publishes nothing.
    return _run_writing_events(lambda planner: _run(handler.call(execution.values), planner), planner)


def _run_atomic_handler(handler: GuardedFunction, execution: _Execution) -> ExecutionPlan[Any]:
    # The write continues the ids of the reads, as if the handler yielded it.
    read_planner = _EffectPlanner(
        execution=execution,
        allowed_effect_types=QUERY_EFFECT_TYPES,
        generate_effect_id=_EffectIdGenerator(execution.seed),
    )
    write = yield from _run(handler.call(execution.values), read_planner)
    if isinstance(write, WriteTransactionEffect):
        # The events of the transaction are written in it.
        transaction_plan = _run_writing_events(
            run=partial(_run_transaction, write),
            planner=read_planner.for_transaction(write.transaction),
        )
        return (yield RunTransactionInstruction(transaction=write.transaction, plan=transaction_plan))
    if isinstance(write, ExternalWriteEffect):
        write_planner = replace(read_planner, allowed_effect_types=_ANY_EFFECT_TYPES)
        return (yield from write_planner.plan(write))
    raise _EngineError(UnsupportedAtomicResultError(write))


def _run_writing_events[ResultT](
    run: Callable[[_EffectPlanner], ExecutionPlan[ResultT]],
    planner: _EffectPlanner,
) -> ExecutionPlan[ResultT]:
    """Runs the plan and then writes the events it published to the outbox."""
    events: _EventBuffer = []
    publishing_planner = replace(planner, events=events)
    result = yield from run(publishing_planner)
    if events:
        yield from publishing_planner.plan(WriteOutboxEffect(events=tuple(events)))
    return result


def _run_transaction(
    effect: ReadTransactionEffect[Any] | WriteTransactionEffect[Any, Any],
    planner: _EffectPlanner,
) -> ExecutionPlan[Any]:
    # Called in the plan so that an error of the call reaches the handler like other errors of the transaction.
    effectful = effect.transaction.function(*effect.args, **effect.kwargs)
    return (yield from _run(effectful, planner))


def _run[ResultT](effectful: Effectful[ResultT, Effect[Any]], planner: _EffectPlanner) -> ExecutionPlan[ResultT]:
    try:
        effect = effectful.send(None)
        while True:
            effect_plan = planner.plan(effect)
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
