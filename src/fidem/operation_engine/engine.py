from collections.abc import Callable, Collection, Iterable, Mapping, MutableSequence, Sequence
from dataclasses import dataclass
from functools import partial
from random import Random
from typing import Any, Final

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
    ExecutionPlan,
    Instruction,
    OperationEngine,
    RunTransactionInstruction,
)
from fidem.outbox import OutboxEvent, WriteOutboxEffect
from fidem.transactions import (
    IMPLICIT_READ_TRANSACTION,
    IMPLICIT_WRITE_TRANSACTION,
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
    message: Message[Any]
    message_context: MessageContext
    values: GuardValues
    seed: str


type _PlanEffect = Callable[[Effect[Any]], Iterable[Instruction[Any]]]
type _EventBuffer = MutableSequence[OutboxEvent]
"""Events published so far by a transaction or a staged handler."""


class OperationEngineImpl(OperationEngine):
    _HANDLED_EFFECT_TYPES: Final = (InternalEffect, ExternalEffect, DatabaseEffect)
    _ANY_EFFECT_TYPES: Final[tuple[type[Effect[Any]], ...]] = (Effect,)

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
            raise NotImplementedError(f"_Execution mode {execution_mode} is not implemented yet")
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
            return (yield from self._run_chain(prepared_message, message, message_context))
        except _EngineError as exc:
            raise exc.error.with_traceback(exc.__traceback__) from None

    def _run_chain[MessageDefT: _AnyMessageDef](
        self,
        prepared_message: _PreparedMessage[MessageDefT],
        message: Message[Any],
        message_context: MessageContext,
    ) -> ExecutionPlan[Any]:
        message_id = message_context.message_id
        execution = _Execution(
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
                    generate_effect_id=_EffectIdGenerator(execution.seed + guard.seed),
                ),
            )

        run_inner: Callable[[], ExecutionPlan[Any]]
        match prepared_message.message_def:
            case AtomicCommandDef() | AtomicEventHandlerDef():
                run_inner = partial(self._run_atomic_handler, execution=execution, handler=prepared_message.handler)
            case StagedCommandDef() | StagedEventHandlerDef():
                run_inner = partial(self._run_staged_handler, execution=execution, handler=prepared_message.handler)
            case QueryDef():
                run_inner = partial(self._run_query_handler, execution=execution, handler=prepared_message.handler)
        for middleware in reversed(prepared_message.middlewares):
            run_inner = partial(
                self._run_middleware,
                execution=execution,
                middleware=middleware,
                run_inner=run_inner,
            )
        return (yield from run_inner())

    def _run_query_handler(self, execution: _Execution, handler: GuardedFunction) -> ExecutionPlan[Any]:
        # Effect ids are derived from the message id so that replaying a message reproduces them.
        return (
            yield from self._run(
                effectful=handler.call(execution.values),
                plan_effect=self._make_plan_effect(
                    execution=execution,
                    allowed_effect_types=QUERY_EFFECT_TYPES,
                    generate_effect_id=_EffectIdGenerator(execution.seed),
                ),
            )
        )

    def _run_staged_handler(self, execution: _Execution, handler: GuardedFunction) -> ExecutionPlan[Any]:
        generate_effect_id = _EffectIdGenerator(execution.seed)
        # Each run buffers its own events, so a failed run publishes nothing.
        return self._run_writing_events(
            run=lambda plan_effect: self._run(handler.call(execution.values), plan_effect),
            make_plan_effect=lambda events: self._make_plan_effect(
                execution=execution,
                allowed_effect_types=self._ANY_EFFECT_TYPES,
                generate_effect_id=generate_effect_id,
                events=events,
            ),
        )

    def _run_atomic_handler(self, execution: _Execution, handler: GuardedFunction) -> ExecutionPlan[Any]:
        # The write continues the ids of the reads, as if the handler yielded it.
        generate_effect_id = _EffectIdGenerator(execution.seed)
        write = yield from self._run(
            effectful=handler.call(execution.values),
            plan_effect=self._make_plan_effect(
                execution=execution,
                allowed_effect_types=QUERY_EFFECT_TYPES,
                generate_effect_id=generate_effect_id,
            ),
        )
        if isinstance(write, WriteTransactionEffect):
            # The events of the transaction are written in it.
            transaction_plan = self._run_writing_events(
                run=lambda plan_effect: self._run_transaction(write, plan_effect),
                make_plan_effect=lambda events: self._make_plan_effect(
                    execution=execution,
                    allowed_effect_types=write.transaction.effect_types,
                    generate_effect_id=generate_effect_id,
                    events=events,
                    in_transaction=True,
                ),
            )
            return (yield RunTransactionInstruction(transaction=write.transaction, plan=transaction_plan))
        if isinstance(write, ExternalWriteEffect):
            plan_effect = self._make_plan_effect(
                execution=execution,
                allowed_effect_types=self._ANY_EFFECT_TYPES,
                generate_effect_id=generate_effect_id,
            )
            return (yield from plan_effect(write))
        raise _EngineError(UnsupportedAtomicResultError(write))

    @staticmethod
    def _run_writing_events[ResultT](
        run: Callable[[_PlanEffect], ExecutionPlan[ResultT]],
        make_plan_effect: Callable[[_EventBuffer], _PlanEffect],
    ) -> ExecutionPlan[ResultT]:
        """Runs the plan and then writes the events it published to the outbox."""
        events: _EventBuffer = []
        plan_effect = make_plan_effect(events)
        result = yield from run(plan_effect)
        if events:
            yield from plan_effect(WriteOutboxEffect(events=tuple(events)))
        return result

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
                    plan_effect=self._make_plan_effect(
                        execution=execution,
                        allowed_effect_types=self._ANY_EFFECT_TYPES,
                        generate_effect_id=_EffectIdGenerator(execution.seed + middleware.seed),
                    ),
                    run_inner=run_inner,
                ),
            )
        )

    @staticmethod
    def _plan_middleware_effect(
        effect: Effect[Any],
        plan_effect: _PlanEffect,
        run_inner: Callable[[], ExecutionPlan[Any]],
    ) -> Iterable[Instruction[Any]]:
        if isinstance(effect, Proceed):
            return run_inner()
        return plan_effect(effect)

    @staticmethod
    def _run[ResultT](effectful: Effectful[ResultT, Effect[Any]], plan_effect: _PlanEffect) -> ExecutionPlan[ResultT]:
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

    def _make_plan_effect(
        self,
        execution: _Execution,
        allowed_effect_types: tuple[type[Effect[Any]], ...],
        generate_effect_id: Callable[[], EffectId],
        events: _EventBuffer | None = None,
        *,
        in_transaction: bool = False,
    ) -> _PlanEffect:
        """`events` is where the published events go, `None` forbids publishing."""
        return partial(
            self._make_instruction,
            allowed_effect_types=allowed_effect_types,
            generate_effect_id=generate_effect_id,
            execution=execution,
            events=events,
            in_transaction=in_transaction,
        )

    def _make_instruction(
        self,
        effect: Effect[Any],
        allowed_effect_types: tuple[type[Effect[Any]], ...],
        generate_effect_id: Callable[[], EffectId],
        execution: _Execution,
        events: _EventBuffer | None,
        in_transaction: bool,
    ) -> Iterable[Instruction[Any]]:
        if isinstance(effect, Event) and events is not None:
            self._publish(
                event=effect,
                execution=execution,
                generate_effect_id=generate_effect_id,
                events=events,
            )
            # A buffered event needs no instruction, so the effectful gets `None` at once.
            return ()
        if not isinstance(effect, allowed_effect_types):
            raise _EngineError(UnsupportedEffectError(effect))
        if isinstance(effect, DatabaseEffect) and not in_transaction:
            # A database effect outside a transaction runs as if the effectful called a transaction of this effect.
            return self._make_instruction(
                effect=self._call_in_implicit_transaction(effect),
                allowed_effect_types=allowed_effect_types,
                generate_effect_id=generate_effect_id,
                execution=execution,
                events=events,
                in_transaction=in_transaction,
            )
        if isinstance(effect, self._HANDLED_EFFECT_TYPES):
            return CallEffectHandlerInstruction(
                message_context=execution.message_context,
                message=execution.message,
                effect_context=EffectContext(effect_id=generate_effect_id()),
                effect=effect,
            )
        if isinstance(effect, ReadTransactionEffect | WriteTransactionEffect):
            # The effects of the transaction continue the ids of the effects around it.
            plan_effect = self._make_plan_effect(
                execution=execution,
                allowed_effect_types=effect.transaction.effect_types,
                generate_effect_id=generate_effect_id,
                # Events of a write transaction go where the events around it go.
                events=None if effect.transaction.read_only else events,
                in_transaction=True,
            )
            return RunTransactionInstruction(
                transaction=effect.transaction,
                plan=self._run_transaction(effect, plan_effect),
            )
        raise _EngineError(UnsupportedEffectError(effect))

    @staticmethod
    def _call_in_implicit_transaction(
        effect: DatabaseEffect[Any],
    ) -> ReadTransactionEffect[Any] | WriteTransactionEffect[Any]:
        if isinstance(effect, DatabaseReadEffect):
            return IMPLICIT_READ_TRANSACTION(effect)
        return IMPLICIT_WRITE_TRANSACTION(effect)

    def _publish(
        self,
        event: Event,
        execution: _Execution,
        generate_effect_id: Callable[[], EffectId],
        events: _EventBuffer,
    ) -> None:
        prepared_event = self._type_to_event.get(type(event))
        if prepared_event is None:
            raise _EngineError(EventNotDefinedError(type(event)))
        event_def = prepared_event.event_def
        event_id = generate_effect_id()
        if event_def.handler_defs:
            events.append(
                OutboxEvent(
                    event_id=event_id,
                    source_message_id=execution.message_context.message_id,
                    name=event_def.name,
                    version=event_def.version,
                    handlers=tuple(handler_def.name for handler_def in event_def.handler_defs),
                    event=event,
                )
            )

    @classmethod
    def _run_transaction(
        cls,
        effect: ReadTransactionEffect[Any] | WriteTransactionEffect[Any, Any],
        plan_effect: _PlanEffect,
    ) -> ExecutionPlan[Any]:
        # Called in the plan so that an error of the call reaches the handler like other errors of the transaction.
        effectful = effect.transaction.function(*effect.args, **effect.kwargs)
        return (yield from cls._run(effectful, plan_effect))
