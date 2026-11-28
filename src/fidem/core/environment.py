from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Collection, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from functools import partial
from inspect import iscoroutinefunction
from types import TracebackType
from typing import Any, ClassVar, Final, Self
from uuid import uuid4

from fidem.core._validation import check_unique
from fidem.core.di.core import DependencyKey, UnsupportedFunctionError
from fidem.core.di.effects import (
    EffectDependencyContainer,
    EffectDependencyDef,
    EffectDependencyLifetime,
    PreparedEffectHandler,
    Scope,
    Scopes,
)
from fidem.core.effects import DatabaseEffect, Effect
from fidem.core.errors import Error
from fidem.core.messages import Command, Event, EventHandlerName, Message, MessageContext, MessageId, Query
from fidem.core.operation_engine.definitions import EventDef
from fidem.core.operation_engine.instructions import (
    CallEffectHandlerInstruction,
    EffectCall,
    ExecutionPlan,
    Instruction,
    OperationEngine,
    RunTransactionInstruction,
)
from fidem.core.transactions import AnyTransaction, TransactionContext

__all__ = [
    "CallNext",
    "DuplicateEffectDefError",
    "EffectDef",
    "EffectMiddleware",
    "EffectMiddlewareDef",
    "EffectNotDefinedError",
    "Environment",
    "EnvironmentAlreadyStartedError",
    "EnvironmentImpl",
    "EnvironmentNotStartedError",
]


type CallNext = Callable[[], Awaitable[object]]
type EffectMiddleware = Callable[[EffectCall, CallNext], Awaitable[object]]


@dataclass(frozen=True, slots=True)
class EffectMiddlewareDef:
    middleware: EffectMiddleware


@dataclass(frozen=True, slots=True)
class EffectDef[EffectT: Effect[Any]]:
    effect_type: type[EffectT]
    handler: Callable[..., Awaitable[object]]
    middleware_defs: Sequence[EffectMiddlewareDef] | None = None
    """Replace the environment middlewares, `None` keeps them."""


class Environment(AbstractAsyncContextManager["Environment", None], ABC):
    @property
    @abstractmethod
    def event_defs(self) -> Collection[EventDef[Any]]:
        """Declared events, by which a delivery finds the type of its event and its handlers."""
        raise NotImplementedError

    @abstractmethod
    async def handle[ResultT](
        self,
        message: Command[ResultT] | Query[ResultT],
        *inputs: object,
        message_id: MessageId | None = None,
    ) -> ResultT:
        """Runs the operation of the command or the query and returns its result, the guards get the inputs."""
        raise NotImplementedError

    @abstractmethod
    async def deliver(self, event: Event, handler: EventHandlerName, message_id: MessageId) -> None:
        """Runs the operation of one handler of the event, the message id identifies the delivery."""
        raise NotImplementedError


_ENVIRONMENT_KEY: Final = DependencyKey(name="environment", type=Environment)
"""The builtin dependency of services, through which they run messages."""


class DuplicateEffectDefError(Error):
    def __init__(self, effect_defs: Collection[EffectDef[Any]]) -> None:
        super().__init__(effect_defs)


class EffectNotDefinedError(Error):
    def __init__(self, effect_type: type[Effect[Any]]) -> None:
        super().__init__(effect_type)


class EnvironmentNotStartedError(Error):
    pass


class EnvironmentAlreadyStartedError(Error):
    pass


@dataclass(frozen=True, slots=True)
class _PreparedEffect:
    handler: PreparedEffectHandler
    middlewares: Sequence[EffectMiddleware]


class EnvironmentImpl(Environment):
    _NO_TRANSACTION_SCOPE: ClassVar[Scope] = Scope({})

    def __init__(
        self,
        operation_engine: OperationEngine,
        effect_defs: Collection[EffectDef[Any]] = (),
        dependency_defs: Collection[EffectDependencyDef[Any]] = (),
        effect_middleware_defs: Sequence[EffectMiddlewareDef] = (),
    ) -> None:
        self._operation_engine: OperationEngine = operation_engine
        type_to_effect_def = self._make_type_to_effect_def(effect_defs)
        self._dependency_container: EffectDependencyContainer = EffectDependencyContainer(
            dependency_defs=dependency_defs,
            effect_types=tuple(type_to_effect_def),
            service_dependencies={_ENVIRONMENT_KEY: self},
        )
        self._type_to_effect: Mapping[type[Effect[Any]], _PreparedEffect] = {
            effect_type: self._prepare_effect(effect_def, effect_middleware_defs)
            for effect_type, effect_def in type_to_effect_def.items()
        }
        self._application_scope: Scope | None = None
        # Replaced on start, closing it before is a no-op.
        self._service_scope: Scope = Scope({})

    @staticmethod
    def _make_type_to_effect_def(
        effect_defs: Collection[EffectDef[Any]],
    ) -> Mapping[type[Effect[Any]], EffectDef[Any]]:
        check_unique(effect_defs, lambda effect_def: effect_def.effect_type, DuplicateEffectDefError)
        return {effect_def.effect_type: effect_def for effect_def in effect_defs}

    def _prepare_effect(
        self,
        effect_def: EffectDef[Any],
        effect_middleware_defs: Sequence[EffectMiddlewareDef],
    ) -> _PreparedEffect:
        middleware_defs = effect_middleware_defs if effect_def.middleware_defs is None else effect_def.middleware_defs
        for middleware_def in middleware_defs:
            if not iscoroutinefunction(middleware_def.middleware):
                raise UnsupportedFunctionError(middleware_def.middleware)
        return _PreparedEffect(
            handler=self._dependency_container.prepare_effect_handler(
                effect_type=effect_def.effect_type,
                handler=effect_def.handler,
                in_transaction=issubclass(effect_def.effect_type, DatabaseEffect),
            ),
            middlewares=tuple(middleware_def.middleware for middleware_def in middleware_defs),
        )

    @property
    def event_defs(self) -> Collection[EventDef[Any]]:
        return self._operation_engine.event_defs

    async def __aenter__(self) -> Self:
        if self._application_scope is not None:
            raise EnvironmentAlreadyStartedError
        application_scope = await self._dependency_container.start()
        # Services run messages while they start and stop, so the application scope is set around them.
        self._application_scope = application_scope
        try:
            self._service_scope = await self._dependency_container.start_services(application_scope)
        except BaseException as exc:
            self._application_scope = None
            await self._dependency_container.stop(application_scope, exc)
            raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        application_scope = self._get_application_scope()
        try:
            await self._service_scope.close(exc)
        finally:
            self._application_scope = None
            await self._dependency_container.stop(application_scope, exc)

    def _get_application_scope(self) -> Scope:
        if self._application_scope is None:
            raise EnvironmentNotStartedError
        return self._application_scope

    async def handle[ResultT](
        self,
        message: Command[ResultT] | Query[ResultT],
        *inputs: object,
        message_id: MessageId | None = None,
    ) -> ResultT:
        message_context = self._make_message_context(message_id)
        return await self._run_message(
            message=message,
            message_context=message_context,
            execution_plan=self._operation_engine.handle(
                message=message,
                message_context=message_context,
                inputs=inputs,
            ),
        )

    @staticmethod
    def _make_message_context(message_id: MessageId | None) -> MessageContext:
        return MessageContext(message_id=uuid4().hex if message_id is None else message_id)

    async def deliver(self, event: Event, handler: EventHandlerName, message_id: MessageId) -> None:
        message_context = MessageContext(message_id=message_id)
        await self._run_message(
            message=event,
            message_context=message_context,
            execution_plan=self._operation_engine.deliver(
                event=event,
                handler=handler,
                message_context=message_context,
            ),
        )

    async def _run_message[ResultT](
        self,
        message: Message[ResultT],
        message_context: MessageContext,
        execution_plan: ExecutionPlan[ResultT],
    ) -> ResultT:
        application_scope = self._get_application_scope()
        async with self._dependency_container.make_message_scope(message, message_context) as message_scope:
            scopes = {
                EffectDependencyLifetime.APPLICATION: application_scope,
                EffectDependencyLifetime.MESSAGE: message_scope,
            }
            return await self._run_execution_plan(execution_plan, scopes)

    async def _run_execution_plan[ResultT](
        self,
        execution_plan: ExecutionPlan[ResultT],
        scopes: Scopes,
        transaction_scope: Scope | None = None,
    ) -> ResultT:
        try:
            instruction = execution_plan.send(None)
            while True:
                run_instruction = self._resolve_instruction(instruction, scopes, transaction_scope)
                try:
                    instruction_result = await run_instruction()
                except Exception as exc:  # noqa: BLE001 - the execution plan decides how to handle the error
                    instruction = execution_plan.throw(exc)
                else:
                    instruction = execution_plan.send(instruction_result)
        except StopIteration as exc:
            return exc.value
        finally:
            execution_plan.close()

    def _resolve_instruction(
        self,
        instruction: Instruction[Any],
        scopes: Scopes,
        transaction_scope: Scope | None,
    ) -> CallNext:
        match instruction:
            case CallEffectHandlerInstruction(call=effect_call):
                return self._make_effect_chain(effect_call, scopes, transaction_scope)
            # A nested transaction would silently take another connection instead of joining the transaction.
            case RunTransactionInstruction(transaction=transaction, plan=plan) if transaction_scope is None:
                return partial(self._run_transaction, transaction=transaction, plan=plan, scopes=scopes)
            case _:
                raise NotImplementedError(instruction)

    def _make_effect_chain(self, effect_call: EffectCall, scopes: Scopes, transaction_scope: Scope | None) -> CallNext:
        """Makes the call of the outer middleware of the effect, the last one calls the handler."""
        prepared_effect = self._get_effect(type(effect_call.effect))
        call_next: CallNext = partial(
            self._call_effect_handler,
            prepared_effect=prepared_effect,
            effect_call=effect_call,
            scopes=scopes,
            transaction_scope=transaction_scope,
        )
        for middleware in reversed(prepared_effect.middlewares):
            call_next = partial(middleware, effect_call, call_next)
        return call_next

    def _get_effect(self, effect_type: type[Effect[Any]]) -> _PreparedEffect:
        prepared_effect = self._type_to_effect.get(effect_type)
        if prepared_effect is None:
            raise EffectNotDefinedError(effect_type)
        return prepared_effect

    async def _run_transaction(
        self,
        transaction: AnyTransaction,
        plan: ExecutionPlan[Any],
        scopes: Scopes,
    ) -> object:
        transaction_context = TransactionContext(transaction=transaction)
        async with self._dependency_container.make_transaction_scope(transaction_context) as transaction_scope:
            return await self._run_execution_plan(plan, scopes, transaction_scope)

    async def _call_effect_handler(
        self,
        prepared_effect: _PreparedEffect,
        effect_call: EffectCall,
        scopes: Scopes,
        transaction_scope: Scope | None,
    ) -> object:
        effect_scope = self._dependency_container.make_effect_scope(effect_call.effect, effect_call.effect_context)
        async with effect_scope:
            effect_scopes = {
                **scopes,
                EffectDependencyLifetime.TRANSACTION: (
                    self._NO_TRANSACTION_SCOPE if transaction_scope is None else transaction_scope
                ),
                EffectDependencyLifetime.EFFECT: effect_scope,
            }
            await prepared_effect.handler.resolve(effect_scopes)
            return await prepared_effect.handler.call(effect_scopes)
