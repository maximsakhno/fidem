from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Collection, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from functools import partial
from inspect import iscoroutinefunction
from types import TracebackType
from typing import Any, ClassVar, Self
from uuid import uuid4

from fidem._validation import check_unique
from fidem.di.core import UnsupportedFunctionError
from fidem.di.effects import (
    EffectDependencyContainer,
    EffectDependencyDef,
    PreparedEffectHandler,
    Scope,
    Scopes,
)
from fidem.effects import DatabaseEffect, Effect, EffectContext, ReadEffect
from fidem.messages import Command, Message, MessageContext, MessageId, Query
from fidem.operation_engine import (
    CallEffectHandlerInstruction,
    ExecutionPlan,
    Instruction,
    OperationEngine,
    RunTransactionInstruction,
)
from fidem.transactions import AnyTransaction, TransactionContext

__all__ = [
    "CallNext",
    "DuplicateEffectDefError",
    "EffectCall",
    "EffectDef",
    "EffectMiddleware",
    "EffectMiddlewareDef",
    "EffectNotDefinedError",
    "Environment",
    "EnvironmentAlreadyStartedError",
    "EnvironmentImpl",
    "EnvironmentNotStartedError",
]


@dataclass(frozen=True, slots=True)
class EffectCall:
    message_context: MessageContext
    message: Message[Any]
    effect_context: EffectContext
    effect: Effect[Any]


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
    @abstractmethod
    async def execute[ResultT](
        self,
        command: Command[ResultT],
        message_id: MessageId | None = None,
    ) -> ResultT:
        raise NotImplementedError

    @abstractmethod
    async def query[ResultT](
        self,
        query: Query[ResultT],
        message_id: MessageId | None = None,
    ) -> ResultT:
        raise NotImplementedError


class DuplicateEffectDefError(Exception):
    def __init__(self, effect_defs: Collection[EffectDef[Any]]) -> None:
        super().__init__(effect_defs)


class EffectNotDefinedError(Exception):
    def __init__(self, effect_type: type[Effect[Any]]) -> None:
        super().__init__(effect_type)


class EnvironmentNotStartedError(Exception):
    pass


class EnvironmentAlreadyStartedError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class _PreparedEffect:
    handler: PreparedEffectHandler
    middlewares: Sequence[EffectMiddleware]
    implicit_transaction_context: TransactionContext | None
    """The context of the transaction the effect runs in outside a transaction, `None` for no transaction."""


class EnvironmentImpl(Environment):
    _IMPLICIT_WRITE_TRANSACTION_CONTEXT: ClassVar[TransactionContext] = TransactionContext(
        read_only=False,
        transaction=None,
    )
    _IMPLICIT_READ_TRANSACTION_CONTEXT: ClassVar[TransactionContext] = TransactionContext(
        read_only=True,
        transaction=None,
    )
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
        )
        self._type_to_effect: Mapping[type[Effect[Any]], _PreparedEffect] = {
            effect_type: self._prepare_effect(effect_def, effect_middleware_defs)
            for effect_type, effect_def in type_to_effect_def.items()
        }
        self._application_scope: Scope | None = None

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
        implicit_transaction_context = self._get_implicit_transaction_context(effect_def.effect_type)
        return _PreparedEffect(
            handler=self._dependency_container.prepare_effect_handler(
                effect_type=effect_def.effect_type,
                handler=effect_def.handler,
                in_transaction=implicit_transaction_context is not None,
            ),
            middlewares=tuple(middleware_def.middleware for middleware_def in middleware_defs),
            implicit_transaction_context=implicit_transaction_context,
        )

    @classmethod
    def _get_implicit_transaction_context(cls, effect_type: type[Effect[Any]]) -> TransactionContext | None:
        if not issubclass(effect_type, DatabaseEffect):
            return None
        if issubclass(effect_type, ReadEffect):
            return cls._IMPLICIT_READ_TRANSACTION_CONTEXT
        return cls._IMPLICIT_WRITE_TRANSACTION_CONTEXT

    async def __aenter__(self) -> Self:
        if self._application_scope is not None:
            raise EnvironmentAlreadyStartedError
        self._application_scope = await self._dependency_container.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        application_scope = self._get_application_scope()
        self._application_scope = None
        await self._dependency_container.stop(application_scope, exc)

    def _get_application_scope(self) -> Scope:
        if self._application_scope is None:
            raise EnvironmentNotStartedError
        return self._application_scope

    async def execute[ResultT](
        self,
        command: Command[ResultT],
        message_id: MessageId | None = None,
    ) -> ResultT:
        message_context = self._make_message_context(message_id)
        return await self._run_message(
            message=command,
            message_context=message_context,
            execution_plan=self._operation_engine.execute(command=command, message_context=message_context),
        )

    async def query[ResultT](
        self,
        query: Query[ResultT],
        message_id: MessageId | None = None,
    ) -> ResultT:
        message_context = self._make_message_context(message_id)
        return await self._run_message(
            message=query,
            message_context=message_context,
            execution_plan=self._operation_engine.query(query=query, message_context=message_context),
        )

    @staticmethod
    def _make_message_context(message_id: MessageId | None) -> MessageContext:
        return MessageContext(message_id=uuid4().hex if message_id is None else message_id)

    async def _run_message[ResultT](
        self,
        message: Message[ResultT],
        message_context: MessageContext,
        execution_plan: ExecutionPlan[ResultT],
    ) -> ResultT:
        application_scope = self._get_application_scope()
        async with self._dependency_container.make_message_scope(message, message_context) as message_scope:
            return await self._run_execution_plan(execution_plan, (application_scope, message_scope))

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
        if isinstance(instruction, CallEffectHandlerInstruction):
            prepared_effect = self._get_effect(type(instruction.effect))
            effect_call = EffectCall(
                message_context=instruction.message_context,
                message=instruction.message,
                effect_context=instruction.effect_context,
                effect=instruction.effect,
            )
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
        # A nested transaction would silently take another connection instead of joining the transaction.
        if isinstance(instruction, RunTransactionInstruction) and transaction_scope is None:
            return partial(
                self._run_transaction,
                transaction=instruction.transaction,
                plan=instruction.plan,
                scopes=scopes,
            )
        raise NotImplementedError(instruction)

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
        transaction_context = TransactionContext(
            read_only=transaction.read_only,
            transaction=transaction,
        )
        async with self._dependency_container.make_transaction_scope(transaction_context) as transaction_scope:
            return await self._run_execution_plan(plan, scopes, transaction_scope)

    async def _call_effect_handler(
        self,
        prepared_effect: _PreparedEffect,
        effect_call: EffectCall,
        scopes: Scopes,
        transaction_scope: Scope | None,
    ) -> object:
        transaction_context = prepared_effect.implicit_transaction_context
        if transaction_scope is not None or transaction_context is None:
            return await self._call_in_effect_scope(
                effect_handler=prepared_effect.handler,
                effect_call=effect_call,
                scopes=scopes,
                transaction_scope=self._NO_TRANSACTION_SCOPE if transaction_scope is None else transaction_scope,
            )
        # Opened here and not by the engine so that each retry of the effect gets a new transaction.
        async with self._dependency_container.make_transaction_scope(transaction_context) as effect_transaction_scope:
            return await self._call_in_effect_scope(
                effect_handler=prepared_effect.handler,
                effect_call=effect_call,
                scopes=scopes,
                transaction_scope=effect_transaction_scope,
            )

    async def _call_in_effect_scope(
        self,
        effect_handler: PreparedEffectHandler,
        effect_call: EffectCall,
        scopes: Scopes,
        transaction_scope: Scope,
    ) -> object:
        effect_scope = self._dependency_container.make_effect_scope(effect_call.effect, effect_call.effect_context)
        async with effect_scope:
            effect_scopes = (*scopes, transaction_scope, effect_scope)
            await effect_handler.resolve(effect_scopes)
            return await effect_handler.call(effect_scopes)
