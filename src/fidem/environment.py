from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Awaitable, Callable, Collection, Mapping, MutableMapping, MutableSequence, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from functools import partial
from inspect import iscoroutinefunction
from types import TracebackType
from typing import Any, Self
from uuid import uuid4

from fidem.di.core import Scope, UnsupportedFunctionError
from fidem.di.effects import (
    ClosableScope,
    ClosableScopes,
    EffectDependencyContainer,
    EffectDependencyDef,
    PreparedEffectHandler,
)
from fidem.effects import Effect, EffectContext
from fidem.messages import Command, Message, MessageContext, MessageId
from fidem.operation_engine import CallEffectHandlerInstruction, ExecutionPlan, Instruction, OperationEngine


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
class _ApplicationScopes:
    effects: ClosableScope
    messages: Scope


@dataclass(frozen=True, slots=True)
class _PreparedEffect:
    handler: PreparedEffectHandler
    middlewares: Sequence[EffectMiddleware]


class EnvironmentImpl(Environment):
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
        self._application_scopes: _ApplicationScopes | None = None

    @classmethod
    def _make_type_to_effect_def(
        cls,
        effect_defs: Collection[EffectDef[Any]],
    ) -> Mapping[type[Effect[Any]], EffectDef[Any]]:
        type_to_effect_defs: MutableMapping[type[Effect[Any]], MutableSequence[EffectDef[Any]]] = defaultdict(list)
        for effect_def in effect_defs:
            type_to_effect_defs[effect_def.effect_type].append(effect_def)

        type_to_effect_def: MutableMapping[type[Effect[Any]], EffectDef[Any]] = {}
        for effect_type, defs in type_to_effect_defs.items():
            if len(defs) > 1:
                raise DuplicateEffectDefError(defs)
            type_to_effect_def[effect_type] = defs[0]

        return type_to_effect_def

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
            handler=self._dependency_container.prepare_effect_handler(effect_def.effect_type, effect_def.handler),
            middlewares=tuple(middleware_def.middleware for middleware_def in middleware_defs),
        )

    async def __aenter__(self) -> Self:
        if self._application_scopes is not None:
            raise EnvironmentAlreadyStartedError
        messages_scope = self._operation_engine.start()
        effects_scope = await self._dependency_container.start()
        self._application_scopes = _ApplicationScopes(effects=effects_scope, messages=messages_scope)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        application_scopes = self._get_application_scopes()
        self._application_scopes = None
        await self._dependency_container.stop(application_scopes.effects, exc)

    def _get_application_scopes(self) -> _ApplicationScopes:
        if self._application_scopes is None:
            raise EnvironmentNotStartedError
        return self._application_scopes

    async def execute[ResultT](
        self,
        command: Command[ResultT],
        message_id: MessageId | None = None,
    ) -> ResultT:
        application_scopes = self._get_application_scopes()
        if message_id is None:
            message_id = uuid4().hex
        message_context = MessageContext(message_id=message_id)
        execution_plan = self._operation_engine.execute(
            command=command,
            message_context=message_context,
            application_scope=application_scopes.messages,
        )
        async with self._dependency_container.make_message_scope(command, message_context) as message_scope:
            return await self._run_execution_plan(execution_plan, (application_scopes.effects, message_scope))

    async def _run_execution_plan[ResultT](
        self,
        execution_plan: ExecutionPlan[ResultT],
        scopes: ClosableScopes,
    ) -> ResultT:
        try:
            instruction = execution_plan.send(None)
            while True:
                run_instruction = self._resolve_instruction(instruction, scopes)
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

    def _resolve_instruction(self, instruction: Instruction[Any], scopes: ClosableScopes) -> CallNext:
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
                effect_handler=prepared_effect.handler,
                effect_call=effect_call,
                scopes=scopes,
            )
            for middleware in reversed(prepared_effect.middlewares):
                call_next = partial(middleware, effect_call, call_next)
            return call_next
        raise NotImplementedError(instruction)

    def _get_effect(self, effect_type: type[Effect[Any]]) -> _PreparedEffect:
        prepared_effect = self._type_to_effect.get(effect_type)
        if prepared_effect is None:
            raise EffectNotDefinedError(effect_type)
        return prepared_effect

    async def _call_effect_handler(
        self,
        effect_handler: PreparedEffectHandler,
        effect_call: EffectCall,
        scopes: ClosableScopes,
    ) -> object:
        effect_scope = self._dependency_container.make_effect_scope(effect_call.effect, effect_call.effect_context)
        async with effect_scope:
            effect_scopes = (*scopes, effect_scope)
            await effect_handler.resolve(effect_scopes)
            return await effect_handler.call(effect_scopes)
