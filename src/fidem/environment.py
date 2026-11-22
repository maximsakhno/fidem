from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Awaitable, Callable, Collection, Mapping, MutableMapping, MutableSequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from functools import partial
from types import TracebackType
from typing import Any, NamedTuple, Self
from uuid import uuid4

from fidem.di.core import Scope
from fidem.di.effects import (
    ClosableScope,
    EffectDependencyContainer,
    EffectDependencyDef,
    PreparedEffectHandler,
)
from fidem.effects import Effect
from fidem.messages import Command, MessageContext, MessageId
from fidem.operation_engine import CallEffectHandlerInstruction, ExecutionPlan, Instruction, OperationEngine


@dataclass(frozen=True, slots=True)
class EffectDef[EffectT: Effect[Any]]:
    effect_type: type[EffectT]
    handler: Callable[..., Awaitable[object]]


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


class _ApplicationScopes(NamedTuple):
    effects: ClosableScope
    messages: Scope


class EnvironmentImpl(Environment):
    def __init__(
        self,
        operation_engine: OperationEngine,
        effect_defs: Collection[EffectDef[Any]] = (),
        dependency_defs: Collection[EffectDependencyDef[Any]] = (),
    ) -> None:
        self._operation_engine: OperationEngine = operation_engine
        type_to_effect_def = self._make_type_to_effect_def(effect_defs)
        self._dependency_container: EffectDependencyContainer = EffectDependencyContainer(
            dependency_defs=dependency_defs,
            effect_types=tuple(type_to_effect_def),
        )
        self._type_to_effect_handler: Mapping[type[Effect[Any]], PreparedEffectHandler] = {
            effect_type: self._dependency_container.prepare_effect_handler(effect_type, effect_def.handler)
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

    async def __aenter__(self) -> Self:
        if self._application_scopes is not None:
            raise EnvironmentAlreadyStartedError
        effects_scope = await self._dependency_container.start()
        try:
            # Application dependencies of messages are resolved outside any message.
            messages_scope = await self._run_execution_plan(
                execution_plan=self._operation_engine.start(),
                application_scope=effects_scope,
                message_scope=None,
            )
        except BaseException as exc:
            await self._dependency_container.stop(effects_scope, exc)
            raise
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
            message_id=message_id,
            application_scope=application_scopes.messages,
        )
        async with self._dependency_container.make_message_scope(command, message_context) as message_scope:
            return await self._run_execution_plan(execution_plan, application_scopes.effects, message_scope)

    async def _run_execution_plan[ResultT](
        self,
        execution_plan: ExecutionPlan[ResultT],
        application_scope: ClosableScope,
        message_scope: ClosableScope | None,
    ) -> ResultT:
        try:
            instruction = execution_plan.send(None)
            while True:
                run_instruction = self._resolve_instruction(instruction, application_scope, message_scope)
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
        application_scope: ClosableScope,
        message_scope: ClosableScope | None,
    ) -> Callable[[], Awaitable[object]]:
        if isinstance(instruction, CallEffectHandlerInstruction):
            effect_handler = self._get_effect_handler(type(instruction.effect))
            return partial(
                self._call_effect_handler,
                effect_handler=effect_handler,
                instruction=instruction,
                application_scope=application_scope,
                message_scope=message_scope,
            )
        raise NotImplementedError(instruction)

    def _get_effect_handler(self, effect_type: type[Effect[Any]]) -> PreparedEffectHandler:
        effect_handler = self._type_to_effect_handler.get(effect_type)
        if effect_handler is None:
            raise EffectNotDefinedError(effect_type)
        return effect_handler

    async def _call_effect_handler(
        self,
        effect_handler: PreparedEffectHandler,
        instruction: CallEffectHandlerInstruction[Any],
        application_scope: ClosableScope,
        message_scope: ClosableScope | None,
    ) -> object:
        effect_scope = self._dependency_container.make_effect_scope(instruction.effect, instruction.context)
        async with effect_scope:
            scopes = (application_scope, message_scope, effect_scope)
            await effect_handler.resolve(scopes)
            return await effect_handler.call(scopes)
