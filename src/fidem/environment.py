from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Awaitable, Callable, Collection, Mapping, MutableMapping, MutableSequence
from dataclasses import dataclass
from functools import partial
from typing import Any

from fidem.effects import Effect, EffectContext
from fidem.messages import Command, MessageId
from fidem.operation_engine import CallEffectHandlerInstruction, ExecutionPlan, Instruction, OperationEngine


@dataclass(frozen=True, slots=True)
class EffectDef[EffectT: Effect[Any]]:
    effect_type: type[EffectT]
    handler: Callable[[EffectContext, EffectT], Awaitable[object]]


class Environment(ABC):
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


class EnvironmentImpl(Environment):
    def __init__(
        self,
        operation_engine: OperationEngine,
        effect_defs: Collection[EffectDef[Any]] = (),
    ) -> None:
        self._operation_engine: OperationEngine = operation_engine
        self._type_to_effect_def: Mapping[type[Effect[Any]], EffectDef[Any]] = self._make_type_to_effect_def(
            effect_defs
        )

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

    async def execute[ResultT](
        self,
        command: Command[ResultT],
        message_id: MessageId | None = None,
    ) -> ResultT:
        execution_plan = self._operation_engine.execute(command, message_id)
        return await self._run_execution_plan(execution_plan)

    async def _run_execution_plan[ResultT](self, execution_plan: ExecutionPlan[ResultT]) -> ResultT:
        try:
            instruction = execution_plan.send(None)
            while True:
                run_instruction = self._resolve_instruction(instruction)
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

    def _resolve_instruction(self, instruction: Instruction[Any]) -> Callable[[], Awaitable[object]]:
        if isinstance(instruction, CallEffectHandlerInstruction):
            effect_def = self._get_effect_def(type(instruction.effect))
            return partial(effect_def.handler, instruction.context, instruction.effect)
        raise NotImplementedError(instruction)

    def _get_effect_def(self, effect_type: type[Effect[Any]]) -> EffectDef[Any]:
        effect_def = self._type_to_effect_def.get(effect_type)
        if effect_def is None:
            raise EffectNotDefinedError(effect_type)
        return effect_def
