from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from unittest.mock import Mock

from fidem.effects import EffectContext
from fidem.messages import MessageContext
from fidem.operation_engine.instructions import (
    CallEffectHandlerInstruction,
    EffectCall,
    ExecutionPlan,
    HandledEffect,
    OperationEngine,
)
from tests.effects import TestInternalEffect
from tests.messages import TestCommand


class Resource:
    pass


@dataclass(frozen=True, slots=True)
class Config:
    value: str


def make_operation_engine_mock(
    execution_plan: Callable[..., ExecutionPlan[Any]] | None = None,
) -> Mock:
    operation_engine_mock = Mock(spec=OperationEngine)
    operation_engine_mock.handle.side_effect = execution_plan
    return operation_engine_mock


def make_instruction(
    effect: HandledEffect[str] | None = None,
    effect_id: str = "test_effect_id",
) -> CallEffectHandlerInstruction[str]:
    effect_call = EffectCall(
        message_context=MessageContext(message_id="test_message_id"),
        message=TestCommand(),
        effect_context=EffectContext(effect_id=effect_id),
        effect=TestInternalEffect() if effect is None else effect,
    )
    return CallEffectHandlerInstruction(call=effect_call)


def make_failing_handler(error: Exception) -> Callable[..., Any]:
    async def handler(effect: TestInternalEffect) -> str:
        raise error

    return handler
