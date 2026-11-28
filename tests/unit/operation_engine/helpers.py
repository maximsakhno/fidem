from collections.abc import Callable, Collection, MutableSequence, Sequence
from typing import Any
from unittest.mock import ANY

from fidem.effects import Effect, EffectContext, Effectful
from fidem.guards import GuardDef, GuardInputDef
from fidem.messages import Message, MessageContext, StagedHandler
from fidem.operation_engine.definitions import (
    AtomicCommandDef,
    EventDef,
    MessageMiddlewareDef,
    QueryDef,
    StagedCommandDef,
)
from fidem.operation_engine.engine import OperationEngineImpl
from fidem.operation_engine.instructions import (
    CallEffectHandlerInstruction,
    EffectCall,
    ExecutionPlan,
    HandledEffect,
    Instruction,
    RunTransactionInstruction,
)
from fidem.testing import YieldStep, assert_generator
from fidem.transactions import AnyTransaction
from tests.effects import TestDatabaseWriteEffect, TestExternalReadEffect, TestInternalEffect
from tests.messages import TestCommand, TestQuery


class Service:
    pass


class OtherService:
    pass


def collect_effects(execution_plan: ExecutionPlan[Any]) -> Sequence[CallEffectHandlerInstruction[Any]]:
    instructions: MutableSequence[CallEffectHandlerInstruction[Any]] = []
    run_plan(execution_plan, instructions.append)
    return instructions


def make_engine(
    handler: StagedHandler[Any, Any],
    guard_defs: Collection[GuardDef[Any] | GuardInputDef[Any]] = (),
    middleware_defs: Sequence[MessageMiddlewareDef] = (),
    command_middleware_defs: Sequence[MessageMiddlewareDef] | None = None,
    command_def_type: type[StagedCommandDef[Any]] | type[AtomicCommandDef[Any]] = StagedCommandDef,
    event_defs: Collection[EventDef[Any]] = (),
) -> OperationEngineImpl:
    return OperationEngineImpl(
        command_defs=[
            command_def_type(
                name="test",
                message_type=TestCommand,
                handler=handler,  # pyright: ignore[reportArgumentType] - atomic tests also check what the handler type forbids
                middleware_defs=command_middleware_defs,
            ),
        ],
        event_defs=event_defs,
        guard_defs=guard_defs,
        middleware_defs=middleware_defs,
    )


def make_query_engine(
    handler: StagedHandler[Any, Any],
    guard_defs: Collection[GuardDef[Any] | GuardInputDef[Any]] = (),
    middleware_defs: Sequence[MessageMiddlewareDef] = (),
    query_middleware_defs: Sequence[MessageMiddlewareDef] | None = None,
) -> OperationEngineImpl:
    return OperationEngineImpl(
        query_defs=[
            QueryDef(
                name="test",
                message_type=TestQuery,
                handler=handler,  # pyright: ignore[reportArgumentType] - query tests also check what the handler type forbids
                middleware_defs=query_middleware_defs,
            ),
        ],
        guard_defs=guard_defs,
        middleware_defs=middleware_defs,
    )


def handle_without_effects() -> Effectful[str, Effect[Any]]:
    yield from ()
    return "command_result"


def handle_with_three_effects() -> Effectful[
    str, TestInternalEffect | TestExternalReadEffect | TestDatabaseWriteEffect
]:
    yield from TestInternalEffect()
    yield from TestExternalReadEffect()
    yield from TestDatabaseWriteEffect()
    return "command_result"


def collect_effect_ids(execution_plan: ExecutionPlan[Any]) -> Sequence[str]:
    return [instruction.call.effect_context.effect_id for instruction in collect_effects(execution_plan)]


def run_plan(
    execution_plan: ExecutionPlan[Any],
    call_effect_handler: Callable[[CallEffectHandlerInstruction[Any]], object] = lambda instruction: None,
    on_transaction: Callable[[object], object] = lambda transaction: None,
) -> object:
    """Runs the plan like an environment, the functions play the effect handlers and the transactions."""
    try:
        instruction = execution_plan.send(None)
        while True:
            try:
                if isinstance(instruction, RunTransactionInstruction):
                    on_transaction(instruction.transaction)
                    result = run_plan(instruction.plan, call_effect_handler, on_transaction)
                else:
                    assert isinstance(instruction, CallEffectHandlerInstruction)
                    result = call_effect_handler(instruction)
            except Exception as exc:  # noqa: BLE001 - the plan decides how to handle the error
                instruction = execution_plan.throw(exc)
            else:
                instruction = execution_plan.send(result)
    except StopIteration as exc:
        return exc.value


def assert_runs_in_transaction(
    instruction: Instruction[Any],
    transaction: AnyTransaction,
    effect_instruction: CallEffectHandlerInstruction[Any],
    result: object,
) -> None:
    """Checks that the instruction runs only the effect in the transaction and returns the result of the effect."""
    assert isinstance(instruction, RunTransactionInstruction)
    assert instruction.transaction is transaction
    assert_generator(
        generator=instruction.plan,
        steps=[YieldStep(effect_instruction, result)],
        return_value=result,
    )


def make_expected_instruction(effect: HandledEffect[Any], message: Message[Any]) -> CallEffectHandlerInstruction[Any]:
    effect_call = EffectCall(
        message_context=MessageContext(message_id="test_message_id"),
        message=message,
        effect_context=EffectContext(effect_id=ANY),
        effect=effect,
    )
    return CallEffectHandlerInstruction(call=effect_call)


MESSAGE_CONTEXT = MessageContext(message_id="test_message_id")
