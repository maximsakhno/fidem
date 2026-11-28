from collections.abc import MutableSequence, Sequence
from typing import Any

import pytest

from fidem.core.di.core import DependencyNotFoundError, UnsupportedFunctionError
from fidem.core.effects import Effect, Effectful
from fidem.core.guards import GuardDef
from fidem.core.messages import MessageContext, QueryEffect
from fidem.core.operation_engine.definitions import (
    AtomicCommandDef,
    MessageMiddlewareDef,
    MessageMiddlewareEffectful,
    Proceed,
    StagedCommandDef,
)
from fidem.core.operation_engine.engine import OperationEngineImpl
from fidem.core.operation_engine.errors import DuplicateMessageMiddlewareDefError, UnsupportedEffectError
from fidem.core.testing import YieldStep, assert_generator
from fidem.core.transactions import WriteTransaction, WriteTransactionEffectful
from tests.effects import TestControlEffect, TestExternalWriteEffect, TestInternalEffect
from tests.ids import type_name
from tests.messages import TestCommand, TestEvent
from tests.stubs import run
from tests.unit.operation_engine.helpers import (
    MESSAGE_CONTEXT,
    Service,
    collect_effect_ids,
    handle_with_three_effects,
    handle_without_effects,
    make_engine,
    make_expected_instruction,
    run_plan,
)


def proceed() -> MessageMiddlewareEffectful[object]:
    return (yield from Proceed())


def proceed_with_three_effects() -> MessageMiddlewareEffectful[object]:
    yield from handle_with_three_effects()
    return (yield from Proceed())


class TestMessageMiddlewares:
    def test_passes_message_context_and_guard_values_into_middleware_and_returns_its_result(self) -> None:
        def service() -> Effectful[Service, QueryEffect]:
            yield from ()
            return Service()

        def middleware(
            context: MessageContext,
            command: TestCommand,
            service: Service,
        ) -> MessageMiddlewareEffectful[str]:
            result = yield from Proceed()
            return f"{context.message_id} {command is message} {type(service).__name__} {result}"

        def handler() -> Effectful[str, Effect[Any]]:
            yield from ()
            return "command_result"

        message = TestCommand()
        engine = make_engine(
            handler=handler,
            guard_defs=[GuardDef(service)],
            middleware_defs=[MessageMiddlewareDef("test", middleware)],
        )

        assert run(engine.handle(message, MESSAGE_CONTEXT)) == "test_message_id True Service command_result"

    def test_yields_call_effect_handler_instruction_when_middleware_yields_write_effect(self) -> None:
        def middleware() -> MessageMiddlewareEffectful[object]:
            result = yield from Proceed()
            yield from effect
            return result

        def handler() -> Effectful[str, Effect[Any]]:
            yield from ()
            return "command_result"

        effect = TestExternalWriteEffect()
        command = TestCommand()
        engine = make_engine(handler, middleware_defs=[MessageMiddlewareDef("test", middleware)])

        assert_generator(
            generator=engine.handle(command, MessageContext(message_id="test_message_id")),
            steps=[YieldStep(make_expected_instruction(effect, command), "effect_result")],
            return_value="command_result",
        )

    def test_runs_guards_then_middlewares_from_outer_one_then_handler(self) -> None:
        def service() -> Effectful[Service, QueryEffect]:
            events.append("guard")
            yield from ()
            return Service()

        def outer() -> MessageMiddlewareEffectful[object]:
            events.append("outer before")
            result = yield from Proceed()
            events.append("outer after")
            return result

        def inner(service: Service) -> MessageMiddlewareEffectful[object]:
            events.append("inner before")
            result = yield from Proceed()
            events.append("inner after")
            return result

        def handler() -> Effectful[str, Effect[Any]]:
            events.append("handler")
            yield from ()
            return "command_result"

        events: MutableSequence[str] = []
        engine = make_engine(
            handler=handler,
            guard_defs=[GuardDef(service)],
            middleware_defs=[MessageMiddlewareDef("outer", outer), MessageMiddlewareDef("inner", inner)],
        )

        assert run(engine.handle(TestCommand(), MESSAGE_CONTEXT)) == "command_result"
        assert events == ["guard", "outer before", "inner before", "handler", "inner after", "outer after"]

    def test_does_not_call_handler_when_middleware_does_not_proceed(self) -> None:
        def middleware() -> MessageMiddlewareEffectful[str]:
            yield from ()
            return "cached_result"

        def handler() -> Effectful[str, Effect[Any]]:
            nonlocal is_handler_called
            is_handler_called = True
            yield from ()
            return "command_result"

        is_handler_called = False
        engine = make_engine(handler, middleware_defs=[MessageMiddlewareDef("test", middleware)])

        assert run(engine.handle(TestCommand(), MESSAGE_CONTEXT)) == "cached_result"
        assert not is_handler_called

    def test_runs_inner_part_anew_with_same_effect_ids_on_each_proceed_and_guards_once(self) -> None:
        def service() -> Effectful[Service, QueryEffect]:
            events.append("guard")
            yield from ()
            return Service()

        def middleware() -> MessageMiddlewareEffectful[object]:
            yield from Proceed()
            return (yield from Proceed())

        def handler(service: Service) -> Effectful[str, Effect[Any]]:
            events.append("handler")
            return (yield from handle_with_three_effects())

        events: MutableSequence[str] = []
        engine = make_engine(
            handler=handler,
            guard_defs=[GuardDef(service)],
            middleware_defs=[MessageMiddlewareDef("test", middleware)],
        )

        effect_ids = collect_effect_ids(engine.handle(TestCommand(), MESSAGE_CONTEXT))

        assert events == ["guard", "handler", "handler"]
        assert effect_ids[:3] == effect_ids[3:]

    def test_does_not_shift_handler_effect_ids_when_middleware_added(self) -> None:
        handler_effect_ids = collect_effect_ids(
            make_engine(handle_with_three_effects).handle(TestCommand(), MESSAGE_CONTEXT)
        )
        engine = make_engine(
            handler=handle_with_three_effects,
            middleware_defs=[MessageMiddlewareDef("test", proceed_with_three_effects)],
        )

        effect_ids = collect_effect_ids(engine.handle(TestCommand(), MESSAGE_CONTEXT))

        assert effect_ids[3:] == handler_effect_ids
        assert set(effect_ids[:3]).isdisjoint(handler_effect_ids)

    def test_generates_different_effect_ids_for_middlewares_with_different_names(self) -> None:
        engine = make_engine(
            handler=handle_with_three_effects,
            middleware_defs=[
                MessageMiddlewareDef("first", proceed_with_three_effects),
                MessageMiddlewareDef("second", proceed_with_three_effects),
            ],
        )

        effect_ids = collect_effect_ids(engine.handle(TestCommand(), MESSAGE_CONTEXT))

        assert len(set(effect_ids)) == 9

    def test_throws_inner_error_into_middleware_at_proceed(self) -> None:
        def middleware() -> MessageMiddlewareEffectful[str]:
            try:
                yield from Proceed()
            except ValueError as exc:
                return f"caught {exc}"
            return "unreachable"

        def handler() -> Effectful[str, Effect[Any]]:
            yield from TestInternalEffect()
            return "unreachable"

        engine = make_engine(handler, middleware_defs=[MessageMiddlewareDef("test", middleware)])
        execution_plan = engine.handle(TestCommand(), MESSAGE_CONTEXT)
        execution_plan.send(None)

        with pytest.raises(StopIteration) as exc_info:
            execution_plan.throw(ValueError("effect_error"))

        assert exc_info.value.value == "caught effect_error"

    def test_raises_unsupported_effect_error_of_inner_part_without_throwing_it_into_middleware(self) -> None:
        def middleware() -> MessageMiddlewareEffectful[str]:
            nonlocal is_middleware_closed
            try:
                yield from Proceed()
            except Exception:  # noqa: BLE001 - checks that no error reaches the middleware
                return "swallowed"
            finally:
                is_middleware_closed = True
            return "unreachable"

        def handler() -> Effectful[str, Effect[Any]]:
            yield from effect
            return "unreachable"

        effect = TestControlEffect()
        is_middleware_closed = False
        engine = make_engine(handler, middleware_defs=[MessageMiddlewareDef("test", middleware)])

        with pytest.raises(UnsupportedEffectError) as exc_info:
            engine.handle(TestCommand(), MESSAGE_CONTEXT).send(None)

        assert exc_info.value.args == (effect,)
        assert is_middleware_closed

    def test_throws_unsupported_effect_error_of_effect_handler_through_handler_into_middleware(self) -> None:
        def middleware() -> MessageMiddlewareEffectful[str]:
            try:
                yield from Proceed()
            except UnsupportedEffectError as exc:
                return f"caught {exc.args == (effect,)}"
            return "unreachable"

        def handler() -> Effectful[str, Effect[Any]]:
            yield from TestInternalEffect()
            return "unreachable"

        effect = TestControlEffect()
        engine = make_engine(handler, middleware_defs=[MessageMiddlewareDef("test", middleware)])
        execution_plan = engine.handle(TestCommand(), MESSAGE_CONTEXT)
        execution_plan.send(None)

        with pytest.raises(StopIteration) as exc_info:
            execution_plan.throw(UnsupportedEffectError(effect))

        assert exc_info.value.value == "caught True"

    def test_raises_unsupported_effect_error_when_transaction_of_middleware_yields_proceed(self) -> None:
        @WriteTransaction
        def proceed_in_transaction() -> WriteTransactionEffectful[object]:
            return (yield from Proceed())  # pyright: ignore[reportReturnType] - checks what the transaction type forbids

        def middleware() -> MessageMiddlewareEffectful[object]:
            return (yield from proceed_in_transaction())

        engine = make_engine(handle_without_effects, middleware_defs=[MessageMiddlewareDef("test", middleware)])

        with pytest.raises(UnsupportedEffectError) as exc_info:
            run_plan(engine.handle(TestCommand(), MESSAGE_CONTEXT))

        assert exc_info.value.args == (Proceed(),)

    @pytest.mark.parametrize("effect", [TestControlEffect(), TestEvent(), TestCommand()], ids=type_name)
    def test_raises_unsupported_effect_error_when_middleware_yields_unsupported_effect(
        self,
        effect: Effect[Any],
    ) -> None:
        def middleware() -> MessageMiddlewareEffectful[str]:
            yield from effect  # pyright: ignore[reportReturnType] - checks a wrong effect
            return "unreachable"

        engine = make_engine(handle_with_three_effects, middleware_defs=[MessageMiddlewareDef("test", middleware)])

        with pytest.raises(UnsupportedEffectError) as exc_info:
            engine.handle(TestCommand(), MESSAGE_CONTEXT).send(None)

        assert exc_info.value.args == (effect,)

    def test_closes_all_generators_of_chain_when_execution_plan_closed(self) -> None:
        def middleware() -> MessageMiddlewareEffectful[object]:
            try:
                return (yield from Proceed())
            finally:
                events.append("middleware closed")

        def handler() -> Effectful[str, Effect[Any]]:
            try:
                yield from TestInternalEffect()
            finally:
                events.append("handler closed")
            return "command_result"

        events: MutableSequence[str] = []
        engine = make_engine(handler, middleware_defs=[MessageMiddlewareDef("test", middleware)])
        execution_plan = engine.handle(TestCommand(), MESSAGE_CONTEXT)
        execution_plan.send(None)

        execution_plan.close()

        assert events == ["handler closed", "middleware closed"]

    @pytest.mark.parametrize(
        argnames=("command_middleware_defs", "expected_result"),
        argvalues=[
            pytest.param(None, "engine command_result", id="engine_middlewares"),
            pytest.param((), "command_result", id="replaced_by_def"),
        ],
    )
    def test_uses_engine_middlewares_unless_command_def_replaces_them(
        self,
        command_middleware_defs: Sequence[MessageMiddlewareDef] | None,
        expected_result: str,
    ) -> None:
        def engine_middleware() -> MessageMiddlewareEffectful[str]:
            return f"engine {(yield from Proceed())}"

        engine = make_engine(
            handler=handle_without_effects,
            middleware_defs=[MessageMiddlewareDef("engine", engine_middleware)],
            command_middleware_defs=command_middleware_defs,
        )

        assert run(engine.handle(TestCommand(), MESSAGE_CONTEXT)) == expected_result

    @pytest.mark.parametrize("is_command_list", [False, True])
    def test_raises_duplicate_message_middleware_def_error_when_middlewares_have_same_name(
        self,
        is_command_list: bool,
    ) -> None:
        middleware_defs = [MessageMiddlewareDef("test", proceed), MessageMiddlewareDef("test", proceed)]
        all_middleware_defs = [*middleware_defs, MessageMiddlewareDef("other", proceed)]

        with pytest.raises(DuplicateMessageMiddlewareDefError) as exc_info:
            make_engine(
                handler=handle_without_effects,
                middleware_defs=() if is_command_list else all_middleware_defs,
                command_middleware_defs=all_middleware_defs if is_command_list else None,
            )

        assert exc_info.value.args == (middleware_defs,)

    def test_raises_unsupported_function_error_when_middleware_is_not_generator_function(self) -> None:
        def middleware() -> str:
            return "middleware_result"

        with pytest.raises(UnsupportedFunctionError) as exc_info:
            make_engine(
                handler=handle_without_effects,
                middleware_defs=[MessageMiddlewareDef("test", middleware)],  # pyright: ignore[reportArgumentType] - checks a wrong middleware
            )

        assert exc_info.value.args == (middleware,)

    @pytest.mark.parametrize("command_def_type", [StagedCommandDef, AtomicCommandDef])
    def test_raises_dependency_not_found_error_when_middleware_parameter_has_no_guard(
        self,
        command_def_type: type[StagedCommandDef[Any]] | type[AtomicCommandDef[Any]],
    ) -> None:
        def middleware(service: Service) -> MessageMiddlewareEffectful[object]:
            return (yield from Proceed())

        with pytest.raises(DependencyNotFoundError) as exc_info:
            OperationEngineImpl(
                command_defs=[command_def_type("test", TestCommand, handle_without_effects)],  # pyright: ignore[reportArgumentType] - the handler is only checked, not run
                middleware_defs=[MessageMiddlewareDef("test", middleware)],
            )

        assert exc_info.value.args == (middleware, "service", Service)
