from collections.abc import MutableSequence, Sequence
from typing import Any

import pytest

from fidem.core.effects import Effect, Effectful
from fidem.core.guards import GuardDef
from fidem.core.messages import MessageContext, QueryEffect, QueryEffectful
from fidem.core.operation_engine.definitions import MessageMiddlewareDef, MessageMiddlewareEffectful, Proceed, QueryDef
from fidem.core.operation_engine.engine import OperationEngineImpl
from fidem.core.operation_engine.errors import DuplicateQueryDefError, MessageNotDefinedError, UnsupportedEffectError
from fidem.core.testing import YieldStep, assert_generator
from tests.effects import TestControlEffect, TestDatabaseWriteEffect, TestExternalReadEffect, TestExternalWriteEffect
from tests.ids import type_name
from tests.messages import TestCommand, TestEvent, TestQuery
from tests.stubs import run
from tests.transactions import write_transaction
from tests.unit.operation_engine.helpers import (
    MESSAGE_CONTEXT,
    Service,
    make_expected_instruction,
    make_query_engine,
    run_plan,
)


def handle_query() -> QueryEffectful[str]:
    yield from ()
    return "query_result"


class TestExecuteQuery:
    def test_passes_query_message_context_and_guard_values_into_handler_and_returns_its_result(self) -> None:
        def service() -> Effectful[Service, QueryEffect]:
            yield from ()
            return Service()

        def handler(context: MessageContext, query: TestQuery, service: Service) -> QueryEffectful[str]:
            calls.append((context, query, service))
            effect_result = yield from read
            return f"query_result {effect_result}"

        calls: MutableSequence[tuple[MessageContext, TestQuery, Service]] = []
        read = TestExternalReadEffect()
        query = TestQuery()
        engine = make_query_engine(handler, guard_defs=[GuardDef(service)])

        assert_generator(
            generator=engine.handle(query, MESSAGE_CONTEXT),
            steps=[YieldStep(make_expected_instruction(read, query), "effect_result")],
            return_value="query_result effect_result",
        )
        assert len(calls) == 1
        assert calls[0][:2] == (MESSAGE_CONTEXT, query)

    @pytest.mark.parametrize(
        argnames="effect",
        argvalues=[
            TestDatabaseWriteEffect(),
            TestExternalWriteEffect(),
            TestControlEffect(),
            TestCommand(),
            TestQuery(),
            TestEvent(),
            Proceed(),
            write_transaction(),
        ],
        ids=type_name,
    )
    def test_raises_unsupported_effect_error_when_handler_yields_unsupported_effect(self, effect: Effect[Any]) -> None:
        def handler() -> Effectful[str, Effect[Any]]:
            yield from effect
            return "unreachable"

        engine = make_query_engine(handler)

        with pytest.raises(UnsupportedEffectError) as exc_info:
            run_plan(engine.handle(TestQuery(), MESSAGE_CONTEXT))

        assert exc_info.value.args == (effect,)

    @pytest.mark.parametrize(
        argnames=("query_middleware_defs", "expected_result"),
        argvalues=[
            pytest.param(None, "engine query_result", id="engine_middlewares"),
            pytest.param((), "query_result", id="replaced_by_def"),
        ],
    )
    def test_uses_engine_middlewares_unless_query_def_replaces_them(
        self,
        query_middleware_defs: Sequence[MessageMiddlewareDef] | None,
        expected_result: str,
    ) -> None:
        def engine_middleware() -> MessageMiddlewareEffectful[str]:
            return f"engine {(yield from Proceed())}"

        def handler() -> QueryEffectful[str]:
            yield from ()
            return "query_result"

        engine = make_query_engine(
            handler=handler,
            middleware_defs=[MessageMiddlewareDef("engine", engine_middleware)],
            query_middleware_defs=query_middleware_defs,
        )

        assert run(engine.handle(TestQuery(), MESSAGE_CONTEXT)) == expected_result

    def test_lets_middleware_yield_write_effects(self) -> None:
        def middleware() -> MessageMiddlewareEffectful[object]:
            result = yield from Proceed()
            yield from TestDatabaseWriteEffect()
            yield from write_transaction()
            return result

        def handler() -> QueryEffectful[str]:
            yield from ()
            return "query_result"

        effect_types: MutableSequence[type[Effect[Any]]] = []
        engine = make_query_engine(handler, middleware_defs=[MessageMiddlewareDef("cache", middleware)])

        result = run_plan(
            execution_plan=engine.handle(TestQuery(), MESSAGE_CONTEXT),
            call_effect_handler=lambda instruction: effect_types.append(type(instruction.call.effect)),
        )

        assert result == "query_result"
        assert effect_types == [TestDatabaseWriteEffect, TestDatabaseWriteEffect]

    def test_raises_query_not_defined_error_when_query_not_defined(self) -> None:
        class OtherQuery(TestQuery):
            pass

        engine = make_query_engine(handle_query)

        with pytest.raises(MessageNotDefinedError) as exc_info:
            engine.handle(OtherQuery(), MESSAGE_CONTEXT).send(None)

        assert exc_info.value.args == (OtherQuery,)

    def test_raises_duplicate_query_def_error_when_define_queries_with_same_type(self) -> None:
        query_defs = [QueryDef(TestQuery, handle_query), QueryDef(TestQuery, handle_query)]

        with pytest.raises(DuplicateQueryDefError) as exc_info:
            OperationEngineImpl(query_defs=query_defs)

        assert exc_info.value.args == (query_defs,)
