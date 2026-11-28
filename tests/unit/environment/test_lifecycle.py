from collections.abc import AsyncGenerator, MutableSequence
from unittest.mock import Mock

import pytest

from fidem.core.di.effects import EffectDependencyDef
from fidem.core.effects import InternalEffect
from fidem.core.environment import (
    DuplicateEffectDefError,
    EffectDef,
    EnvironmentAlreadyStartedError,
    EnvironmentImpl,
    EnvironmentNotStartedError,
)
from fidem.core.operation_engine.instructions import OperationEngine
from tests.effects import TestInternalEffect
from tests.messages import TestCommand
from tests.stubs import GeneratorFunctionStub
from tests.unit.environment.helpers import Resource, make_failing_handler, make_operation_engine_mock


class TestEffectDefinition:
    def test_raises_duplicate_effect_def_error_when_defines_effects_with_same_type(self) -> None:
        class OtherEffect(InternalEffect[str]):
            pass

        handler = make_failing_handler(ValueError())
        duplicate_effect_defs = [EffectDef(TestInternalEffect, handler), EffectDef(TestInternalEffect, handler)]
        other_effect_defs = [EffectDef(OtherEffect, handler)]

        with pytest.raises(DuplicateEffectDefError) as exc_info:
            EnvironmentImpl(
                operation_engine=Mock(spec=OperationEngine),
                effect_defs=duplicate_effect_defs + other_effect_defs,
            )

        assert exc_info.value.args == (duplicate_effect_defs,)


class TestLifecycle:
    async def test_raises_environment_not_started_error_when_executes_before_start(self) -> None:
        environment = EnvironmentImpl(operation_engine=make_operation_engine_mock(GeneratorFunctionStub()))

        with pytest.raises(EnvironmentNotStartedError):
            await environment.handle(TestCommand())

    async def test_raises_environment_not_started_error_when_executes_after_stop(self) -> None:
        environment = EnvironmentImpl(operation_engine=make_operation_engine_mock(GeneratorFunctionStub()))
        async with environment:
            pass

        with pytest.raises(EnvironmentNotStartedError):
            await environment.handle(TestCommand())

    async def test_raises_environment_not_started_error_when_stops_before_start(self) -> None:
        environment = EnvironmentImpl(operation_engine=make_operation_engine_mock())

        with pytest.raises(EnvironmentNotStartedError):
            await environment.__aexit__(None, None, None)

    async def test_raises_environment_already_started_error_when_starts_twice(self) -> None:
        environment = EnvironmentImpl(operation_engine=make_operation_engine_mock())

        async with environment:
            with pytest.raises(EnvironmentAlreadyStartedError):
                await environment.__aenter__()

    async def test_resolves_application_dependencies_on_start_and_closes_them_on_stop(self) -> None:
        async def resource() -> AsyncGenerator[Resource]:
            events.append("start")
            yield Resource()
            events.append("stop")

        events: MutableSequence[str] = []
        environment = EnvironmentImpl(
            operation_engine=make_operation_engine_mock(),
            dependency_defs=[EffectDependencyDef(resource)],
        )

        async with environment:
            assert events == ["start"]

        assert events == ["start", "stop"]
