from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from fidem.effects import InternalEffect
from fidem.environment import EnvironmentImpl
from fidem.messages import Command, StagedEffectful
from fidem.operation_engine.definitions import StagedCommandDef
from fidem.operation_engine.engine import OperationEngineImpl
from fidem.stdlib import STDLIB_EFFECT_DEFS, NewUuidEffect, NowEffect, RandomBytesEffect, RandomIntEffect


@dataclass(frozen=True, slots=True)
class RunEffectCommand[ResultT](Command[ResultT]):
    effect: InternalEffect[ResultT]


def run_effect(command: RunEffectCommand[Any]) -> StagedEffectful[object]:
    return (yield from command.effect)


async def run[ResultT](effect: InternalEffect[ResultT]) -> ResultT:
    environment = EnvironmentImpl(
        operation_engine=OperationEngineImpl(
            command_defs=[StagedCommandDef("run_effect", RunEffectCommand, run_effect)],
        ),
        effect_defs=STDLIB_EFFECT_DEFS,
    )
    async with environment:
        return await environment.handle(RunEffectCommand(effect))


class TestNowEffect:
    async def test_returns_current_time_in_utc(self) -> None:
        before = datetime.now(UTC)

        now = await run(NowEffect())

        assert before <= now <= datetime.now(UTC)
        assert now.tzinfo is UTC


class TestNewUuidEffect:
    async def test_returns_random_uuid(self) -> None:
        first = await run(NewUuidEffect())
        second = await run(NewUuidEffect())

        assert first.version == 4
        assert first != second


class TestRandomIntEffect:
    async def test_returns_integer_below_bound(self) -> None:
        assert await run(RandomIntEffect(below=1)) == 0


class TestRandomBytesEffect:
    async def test_returns_bytes_of_size(self) -> None:
        assert len(await run(RandomBytesEffect(size=16))) == 16
