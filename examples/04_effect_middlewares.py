from asyncio import run
from collections.abc import AsyncGenerator, MutableMapping
from dataclasses import dataclass
from itertools import count
from time import perf_counter

from fidem.di.effects import EffectDependencyDef, EffectDependencyLifetime
from fidem.effects import ExternalReadEffect, InternalEffect
from fidem.environment import CallNext, EffectCall, EffectDef, EffectMiddlewareDef, EnvironmentImpl
from fidem.messages import Command, StagedEffectful
from fidem.operation_engine import OperationEngineImpl, StagedCommandDef


# ── Middlewares ───────────────────────────────────────────
async def log(effect_call: EffectCall, call_next: CallNext) -> object:
    name = type(effect_call.effect).__name__
    started = perf_counter()
    try:
        return await call_next()
    finally:
        elapsed = (perf_counter() - started) * 1000
        print(f"[log {effect_call.message_context.message_id}] {name} took {elapsed:.1f} ms")


# Middlewares get their settings through a closure, they run before dependency injection.
def make_retry(attempts: int) -> EffectMiddlewareDef:
    async def retry(effect_call: EffectCall, call_next: CallNext) -> object:
        for attempt in range(1, attempts):
            try:
                return await call_next()
            except ConnectionError as exc:
                print(f"[retry] attempt {attempt} failed: {exc}")
        return await call_next()

    return EffectMiddlewareDef(retry)


LOG = EffectMiddlewareDef(log)


# ── Dependencies ──────────────────────────────────────────
class Connection:
    def __init__(self, number: int) -> None:
        self.number: int = number


connection_numbers = count(1)


# Each attempt gets a new connection because effect dependencies are resolved after the middlewares.
async def connection() -> AsyncGenerator[Connection]:
    number = next(connection_numbers)
    print(f"[connection {number}] open")
    try:
        yield Connection(number)
    finally:
        print(f"[connection {number}] close")


# ── Effects ───────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class GetRateEffect(ExternalReadEffect[float]):
    currency: str


@dataclass(frozen=True, slots=True)
class GetGreetingEffect(InternalEffect[str]):
    pass


RATES: MutableMapping[str, float] = {"EUR": 1.08}


async def handle_get_rate(effect: GetRateEffect, connection: Connection) -> float:
    if connection.number < 3:
        raise ConnectionError(f"connection {connection.number} is broken")
    return RATES[effect.currency]


async def handle_get_greeting() -> str:
    return "Hello"


# ── Command ───────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class ConvertCommand(Command[str]):
    amount: float
    currency: str


def convert(command: ConvertCommand) -> StagedEffectful[str]:
    greeting = yield from GetGreetingEffect()
    rate = yield from GetRateEffect(command.currency)
    return f"{greeting}! {command.amount} USD = {command.amount / rate:.2f} {command.currency}"


async def main() -> None:
    environment = EnvironmentImpl(
        operation_engine=OperationEngineImpl(command_defs=[StagedCommandDef("convert", ConvertCommand, convert)]),
        effect_defs=[
            EffectDef(GetGreetingEffect, handle_get_greeting),
            # The list of the effect replaces the environment list, so the logging is listed again.
            EffectDef(GetRateEffect, handle_get_rate, middleware_defs=[LOG, make_retry(attempts=3)]),
        ],
        dependency_defs=[
            EffectDependencyDef(connection, lifetime=EffectDependencyLifetime.EFFECT),
        ],
        effect_middleware_defs=[LOG],
    )

    async with environment:
        print()
        result = await environment.execute(ConvertCommand(amount=100, currency="EUR"), "convert-1")
        print(f"Result: {result}\n")


if __name__ == "__main__":
    run(main())
