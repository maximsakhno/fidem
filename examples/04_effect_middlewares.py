from asyncio import run
from collections.abc import AsyncGenerator, MutableMapping
from dataclasses import dataclass
from itertools import count
from time import perf_counter
from typing import Final

from fidem import (
    CallNext,
    Command,
    EffectCall,
    EffectDependencyLifetime,
    EffectMiddleware,
    ExternalReadEffect,
    Fidem,
    InternalEffect,
    StagedEffectful,
    command,
    command_handler,
    dependency,
    effect,
    effect_handler,
    effect_middleware,
)


# ── Middlewares ───────────────────────────────────────────
@effect_middleware
async def log(effect_call: EffectCall, call_next: CallNext) -> object:
    name = type(effect_call.effect).__name__
    started = perf_counter()
    try:
        return await call_next()
    finally:
        elapsed = (perf_counter() - started) * 1000
        print(f"[log {effect_call.message_context.message_id}] {name} took {elapsed:.1f} ms")


# Middlewares get their settings through a closure, they run before dependency injection.
def make_retry(attempts: int) -> EffectMiddleware:
    async def retry(effect_call: EffectCall, call_next: CallNext) -> object:
        for attempt in range(1, attempts):
            try:
                return await call_next()
            except ConnectionError as exc:
                print(f"[retry] attempt {attempt} failed: {exc}")
        return await call_next()

    return retry


# ── Dependencies ──────────────────────────────────────────
class Connection:
    def __init__(self, number: int) -> None:
        self.number: Final[int] = number


connection_numbers = count(1)


# Each attempt gets a new connection because effect dependencies are resolved after the middlewares.
@dependency(lifetime=EffectDependencyLifetime.EFFECT)
async def connection() -> AsyncGenerator[Connection]:
    number = next(connection_numbers)
    print(f"[connection {number}] open")
    try:
        yield Connection(number)
    finally:
        print(f"[connection {number}] close")


# ── Effects ───────────────────────────────────────────────
@effect("get_rate")
@dataclass(frozen=True, slots=True)
class GetRateEffect(ExternalReadEffect[float]):
    currency: str


@effect("get_greeting")
@dataclass(frozen=True, slots=True)
class GetGreetingEffect(InternalEffect[str]):
    pass


RATES: MutableMapping[str, float] = {"EUR": 1.08}


# The list of the effect replaces the list of Fidem, so the logging is listed again.
@effect_handler(middlewares=[log, make_retry(attempts=3)])
async def handle_get_rate(effect: GetRateEffect, connection: Connection) -> float:
    if connection.number < 3:
        raise ConnectionError(f"connection {connection.number} is broken")
    return RATES[effect.currency]


@effect_handler
async def handle_get_greeting(effect: GetGreetingEffect) -> str:
    return "Hello"


# ── Command ───────────────────────────────────────────────
@command("convert")
@dataclass(frozen=True, slots=True)
class ConvertCommand(Command[str]):
    amount: float
    currency: str


@command_handler
def convert(command: ConvertCommand) -> StagedEffectful[str]:
    greeting = yield from GetGreetingEffect()
    rate = yield from GetRateEffect(command.currency)
    return f"{greeting}! {command.amount} USD = {command.amount / rate:.2f} {command.currency}"


async def main() -> None:
    fidem = Fidem(
        definitions=[convert, handle_get_greeting, handle_get_rate, connection],
        effect_middlewares=[log],
    )

    async with fidem:
        print()
        result = await fidem.handle(ConvertCommand(amount=100, currency="EUR"), message_id="convert-1")
        print(f"Result: {result}\n")


if __name__ == "__main__":
    run(main())
