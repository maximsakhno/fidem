from asyncio import run
from collections.abc import MutableMapping, MutableSet
from dataclasses import dataclass
from typing import Any

from fidem.effects import EffectContext, ExternalReadEffect, InternalEffect
from fidem.environment import EffectDef, EnvironmentImpl
from fidem.messages import Command, Message, MessageContext, StagedEffectful
from fidem.operation_engine import (
    MessageMiddleware,
    MessageMiddlewareDef,
    MessageMiddlewareEffectful,
    OperationEngineImpl,
    Proceed,
    StagedCommandDef,
)


class TemporaryError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class CacheEntry:
    result: object


CACHE: MutableMapping[Message[Any], CacheEntry] = {}
FAILED_EFFECT_IDS: MutableSet[str] = set()


# ── Effects ───────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class GetCachedEffect(InternalEffect[CacheEntry | None]):
    message: Message[Any]


@dataclass(frozen=True, slots=True)
class SetCachedEffect(InternalEffect[None]):
    message: Message[Any]
    result: object


@dataclass(frozen=True, slots=True)
class LogEffect(InternalEffect[None]):
    text: str


@dataclass(frozen=True, slots=True)
class FetchRateEffect(ExternalReadEffect[float]):
    currency: str


async def handle_get_cached(effect: GetCachedEffect) -> CacheEntry | None:
    return CACHE.get(effect.message)


async def handle_set_cached(effect: SetCachedEffect) -> None:
    CACHE[effect.message] = CacheEntry(effect.result)


async def handle_log(effect: LogEffect) -> None:
    print(f"[log] {effect.text}")


# The rate service fails the first request with each effect id.
async def handle_fetch_rate(effect: FetchRateEffect, effect_context: EffectContext) -> float:
    effect_id = effect_context.effect_id
    if effect_id not in FAILED_EFFECT_IDS:
        FAILED_EFFECT_IDS.add(effect_id)
        print(f"[rates] effect {effect_id[:8]}: service is unavailable")
        raise TemporaryError("The rate service is unavailable")
    print(f"[rates] effect {effect_id[:8]}: {effect.currency} rate fetched")
    return 0.9


# ── Middlewares ───────────────────────────────────────────
# The cache returns a stored result without proceeding, the result is stored by an effect as well.
def cache(message: Message[Any]) -> MessageMiddlewareEffectful[object]:
    entry = yield from GetCachedEffect(message)
    if entry is not None:
        yield from LogEffect(f"cache hit for {message}")
        return entry.result
    result = yield from Proceed()
    yield from SetCachedEffect(message, result)
    return result


# Each attempt runs the handler anew with the same effect ids, so writes stay idempotent.
def make_retry(attempts: int) -> MessageMiddleware:
    def retry(context: MessageContext) -> MessageMiddlewareEffectful[object]:
        attempt = 1
        while True:
            try:
                return (yield from Proceed())
            except TemporaryError as exc:
                if attempt == attempts:
                    raise
                yield from LogEffect(f"message {context.message_id}, attempt {attempt} failed: {exc}")
                attempt += 1

    return retry


# ── Command ───────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class ConvertCommand(Command[float]):
    amount: float
    currency: str


def convert(command: ConvertCommand) -> StagedEffectful[float]:
    rate = yield from FetchRateEffect(command.currency)
    return command.amount * rate


async def main() -> None:
    environment = EnvironmentImpl(
        operation_engine=OperationEngineImpl(
            command_defs=[StagedCommandDef("convert", ConvertCommand, convert)],
            # The first middleware is the outer one, so a cache hit skips the retries.
            middleware_defs=[
                MessageMiddlewareDef("cache", cache),
                MessageMiddlewareDef("retry", make_retry(attempts=3)),
            ],
        ),
        effect_defs=[
            EffectDef(GetCachedEffect, handle_get_cached),
            EffectDef(SetCachedEffect, handle_set_cached),
            EffectDef(LogEffect, handle_log),
            EffectDef(FetchRateEffect, handle_fetch_rate),
        ],
    )

    async with environment:
        print("\n=== The retry repeats the failed handler ===")
        result = await environment.execute(ConvertCommand(amount=100, currency="EUR"), "convert-1")
        print(f"Result: {result}")

        print("\n=== The cache returns the stored result ===")
        result = await environment.execute(ConvertCommand(amount=100, currency="EUR"), "convert-2")
        print(f"Result: {result}")
        print()


if __name__ == "__main__":
    run(main())
