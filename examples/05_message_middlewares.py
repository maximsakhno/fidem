from asyncio import run
from collections.abc import MutableMapping, MutableSet
from dataclasses import dataclass
from typing import Any

from fidem import (
    Command,
    EffectContext,
    ExternalReadEffect,
    Fidem,
    InternalEffect,
    Message,
    MessageContext,
    MessageMiddleware,
    MessageMiddlewareEffectful,
    Proceed,
    StagedEffectful,
    command,
    command_handler,
    effect,
    effect_handler,
    middleware,
)


class TemporaryError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class CacheEntry:
    result: object


CACHE: MutableMapping[Message[Any], CacheEntry] = {}
FAILED_EFFECT_IDS: MutableSet[str] = set()


# ── Effects ───────────────────────────────────────────────
@effect("get_cached")
@dataclass(frozen=True, slots=True)
class GetCachedEffect(InternalEffect[CacheEntry | None]):
    message: Message[Any]


@effect("set_cached")
@dataclass(frozen=True, slots=True)
class SetCachedEffect(InternalEffect[None]):
    message: Message[Any]
    result: object


@effect("log")
@dataclass(frozen=True, slots=True)
class LogEffect(InternalEffect[None]):
    text: str


@effect("fetch_rate")
@dataclass(frozen=True, slots=True)
class FetchRateEffect(ExternalReadEffect[float]):
    currency: str


@effect_handler
async def handle_get_cached(effect: GetCachedEffect) -> CacheEntry | None:
    return CACHE.get(effect.message)


@effect_handler
async def handle_set_cached(effect: SetCachedEffect) -> None:
    CACHE[effect.message] = CacheEntry(effect.result)


@effect_handler
async def handle_log(effect: LogEffect) -> None:
    print(f"[log] {effect.text}")


# The rate service fails the first request with each effect id.
@effect_handler
async def handle_fetch_rate(effect: FetchRateEffect, effect_context: EffectContext) -> float:
    effect_id = effect_context.effect_id
    if effect_id not in FAILED_EFFECT_IDS:
        FAILED_EFFECT_IDS.add(effect_id)
        print(f"[rates] effect {effect_id[:8]}: service is unavailable")
        raise TemporaryError("The rate service is unavailable")
    print(f"[rates] effect {effect_id[:8]}: {effect.currency} rate fetched")
    return 0.9


# ── Middlewares ───────────────────────────────────────────
# A middleware is named after its function, and the name is the seed of the ids of its effects.
# The cache returns a stored result without proceeding, the result is stored by an effect as well.
@middleware
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
@command("convert")
@dataclass(frozen=True, slots=True)
class ConvertCommand(Command[float]):
    amount: float
    currency: str


@command_handler
def convert(command: ConvertCommand) -> StagedEffectful[float]:
    rate = yield from FetchRateEffect(command.currency)
    return command.amount * rate


async def main() -> None:
    fidem = Fidem(
        definitions=[convert, handle_get_cached, handle_set_cached, handle_log, handle_fetch_rate],
        # The first middleware is the outer one, so a cache hit skips the retries.
        middlewares=[cache, make_retry(attempts=3)],
    )

    async with fidem:
        print("\n=== The retry repeats the failed handler ===")
        result = await fidem.handle(ConvertCommand(amount=100, currency="EUR"), message_id="convert-1")
        print(f"Result: {result}")

        print("\n=== The cache returns the stored result ===")
        result = await fidem.handle(ConvertCommand(amount=100, currency="EUR"), message_id="convert-2")
        print(f"Result: {result}")
        print()


if __name__ == "__main__":
    run(main())
