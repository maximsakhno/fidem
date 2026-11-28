"""Effects of the standard library that business logic may not call directly: time, UUIDs and random values.

Each effect is an `InternalEffect`, so guards, queries and the first phase of an atomic handler may yield it.
The environment gets `STDLIB_EFFECT_DEFS`, which run every effect with the standard library.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from secrets import randbelow, token_bytes
from typing import Any, Final
from uuid import UUID, uuid4

from fidem.effects import InternalEffect
from fidem.environment import EffectDef

__all__ = [
    "STDLIB_EFFECT_DEFS",
    "NewUuidEffect",
    "NowEffect",
    "RandomBytesEffect",
    "RandomIntEffect",
]


@dataclass(frozen=True, slots=True)
class NowEffect(InternalEffect[datetime]):
    """The current time in UTC with the time zone."""


@dataclass(frozen=True, slots=True)
class NewUuidEffect(InternalEffect[UUID]):
    """A random UUID of version 4."""


@dataclass(frozen=True, slots=True)
class RandomIntEffect(InternalEffect[int]):
    """A random integer from 0 to `below`, excluding `below`, of cryptographic strength."""

    below: int


@dataclass(frozen=True, slots=True)
class RandomBytesEffect(InternalEffect[bytes]):
    """`size` random bytes of cryptographic strength, for example for a token."""

    size: int


async def _now() -> datetime:
    return datetime.now(UTC)


async def _new_uuid() -> UUID:
    return uuid4()


async def _random_int(effect: RandomIntEffect) -> int:
    return randbelow(effect.below)


async def _random_bytes(effect: RandomBytesEffect) -> bytes:
    return token_bytes(effect.size)


STDLIB_EFFECT_DEFS: Final[Sequence[EffectDef[Any]]] = (
    EffectDef(NowEffect, _now),
    EffectDef(NewUuidEffect, _new_uuid),
    EffectDef(RandomIntEffect, _random_int),
    EffectDef(RandomBytesEffect, _random_bytes),
)
