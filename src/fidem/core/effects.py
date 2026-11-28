from collections.abc import Awaitable, Callable, Generator
from dataclasses import dataclass
from typing import Any, Self

__all__ = [
    "ControlEffect",
    "DatabaseEffect",
    "DatabaseReadEffect",
    "DatabaseWriteEffect",
    "Effect",
    "EffectContext",
    "EffectHandler",
    "EffectId",
    "Effectful",
    "ExternalEffect",
    "ExternalReadEffect",
    "ExternalWriteEffect",
    "HandledEffect",
    "InternalEffect",
    "ReadEffect",
    "WriteEffect",
]


class Effect[ResultT]:
    def __iter__(self) -> Generator[Self, ResultT, ResultT]:
        return (yield self)


type Effectful[ResultT, EffectT: Effect[Any]] = Generator[EffectT, Any, ResultT]


type EffectId = str


@dataclass(slots=True, frozen=True)
class EffectContext:
    effect_id: EffectId


class ControlEffect[ResultT](Effect[ResultT]):
    pass


class InternalEffect[ResultT](Effect[ResultT]):
    pass


class ExternalEffect[ResultT](Effect[ResultT]):
    pass


class DatabaseEffect[ResultT](Effect[ResultT]):
    pass


class ReadEffect[ResultT](Effect[ResultT]):
    pass


class WriteEffect[ResultT](Effect[ResultT]):
    pass


class ExternalReadEffect[ResultT](ExternalEffect[ResultT], ReadEffect[ResultT]):
    pass


class ExternalWriteEffect[ResultT](ExternalEffect[ResultT], WriteEffect[ResultT]):
    pass


class DatabaseReadEffect[ResultT](DatabaseEffect[ResultT], ReadEffect[ResultT]):
    pass


class DatabaseWriteEffect[ResultT](DatabaseEffect[ResultT], WriteEffect[ResultT]):
    pass


type HandledEffect[ResultT] = InternalEffect[ResultT] | ExternalEffect[ResultT] | DatabaseEffect[ResultT]
"""Effects that handlers of the environment run, unlike control effects."""


type EffectHandler[ResultT] = Callable[..., Awaitable[ResultT]]
