from collections.abc import Awaitable, Callable, Generator
from dataclasses import dataclass
from typing import Any, Self


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


type EffectHandler[ResultT] = Callable[..., Awaitable[ResultT]]
