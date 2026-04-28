from collections.abc import Awaitable, Callable, Generator
from dataclasses import dataclass
from typing import Any

type IntentId = str


@dataclass(frozen=True, slots=True)
class IntentContext:
    intent_id: IntentId


@dataclass(frozen=True, slots=True)
class IntentCompensationContext[ResultT](IntentContext):
    original_context: IntentContext
    original_result: ResultT


class Intent[ResultT]:
    pass


class ReadIntent[ResultT](Intent[ResultT]):
    pass


class WriteIntent[ResultT](Intent[ResultT]):
    pass


class DatabaseReadIntent[ResultT](ReadIntent[ResultT]):
    pass


class DatabaseWriteIntent[ResultT](WriteIntent[ResultT]):
    pass


type IntentHandler[
    IntentContextT: IntentContext = Any,
    IntentT: ReadIntent[Any] | WriteIntent[Any] = Any,
    ResultT = Any,
] = Callable[
    [IntentContextT, IntentT],
    Awaitable[ResultT],
]
type IntentMiddleware[
    IntentContextT: IntentContext = Any,
    IntentT: ReadIntent[Any] | WriteIntent[Any] = Any,
    ResultT = Any,
] = Callable[
    [IntentHandler[IntentContextT, IntentT, ResultT]],
    IntentHandler[IntentContextT, IntentT, ResultT],
]


type DatabaseIntentHandler[
    SessionT = Any,
    IntentContextT: IntentContext = Any,
    IntentT: DatabaseReadIntent[Any] | DatabaseWriteIntent[Any] = Any,
    ResultT = Any,
] = Callable[
    [SessionT, IntentContextT, IntentT],
    Awaitable[ResultT],
]


def ask[ResultT](intent: Intent[ResultT]) -> Generator[Intent[ResultT], ResultT, ResultT]:
    return (yield intent)
