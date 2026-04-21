from collections.abc import Awaitable, Callable, Generator, Hashable
from dataclasses import dataclass
from typing import Any

from fidem.serialization import Serializable

type IntentId = Hashable


@dataclass(frozen=True)
class IntentContext:
    intent_id: IntentId


class Intent[ResultT]:
    pass


class UserIntent[ResultT](Intent[ResultT], Serializable):
    pass


class IntegrationIntent[ResultT](UserIntent[ResultT]):
    pass


class ReadIntent[ResultT](IntegrationIntent[ResultT]):
    pass


class DatabaseReadIntent[ResultT](ReadIntent[ResultT]):
    pass


class ExternalReadIntent[ResultT](ReadIntent[ResultT]):
    pass


class WriteIntent[ResultT](IntegrationIntent[ResultT]):
    pass


class DatabaseWriteIntent[ResultT](WriteIntent[ResultT]):
    pass


class ExternalWriteIntent[ResultT](WriteIntent[ResultT]):
    pass


type DatabaseIntent[ResultT] = DatabaseReadIntent[ResultT] | DatabaseWriteIntent[ResultT]
type ExternalIntent[ResultT] = ExternalReadIntent[ResultT] | ExternalWriteIntent[ResultT]


type IntentHandler[
    ResourceT = Any,
    IntentT: IntegrationIntent[Any] = Any,
    ResultT = Any,
] = Callable[
    [ResourceT, IntentContext, IntentT],
    Awaitable[ResultT],
]
type IntentCompensationHandler[
    ResourceT = Any,
    IntentT: WriteIntent[Any] = Any,
    ResultT = Any,
] = Callable[
    [ResourceT, IntentContext, IntentT, ResultT],
    Awaitable[None],
]
type IntentMiddleware[
    ResourceT = Any,
    IntentT: IntegrationIntent[Any] = Any,
    ResultT = Any,
] = Callable[
    [IntentHandler[ResourceT, IntentT, ResultT]],
    IntentHandler[ResourceT, IntentT, ResultT],
]
type IntentCompensationMiddleware[
    ResourceT = Any,
    IntentT: WriteIntent[Any] = Any,
    ResultT = Any,
] = Callable[
    [IntentCompensationHandler[ResourceT, IntentT, ResultT]],
    IntentCompensationHandler[ResourceT, IntentT, ResultT],
]


def ask[ResultT](intent: Intent[ResultT]) -> Generator[Intent[ResultT], ResultT, ResultT]:
    return (yield intent)
