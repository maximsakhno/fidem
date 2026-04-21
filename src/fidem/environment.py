from abc import ABC, abstractmethod
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from typing import Any

from fidem.intents import (
    DatabaseIntent,
    DatabaseWriteIntent,
    ExternalIntent,
    ExternalWriteIntent,
    IntentCompensationHandler,
    IntentCompensationMiddleware,
    IntentContext,
    IntentHandler,
    IntentMiddleware,
    ReadIntent,
    WriteIntent,
)
from fidem.operation_engine import CommandContextIn, EventContextIn, OperationEngine
from fidem.operations import Command, Event


class DatabaseIntegration[ResourceT](ABC):
    @abstractmethod
    async def execute[ResultT](
        self,
        handler: IntentHandler[ResourceT, DatabaseIntent[ResultT], ResultT],
        context: IntentContext,
        intent: DatabaseIntent[ResultT],
    ) -> ResultT:
        raise NotImplementedError

    @abstractmethod
    async def execute_compensation[ResultT](
        self,
        handler: IntentCompensationHandler[ResourceT, DatabaseWriteIntent[ResultT], ResultT],
        context: IntentContext,
        intent: DatabaseWriteIntent[ResultT],
        result: ResultT,
    ) -> None:
        raise NotImplementedError


class ExternalIntegration[ResourceT](ABC):
    @abstractmethod
    async def execute[ResultT](
        self,
        handler: IntentHandler[ResourceT, ExternalIntent[ResultT], ResultT],
        context: IntentContext,
        intent: ExternalIntent[ResultT],
    ) -> ResultT:
        raise NotImplementedError

    @abstractmethod
    async def execute_compensation[ResultT](
        self,
        handler: IntentCompensationHandler[ResourceT, ExternalWriteIntent[ResultT], ResultT],
        context: IntentContext,
        intent: ExternalWriteIntent[ResultT],
        result: ResultT,
    ) -> None:
        raise NotImplementedError


@dataclass(frozen=True)
class ReadIntentDef[ResourceT = Any, ReadIntentT: ReadIntent[Any] = Any, ResultT = Any]:
    intent_type: type[ReadIntent[ResultT]]
    handler: IntentHandler[ResourceT, ReadIntentT, ResultT]
    middlewares: Sequence[IntentMiddleware[ResourceT, ReadIntentT, ResultT]] | None = None
    name: str | None = None
    version: int = 1


@dataclass(frozen=True)
class WriteIntentDef[ResourceT = Any, WriteIntentT: WriteIntent[Any] = Any, ResultT = Any]:
    intent_type: type[WriteIntent[ResultT]]
    handler: IntentHandler[ResourceT, WriteIntentT, ResultT]
    compensation_handler: IntentCompensationHandler[ResourceT, WriteIntentT, ResultT]
    middlewares: Sequence[IntentMiddleware[ResourceT, WriteIntentT, ResultT]] | None = None
    compensation_middlewares: Sequence[IntentCompensationMiddleware[ResourceT, WriteIntentT, ResultT]] | None = None
    name: str | None = None
    version: int = 1


@dataclass(frozen=True)
class IntegrationDef[ResourceT = Any]:
    integration: DatabaseIntegration[ResourceT] | ExternalIntegration[ResourceT]
    intent_routes: Collection[ReadIntentDef[ResourceT] | WriteIntentDef[ResourceT]] = ()
    middlewares: Sequence[IntentMiddleware[ResourceT]] | None = None
    compensation_middlewares: Sequence[IntentCompensationMiddleware[ResourceT]] | None = None


@dataclass(frozen=True)
class DatabaseIntegrationDef[ResourceT = Any](IntegrationDef[ResourceT]):
    integration: DatabaseIntegration[ResourceT]


@dataclass(frozen=True)
class ExternalIntegrationDef[ResourceT = Any](IntegrationDef[ResourceT]):
    integration: ExternalIntegration[ResourceT]


class Environment(ABC):
    @abstractmethod
    def __init__(
        self,
        operation_engine: OperationEngine,
        database_integration_def: DatabaseIntegrationDef,
        external_integration_defs: Collection[IntegrationDef] = (),
        middlewares: Sequence[IntentMiddleware] | None = None,
        compensation_middlewares: Sequence[IntentCompensationMiddleware] | None = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def execute[ResultT](self, command: Command[ResultT], context: CommandContextIn | None = None) -> ResultT:
        raise NotImplementedError

    @abstractmethod
    async def publish(self, event: Event, context: EventContextIn | None = None) -> None:
        raise NotImplementedError

    @abstractmethod
    async def schedule(self, name: str, cron: str, event: Event, context: EventContextIn | None = None) -> None:
        raise NotImplementedError

    @abstractmethod
    def unschedule(self, name: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def run_outbox_dispatcher(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def run_scheduler(self) -> None:
        raise NotImplementedError
