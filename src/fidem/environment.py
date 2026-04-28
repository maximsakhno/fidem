from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Collection, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Any

from fidem.intents import (
    DatabaseIntentHandler,
    DatabaseReadIntent,
    DatabaseWriteIntent,
    Intent,
    IntentCompensationContext,
    IntentContext,
    IntentHandler,
    IntentMiddleware,
    ReadIntent,
    WriteIntent,
)
from fidem.operation_engine import (
    CallIntentCompensationHandlerInstruction,
    CallIntentHandlerInstruction,
    CommandContextIn,
    EventContextIn,
    Instruction,
    OperationEngine,
)
from fidem.operations import Command, Event
from fidem.serializer import Serializer


@dataclass(frozen=True, slots=True)
class BaseReadIntentDef[
    IntentT: ReadIntent[Any] = Any,
    HandlerT: Callable[..., Any] = Any,
    MiddlewareT: Callable[..., Any] = Any,
]:
    intent_type: type[IntentT]
    handler: HandlerT
    middlewares: Sequence[MiddlewareT] | None = None


@dataclass(frozen=True, slots=True)
class BaseWriteIntentDef[
    IntentT: WriteIntent[Any] = Any,
    HandlerT: Callable[..., Any] = Any,
    CompensationHandlerT: Callable[..., Any] = Any,
    MiddlewareT: Callable[..., Any] = Any,
    CompensationMiddlewareT: Callable[..., Any] = Any,
]:
    intent_type: type[IntentT]
    handler: HandlerT
    compensation_handler: CompensationHandlerT
    middlewares: Sequence[MiddlewareT] | None = None
    compensation_middlewares: Sequence[CompensationMiddlewareT] | None = None


@dataclass(frozen=True, slots=True)
class ReadIntentDef[
    IntentT: ReadIntent[Any] = Any,
    ResultT = Any,
](
    BaseReadIntentDef[
        ReadIntent[ResultT],
        IntentHandler[IntentContext, IntentT, ResultT],
        IntentMiddleware[IntentContext, IntentT, ResultT],
    ]
):
    pass


@dataclass(frozen=True, slots=True)
class WriteIntentDef[
    IntentT: WriteIntent[Any] = Any,
    ResultT = Any,
](
    BaseWriteIntentDef[
        WriteIntent[ResultT],
        IntentHandler[IntentContext, IntentT, ResultT],
        IntentHandler[IntentCompensationContext[ResultT], IntentT, None],
        IntentMiddleware[IntentContext, IntentT, ResultT],
        IntentMiddleware[IntentCompensationContext[ResultT], IntentT, None],
    ]
):
    pass


@dataclass(frozen=True, slots=True)
class DatabaseReadIntentDef[
    SessionT = Any,
    IntentT: DatabaseReadIntent[Any] = Any,
    ResultT = Any,
](
    BaseReadIntentDef[
        IntentT,
        DatabaseIntentHandler[SessionT, IntentContext, IntentT, ResultT],
        IntentMiddleware[IntentContext, IntentT, ResultT],
    ]
):
    pass


@dataclass(frozen=True, slots=True)
class DatabaseWriteIntentDef[
    SessionT = Any,
    IntentT: DatabaseWriteIntent[Any] = Any,
    ResultT = Any,
](
    BaseWriteIntentDef[
        IntentT,
        DatabaseIntentHandler[SessionT, IntentContext, IntentT, ResultT],
        DatabaseIntentHandler[SessionT, IntentCompensationContext[ResultT], IntentT, ResultT],
        IntentMiddleware[IntentContext, IntentT, ResultT],
        IntentMiddleware[IntentCompensationContext[ResultT], IntentT, ResultT],
    ]
):
    pass


class Database[SessionT = Any]:
    def start_session(self) -> AbstractAsyncContextManager[SessionT]:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class DatabaseDef[SessionT = Any]:
    database: Database[SessionT]
    intent_defs: Collection[DatabaseReadIntentDef[SessionT] | DatabaseWriteIntentDef[SessionT]] = ()


class Environment(ABC):
    @abstractmethod
    async def execute[ResultT](
        self,
        command: Command[ResultT],
        context: CommandContextIn | None = None,
    ) -> ResultT:
        raise NotImplementedError

    @abstractmethod
    async def publish(
        self,
        event: Event,
        context: EventContextIn | None = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def schedule(
        self,
        name: str,
        cron: str,
        event: Event,
        context: EventContextIn | None = None,
    ) -> None:
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


class DuplicateIntentDefError(Exception):
    def __init__(self, intent_defs: Collection[BaseReadIntentDef | BaseWriteIntentDef]) -> None:
        super().__init__(intent_defs)


class IntentNotDefinedError(Exception):
    def __init__(self, intent_type: type[Intent[Any]]) -> None:
        super().__init__(intent_type)


class IntentCompensationHandlerNotDefinedError(Exception):
    def __init__(self, intent_type: type[WriteIntent[Any]]) -> None:
        super().__init__(intent_type)


@dataclass(frozen=True, slots=True)
class IntentRecord:
    intent_def: BaseReadIntentDef | BaseWriteIntentDef
    wrapped_handler: IntentHandler
    wrapped_compensation_handler: IntentHandler | None


class EnvironmentImpl(Environment):
    def __init__(
        self,
        operation_engine: OperationEngine,
        database_def: DatabaseDef | None = None,
        serializer: Serializer | None = None,
        intent_defs: Collection[BaseReadIntentDef | BaseWriteIntentDef] = (),
        middlewares: Sequence[IntentMiddleware] | None = None,
        compensation_middlewares: Sequence[IntentMiddleware] | None = None,
    ) -> None:
        self._operation_engine = operation_engine
        self._database_def = database_def
        self._serializer = serializer
        self._type_to_intent_record = self._make_type_to_intent_record(
            intent_defs,
            middlewares,
            compensation_middlewares,
        )

    @classmethod
    def _make_type_to_intent_record(
        cls,
        intent_defs: Collection[BaseReadIntentDef | BaseWriteIntentDef],
        middlewares: Sequence[IntentMiddleware] | None,
        compensation_middlewares: Sequence[IntentMiddleware] | None,
    ) -> Mapping[type[Intent[Any]], IntentRecord]:
        type_to_intent_defs = defaultdict[type[Intent[Any]], list[BaseReadIntentDef | BaseWriteIntentDef]](list)
        for intent_def in intent_defs:
            type_to_intent_defs[intent_def.intent_type].append(intent_def)

        type_to_intent_record: dict[type[Intent[Any]], IntentRecord] = {}
        for intent_type, intent_defs in type_to_intent_defs.items():
            if len(intent_defs) > 1:
                raise DuplicateIntentDefError(intent_defs)

            intent_def = intent_defs[0]
            type_to_intent_record[intent_type] = cls._make_intent_record(
                intent_def,
                middlewares,
                compensation_middlewares,
            )

        return type_to_intent_record

    @classmethod
    def _make_intent_record(
        cls,
        intent_def: BaseReadIntentDef | BaseWriteIntentDef,
        middlewares: Sequence[IntentMiddleware] | None,
        compensation_middlewares: Sequence[IntentMiddleware] | None,
    ) -> IntentRecord:
        if isinstance(intent_def, BaseReadIntentDef):
            wrapped_handler = intent_def.handler
            middlewares_to_apply = cls._first_not_none([intent_def.middlewares, middlewares], [])
            for middleware in reversed(middlewares_to_apply):
                wrapped_handler = middleware(wrapped_handler)
            return IntentRecord(intent_def, wrapped_handler, None)
        else:
            wrapped_handler = intent_def.handler
            middlewares_to_apply = cls._first_not_none([intent_def.middlewares, middlewares], [])
            for middleware in reversed(middlewares_to_apply):
                wrapped_handler = middleware(wrapped_handler)
            wrapped_compensation_handler = intent_def.compensation_handler
            compensaction_middlewares_in_priority_order = [
                intent_def.compensation_middlewares,
                compensation_middlewares,
                intent_def.middlewares,
                middlewares,
            ]
            compensation_middlewares_to_apply = cls._first_not_none(compensaction_middlewares_in_priority_order, [])
            for compensation_middleware in reversed(compensation_middlewares_to_apply):
                wrapped_compensation_handler = compensation_middleware(wrapped_compensation_handler)
            return IntentRecord(intent_def, wrapped_handler, wrapped_compensation_handler)

    @classmethod
    def _first_not_none[T](cls, values: Sequence[T | None], default: T) -> T:
        return next(iter(value for value in values if value is not None), default)

    async def execute[ResultT](
        self,
        command: Command[ResultT],
        context: CommandContextIn | None = None,
    ) -> ResultT:
        execution_plan = self._operation_engine.execute(command, context)
        instruction_result = None
        while True:
            try:
                instruction = execution_plan.send(instruction_result)
            except StopIteration as e:
                return e.value
            else:
                instruction_result = await self._process_instruction(instruction)

    async def publish(self, event: Event, context: EventContextIn | None = None) -> None:
        raise NotImplementedError

    async def schedule(self, name: str, cron: str, event: Event, context: EventContextIn | None = None) -> None:
        raise NotImplementedError

    def unschedule(self, name: str) -> None:
        raise NotImplementedError

    def run_outbox_dispatcher(self) -> None:
        raise NotImplementedError

    def run_scheduler(self) -> None:
        raise NotImplementedError

    async def _process_instruction[ResultT](self, instruction: Instruction[ResultT]) -> ResultT:
        if isinstance(instruction, CallIntentHandlerInstruction):
            return await self._call_intent_handler(instruction)
        elif isinstance(instruction, CallIntentCompensationHandlerInstruction):
            return await self._call_intent_compensation_handler(instruction)  # type: ignore
        else:
            raise NotImplementedError(instruction)

    async def _call_intent_handler[ResultT](self, instruction: CallIntentHandlerInstruction[ResultT]) -> ResultT:
        context = instruction.context
        intent = instruction.intent
        intent_type = type(intent)
        intent_record = self._get_intent_record(intent_type)
        return await intent_record.wrapped_handler(context, intent)

    async def _call_intent_compensation_handler(
        self, instruction: CallIntentCompensationHandlerInstruction[Any]
    ) -> None:
        context = instruction.context
        intent = instruction.intent
        intent_type = type(intent)
        intent_record = self._get_intent_record(intent_type)
        if intent_record.wrapped_compensation_handler is None:
            raise IntentCompensationHandlerNotDefinedError(intent_type)
        return await intent_record.wrapped_compensation_handler(context, intent)

    def _get_intent_record(self, intent_type: type[Intent[Any]]) -> IntentRecord:
        intent_record = self._type_to_intent_record.get(intent_type)
        if not intent_record:
            raise IntentNotDefinedError(intent_type)
        return intent_record
