import os
import string
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Collection, Generator, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum, auto
from random import Random
from typing import Any, Final, TypeGuard, cast

from fidem.intents import DatabaseWriteIntent, Intent, IntentCompensationContext, IntentContext, ReadIntent, WriteIntent
from fidem.operations import (
    Command,
    CommandContext,
    CommandHandler,
    CommandMiddleware,
    Event,
    EventHandler,
    EventMiddleware,
    Operation,
    OperationGenerator,
    OperationId,
    OperationResult,
)


class ExecutionMode(Enum):
    EPHEMERAL = auto()
    DURABLE = auto()


@dataclass(frozen=True, slots=True)
class OperationContextIn:
    operation_id: OperationId | None = None


@dataclass(frozen=True, slots=True)
class CommandContextIn(OperationContextIn):
    pass


@dataclass(frozen=True, slots=True)
class EventContextIn(OperationContextIn):
    pass


@dataclass(frozen=True, slots=True)
class OperationDef[
    OperationT: Operation[Any] = Any,
    HandlerT: Callable[..., Any] = Any,
    MiddlewareT: Callable[..., Any] = Any,
]:
    operation_type: type[OperationT]
    handler: HandlerT
    middlewares: Sequence[MiddlewareT] | None = None
    execution_mode: ExecutionMode = ExecutionMode.EPHEMERAL
    name: str | None = None
    version: int = 1


@dataclass(frozen=True, slots=True)
class CommandDef[
    CommandT: Command[Any] = Any,
    ResultT = Any,
    EventOutT: Event | None = None,
](
    OperationDef[
        Command[ResultT],
        CommandHandler[CommandT, ResultT, EventOutT],
        CommandMiddleware[CommandT, ResultT, EventOutT],
    ]
):
    pass


@dataclass(frozen=True, slots=True)
class EventDef[
    EventT: Event = Any,
    EventOutT: Event | None = None,
](
    OperationDef[
        EventT,
        EventHandler[EventT, EventOutT],
        EventMiddleware[EventT, EventOutT],
    ]
):
    pass


@dataclass(frozen=True, slots=True)
class ScheduledEventDef:
    name: str
    cron: str
    event: Event


class Instruction[ResultT]:
    pass


@dataclass(frozen=True, slots=True)
class CallIntentHandlerInstruction[ResultT](Instruction[ResultT]):
    context: IntentContext
    intent: ReadIntent[ResultT] | WriteIntent[ResultT]


@dataclass(frozen=True, slots=True)
class CallIntentCompensationHandlerInstruction[ResultT](Instruction[None]):
    context: IntentCompensationContext[ResultT]
    intent: WriteIntent[ResultT]


type ExecutionPlan[
    ResultT,
] = Generator[
    Instruction[Any],
    Any,
    ResultT,
]


class OperationEngine(ABC):
    @abstractmethod
    def execute[ResultT](
        self,
        command: Command[ResultT],
        context: CommandContextIn | None = None,
    ) -> ExecutionPlan[ResultT]:
        raise NotImplementedError

    @abstractmethod
    def publish(
        self,
        event: Event,
        context: EventContextIn | None = None,
    ) -> ExecutionPlan[None]:
        raise NotImplementedError

    @abstractmethod
    def schedule(
        self,
        name: str,
        cron: str,
        event: Event,
        context: EventContextIn | None = None,
    ) -> ExecutionPlan[None]:
        raise NotImplementedError

    @abstractmethod
    def unschedule(
        self,
        name: str,
    ) -> ExecutionPlan[None]:
        raise NotImplementedError

    @abstractmethod
    def run_outbox_dispatcher(self) -> ExecutionPlan[None]:
        raise NotImplementedError

    @abstractmethod
    def run_scheduler(self) -> ExecutionPlan[None]:
        raise NotImplementedError


class DuplicateCommandDefError(Exception):
    def __init__(self, command_defs: Collection[CommandDef]) -> None:
        super().__init__(command_defs)


class CommandNotDefinedError(Exception):
    def __init__(self, command_type: type[Command[Any]]) -> None:
        super().__init__(command_type)


@dataclass(frozen=True, slots=True)
class CommandRecord[CommandT: Command[Any] = Any, ResultT = Any, EventOutT: Event | None = None]:
    command_def: CommandDef[CommandT, ResultT, EventOutT]
    wrapped_handler: CommandHandler[CommandT, ResultT, EventOutT]


@dataclass(frozen=True, slots=True)
class EventRecord[EventT: Event = Any, EventOutT: Event | None = None]:
    event_def: EventDef[EventT, EventOutT]
    wrapped_handler: EventHandler[EventT, EventOutT]


@dataclass(frozen=True, slots=True)
class InstructionLogRecord[ResultT]:
    instruction: Instruction[ResultT]
    result: ResultT


class OperationEngineImpl(OperationEngine):
    _ID_ALPHABET: Final = string.digits + string.ascii_lowercase + string.ascii_uppercase

    def __init__(
        self,
        command_defs: Collection[CommandDef] = (),
        event_defs: Collection[EventDef] = (),
        scheduled_event_defs: Collection[ScheduledEventDef] = (),
        command_middlewares: Sequence[CommandMiddleware] = (),
        event_middlewares: Sequence[EventMiddleware] = (),
    ) -> None:
        self._type_to_command_record = self._make_type_to_command_records(command_defs, command_middlewares)
        self._type_to_event_records = self._make_type_to_event_records(event_defs, event_middlewares)

    @classmethod
    def _make_type_to_command_records(
        cls,
        command_defs: Collection[CommandDef],
        command_middlewares: Sequence[CommandMiddleware],
    ) -> Mapping[type[Command[Any]], CommandRecord]:
        type_to_command_defs: defaultdict[type[Command[Any]], list[CommandDef]] = defaultdict(list)
        for command_def in command_defs:
            type_to_command_defs[command_def.operation_type].append(command_def)

        type_to_command_record: dict[type[Command[Any]], CommandRecord] = {}
        for command_type, defs in type_to_command_defs.items():
            if len(defs) > 1:
                raise DuplicateCommandDefError(defs)

            command_def = defs[0]
            type_to_command_record[command_type] = cls._make_command_record(command_def, command_middlewares)

        return type_to_command_record

    @classmethod
    def _make_command_record(
        cls,
        command_def: CommandDef,
        command_middlewares: Sequence[CommandMiddleware],
    ) -> CommandRecord:
        wrapped_handler = command_def.handler
        command_middlewares = command_def.middlewares or command_middlewares
        for middleware in reversed(command_middlewares):
            wrapped_handler = middleware(wrapped_handler)
        return CommandRecord(command_def, wrapped_handler)

    @classmethod
    def _make_type_to_event_records(
        cls,
        event_defs: Collection[EventDef],
        event_middlewares: Sequence[EventMiddleware],
    ) -> Mapping[type[Event], list[EventRecord]]:
        type_to_event_defs: defaultdict[type[Event], list[EventDef]] = defaultdict(list)
        for event_def in event_defs:
            type_to_event_defs[event_def.operation_type].append(event_def)
        return {
            event_type: [cls._make_event_record(event_def, event_middlewares) for event_def in event_defs]
            for event_type, event_defs in type_to_event_defs.items()
        }

    @classmethod
    def _make_event_record(cls, event_def: EventDef, event_middlewares: Sequence[EventMiddleware]) -> EventRecord:
        wrapped_handler = event_def.handler
        event_middlewares = event_def.middlewares or event_middlewares
        for middleware in reversed(event_middlewares):
            wrapped_handler = middleware(wrapped_handler)
        return EventRecord(event_def=event_def, wrapped_handler=wrapped_handler)

    def execute[ResultT](
        self,
        command: Command[ResultT],
        context: CommandContextIn | None = None,
    ) -> ExecutionPlan[ResultT]:
        if not context:
            context = CommandContextIn()

        command_type = type(command)
        command_record = self._get_command_record(command_type)
        command_def = command_record.command_def
        execution_mode = command_def.execution_mode

        if execution_mode != ExecutionMode.EPHEMERAL:
            raise NotImplementedError(f"Execution mode {execution_mode} is not implemented yet")

        random = self._make_random(context.operation_id)
        command_id = self._generate_id(random)
        command_context = CommandContext(command_id)
        handler = command_record.wrapped_handler
        generator = handler(command_context, command)
        return (yield from self._execute_ephermeral(random, generator))

    def publish(
        self,
        event: Event,
        context: EventContextIn | None = None,
    ) -> ExecutionPlan[None]:
        raise NotImplementedError

    def schedule(
        self,
        name: str,
        cron: str,
        event: Event,
        context: EventContextIn | None = None,
    ) -> ExecutionPlan[None]:
        raise NotImplementedError

    def unschedule(
        self,
        name: str,
    ) -> ExecutionPlan[None]:
        raise NotImplementedError

    def run_outbox_dispatcher(self) -> ExecutionPlan[None]:
        raise NotImplementedError

    def run_scheduler(self) -> ExecutionPlan[None]:
        raise NotImplementedError

    def _get_command_record(self, command_type: type[Command[Any]]) -> CommandRecord:
        command_record = self._type_to_command_record.get(command_type)
        if not command_record:
            raise CommandNotDefinedError(command_type)
        return command_record

    def _make_random(self, operation_id: str | None) -> Random:
        return Random(operation_id or os.urandom(16))

    def _generate_id(self, random: Random, length: int = 16) -> str:
        return "".join(random.choice(self._ID_ALPHABET) for _ in range(length))

    def _execute_ephermeral[ResultT](
        self,
        random: Random,
        generator: OperationGenerator[ReadIntent[Any] | WriteIntent[Any], ResultT],
    ) -> ExecutionPlan[ResultT]:
        execution_log: list[InstructionLogRecord[Any]] = []
        try:
            forward_generator = self._execute_ephermeral_forward(random, generator)
            instruction_result = None
            while True:
                try:
                    instruction = forward_generator.send(instruction_result)
                except StopIteration as exc:
                    return exc.value
                else:
                    instruction_result = yield instruction
                    execution_log.append(InstructionLogRecord(instruction, instruction_result))
        except Exception as exc:
            try:
                yield from self._compensate_ephermeral(random, execution_log)
            finally:
                raise exc

    def _execute_ephermeral_forward[ResultT](
        self,
        random: Random,
        generator: OperationGenerator[ReadIntent[Any] | WriteIntent[Any], ResultT],
    ) -> ExecutionPlan[ResultT]:
        intent_result = None
        while True:
            try:
                intent = generator.send(intent_result)
            except StopIteration as exc:
                return (yield from self._process_operation_result(random, exc.value))
            else:
                intent_result = yield self._map_intent_to_instruction(random, intent)

    def _compensate_ephermeral(
        self,
        random: Random,
        execution_log: list[InstructionLogRecord[Any]],
    ) -> ExecutionPlan[None]:
        for record in reversed(execution_log):
            instruction = record.instruction
            result = record.result
            if isinstance(instruction, CallIntentHandlerInstruction) and isinstance(instruction.intent, WriteIntent):
                instruction = CallIntentCompensationHandlerInstruction(
                    context=IntentCompensationContext(
                        intent_id=self._generate_id(random),
                        original_context=instruction.context,
                        original_result=result,
                    ),
                    intent=instruction.intent,
                )
                yield instruction

    def _process_operation_result[ResultT, EventOutT: Event | None](
        self,
        random: Random,
        operation_result: OperationResult[ResultT, EventOutT],
    ) -> ExecutionPlan[ResultT]:
        if self._is_write_intent(operation_result):
            return (yield self._map_intent_to_instruction(random, operation_result))
        elif (
            self._is_event_collection(operation_result)
            or self._is_result_event_collection_pair(operation_result)
            or self._is_database_write_intent_event_collection_pair(operation_result)
        ):
            raise NotImplementedError from None
        else:
            return cast(ResultT, operation_result)

    def _map_intent_to_instruction[ResultT](self, random: Random, intent: Intent[ResultT]) -> Instruction[ResultT]:
        if isinstance(intent, (ReadIntent, WriteIntent)):
            intent_id = self._generate_id(random)
            return CallIntentHandlerInstruction(context=IntentContext(intent_id=intent_id), intent=intent)
        else:
            raise NotImplementedError(intent)

    def _is_write_intent[ResultT, EventOutT: Event | None](
        self,
        result: OperationResult[ResultT, EventOutT],
    ) -> TypeGuard[WriteIntent[ResultT]]:
        return isinstance(result, WriteIntent)

    def _is_event_collection[ResultT, EventOutT: Event | None](
        self,
        result: OperationResult[ResultT, EventOutT],
    ) -> TypeGuard[Collection[EventOutT]]:
        return isinstance(result, Collection) and all(isinstance(item, Event) for item in result)

    def _is_result_event_collection_pair[ResultT, EventOutT: Event | None](
        self,
        result: OperationResult[ResultT, EventOutT],
    ) -> TypeGuard[tuple[ResultT, Collection[EventOutT]]]:
        return (
            isinstance(result, tuple)
            and len(result) == 2
            and not isinstance(result[0], Intent)
            and self._is_event_collection(result[1])
        )

    def _is_database_write_intent_event_collection_pair[ResultT, EventOutT: Event | None](
        self,
        result: OperationResult[ResultT, EventOutT],
    ) -> TypeGuard[
        tuple[DatabaseWriteIntent[ResultT], Collection[EventOutT] | Callable[[ResultT], Collection[EventOutT]]]
    ]:
        return (
            isinstance(result, tuple)
            and len(result) == 2
            and isinstance(result[0], DatabaseWriteIntent)
            and (self._is_event_collection(result[1]) or callable(result[1]))
        )
