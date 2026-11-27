from collections.abc import Collection
from typing import Any

from fidem.effects import Effect
from fidem.errors import Error
from fidem.messages import Event, Message
from fidem.operation_engine.definitions import AnyEventHandlerDef, CommandDef, EventDef, MessageMiddlewareDef, QueryDef

__all__ = [
    "DuplicateCommandDefError",
    "DuplicateEventDefError",
    "DuplicateEventHandlerDefError",
    "DuplicateMessageMiddlewareDefError",
    "DuplicateQueryDefError",
    "EventNotDefinedError",
    "MessageNotDefinedError",
    "UnsupportedAtomicResultError",
    "UnsupportedEffectError",
]


class DuplicateCommandDefError(Error):
    def __init__(self, command_defs: Collection[CommandDef[Any]]) -> None:
        super().__init__(command_defs)


class DuplicateQueryDefError(Error):
    def __init__(self, query_defs: Collection[QueryDef[Any]]) -> None:
        super().__init__(query_defs)


class DuplicateEventDefError(Error):
    """Events have the same type or the same name."""

    def __init__(self, event_defs: Collection[EventDef[Any]]) -> None:
        super().__init__(event_defs)


class DuplicateEventHandlerDefError(Error):
    def __init__(self, handler_defs: Collection[AnyEventHandlerDef]) -> None:
        super().__init__(handler_defs)


class DuplicateMessageMiddlewareDefError(Error):
    def __init__(self, middleware_defs: Collection[MessageMiddlewareDef]) -> None:
        super().__init__(middleware_defs)


class MessageNotDefinedError(Error):
    """No command or query of the type is declared."""

    def __init__(self, message_type: type[Message[Any]]) -> None:
        super().__init__(message_type)


class EventNotDefinedError(Error):
    def __init__(self, event_type: type[Event]) -> None:
        super().__init__(event_type)


class UnsupportedEffectError(Error):
    def __init__(self, effect: Effect[Any]) -> None:
        super().__init__(effect)


class UnsupportedAtomicResultError(Error):
    """An atomic handler returned neither a write transaction effect nor an external write effect."""

    def __init__(self, result: object) -> None:
        super().__init__(result)
