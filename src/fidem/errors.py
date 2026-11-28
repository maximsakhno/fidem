"""Errors of the decorators of the user API, the decorators and `Fidem` raise them before the application runs.

They mean a mistake in the code of the application, so they are caught rarely and are not in the root of the package.
"""

from collections.abc import Callable, Collection
from typing import Any

from fidem.core.errors import Error

__all__ = [
    "AlreadyDeclaredError",
    "HandledTypeError",
    "UndeclaredClassError",
    "UnsupportedDefinitionError",
]


class AlreadyDeclaredError(Error):
    """A function or a class takes one decorator of fidem only, for example it is not a guard and a handler at once."""

    def __init__(self, declared: object) -> None:
        super().__init__(declared)


class UndeclaredClassError(Error):
    """A handler handles a command, an event or an effect whose class has no `@command`, `@event` or `@effect`."""

    def __init__(self, function: Callable[..., object], handled_type: type[Any]) -> None:
        super().__init__(function, handled_type)


class HandledTypeError(Error):
    """A handler has no parameter or several parameters annotated with the type it handles."""

    def __init__(self, function: Callable[..., object], handled_types: Collection[type[Any]]) -> None:
        super().__init__(function, handled_types)


class UnsupportedDefinitionError(Error):
    """The object is neither marked by a decorator nor a definition of the core, for example a plain class."""

    def __init__(self, definition: object) -> None:
        super().__init__(definition)
