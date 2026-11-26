from collections.abc import Callable
from inspect import isgeneratorfunction

from fidem.errors import Error

__all__ = [
    "UnsupportedFunctionError",
    "check_generator_function",
]


class UnsupportedFunctionError(Error):
    def __init__(self, function: Callable[..., object]) -> None:
        super().__init__(function)


def check_generator_function(function: Callable[..., object]) -> None:
    if not isgeneratorfunction(function):
        raise UnsupportedFunctionError(function)
