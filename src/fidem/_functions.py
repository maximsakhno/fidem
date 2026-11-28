"""Reduces the shorter forms of functions of the user API to the only forms the core accepts.

A wrapper gets the signature of the function with the return type the core expects, so dependency injection binds
its parameters as before. The signature is read when `Fidem` resolves the declaration, not when the function is
decorated, so annotations may refer to classes declared below.
"""

from collections.abc import AsyncGenerator, Callable, Generator
from functools import wraps
from inspect import isasyncgenfunction, iscoroutinefunction, isfunction, isgeneratorfunction
from typing import Any, Never

from fidem.core.di.core import get_return_type, get_signature
from fidem.core.effects import Effectful

__all__ = [
    "as_effectful",
    "as_factory",
    "is_plain_function",
]


def is_plain_function(function: object) -> bool:
    """Tells a synchronous function that returns its result at once, a generator or an async function is not one."""
    return (
        isfunction(function)
        and not isgeneratorfunction(function)
        and not iscoroutinefunction(function)
        and not isasyncgenfunction(function)
    )


def as_effectful(function: Callable[..., object]) -> Callable[..., Any]:
    """Makes a generator function of a plain function, a generator function or another callable stays as it is.

    The core rejects an async function or a class later, so they are not wrapped into a generator that returns
    a coroutine or an instance.
    """
    if not is_plain_function(function):
        return function

    @wraps(function)
    def effectful(*args: object, **kwargs: object) -> Generator[Never, object, object]:
        yield from ()
        return function(*args, **kwargs)

    return _returning(effectful, function, Effectful[get_return_type(function), Never])


def as_factory(function: Callable[..., object]) -> Callable[..., Any]:
    """Makes the factory of a dependency of an async function, another callable stays as it is."""
    if not iscoroutinefunction(function):
        return function

    @wraps(function)
    async def factory(*args: object, **kwargs: object) -> AsyncGenerator[object]:
        yield await function(*args, **kwargs)

    return _returning(factory, function, AsyncGenerator[get_return_type(function)])


def _returning[WrapperT: Callable[..., object]](
    wrapper: WrapperT,
    function: Callable[..., object],
    return_type: object,
) -> WrapperT:
    """Gives the wrapper the signature of the function with another return type."""
    wrapper.__signature__ = get_signature(function).replace(
        return_annotation=return_type,
    )
    return wrapper
