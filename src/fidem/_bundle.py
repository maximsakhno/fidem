from collections.abc import Callable, Iterable, Iterator, Sequence
from typing import Final

from fidem._decorators import CoreDefinition

__all__ = [
    "Bundle",
    "Definition",
    "flatten",
]

type Definition = Callable[..., object] | CoreDefinition | Bundle
"""A function or a class marked by a decorator of the user API, a definition of the core or a bundle of them."""


class Bundle:
    """Several related definitions that go into `Fidem` together and in their order, for example the parts of a driver.

    A bundle may hold other bundles. Discovery does not find bundles, they are given by hand.
    """

    def __init__(self, *definitions: Definition) -> None:
        self.definitions: Final[Sequence[Definition]] = definitions


def flatten(definitions: Iterable[object]) -> Iterator[object]:
    """Yields the definitions with the contents of the bundles in their place."""
    for definition in definitions:
        if isinstance(definition, Bundle):
            yield from flatten(definition.definitions)
        else:
            yield definition
