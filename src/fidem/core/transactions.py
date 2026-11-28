from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Final, Generic, Never, ParamSpec, TypeVar

from fidem.core._functions import check_generator_function
from fidem.core.effects import ControlEffect, DatabaseEffect, DatabaseReadEffect, Effect, Effectful

if TYPE_CHECKING:
    # Bounds of type parameters are evaluated lazily, and fidem.core.messages imports this module.
    from fidem.core.messages import Event

__all__ = [
    "IMPLICIT_READ_TRANSACTION",
    "IMPLICIT_WRITE_TRANSACTION",
    "AnyTransaction",
    "ReadTransaction",
    "ReadTransactionEffect",
    "ReadTransactionEffectful",
    "TransactionContext",
    "WriteTransaction",
    "WriteTransactionEffect",
    "WriteTransactionEffectful",
]


type ReadTransactionEffectful[ResultT] = Effectful[ResultT, DatabaseReadEffect[Any]]
"""The generator of a read transaction."""
type WriteTransactionEffectful[ResultT, EventT: Event = Never] = Effectful[ResultT, DatabaseEffect[Any] | EventT]
"""The generator of a write transaction, `EventT` are the events of the transaction."""

# Declared explicitly because type checkers infer the type parameters of frozen dataclasses as invariant.
_ParamsT = ParamSpec("_ParamsT")
_ResultT_co = TypeVar("_ResultT_co", covariant=True)
_EventT_co = TypeVar("_EventT_co", bound="Event", covariant=True, default=Never)


@dataclass(frozen=True, slots=True, eq=False)
class ReadTransaction(Generic[_ParamsT, _ResultT_co]):  # noqa: UP046 - type parameters would be invariant
    """Database reads in one transaction, a call makes the effect that runs them."""

    read_only: ClassVar[bool] = True
    effect_types: ClassVar[tuple[type[Effect[Any]], ...]] = (DatabaseReadEffect,)
    """Runtime counterpart of the effects of `ReadTransactionEffectful`."""

    function: Callable[_ParamsT, ReadTransactionEffectful[_ResultT_co]]

    def __post_init__(self) -> None:
        check_generator_function(self.function)

    def __call__(self, *args: _ParamsT.args, **kwargs: _ParamsT.kwargs) -> "ReadTransactionEffect[_ResultT_co]":
        return ReadTransactionEffect(transaction=self, args=args, kwargs=kwargs)


@dataclass(frozen=True, slots=True, eq=False)
class WriteTransaction(Generic[_ParamsT, _ResultT_co, _EventT_co]):
    """Database effects in one transaction, a call makes the effect that runs them."""

    read_only: ClassVar[bool] = False
    effect_types: ClassVar[tuple[type[Effect[Any]], ...]] = (DatabaseEffect,)
    """Runtime counterpart of the effects of `WriteTransactionEffectful`."""

    function: Callable[_ParamsT, WriteTransactionEffectful[_ResultT_co, _EventT_co]]

    def __post_init__(self) -> None:
        check_generator_function(self.function)

    def __call__(
        self,
        *args: _ParamsT.args,
        **kwargs: _ParamsT.kwargs,
    ) -> "WriteTransactionEffect[_ResultT_co, _EventT_co]":
        return WriteTransactionEffect(transaction=self, args=args, kwargs=kwargs)


@dataclass(frozen=True, slots=True)
class ReadTransactionEffect(ControlEffect[_ResultT_co]):
    transaction: ReadTransaction[..., _ResultT_co]
    args: Sequence[object]
    kwargs: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class WriteTransactionEffect(ControlEffect[_ResultT_co], Generic[_ResultT_co, _EventT_co]):
    transaction: WriteTransaction[..., _ResultT_co, _EventT_co]
    args: Sequence[object]
    kwargs: Mapping[str, object]


type AnyTransaction = ReadTransaction[..., Any] | WriteTransaction[..., Any, Any]


def _read_effect[ResultT](effect: DatabaseReadEffect[ResultT]) -> ReadTransactionEffectful[ResultT]:
    return (yield from effect)


def _write_effect[ResultT](effect: DatabaseEffect[ResultT]) -> WriteTransactionEffectful[ResultT]:
    return (yield from effect)


IMPLICIT_READ_TRANSACTION: Final = ReadTransaction(_read_effect)
"""The transaction of a database read outside an explicit transaction, as if the read were called in it."""
IMPLICIT_WRITE_TRANSACTION: Final = WriteTransaction(_write_effect)
"""The transaction of a database write outside an explicit transaction, as if the write were called in it."""


@dataclass(slots=True, frozen=True)
class TransactionContext:
    transaction: AnyTransaction

    @property
    def read_only(self) -> bool:
        return self.transaction.read_only
