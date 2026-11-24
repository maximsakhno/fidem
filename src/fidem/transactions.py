from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Never, ParamSpec, TypeVar

from fidem.effects import ControlEffect, DatabaseEffect, DatabaseReadEffect, Effect, Effectful

if TYPE_CHECKING:
    # Bounds of type parameters are evaluated lazily, and fidem.messages imports this module.
    from fidem.messages import Event

__all__ = [
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
ParamsT = ParamSpec("ParamsT")
ResultT_co = TypeVar("ResultT_co", covariant=True)
EventT_co = TypeVar("EventT_co", bound="Event", covariant=True, default=Never)


def check_transaction_function(function: Callable[..., object]) -> None:
    # Imported here because fidem.di.core imports this module through fidem.messages.
    from fidem.di.core import check_generator_function

    check_generator_function(function)


@dataclass(frozen=True, slots=True, eq=False)
class ReadTransaction(Generic[ParamsT, ResultT_co]):  # noqa: UP046 - type parameters would be invariant
    """Database reads in one transaction, a call makes the effect that runs them."""

    read_only: ClassVar[bool] = True
    effect_types: ClassVar[tuple[type[Effect[Any]], ...]] = (DatabaseReadEffect,)
    """Runtime counterpart of the effects of `ReadTransactionEffectful`."""

    function: Callable[ParamsT, ReadTransactionEffectful[ResultT_co]]

    def __post_init__(self) -> None:
        check_transaction_function(self.function)

    def __call__(self, *args: ParamsT.args, **kwargs: ParamsT.kwargs) -> "ReadTransactionEffect[ResultT_co]":
        return ReadTransactionEffect(transaction=self, args=args, kwargs=kwargs)


@dataclass(frozen=True, slots=True, eq=False)
class WriteTransaction(Generic[ParamsT, ResultT_co, EventT_co]):
    """Database effects in one transaction, a call makes the effect that runs them."""

    read_only: ClassVar[bool] = False
    effect_types: ClassVar[tuple[type[Effect[Any]], ...]] = (DatabaseEffect,)
    """Runtime counterpart of the effects of `WriteTransactionEffectful`."""

    function: Callable[ParamsT, WriteTransactionEffectful[ResultT_co, EventT_co]]

    def __post_init__(self) -> None:
        check_transaction_function(self.function)

    def __call__(
        self,
        *args: ParamsT.args,
        **kwargs: ParamsT.kwargs,
    ) -> "WriteTransactionEffect[ResultT_co, EventT_co]":
        return WriteTransactionEffect(transaction=self, args=args, kwargs=kwargs)


@dataclass(frozen=True, slots=True)
class ReadTransactionEffect(ControlEffect[ResultT_co]):
    transaction: ReadTransaction[..., ResultT_co]
    args: Sequence[object]
    kwargs: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class WriteTransactionEffect(ControlEffect[ResultT_co], Generic[ResultT_co, EventT_co]):
    transaction: WriteTransaction[..., ResultT_co, EventT_co]
    args: Sequence[object]
    kwargs: Mapping[str, object]


type AnyTransaction = ReadTransaction[..., Any] | WriteTransaction[..., Any, Any]


@dataclass(slots=True, frozen=True)
class TransactionContext:
    read_only: bool
    transaction: AnyTransaction | None
    """The explicit transaction, `None` for the implicit one around a single effect."""
