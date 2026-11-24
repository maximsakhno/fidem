from collections.abc import Callable, Collection, MutableMapping, Sequence
from dataclasses import dataclass
from typing import Any, ClassVar

from fidem.di.core import (
    MESSAGE_CONTEXT_KEY,
    MESSAGE_KEY,
    BoundProvider,
    DependencyArgument,
    DependencyGraph,
    DependencyKey,
    Node,
    Provider,
    check_generator_function,
    get_arguments,
    get_parameters,
    make_key,
)
from fidem.effects import Effectful
from fidem.messages import Message, QueryEffect

__all__ = [
    "Guard",
    "GuardContainer",
    "GuardDef",
    "GuardProvider",
    "GuardResolutionStep",
    "GuardValues",
    "GuardedFunction",
    "PreparedGuards",
]

type Guard[ValueT] = Callable[..., Effectful[ValueT, QueryEffect]]


@dataclass(slots=True, frozen=True)
class GuardDef[ValueT]:
    guard: Guard[ValueT]
    name: str = ""
    type: object = None
    """The type of the value the guard returns is used when the type is not set."""


@dataclass(slots=True, frozen=True)
class GuardProvider(Provider):
    guard: Guard[Any]

    _RETURN_ORIGINS: ClassVar[Collection[object]] = (Effectful,)

    @classmethod
    def make(cls, guard_def: GuardDef[Any]) -> "GuardProvider":
        guard = guard_def.guard
        check_generator_function(guard)
        return cls(
            key=make_key(
                function=guard,
                return_origins=cls._RETURN_ORIGINS,
                name=guard_def.name,
                dependency_type=guard_def.type,
            ),
            parameters=get_parameters(guard),
            guard=guard,
        )


type GuardValues = MutableMapping[DependencyKey, object]
"""Values of the guards and the builtin values of one message."""


@dataclass(slots=True, frozen=True)
class GuardResolutionStep(BoundProvider[GuardProvider]):
    def run(self, values: GuardValues) -> Effectful[None, QueryEffect]:
        values[self.provider.key] = yield from self.provider.guard(**get_arguments(self.arguments, values))


@dataclass(slots=True, frozen=True)
class GuardedFunction:
    """A function that gets the guard values."""

    function: Callable[..., Effectful[Any, Any]]
    arguments: Sequence[DependencyArgument]

    def call(self, values: GuardValues) -> Effectful[Any, Any]:
        return self.function(**get_arguments(self.arguments, values))


@dataclass(slots=True, frozen=True)
class PreparedGuards:
    steps: Sequence[GuardResolutionStep]
    """Guards the functions need, each one after its guards."""
    functions: Sequence[GuardedFunction]


class GuardContainer:
    """Validates guards and prepares everything needed to run them."""

    _BUILTIN_NODES: ClassVar[Collection[Node]] = (Node(key=MESSAGE_CONTEXT_KEY),)

    def __init__(
        self,
        guard_defs: Collection[GuardDef[Any]] = (),
        message_types: Collection[type[Message[Any]]] = (),
    ) -> None:
        self._graph: DependencyGraph[GuardProvider] = DependencyGraph(
            providers=tuple(GuardProvider.make(guard_def) for guard_def in guard_defs),
            subject_key=MESSAGE_KEY,
            subject_types=message_types,
            builtin_nodes=self._BUILTIN_NODES,
        )

    def prepare(
        self,
        message_type: type[Message[Any]],
        functions: Sequence[Callable[..., Effectful[Any, Any]]],
    ) -> PreparedGuards:
        guarded_functions = tuple(self._bind(message_type, function) for function in functions)
        return PreparedGuards(
            steps=tuple(
                GuardResolutionStep(provider=bound.provider, arguments=bound.arguments)
                for bound in self._graph.collect(
                    arguments=[argument for function in guarded_functions for argument in function.arguments],
                    subject_type=message_type,
                )
            ),
            functions=guarded_functions,
        )

    def _bind(
        self,
        message_type: type[Message[Any]],
        function: Callable[..., Effectful[Any, Any]],
    ) -> GuardedFunction:
        check_generator_function(function)
        return GuardedFunction(
            function=function,
            arguments=self._graph.bind(function, get_parameters(function), message_type),
        )
