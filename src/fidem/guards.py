from collections.abc import Callable, Collection, MutableMapping, Sequence
from dataclasses import dataclass
from inspect import isgeneratorfunction
from typing import Any, ClassVar, Final

from fidem.di.core import (
    MESSAGE_CONTEXT_KEY,
    MESSAGE_KEY,
    BoundProvider,
    DependencyArgument,
    DependencyGraph,
    DependencyKey,
    Node,
    Provider,
    UnsupportedFunctionError,
    get_arguments,
    get_parameters,
    make_key,
)
from fidem.effects import Effect, Effectful, InternalEffect, ReadEffect
from fidem.messages import Message, MessageHandler

type GuardEffect = ReadEffect[Any] | InternalEffect[Any]
"""Effects a guard may yield."""
GUARD_EFFECT_TYPES: Final[tuple[type[Effect[Any]], ...]] = (ReadEffect, InternalEffect)
"""Runtime counterpart of `GuardEffect`."""

type Guard[ValueT] = Callable[..., Effectful[ValueT, GuardEffect]]


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
        if not isgeneratorfunction(guard):
            raise UnsupportedFunctionError(guard)
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
    def run(self, values: GuardValues) -> Effectful[None, GuardEffect]:
        values[self.provider.key] = yield from self.provider.guard(**get_arguments(self.arguments, values))


@dataclass(slots=True, frozen=True)
class PreparedMessageHandler:
    handler: MessageHandler[Any, Any]
    steps: Sequence[GuardResolutionStep]
    """Guards the handler needs, each one after its guards."""
    arguments: Sequence[DependencyArgument]

    def call(self, values: GuardValues) -> Effectful[Any, Any]:
        return self.handler(**get_arguments(self.arguments, values))


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

    def prepare_message_handler(
        self,
        message_type: type[Message[Any]],
        handler: MessageHandler[Any, Any],
    ) -> PreparedMessageHandler:
        if not isgeneratorfunction(handler):
            raise UnsupportedFunctionError(handler)
        binding = self._graph.bind(handler, get_parameters(handler), message_type)
        return PreparedMessageHandler(
            handler=handler,
            steps=tuple(
                GuardResolutionStep(provider=bound.provider, arguments=bound.arguments) for bound in binding.providers
            ),
            arguments=binding.arguments,
        )
