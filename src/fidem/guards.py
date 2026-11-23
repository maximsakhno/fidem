from collections.abc import Callable, Collection, Sequence
from dataclasses import dataclass
from enum import IntEnum
from inspect import isgeneratorfunction
from typing import Any, ClassVar, Final, get_args, get_origin

from fidem.di.core import (
    MESSAGE_CONTEXT_KEY,
    MESSAGE_KEY,
    DependencyArgument,
    DependencyDef,
    DependencyGraph,
    DependencyResolver,
    Node,
    ResolutionStep,
    Scope,
    UnsupportedFunctionError,
    UnsupportedReturnTypeError,
    get_arguments,
    get_parameters,
    get_return_type,
    put_message,
)
from fidem.effects import Effect, Effectful, InternalEffect, ReadEffect
from fidem.messages import Message, MessageContext, MessageHandler

type GuardEffect = ReadEffect[Any] | InternalEffect[Any]
"""Effects a guard may yield."""
GUARD_EFFECT_TYPES: Final[tuple[type[Effect[Any]], ...]] = (ReadEffect, InternalEffect)
"""Runtime counterpart of `GuardEffect`."""

type Guard[ValueT] = Callable[..., Effectful[ValueT, GuardEffect]]


class _GuardLifetime(IntEnum):
    """The only lifetime of guard values, the value is the index of the scope in `Scopes`."""

    MESSAGE = 0


class GuardResolver[ValueT](DependencyResolver[_GuardLifetime]):
    def __init__(self, guard: Guard[ValueT]) -> None:
        if not isgeneratorfunction(guard):
            raise UnsupportedFunctionError(guard)
        return_type = get_return_type(guard)
        if get_origin(return_type) is not Effectful:
            raise UnsupportedReturnTypeError(guard, return_type)
        super().__init__(
            parameters=get_parameters(guard),
            dependency_type=get_args(return_type)[0],
            lifetimes=(_GuardLifetime.MESSAGE,),
        )
        self.guard: Final[Guard[ValueT]] = guard


@dataclass(slots=True, frozen=True)
class GuardDef[ValueT]:
    guard: Guard[ValueT]
    name: str = ""
    type: object = None
    """The type of the value the guard returns is used when the type is not set."""


type _GuardDependencyDef = DependencyDef[GuardResolver[Any], _GuardLifetime]


@dataclass(slots=True, frozen=True)
class GuardResolutionStep(ResolutionStep[GuardResolver[Any]]):
    def run(self, scope: Scope) -> Effectful[None, GuardEffect]:
        value = yield from self.resolver.guard(**get_arguments(self.arguments, (scope,)))
        scope.set(self.key, value)


@dataclass(slots=True, frozen=True)
class PreparedMessageHandler:
    handler: MessageHandler[Any, Any]
    steps: Sequence[GuardResolutionStep]
    """Guards the handler needs, each one after its guards."""
    arguments: Sequence[DependencyArgument]

    def call(self, scope: Scope) -> Effectful[Any, Any]:
        return self.handler(**get_arguments(self.arguments, (scope,)))


class GuardContainer:
    """Validates guards and prepares everything needed to run them."""

    _MESSAGE_NODE: ClassVar[Node] = Node(key=MESSAGE_KEY, lifetime=_GuardLifetime.MESSAGE)
    _BUILTIN_NODES: ClassVar[Collection[Node]] = (Node(key=MESSAGE_CONTEXT_KEY, lifetime=_GuardLifetime.MESSAGE),)

    def __init__(
        self,
        guard_defs: Collection[GuardDef[Any]] = (),
        message_types: Collection[type[Message[Any]]] = (),
    ) -> None:
        self._graph: DependencyGraph[_GuardDependencyDef] = DependencyGraph(
            dependency_defs=tuple(self._make_dependency_def(guard_def) for guard_def in guard_defs),
            application_lifetime=None,
            subject_node=self._MESSAGE_NODE,
            subject_types=message_types,
            builtin_nodes=self._BUILTIN_NODES,
        )

    @classmethod
    def _make_dependency_def(cls, guard_def: GuardDef[Any]) -> _GuardDependencyDef:
        return DependencyDef(
            resolver=GuardResolver(guard_def.guard),
            lifetime=_GuardLifetime.MESSAGE,
            name=guard_def.name,
            type=guard_def.type,
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
                GuardResolutionStep.make(bound.dependency_def, bound.arguments) for bound in binding.dependency_defs
            ),
            arguments=binding.arguments,
        )

    @classmethod
    def make_message_scope(cls, message: Message[Any], message_context: MessageContext) -> Scope:
        scope = Scope()
        put_message(scope, message, message_context)
        return scope
