from abc import ABC, abstractmethod
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass
from enum import IntEnum
from inspect import isasyncgenfunction, iscoroutinefunction, isfunction, isgeneratorfunction, ismethod
from typing import Any, ClassVar, Final, get_args, get_origin, override

from fidem.di.core import (
    MESSAGE_CONTEXT_KEY,
    MESSAGE_KEY,
    BoundDependencyDef,
    DependencyArgument,
    DependencyDef,
    DependencyGraph,
    DependencyResolver,
    Node,
    ResolutionStep,
    Scope,
    Scopes,
    UnsupportedFunctionError,
    UnsupportedLifetimeError,
    UnsupportedReturnTypeError,
    get_arguments,
    get_parameters,
    get_return_type,
    put_message,
)
from fidem.effects import Effect, Effectful, InternalEffect, ReadEffect
from fidem.messages import Message, MessageContext, MessageHandler

type DependencyEffect = ReadEffect[Any] | InternalEffect[Any]
"""Effects a message dependency factory may yield."""
DEPENDENCY_EFFECT_TYPES: Final[tuple[type[Effect[Any]], ...]] = (ReadEffect, InternalEffect)
"""Runtime counterpart of `DependencyEffect`."""


class MessageDependencyLifetime(IntEnum):
    """Lifetimes from the widest to the narrowest, the value is the index of the scope in `Scopes`."""

    APPLICATION = 0
    MESSAGE = 1


class MessageDependencyResolver[DependencyT](DependencyResolver[MessageDependencyLifetime], ABC):
    # Application dependencies are resolved outside any message, where there is no message id for effect ids.
    EFFECTFUL_LIFETIMES: ClassVar[Collection[MessageDependencyLifetime]] = (MessageDependencyLifetime.MESSAGE,)

    def __init__(
        self,
        parameters: Mapping[str, object],
        dependency_type: object,
        lifetimes: Collection[MessageDependencyLifetime] = EFFECTFUL_LIFETIMES,
    ) -> None:
        super().__init__(parameters=parameters, dependency_type=dependency_type, lifetimes=lifetimes)

    @abstractmethod
    def resolve(self, arguments: Mapping[str, object]) -> Effectful[DependencyT, DependencyEffect]:
        raise NotImplementedError


class ValueMessageDependencyResolver[DependencyT](MessageDependencyResolver[DependencyT], ABC):
    """Resolves a dependency without effects, so it can be resolved outside any message."""

    ALL_LIFETIMES: ClassVar[Collection[MessageDependencyLifetime]] = tuple(MessageDependencyLifetime)

    def __init__(self, parameters: Mapping[str, object], dependency_type: object) -> None:
        super().__init__(parameters=parameters, dependency_type=dependency_type, lifetimes=self.ALL_LIFETIMES)

    @abstractmethod
    def resolve_value(self, arguments: Mapping[str, object]) -> DependencyT:
        raise NotImplementedError

    @override
    def resolve(self, arguments: Mapping[str, object]) -> Effectful[DependencyT, DependencyEffect]:
        yield from ()
        return self.resolve_value(arguments)


class InstanceMessageDependencyResolver[DependencyT](ValueMessageDependencyResolver[DependencyT]):
    def __init__(self, instance: DependencyT) -> None:
        super().__init__(parameters={}, dependency_type=type(instance))
        self._instance: DependencyT = instance

    @override
    def resolve_value(self, arguments: Mapping[str, object]) -> DependencyT:
        return self._instance


class FunctionMessageDependencyResolver[DependencyT](ValueMessageDependencyResolver[DependencyT]):
    def __init__(self, function: Callable[..., DependencyT]) -> None:
        if (
            not (isfunction(function) or ismethod(function))
            or isgeneratorfunction(function)
            or iscoroutinefunction(function)
            or isasyncgenfunction(function)
        ):
            raise UnsupportedFunctionError(function)
        super().__init__(parameters=get_parameters(function), dependency_type=get_return_type(function))
        self._function: Callable[..., DependencyT] = function

    @override
    def resolve_value(self, arguments: Mapping[str, object]) -> DependencyT:
        return self._function(**arguments)


class GeneratorFunctionMessageDependencyResolver[DependencyT](MessageDependencyResolver[DependencyT]):
    def __init__(self, function: Callable[..., Effectful[DependencyT, DependencyEffect]]) -> None:
        if not isgeneratorfunction(function):
            raise UnsupportedFunctionError(function)
        return_type = get_return_type(function)
        if get_origin(return_type) is not Effectful:
            raise UnsupportedReturnTypeError(function, return_type)
        super().__init__(parameters=get_parameters(function), dependency_type=get_args(return_type)[0])
        self._function: Callable[..., Effectful[DependencyT, DependencyEffect]] = function

    @override
    def resolve(self, arguments: Mapping[str, object]) -> Effectful[DependencyT, DependencyEffect]:
        return self._function(**arguments)


@dataclass(slots=True, frozen=True)
class MessageDependencyDef[DependencyT](
    DependencyDef[MessageDependencyResolver[DependencyT], MessageDependencyLifetime]
):
    lifetime: MessageDependencyLifetime = MessageDependencyLifetime.MESSAGE


@dataclass(slots=True, frozen=True)
class MessageResolutionStep(ResolutionStep[MessageDependencyResolver[Any]]):
    def run(self, scopes: Scopes) -> Effectful[None, DependencyEffect]:
        dependency = yield from self.resolver.resolve(get_arguments(self.arguments, scopes))
        scopes[self.lifetime].set(self.key, dependency)


@dataclass(slots=True, frozen=True)
class ApplicationResolutionStep(ResolutionStep[ValueMessageDependencyResolver[Any]]):
    def run(self, scope: Scope) -> None:
        scope.set(self.key, self.resolver.resolve_value(get_arguments(self.arguments, (scope,))))


@dataclass(slots=True, frozen=True)
class PreparedMessageHandler:
    handler: MessageHandler[Any, Any]
    steps: Sequence[MessageResolutionStep]
    """Message dependencies the handler needs, each one after its dependencies."""
    arguments: Sequence[DependencyArgument]

    def call(self, scopes: Scopes) -> Effectful[Any, Any]:
        return self.handler(**get_arguments(self.arguments, scopes))


type _MessageDependencyGraph = DependencyGraph[MessageDependencyDef[Any]]


class MessageDependencyContainer:
    """Validates the dependencies of message handlers and prepares everything needed to resolve them."""

    _MESSAGE_NODE: ClassVar[Node] = Node(key=MESSAGE_KEY, lifetime=MessageDependencyLifetime.MESSAGE)
    _BUILTIN_NODES: ClassVar[Collection[Node]] = (
        Node(key=MESSAGE_CONTEXT_KEY, lifetime=MessageDependencyLifetime.MESSAGE),
    )

    def __init__(
        self,
        dependency_defs: Collection[MessageDependencyDef[Any]] = (),
        message_types: Collection[type[Message[Any]]] = (),
    ) -> None:
        self._graph: _MessageDependencyGraph = DependencyGraph(
            dependency_defs=dependency_defs,
            application_lifetime=MessageDependencyLifetime.APPLICATION,
            subject_node=self._MESSAGE_NODE,
            subject_types=message_types,
            builtin_nodes=self._BUILTIN_NODES,
        )
        self.application_steps: Final[Sequence[ApplicationResolutionStep]] = tuple(
            self._make_application_step(bound) for bound in self._graph.application_dependency_defs
        )
        """Application dependencies, each one after its dependencies."""

    @classmethod
    def _make_application_step(
        cls,
        bound: BoundDependencyDef[MessageDependencyDef[Any]],
    ) -> ApplicationResolutionStep:
        resolver = bound.dependency_def.resolver
        if not isinstance(resolver, ValueMessageDependencyResolver):
            raise UnsupportedLifetimeError(bound.dependency_def)
        return ApplicationResolutionStep(
            key=bound.dependency_def.key,
            lifetime=bound.dependency_def.lifetime,
            resolver=resolver,
            arguments=bound.arguments,
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
                MessageResolutionStep.make(bound.dependency_def, bound.arguments) for bound in binding.dependency_defs
            ),
            arguments=binding.arguments,
        )

    @classmethod
    def make_message_scope(cls, message: Message[Any], message_context: MessageContext) -> Scope:
        scope = Scope()
        put_message(scope, message, message_context)
        return scope
