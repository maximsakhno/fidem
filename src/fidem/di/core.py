import inspect
from abc import ABC
from collections import defaultdict
from collections.abc import Callable, Collection, Mapping, MutableMapping, MutableSequence, Sequence
from dataclasses import dataclass
from enum import IntEnum
from graphlib import CycleError, TopologicalSorter
from typing import Any, Final, NamedTuple, Self, get_args, get_origin

from fidem.messages import Message, MessageContext


@dataclass(slots=True, frozen=True)
class DependencyKey:
    name: str
    type: object


MESSAGE_KEY: Final = DependencyKey(name="message", type=Message)
MESSAGE_CONTEXT_KEY: Final = DependencyKey(name="message_context", type=MessageContext)


class UnsupportedFunctionError(Exception):
    def __init__(self, function: Callable[..., object]) -> None:
        super().__init__(function)


class UnsupportedParameterError(Exception):
    def __init__(self, function: Callable[..., object], parameter: str) -> None:
        super().__init__(function, parameter)


class MissingAnnotationError(Exception):
    def __init__(self, function: Callable[..., object], parameter: str) -> None:
        super().__init__(function, parameter)


class UnresolvedAnnotationError(Exception):
    def __init__(self, function: Callable[..., object]) -> None:
        super().__init__(function)


class UnsupportedReturnTypeError(Exception):
    def __init__(self, function: Callable[..., object], return_type: object) -> None:
        super().__init__(function, return_type)


class UnsupportedLifetimeError(Exception):
    def __init__(self, dependency_def: "DependencyDef[Any, Any]") -> None:
        super().__init__(dependency_def)


class DuplicateDependencyError(Exception):
    def __init__(self, key: DependencyKey) -> None:
        super().__init__(key)


class DependencyNotFoundError(Exception):
    def __init__(self, consumer: object, parameter: str, parameter_type: object) -> None:
        super().__init__(consumer, parameter, parameter_type)


class AmbiguousDependencyError(Exception):
    def __init__(self, consumer: object, parameter: str, candidates: Collection[DependencyKey]) -> None:
        super().__init__(consumer, parameter, candidates)


class DependencyCycleError(Exception):
    def __init__(self, cycle: Sequence[object]) -> None:
        super().__init__(cycle)


class DependencyLifetimeError(Exception):
    """A dependency requires a dependency with a narrower lifetime."""

    def __init__(self, consumer: object, dependency: DependencyKey) -> None:
        super().__init__(consumer, dependency)


class ScopeNotAvailableError(Exception):
    def __init__(self, lifetime: IntEnum, key: DependencyKey) -> None:
        super().__init__(lifetime, key)


class DependencyResolver[LifetimeT: IntEnum](ABC):
    parameters: Final[Mapping[str, object]]
    """Parameter names mapped to their annotations."""
    dependency_type: Final[object]
    lifetimes: Final[Collection[LifetimeT]]
    """Lifetimes the resolver can be used with."""

    def __init__(
        self,
        parameters: Mapping[str, object],
        dependency_type: object,
        lifetimes: Collection[LifetimeT],
    ) -> None:
        super().__init__()
        self.parameters = parameters
        self.dependency_type = dependency_type
        self.lifetimes = lifetimes


@dataclass(slots=True, frozen=True)
class DependencyDef[ResolverT: DependencyResolver[Any], LifetimeT: IntEnum]:
    resolver: ResolverT
    lifetime: LifetimeT
    name: str = ""
    type: object = None
    """The resolver's dependency type is used when the type is not set."""

    @property
    def key(self) -> DependencyKey:
        dependency_type = self.resolver.dependency_type if self.type is None else self.type
        return DependencyKey(name=self.name, type=dependency_type)


def get_signature(function: Callable[..., object]) -> inspect.Signature:
    try:
        return inspect.signature(function, eval_str=True)
    except NameError as exc:
        raise UnresolvedAnnotationError(function) from exc


def get_parameters(function: Callable[..., object]) -> Mapping[str, object]:
    """Returns the annotations of the parameters that dependency injection passes by name."""
    parameters: MutableMapping[str, object] = {}
    for parameter in get_signature(function).parameters.values():
        if parameter.kind not in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY):
            raise UnsupportedParameterError(function, parameter.name)
        if parameter.annotation is inspect.Parameter.empty:
            raise MissingAnnotationError(function, parameter.name)
        parameters[parameter.name] = parameter.annotation
    return parameters


def get_return_type(function: Callable[..., object]) -> object:
    return_type = get_signature(function).return_annotation
    if return_type is inspect.Signature.empty:
        raise MissingAnnotationError(function, "return")
    return return_type


class Scope:
    """Dependencies of one application, message or effect."""

    def __init__(self) -> None:
        self._dependencies: MutableMapping[DependencyKey, object] = {}

    def __contains__(self, key: DependencyKey) -> bool:
        return key in self._dependencies

    def get(self, key: DependencyKey) -> object:
        return self._dependencies[key]

    def set(self, key: DependencyKey, dependency: object) -> None:
        self._dependencies[key] = dependency


type Scopes = Sequence[Scope | None]
"""Scopes indexed by lifetime, `None` for a scope that does not exist, like the message scope on start."""


def get_scope[ScopeT: Scope](scopes: Sequence[ScopeT | None], lifetime: IntEnum, key: DependencyKey) -> ScopeT:
    scope = scopes[lifetime]
    if scope is None:
        raise ScopeNotAvailableError(lifetime, key)
    return scope


def put_message(scope: Scope, message: Message[Any], message_context: MessageContext) -> None:
    scope.set(MESSAGE_KEY, message)
    scope.set(MESSAGE_CONTEXT_KEY, message_context)


@dataclass(slots=True, frozen=True)
class DependencyArgument:
    consumer: object
    parameter: str
    key: DependencyKey
    lifetime: IntEnum
    required_type: type | None = None
    """The type to check at runtime when the dependency type is known only then."""

    def get(self, scopes: Scopes) -> object:
        dependency = get_scope(scopes, self.lifetime, self.key).get(self.key)
        if self.required_type is not None and not isinstance(dependency, self.required_type):
            raise DependencyNotFoundError(self.consumer, self.parameter, self.required_type)
        return dependency


def get_arguments(arguments: Sequence[DependencyArgument], scopes: Scopes) -> Mapping[str, object]:
    return {argument.parameter: argument.get(scopes) for argument in arguments}


@dataclass(slots=True, frozen=True)
class Node:
    key: DependencyKey
    lifetime: IntEnum
    narrowable: bool = False
    """The node matches its type and its subclasses but not its bases, the exact type is checked at runtime."""


type _Index = Mapping[object, Sequence[Node]]


class BoundDependencyDef[DependencyDefT](NamedTuple):
    dependency_def: DependencyDefT
    arguments: Sequence[DependencyArgument]


@dataclass(slots=True, frozen=True)
class ResolutionStep[ResolverT: DependencyResolver[Any]]:
    """Resolves one dependency from the dependencies in the scopes and puts it into its scope."""

    key: DependencyKey
    lifetime: IntEnum
    resolver: ResolverT
    arguments: Sequence[DependencyArgument]

    @classmethod
    def make(cls, dependency_def: DependencyDef[ResolverT, Any], arguments: Sequence[DependencyArgument]) -> Self:
        return cls(
            key=dependency_def.key,
            lifetime=dependency_def.lifetime,
            resolver=dependency_def.resolver,
            arguments=arguments,
        )


class Binding[DependencyDefT](NamedTuple):
    arguments: Sequence[DependencyArgument]
    dependency_defs: Sequence[BoundDependencyDef[DependencyDefT]]
    """Dependencies the consumer needs besides the application ones, each one after its dependencies."""


class DependencyGraph[DependencyDefT: DependencyDef[Any, Any]]:
    """Validates dependencies and binds parameters to them."""

    # The subject is the builtin dependency whose exact type each consumer knows in advance.

    def __init__(
        self,
        dependency_defs: Collection[DependencyDefT],
        application_lifetime: IntEnum,
        subject_node: Node,
        subject_types: Collection[type],
        builtin_nodes: Collection[Node] = (),
    ) -> None:
        self._application_lifetime: IntEnum = application_lifetime
        self._subject_node: Node = subject_node
        self._builtin_nodes: Collection[Node] = builtin_nodes
        self.dependency_defs: Final[Mapping[DependencyKey, DependencyDefT]] = self._make_key_to_dependency_def(
            dependency_defs
        )
        self._index: _Index = self._make_index()
        index = self._add_subject_to_index(subject_types)
        arguments = {key: self._bind_dependency(key, index) for key in self.dependency_defs}
        self.graph: Final[Mapping[DependencyKey, Collection[DependencyKey]]] = {
            key: tuple(argument.key for argument in key_arguments if argument.key in self.dependency_defs)
            for key, key_arguments in arguments.items()
        }
        order = self.sort(self.graph)
        self.application_dependency_defs: Final[Sequence[BoundDependencyDef[DependencyDefT]]] = tuple(
            BoundDependencyDef(dependency_def=self.dependency_defs[key], arguments=arguments[key])
            for key in order
            if self.dependency_defs[key].lifetime == application_lifetime
        )
        """Application dependencies, each one after its dependencies."""

    def _make_key_to_dependency_def(
        self, dependency_defs: Collection[DependencyDefT]
    ) -> Mapping[DependencyKey, DependencyDefT]:
        key_to_dependency_def: MutableMapping[DependencyKey, DependencyDefT] = {}
        builtin_keys = {self._subject_node.key, *(node.key for node in self._builtin_nodes)}
        for dependency_def in dependency_defs:
            if dependency_def.lifetime not in dependency_def.resolver.lifetimes:
                raise UnsupportedLifetimeError(dependency_def)
            key = dependency_def.key
            if key in key_to_dependency_def or key in builtin_keys:
                raise DuplicateDependencyError(key)
            key_to_dependency_def[key] = dependency_def
        return key_to_dependency_def

    def _make_index(self) -> _Index:
        """Maps each type to the nodes a parameter of this type matches."""
        index: MutableMapping[object, MutableSequence[Node]] = defaultdict(list)
        nodes = [
            Node(key=key, lifetime=dependency_def.lifetime) for key, dependency_def in self.dependency_defs.items()
        ]
        for node in [*nodes, *self._builtin_nodes]:
            for base in (node.key.type,) if node.narrowable else self._get_classes(node.key.type):
                index[base].append(node)
        return index

    def _add_subject_to_index(self, subject_types: Collection[type]) -> _Index:
        subject_index: MutableMapping[object, Sequence[Node]] = dict(self._index)
        for base in {base for subject_type in subject_types for base in self._get_classes(subject_type)}:
            subject_index[base] = (*self._index.get(base, ()), self._subject_node)
        return subject_index

    @classmethod
    def _get_classes(cls, dependency_type: object) -> Collection[object]:
        """Returns the types a dependency of the given type matches."""
        dependency_type = cls._normalize_type(dependency_type)
        if isinstance(dependency_type, type):
            return tuple(base for base in dependency_type.__mro__ if base is not object)
        return (dependency_type,)

    @classmethod
    def _normalize_type(cls, annotation: object) -> object:
        """Treats a generic class parameterized only with `Any` as the class itself."""
        origin = get_origin(annotation)
        if isinstance(origin, type) and all(argument is Any for argument in get_args(annotation)):
            return origin
        return annotation

    def _bind_dependency(self, key: DependencyKey, index: _Index) -> Sequence[DependencyArgument]:
        dependency_def = self.dependency_defs[key]
        arguments = self._bind_parameters(key, dependency_def.resolver.parameters, index)
        for argument in arguments:
            if argument.lifetime > dependency_def.lifetime:
                raise DependencyLifetimeError(key, argument.key)
        return arguments

    def _bind_parameters(
        self,
        consumer: object,
        parameters: Mapping[str, object],
        index: _Index,
    ) -> Sequence[DependencyArgument]:
        return tuple(
            self._bind_parameter(
                consumer=consumer,
                parameter=parameter,
                parameter_type=parameter_type,
                index=index,
            )
            for parameter, parameter_type in parameters.items()
        )

    def _bind_parameter(
        self,
        consumer: object,
        parameter: str,
        parameter_type: object,
        index: _Index,
    ) -> DependencyArgument:
        parameter_type = self._normalize_type(parameter_type)
        exact_candidates = index.get(parameter_type, ())
        # A subclass checked only at runtime is a fallback, so the subject of an exact type wins over it.
        candidates = list(exact_candidates or self._get_narrowable_candidates(parameter_type))

        if len(candidates) > 1:
            named_candidates = [node for node in candidates if node.key.name == parameter]
            if len(named_candidates) != 1:
                raise AmbiguousDependencyError(consumer, parameter, [node.key for node in candidates])
            candidates = named_candidates
        if not candidates:
            raise DependencyNotFoundError(consumer, parameter, parameter_type)

        node = candidates[0]
        return DependencyArgument(
            consumer=consumer,
            parameter=parameter,
            key=node.key,
            lifetime=node.lifetime,
            required_type=(
                parameter_type if node not in exact_candidates and isinstance(parameter_type, type) else None
            ),
        )

    def _get_narrowable_candidates(self, parameter_type: object) -> Sequence[Node]:
        if not isinstance(parameter_type, type):
            return ()
        return [
            node
            for node in self._builtin_nodes
            if node.narrowable and isinstance(node.key.type, type) and issubclass(parameter_type, node.key.type)
        ]

    @classmethod
    def sort(cls, graph: Mapping[DependencyKey, Collection[DependencyKey]]) -> Sequence[DependencyKey]:
        """Orders the dependencies so that each one follows its dependencies."""
        try:
            return tuple(TopologicalSorter(graph).static_order())
        except CycleError as exc:
            raise DependencyCycleError(exc.args[1]) from exc

    def bind(self, consumer: object, parameters: Mapping[str, object], subject_type: type) -> Binding[DependencyDefT]:
        """Binds the parameters of a consumer of the given subject and collects the dependencies it needs."""
        index = self._add_subject_to_index((subject_type,))
        arguments = self._bind_parameters(consumer, parameters, index)

        key_to_arguments: MutableMapping[DependencyKey, Sequence[DependencyArgument]] = {}
        pending = [argument.key for argument in arguments]
        while pending:
            key = pending.pop()
            dependency_def = self.dependency_defs.get(key)
            # Builtin dependencies are put into scopes on creation, application ones are resolved on start.
            if (
                dependency_def is None
                or dependency_def.lifetime == self._application_lifetime
                or key in key_to_arguments
            ):
                continue
            # Binding to the exact subject type can differ from the binding to any subject type.
            key_to_arguments[key] = self._bind_dependency(key, index)
            pending.extend(argument.key for argument in key_to_arguments[key])

        graph = {
            key: tuple(argument.key for argument in key_arguments if argument.key in key_to_arguments)
            for key, key_arguments in key_to_arguments.items()
        }
        dependency_defs = tuple(
            BoundDependencyDef(dependency_def=self.dependency_defs[key], arguments=key_to_arguments[key])
            for key in self.sort(graph)
        )
        return Binding(arguments=arguments, dependency_defs=dependency_defs)
