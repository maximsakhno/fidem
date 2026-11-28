from collections import defaultdict
from collections.abc import Callable, Collection, Mapping, MutableMapping, MutableSequence, Sequence
from dataclasses import dataclass
from graphlib import CycleError, TopologicalSorter
from inspect import Parameter, Signature, signature
from typing import Any, Final, get_args, get_origin

from fidem.core._functions import UnsupportedFunctionError, check_generator_function
from fidem.core._validation import check_unique
from fidem.core.errors import Error
from fidem.core.messages import Message, MessageContext

__all__ = [
    "MESSAGE_CONTEXT_KEY",
    "MESSAGE_KEY",
    "AmbiguousDependencyError",
    "BoundProvider",
    "DependencyArgument",
    "DependencyCycleError",
    "DependencyGraph",
    "DependencyKey",
    "DependencyName",
    "DependencyNotFoundError",
    "DuplicateDependencyError",
    "MissingAnnotationError",
    "Node",
    "ParameterName",
    "Provider",
    "UnresolvedAnnotationError",
    "UnsupportedFunctionError",
    "UnsupportedParameterError",
    "UnsupportedReturnTypeError",
    "check_generator_function",
    "get_arguments",
    "get_parameters",
    "make_key",
    "make_message_dependencies",
]


type DependencyName = str
type ParameterName = str


@dataclass(slots=True, frozen=True)
class DependencyKey:
    name: DependencyName
    type: object


MESSAGE_KEY: Final = DependencyKey(name="message", type=Message)
MESSAGE_CONTEXT_KEY: Final = DependencyKey(name="message_context", type=MessageContext)


class UnsupportedParameterError(Error):
    def __init__(self, function: Callable[..., object], parameter: ParameterName) -> None:
        super().__init__(function, parameter)


class MissingAnnotationError(Error):
    def __init__(self, function: Callable[..., object], parameter: ParameterName) -> None:
        super().__init__(function, parameter)


class UnresolvedAnnotationError(Error):
    def __init__(self, function: Callable[..., object]) -> None:
        super().__init__(function)


class UnsupportedReturnTypeError(Error):
    def __init__(self, function: Callable[..., object], return_type: object) -> None:
        super().__init__(function, return_type)


class DuplicateDependencyError(Error):
    def __init__(self, key: DependencyKey) -> None:
        super().__init__(key)


class DependencyNotFoundError(Error):
    def __init__(self, consumer: object, parameter: ParameterName, parameter_type: object) -> None:
        super().__init__(consumer, parameter, parameter_type)


class AmbiguousDependencyError(Error):
    def __init__(self, consumer: object, parameter: ParameterName, candidates: Collection[DependencyKey]) -> None:
        super().__init__(consumer, parameter, candidates)


class DependencyCycleError(Error):
    def __init__(self, cycle: Sequence[object]) -> None:
        super().__init__(cycle)


@dataclass(slots=True, frozen=True)
class Provider:
    """Provides the dependency with the key from the dependencies bound to its parameters."""

    key: DependencyKey
    parameters: Mapping[ParameterName, object]
    """Annotations of the parameters."""


def _get_signature(function: Callable[..., object]) -> Signature:
    try:
        return signature(function, eval_str=True)
    except NameError as exc:
        raise UnresolvedAnnotationError(function) from exc


def get_parameters(function: Callable[..., object]) -> Mapping[ParameterName, object]:
    """Returns the annotations of the parameters that dependency injection passes by name."""
    parameters: MutableMapping[ParameterName, object] = {}
    for parameter in _get_signature(function).parameters.values():
        if parameter.kind not in (Parameter.POSITIONAL_OR_KEYWORD, Parameter.KEYWORD_ONLY):
            raise UnsupportedParameterError(function, parameter.name)
        if parameter.annotation is Parameter.empty:
            raise MissingAnnotationError(function, parameter.name)
        parameters[parameter.name] = parameter.annotation
    return parameters


def _get_return_type(function: Callable[..., object]) -> object:
    return_type = _get_signature(function).return_annotation
    if return_type is Signature.empty:
        raise MissingAnnotationError(function, "return")
    return return_type


def make_key(
    function: Callable[..., object],
    return_origins: Collection[object],
    name: DependencyName,
    dependency_type: object,
) -> DependencyKey:
    """Makes the key of the value the function produces."""
    return_type = _get_return_type(function)
    if get_origin(return_type) not in return_origins:
        raise UnsupportedReturnTypeError(function, return_type)
    return DependencyKey(name=name, type=get_args(return_type)[0] if dependency_type is None else dependency_type)


def make_message_dependencies(
    message: Message[Any],
    message_context: MessageContext,
) -> MutableMapping[DependencyKey, object]:
    return {MESSAGE_KEY: message, MESSAGE_CONTEXT_KEY: message_context}


@dataclass(slots=True, frozen=True)
class DependencyArgument:
    consumer: object
    parameter: ParameterName
    key: DependencyKey
    required_type: type | None = None
    """The type to check at runtime when the dependency type is known only then."""

    def get(self, dependencies: Mapping[DependencyKey, object]) -> object:
        dependency = dependencies[self.key]
        if self.required_type is not None and not isinstance(dependency, self.required_type):
            raise DependencyNotFoundError(self.consumer, self.parameter, self.required_type)
        return dependency


def get_arguments(
    arguments: Sequence[DependencyArgument],
    dependencies: Mapping[DependencyKey, object],
) -> Mapping[ParameterName, object]:
    return {argument.parameter: argument.get(dependencies) for argument in arguments}


@dataclass(slots=True, frozen=True)
class Node:
    key: DependencyKey
    narrowable: bool = False
    """The node matches its type and its subclasses but not its bases, the exact type is checked at runtime."""


@dataclass(frozen=True, slots=True)
class _Index:
    type_to_nodes: Mapping[object, Sequence[Node]]
    """Nodes a parameter of each type matches."""
    builtin_nodes: Collection[Node]
    """Builtin nodes the consumer may get."""


@dataclass(frozen=True, slots=True)
class BoundProvider[ProviderT: Provider]:
    provider: ProviderT
    arguments: Sequence[DependencyArgument]


class DependencyGraph[ProviderT: Provider]:
    """Validates dependencies and binds parameters to them."""

    # The subject is the builtin dependency whose exact type each consumer knows in advance.
    # Providers may get every builtin node, a consumer may get only some of them.

    def __init__(
        self,
        providers: Collection[ProviderT],
        subject_key: DependencyKey,
        subject_types: Collection[type],
        builtin_nodes: Collection[Node] = (),
    ) -> None:
        self._subject_node: Node = Node(key=subject_key)
        self._builtin_nodes: Collection[Node] = builtin_nodes
        self.providers: Final[Mapping[DependencyKey, ProviderT]] = self._make_key_to_provider(providers)
        index = self._make_index(subject_types, builtin_nodes)
        self.arguments: Final[Mapping[DependencyKey, Sequence[DependencyArgument]]] = {
            key: self._bind_parameters(key, provider.parameters, index) for key, provider in self.providers.items()
        }
        """Arguments of each provider bound to any subject type."""
        self.dependencies: Final[Mapping[DependencyKey, Collection[DependencyKey]]] = self._make_graph(self.arguments)
        """Providers each provider depends on."""
        self.order: Final[Sequence[DependencyKey]] = self._sort(self.dependencies)
        """Providers, each one after its dependencies."""

    def _make_key_to_provider(self, providers: Collection[ProviderT]) -> Mapping[DependencyKey, ProviderT]:
        check_unique(
            defs=[
                self._subject_node.key,
                *(node.key for node in self._builtin_nodes),
                *(provider.key for provider in providers),
            ],
            get_key=lambda key: key,
            duplicate_error_type=lambda keys: DuplicateDependencyError(keys[0]),
        )
        return {provider.key: provider for provider in providers}

    def _make_index(self, subject_types: Collection[type], builtin_nodes: Collection[Node]) -> _Index:
        type_to_nodes: MutableMapping[object, MutableSequence[Node]] = defaultdict(list)
        for node in [*(Node(key=key) for key in self.providers), *builtin_nodes]:
            for base in (node.key.type,) if node.narrowable else self._get_classes(node.key.type):
                type_to_nodes[base].append(node)
        for base in {base for subject_type in subject_types for base in self._get_classes(subject_type)}:
            type_to_nodes[base].append(self._subject_node)
        return _Index(type_to_nodes=type_to_nodes, builtin_nodes=builtin_nodes)

    @classmethod
    def _get_classes(cls, dependency_type: object) -> Collection[object]:
        """Returns the types a dependency of the given type matches."""
        dependency_type = cls._normalize_type(dependency_type)
        if isinstance(dependency_type, type):
            return tuple(base for base in dependency_type.__mro__ if base is not object)
        return (dependency_type,)

    @staticmethod
    def _normalize_type(annotation: object) -> object:
        """Treats a generic class parameterized only with `Any` as the class itself."""
        origin = get_origin(annotation)
        if isinstance(origin, type) and all(argument is Any for argument in get_args(annotation)):
            return origin
        return annotation

    def _bind_parameters(
        self,
        consumer: object,
        parameters: Mapping[ParameterName, object],
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
        parameter: ParameterName,
        parameter_type: object,
        index: _Index,
    ) -> DependencyArgument:
        parameter_type = self._normalize_type(parameter_type)
        exact_candidates = index.type_to_nodes.get(parameter_type, ())
        # A subclass checked only at runtime is a fallback, so the subject of an exact type wins over it.
        candidates = list(exact_candidates or self._get_narrowable_candidates(parameter_type, index.builtin_nodes))

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
            required_type=(
                parameter_type if node not in exact_candidates and isinstance(parameter_type, type) else None
            ),
        )

    @staticmethod
    def _get_narrowable_candidates(parameter_type: object, builtin_nodes: Collection[Node]) -> Sequence[Node]:
        if not isinstance(parameter_type, type):
            return ()
        return [
            node
            for node in builtin_nodes
            if node.narrowable and isinstance(node.key.type, type) and issubclass(parameter_type, node.key.type)
        ]

    def _make_graph(
        self,
        arguments: Mapping[DependencyKey, Sequence[DependencyArgument]],
    ) -> Mapping[DependencyKey, Collection[DependencyKey]]:
        """Maps each provider to the providers it depends on."""
        return {
            key: tuple(argument.key for argument in key_arguments if argument.key in self.providers)
            for key, key_arguments in arguments.items()
        }

    @staticmethod
    def _sort(graph: Mapping[DependencyKey, Collection[DependencyKey]]) -> Sequence[DependencyKey]:
        """Orders the dependencies so that each one follows its dependencies."""
        try:
            return tuple(TopologicalSorter(graph).static_order())
        except CycleError as exc:
            raise DependencyCycleError(exc.args[1]) from exc

    def bind(
        self,
        consumer: object,
        parameters: Mapping[ParameterName, object],
        subject_type: type,
        builtin_nodes: Collection[Node] | None = None,
    ) -> Sequence[DependencyArgument]:
        """Binds the parameters of a consumer of the given subject, by default to any builtin node."""
        index = self._make_index((subject_type,), self._builtin_nodes if builtin_nodes is None else builtin_nodes)
        return self._bind_parameters(consumer, parameters, index)

    def collect(
        self,
        arguments: Collection[DependencyArgument],
        subject_type: type,
        builtin_nodes: Collection[Node] | None = None,
    ) -> Sequence[BoundProvider[ProviderT]]:
        """Collects the providers the arguments need, each one after its dependencies, by default with any builtin node."""
        index = self._make_index((subject_type,), self._builtin_nodes if builtin_nodes is None else builtin_nodes)
        key_to_arguments: MutableMapping[DependencyKey, Sequence[DependencyArgument]] = {}
        pending = [argument.key for argument in arguments]
        while pending:
            key = pending.pop()
            provider = self.providers.get(key)
            # Builtin dependencies have no providers.
            if provider is None or key in key_to_arguments:
                continue
            # Binding to the exact subject type can differ from the binding to any subject type.
            key_to_arguments[key] = self._bind_parameters(key, provider.parameters, index)
            pending.extend(argument.key for argument in key_to_arguments[key])

        return tuple(
            BoundProvider(provider=self.providers[key], arguments=key_to_arguments[key])
            for key in self._sort(self._make_graph(key_to_arguments))
        )
