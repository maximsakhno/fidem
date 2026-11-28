from collections.abc import Callable, Collection, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from typing import Any, ClassVar

from fidem.core.di.core import (
    MESSAGE_CONTEXT_KEY,
    MESSAGE_KEY,
    BoundProvider,
    DependencyArgument,
    DependencyGraph,
    DependencyKey,
    DependencyName,
    DuplicateDependencyError,
    Node,
    Provider,
    check_generator_function,
    get_arguments,
    get_parameters,
    make_key,
)
from fidem.core.effects import Effectful
from fidem.core.errors import Error
from fidem.core.messages import Message, QueryEffect

__all__ = [
    "AmbiguousGuardInputError",
    "Guard",
    "GuardContainer",
    "GuardDef",
    "GuardInputDef",
    "GuardInputNotDefinedError",
    "GuardProvider",
    "GuardResolutionStep",
    "GuardValues",
    "GuardedFunction",
    "MissingGuardInputError",
    "PreparedGuards",
    "UnsupportedGuardInputTypeError",
]

type Guard[ValueT] = Callable[..., Effectful[ValueT, QueryEffect]]


@dataclass(slots=True, frozen=True)
class GuardDef[ValueT]:
    guard: Guard[ValueT]
    name: DependencyName = ""
    type: object = None
    """The type of the value the guard returns is used when the type is not set."""


@dataclass(slots=True, frozen=True)
class GuardInputDef[InputT]:
    """A value that the caller passes into `Environment.handle` along with the message, only guards get it.

    An input is matched by `isinstance` with its type, so the type is a class of its own, not `str` or a `NewType`.
    """

    input_type: type[InputT]


class UnsupportedGuardInputTypeError(Error):
    """`isinstance` cannot check the type of an input, so inputs cannot be matched with it."""

    def __init__(self, input_type: object) -> None:
        super().__init__(input_type)


class GuardInputNotDefinedError(Error):
    """An input matches no declared input."""

    def __init__(self, guard_input: object) -> None:
        super().__init__(type(guard_input))


class AmbiguousGuardInputError(Error):
    """An input matches several declared inputs, or several inputs match one."""

    def __init__(self, input_types: Collection[object]) -> None:
        super().__init__(input_types)


class MissingGuardInputError(Error):
    """The guards of the operation need inputs the caller did not pass."""

    def __init__(self, input_types: Collection[object]) -> None:
        super().__init__(input_types)


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


def _make_input_key(input_type: type[Any]) -> DependencyKey:
    return DependencyKey(name="", type=input_type)


type GuardValues = MutableMapping[DependencyKey, object]
"""Values of the guards, the inputs and the builtin values of one message."""


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
    input_keys: Collection[DependencyKey]
    """Inputs the guards need."""


class GuardContainer:
    """Validates guards and prepares everything needed to run them."""

    _BUILTIN_NODES: ClassVar[Collection[Node]] = (Node(key=MESSAGE_CONTEXT_KEY),)

    def __init__(
        self,
        guard_defs: Collection[GuardDef[Any] | GuardInputDef[Any]] = (),
        message_types: Collection[type[Message[Any]]] = (),
    ) -> None:
        self._input_types: Sequence[type[Any]] = tuple(
            self._check_input_type(guard_def.input_type)
            for guard_def in guard_defs
            if isinstance(guard_def, GuardInputDef)
        )
        self._guard_builtin_nodes: Collection[Node] = (
            *self._BUILTIN_NODES,
            *(Node(key=_make_input_key(input_type)) for input_type in self._input_types),
        )
        providers = tuple(GuardProvider.make(guard_def) for guard_def in guard_defs if isinstance(guard_def, GuardDef))
        self._check_guard_types(providers)
        self._graph: DependencyGraph[GuardProvider] = DependencyGraph(
            providers=providers,
            subject_key=MESSAGE_KEY,
            subject_types=message_types,
            builtin_nodes=self._guard_builtin_nodes,
        )

    @staticmethod
    def _check_input_type(input_type: object) -> type[Any]:
        if not isinstance(input_type, type):
            raise UnsupportedGuardInputTypeError(input_type)
        try:
            isinstance(object(), input_type)
        except TypeError as exc:
            # A protocol that is not runtime checkable or a special form such as `Any` is a class too.
            raise UnsupportedGuardInputTypeError(input_type) from exc
        return input_type

    def _check_guard_types(self, providers: Collection[GuardProvider]) -> None:
        """Rejects a guard whose value could be taken for an input, so a guard never shadows an input."""
        for provider in providers:
            guard_type = provider.key.type
            if isinstance(guard_type, type) and any(
                issubclass(guard_type, input_type) or issubclass(input_type, guard_type)
                for input_type in self._input_types
            ):
                raise DuplicateDependencyError(provider.key)

    def prepare(
        self,
        message_type: type[Message[Any]],
        functions: Sequence[Callable[..., Effectful[Any, Any]]],
    ) -> PreparedGuards:
        """Prepares the guards of a message whose caller passes the inputs."""
        return self._prepare(message_type, functions, self._guard_builtin_nodes)

    def prepare_without_inputs(
        self,
        message_type: type[Message[Any]],
        functions: Sequence[Callable[..., Effectful[Any, Any]]],
    ) -> PreparedGuards:
        """Prepares the guards of a message that has no caller to pass the inputs, such as an event."""
        return self._prepare(message_type, functions, self._BUILTIN_NODES)

    def _prepare(
        self,
        message_type: type[Message[Any]],
        functions: Sequence[Callable[..., Effectful[Any, Any]]],
        guard_builtin_nodes: Collection[Node],
    ) -> PreparedGuards:
        # Only guards get the inputs, the functions get what the guards make of them.
        guarded_functions = tuple(self._bind(message_type, function) for function in functions)
        steps = tuple(
            GuardResolutionStep(provider=bound.provider, arguments=bound.arguments)
            for bound in self._graph.collect(
                arguments=[argument for function in guarded_functions for argument in function.arguments],
                subject_type=message_type,
                builtin_nodes=guard_builtin_nodes,
            )
        )
        input_keys = {_make_input_key(input_type) for input_type in self._input_types}
        return PreparedGuards(
            steps=steps,
            functions=guarded_functions,
            input_keys=frozenset(
                argument.key for step in steps for argument in step.arguments if argument.key in input_keys
            ),
        )

    def match_inputs(
        self,
        inputs: Collection[object],
        required_keys: Collection[DependencyKey],
    ) -> Mapping[DependencyKey, object]:
        """Matches the inputs with the declared ones and checks that the required ones are passed."""
        key_to_input: MutableMapping[DependencyKey, object] = {}
        for guard_input in inputs:
            key = self._match_input(guard_input)
            if key in key_to_input:
                raise AmbiguousGuardInputError([type(key_to_input[key]), type(guard_input)])
            key_to_input[key] = guard_input
        missing_keys = [key for key in required_keys if key not in key_to_input]
        if missing_keys:
            raise MissingGuardInputError([key.type for key in missing_keys])
        return key_to_input

    def _match_input(self, guard_input: object) -> DependencyKey:
        input_types = [input_type for input_type in self._input_types if isinstance(guard_input, input_type)]
        if not input_types:
            raise GuardInputNotDefinedError(guard_input)
        if len(input_types) > 1:
            raise AmbiguousGuardInputError(input_types)
        return _make_input_key(input_types[0])

    def _bind(
        self,
        message_type: type[Message[Any]],
        function: Callable[..., Effectful[Any, Any]],
    ) -> GuardedFunction:
        check_generator_function(function)
        return GuardedFunction(
            function=function,
            arguments=self._graph.bind(
                consumer=function,
                parameters=get_parameters(function),
                subject_type=message_type,
                builtin_nodes=self._BUILTIN_NODES,
            ),
        )
