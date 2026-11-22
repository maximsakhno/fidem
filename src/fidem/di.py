import asyncio
import inspect
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    Collection,
    Mapping,
    MutableMapping,
    MutableSequence,
    Sequence,
)
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from enum import IntEnum
from graphlib import CycleError, TopologicalSorter
from types import TracebackType
from typing import Any, ClassVar, Final, Self, get_args, get_origin, override

from fidem.effects import Effect, EffectContext
from fidem.messages import Message, MessageContext


@dataclass(slots=True, frozen=True)
class DependencyKey:
    name: str
    type: object


MESSAGE_KEY: Final = DependencyKey(name="message", type=Message)
MESSAGE_CONTEXT_KEY: Final = DependencyKey(name="message_context", type=MessageContext)
EFFECT_KEY: Final = DependencyKey(name="effect", type=Effect)
EFFECT_CONTEXT_KEY: Final = DependencyKey(name="effect_context", type=EffectContext)


class DependencyLifetime(IntEnum):
    """Lifetimes from the widest to the narrowest, the value is the index of the scope in `Scopes`."""

    APPLICATION = 0
    MESSAGE = 1
    EFFECT = 2


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
    def __init__(self, dependency: "Dependency[Any]") -> None:
        super().__init__(dependency)


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


class DependencyGuard[DependencyT](ABC):
    dependency: Final[DependencyT]

    def __init__(self, dependency: DependencyT) -> None:
        super().__init__()
        self.dependency = dependency

    @abstractmethod
    async def close(self, exc: BaseException | None) -> None:
        raise NotImplementedError


class ValueDependencyGuard[DependencyT](DependencyGuard[DependencyT]):
    @override
    async def close(self, exc: BaseException | None) -> None:
        pass


class AsyncContextManagerDependencyGuard[DependencyT](DependencyGuard[DependencyT]):
    def __init__(self, dependency: DependencyT, context_manager: AbstractAsyncContextManager[DependencyT]) -> None:
        super().__init__(dependency)
        self._context_manager: AbstractAsyncContextManager[DependencyT] = context_manager

    @override
    async def close(self, exc: BaseException | None) -> None:
        if exc is None:
            await self._context_manager.__aexit__(None, None, None)
        else:
            # The context manager may suppress the error, but the scope exits with it anyway.
            await self._context_manager.__aexit__(type(exc), exc, exc.__traceback__)


class DependencyResolver[DependencyT](ABC):
    ALL_LIFETIMES: ClassVar[Collection[DependencyLifetime]] = tuple(DependencyLifetime)

    parameters: Final[Mapping[str, object]]
    """Parameter names mapped to their annotations."""
    dependency_type: Final[object]
    lifetimes: Final[Collection[DependencyLifetime]]
    """Lifetimes the resolver can be used with."""

    def __init__(
        self,
        parameters: Mapping[str, object],
        dependency_type: object,
        lifetimes: Collection[DependencyLifetime] = ALL_LIFETIMES,
    ) -> None:
        super().__init__()
        self.parameters = parameters
        self.dependency_type = dependency_type
        self.lifetimes = lifetimes

    @abstractmethod
    async def resolve(self, arguments: Mapping[str, object]) -> DependencyGuard[DependencyT]:
        raise NotImplementedError


class InstanceDependencyResolver[DependencyT](DependencyResolver[DependencyT]):
    def __init__(self, instance: DependencyT) -> None:
        super().__init__(
            parameters={},
            dependency_type=type(instance),
            lifetimes=(DependencyLifetime.APPLICATION,),
        )
        self._instance: DependencyT = instance

    @override
    async def resolve(self, arguments: Mapping[str, object]) -> DependencyGuard[DependencyT]:
        return ValueDependencyGuard(self._instance)


class AsyncFunctionDependencyResolver[DependencyT](DependencyResolver[DependencyT]):
    def __init__(self, function: Callable[..., Awaitable[DependencyT]]) -> None:
        if not inspect.iscoroutinefunction(function):
            raise UnsupportedFunctionError(function)
        super().__init__(parameters=get_parameters(function), dependency_type=get_return_type(function))
        self._function: Callable[..., Awaitable[DependencyT]] = function

    async def resolve(self, arguments: Mapping[str, object]) -> DependencyGuard[DependencyT]:
        return ValueDependencyGuard(await self._function(**arguments))


class AsyncGeneratorFunctionDependencyResolver[DependencyT](DependencyResolver[DependencyT]):
    _GENERATOR_TYPES: ClassVar[Collection[object]] = (AsyncGenerator, AsyncIterator)

    def __init__(self, function: Callable[..., AsyncIterator[DependencyT]]) -> None:
        if not inspect.isasyncgenfunction(function):
            raise UnsupportedFunctionError(function)
        super().__init__(parameters=get_parameters(function), dependency_type=self._get_yield_type(function))
        self._context_manager_factory: Callable[..., AbstractAsyncContextManager[DependencyT]] = asynccontextmanager(
            function
        )

    @classmethod
    def _get_yield_type(cls, function: Callable[..., object]) -> object:
        return_type = get_return_type(function)
        if get_origin(return_type) not in cls._GENERATOR_TYPES:
            raise UnsupportedReturnTypeError(function, return_type)
        return get_args(return_type)[0]

    async def resolve(self, arguments: Mapping[str, object]) -> DependencyGuard[DependencyT]:
        context_manager = self._context_manager_factory(**arguments)
        dependency = await context_manager.__aenter__()
        return AsyncContextManagerDependencyGuard(dependency, context_manager)


@dataclass(slots=True, frozen=True)
class Dependency[DependencyT]:
    resolver: DependencyResolver[DependencyT]
    lifetime: DependencyLifetime = DependencyLifetime.APPLICATION
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
    """Dependencies of one application, message or effect, closed on exit."""

    def __init__(self) -> None:
        self._dependencies: MutableMapping[DependencyKey, object] = {}
        self._guards: MutableMapping[DependencyKey, DependencyGuard[object]] = {}

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close(exc)

    def __contains__(self, key: DependencyKey) -> bool:
        return key in self._dependencies

    @property
    def guards(self) -> Mapping[DependencyKey, DependencyGuard[object]]:
        """Guards in the order the dependencies were resolved."""
        return self._guards

    def get(self, key: DependencyKey) -> object:
        return self._dependencies[key]

    def set(self, key: DependencyKey, dependency: object) -> None:
        self._dependencies[key] = dependency

    def add(self, key: DependencyKey, guard: DependencyGuard[object]) -> None:
        self._dependencies[key] = guard.dependency
        self._guards[key] = guard

    async def close(self, exc: BaseException | None) -> None:
        """Closes the dependencies in the reverse order and raises the closing errors."""
        if self._guards:
            errors: MutableSequence[BaseException] = []
            for guard in reversed(tuple(self._guards.values())):
                try:
                    await guard.close(exc)
                except BaseException as close_exc:  # noqa: BLE001 - the remaining dependencies must be closed too
                    errors.append(close_exc)
            self._guards.clear()
            raise_close_errors(exc, errors)


type Scopes = Sequence[Scope]


def raise_close_errors(exc: BaseException | None, errors: Sequence[BaseException]) -> None:
    """Raises closing errors, the caller reraises the scope error itself when it is not raised here."""
    if isinstance(exc, Exception) or exc is None:
        if exc is None and len(errors) == 1:
            raise errors[0]
        if errors:
            raise BaseExceptionGroup("Failed to close dependencies", [exc, *errors] if exc is not None else errors)
    else:
        # Grouping would hide a cancellation or an interrupt from asyncio and the caller.
        for error in errors:
            exc.add_note(f"Failed to close a dependency: {error!r}")


@dataclass(slots=True, frozen=True)
class DependencyArgument:
    consumer: object
    parameter: str
    key: DependencyKey
    lifetime: DependencyLifetime
    required_type: type | None = None
    """The type to check at runtime when the dependency type is known only then."""

    def get(self, scopes: Scopes) -> object:
        dependency = scopes[self.lifetime].get(self.key)
        if self.required_type is not None and not isinstance(dependency, self.required_type):
            raise DependencyNotFoundError(self.consumer, self.parameter, self.required_type)
        return dependency


def get_arguments(arguments: Sequence[DependencyArgument], scopes: Scopes) -> Mapping[str, object]:
    return {argument.parameter: argument.get(scopes) for argument in arguments}


@dataclass(slots=True, frozen=True)
class ResolutionStep:
    """Resolves one dependency from the dependencies in the scopes and puts it into its scope."""

    key: DependencyKey
    lifetime: DependencyLifetime
    resolver: DependencyResolver[Any]
    arguments: Sequence[DependencyArgument]

    async def run(self, scopes: Scopes) -> None:
        scope = scopes[self.lifetime]
        # A message dependency is resolved by the first effect that needs it.
        if self.key not in scope:
            guard = await self.resolver.resolve(get_arguments(self.arguments, scopes))
            scope.add(self.key, guard)


@dataclass(slots=True, frozen=True)
class PreparedEffectHandler:
    handler: Callable[..., Awaitable[object]]
    steps: Sequence[ResolutionStep]
    arguments: Sequence[DependencyArgument]

    async def resolve(self, scopes: Scopes) -> None:
        for step in self.steps:
            await step.run(scopes)

    async def call(self, scopes: Scopes) -> object:
        return await self.handler(**get_arguments(self.arguments, scopes))


@dataclass(slots=True, frozen=True)
class _Node:
    key: DependencyKey
    lifetime: DependencyLifetime
    narrowable: bool = False
    """The node matches its type and its subclasses but not its bases, the exact type is checked at runtime."""


type _Index = Mapping[object, Sequence[_Node]]


class DependencyContainer:
    """Validates the dependency graph and prepares everything needed to resolve dependencies."""

    _EFFECT_NODE: ClassVar[_Node] = _Node(key=EFFECT_KEY, lifetime=DependencyLifetime.EFFECT)
    # The effect is matched by the effect types of a handler, so it is indexed separately.
    _BUILTIN_NODES: ClassVar[Collection[_Node]] = (
        _Node(key=MESSAGE_KEY, lifetime=DependencyLifetime.MESSAGE, narrowable=True),
        _Node(key=MESSAGE_CONTEXT_KEY, lifetime=DependencyLifetime.MESSAGE),
        _Node(key=EFFECT_CONTEXT_KEY, lifetime=DependencyLifetime.EFFECT),
    )

    def __init__(
        self,
        dependencies: Collection[Dependency[Any]] = (),
        effect_types: Collection[type[Effect[Any]]] = (),
    ) -> None:
        self._dependencies: Mapping[DependencyKey, Dependency[Any]] = self._make_key_to_dependency(dependencies)
        self._index: _Index = self._make_index(self._dependencies)
        index = self._add_effect_to_index(self._index, effect_types)
        arguments = {key: self._bind_dependency(key, index) for key in self._dependencies}
        graph = {
            key: tuple(argument.key for argument in key_arguments if argument.key in self._dependencies)
            for key, key_arguments in arguments.items()
        }
        self._sort(graph)
        self._application_graph: Mapping[DependencyKey, Collection[DependencyKey]] = {
            key: dependency_keys
            for key, dependency_keys in graph.items()
            if self._dependencies[key].lifetime == DependencyLifetime.APPLICATION
        }
        self._application_steps: Mapping[DependencyKey, ResolutionStep] = {
            key: self._make_step(key, arguments[key]) for key in self._application_graph
        }

    @classmethod
    def _make_key_to_dependency(
        cls,
        dependencies: Collection[Dependency[Any]],
    ) -> Mapping[DependencyKey, Dependency[Any]]:
        key_to_dependency: MutableMapping[DependencyKey, Dependency[Any]] = {}
        builtin_keys = {EFFECT_KEY, *(node.key for node in cls._BUILTIN_NODES)}
        for dependency in dependencies:
            if dependency.lifetime not in dependency.resolver.lifetimes:
                raise UnsupportedLifetimeError(dependency)
            key = dependency.key
            if key in key_to_dependency or key in builtin_keys:
                raise DuplicateDependencyError(key)
            key_to_dependency[key] = dependency
        return key_to_dependency

    @classmethod
    def _make_index(cls, dependencies: Mapping[DependencyKey, Dependency[Any]]) -> _Index:
        """Maps each type to the nodes a parameter of this type matches."""
        index: MutableMapping[object, MutableSequence[_Node]] = defaultdict(list)
        nodes = [_Node(key=key, lifetime=dependency.lifetime) for key, dependency in dependencies.items()]
        for node in [*nodes, *cls._BUILTIN_NODES]:
            for base in (node.key.type,) if node.narrowable else cls._get_classes(node.key.type):
                index[base].append(node)
        return index

    @classmethod
    def _add_effect_to_index(cls, index: _Index, effect_types: Collection[type[Effect[Any]]]) -> _Index:
        effect_index: MutableMapping[object, Sequence[_Node]] = dict(index)
        for base in {base for effect_type in effect_types for base in cls._get_classes(effect_type)}:
            effect_index[base] = (*index.get(base, ()), cls._EFFECT_NODE)
        return effect_index

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
        dependency = self._dependencies[key]
        arguments = self._bind(key, dependency.resolver.parameters, index)
        for argument in arguments:
            if argument.lifetime > dependency.lifetime:
                raise DependencyLifetimeError(key, argument.key)
        return arguments

    def _bind(self, consumer: object, parameters: Mapping[str, object], index: _Index) -> Sequence[DependencyArgument]:
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
        # A subclass checked only at runtime is a fallback, so the effect of an exact type wins over the message.
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

    @classmethod
    def _get_narrowable_candidates(cls, parameter_type: object) -> Sequence[_Node]:
        if not isinstance(parameter_type, type):
            return ()
        return [
            node
            for node in cls._BUILTIN_NODES
            if node.narrowable and isinstance(node.key.type, type) and issubclass(parameter_type, node.key.type)
        ]

    @classmethod
    def _sort(cls, graph: Mapping[DependencyKey, Collection[DependencyKey]]) -> Sequence[DependencyKey]:
        """Orders the dependencies so that each one follows its dependencies."""
        try:
            return tuple(TopologicalSorter(graph).static_order())
        except CycleError as exc:
            raise DependencyCycleError(exc.args[1]) from exc

    def _make_step(self, key: DependencyKey, arguments: Sequence[DependencyArgument]) -> ResolutionStep:
        dependency = self._dependencies[key]
        return ResolutionStep(
            key=key,
            lifetime=dependency.lifetime,
            resolver=dependency.resolver,
            arguments=arguments,
        )

    def prepare_effect_handler(
        self,
        effect_type: type[Effect[Any]],
        handler: Callable[..., Awaitable[object]],
    ) -> PreparedEffectHandler:
        if not inspect.iscoroutinefunction(handler):
            raise UnsupportedFunctionError(handler)
        index = self._add_effect_to_index(self._index, (effect_type,))
        arguments = self._bind(handler, get_parameters(handler), index)

        key_to_arguments: MutableMapping[DependencyKey, Sequence[DependencyArgument]] = {}
        pending = [argument.key for argument in arguments]
        while pending:
            key = pending.pop()
            dependency = self._dependencies.get(key)
            # Builtin dependencies are put into scopes on creation, application ones are resolved on start.
            if dependency is None or dependency.lifetime == DependencyLifetime.APPLICATION or key in key_to_arguments:
                continue
            # Binding to the exact effect type can differ from the binding to any registered effect type.
            key_to_arguments[key] = self._bind_dependency(key, index)
            pending.extend(argument.key for argument in key_to_arguments[key])

        graph = {
            key: tuple(argument.key for argument in key_arguments if argument.key in key_to_arguments)
            for key, key_arguments in key_to_arguments.items()
        }
        steps = tuple(self._make_step(key, key_to_arguments[key]) for key in self._sort(graph))
        return PreparedEffectHandler(handler=handler, steps=steps, arguments=arguments)

    @classmethod
    def make_message_scope(cls, message: Message[Any], message_context: MessageContext) -> Scope:
        scope = Scope()
        scope.set(MESSAGE_KEY, message)
        scope.set(MESSAGE_CONTEXT_KEY, message_context)
        return scope

    @classmethod
    def make_effect_scope(cls, effect: Effect[Any], effect_context: EffectContext) -> Scope:
        scope = Scope()
        scope.set(EFFECT_KEY, effect)
        scope.set(EFFECT_CONTEXT_KEY, effect_context)
        return scope

    async def start(self) -> Scope:
        """Resolves application dependencies, each one as soon as its dependencies are resolved."""
        scope = Scope()
        events = {key: asyncio.Event() for key in self._application_graph}
        try:
            async with asyncio.TaskGroup() as task_group:
                for key, dependency_keys in self._application_graph.items():
                    task_group.create_task(
                        self._start_dependency(
                            step=self._application_steps[key],
                            scopes=(scope,),
                            dependency_events=[events[dependency_key] for dependency_key in dependency_keys],
                            event=events[key],
                        )
                    )
        except BaseException as exc:
            await self.stop(scope, exc)
            raise
        return scope

    @classmethod
    async def _start_dependency(
        cls,
        step: ResolutionStep,
        scopes: Scopes,
        dependency_events: Collection[asyncio.Event],
        event: asyncio.Event,
    ) -> None:
        for dependency_event in dependency_events:
            await dependency_event.wait()
        await step.run(scopes)
        event.set()

    async def stop(self, scope: Scope, exc: BaseException | None) -> None:
        """Closes application dependencies, each one as soon as its dependents are closed."""
        guards = dict(scope.guards)
        dependents: MutableMapping[DependencyKey, MutableSequence[DependencyKey]] = defaultdict(list)
        for key in guards:
            for dependency_key in self._application_graph[key]:
                dependents[dependency_key].append(key)
        events = {key: asyncio.Event() for key in guards}
        errors: MutableSequence[BaseException] = []

        async def close(key: DependencyKey) -> None:
            try:
                for dependent_key in dependents[key]:
                    await events[dependent_key].wait()
                await guards[key].close(exc)
            except BaseException as close_exc:  # noqa: BLE001 - the remaining dependencies must be closed too
                errors.append(close_exc)
            finally:
                events[key].set()

        await asyncio.gather(*(close(key) for key in guards))
        raise_close_errors(exc, errors)
