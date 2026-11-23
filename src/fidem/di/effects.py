from abc import ABC, abstractmethod
from asyncio import Event, Lock, TaskGroup, gather
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
from inspect import isasyncgenfunction, iscoroutinefunction
from types import TracebackType
from typing import Any, ClassVar, Final, Self, get_args, get_origin, override

from fidem.di.core import (
    MESSAGE_CONTEXT_KEY,
    MESSAGE_KEY,
    DependencyArgument,
    DependencyDef,
    DependencyGraph,
    DependencyKey,
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
from fidem.effects import Effect, EffectContext
from fidem.messages import Message, MessageContext

EFFECT_KEY: Final = DependencyKey(name="effect", type=Effect)
EFFECT_CONTEXT_KEY: Final = DependencyKey(name="effect_context", type=EffectContext)


class EffectDependencyLifetime(IntEnum):
    """Lifetimes from the widest to the narrowest, the value is the index of the scope in `Scopes`."""

    APPLICATION = 0
    MESSAGE = 1
    EFFECT = 2


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


class EffectDependencyResolver[DependencyT](DependencyResolver[EffectDependencyLifetime], ABC):
    ALL_LIFETIMES: ClassVar[Collection[EffectDependencyLifetime]] = tuple(EffectDependencyLifetime)

    def __init__(
        self,
        parameters: Mapping[str, object],
        dependency_type: object,
        lifetimes: Collection[EffectDependencyLifetime] = ALL_LIFETIMES,
    ) -> None:
        super().__init__(parameters=parameters, dependency_type=dependency_type, lifetimes=lifetimes)

    @abstractmethod
    async def resolve(self, arguments: Mapping[str, object]) -> DependencyGuard[DependencyT]:
        raise NotImplementedError


class InstanceEffectDependencyResolver[DependencyT](EffectDependencyResolver[DependencyT]):
    def __init__(self, instance: DependencyT) -> None:
        super().__init__(
            parameters={},
            dependency_type=type(instance),
            lifetimes=(EffectDependencyLifetime.APPLICATION,),
        )
        self._instance: DependencyT = instance

    @override
    async def resolve(self, arguments: Mapping[str, object]) -> DependencyGuard[DependencyT]:
        return ValueDependencyGuard(self._instance)


class AsyncFunctionEffectDependencyResolver[DependencyT](EffectDependencyResolver[DependencyT]):
    def __init__(self, function: Callable[..., Awaitable[DependencyT]]) -> None:
        if not iscoroutinefunction(function):
            raise UnsupportedFunctionError(function)
        super().__init__(parameters=get_parameters(function), dependency_type=get_return_type(function))
        self._function: Callable[..., Awaitable[DependencyT]] = function

    @override
    async def resolve(self, arguments: Mapping[str, object]) -> DependencyGuard[DependencyT]:
        return ValueDependencyGuard(await self._function(**arguments))


class AsyncGeneratorFunctionEffectDependencyResolver[DependencyT](EffectDependencyResolver[DependencyT]):
    _GENERATOR_TYPES: ClassVar[Collection[object]] = (AsyncGenerator, AsyncIterator)

    def __init__(self, function: Callable[..., AsyncIterator[DependencyT]]) -> None:
        if not isasyncgenfunction(function):
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

    @override
    async def resolve(self, arguments: Mapping[str, object]) -> DependencyGuard[DependencyT]:
        context_manager = self._context_manager_factory(**arguments)
        dependency = await context_manager.__aenter__()
        return AsyncContextManagerDependencyGuard(dependency, context_manager)


@dataclass(slots=True, frozen=True)
class EffectDependencyDef[DependencyT](DependencyDef[EffectDependencyResolver[DependencyT], EffectDependencyLifetime]):
    lifetime: EffectDependencyLifetime = EffectDependencyLifetime.APPLICATION


class ScopeClosedError(Exception):
    def __init__(self, key: DependencyKey) -> None:
        super().__init__(key)


class ClosableScope(Scope):
    """Scope that closes its dependencies on exit."""

    def __init__(self) -> None:
        super().__init__()
        self._guards: MutableMapping[DependencyKey, DependencyGuard[object]] = {}
        self._is_closed: bool = False
        self._lock: Lock | None = None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close(exc)

    @property
    def guards(self) -> Mapping[DependencyKey, DependencyGuard[object]]:
        """Guards in the order the dependencies were resolved."""
        return self._guards

    @property
    def lock(self) -> Lock:
        """Serializes lazy resolution, created on first use because most scopes never need it."""
        if self._lock is None:
            self._lock = Lock()
        return self._lock

    async def add(self, key: DependencyKey, guard: DependencyGuard[object]) -> None:
        if self._is_closed:
            # Nobody would close a dependency resolved after its scope had been closed.
            error = ScopeClosedError(key)
            await guard.close(error)
            raise error
        self.set(key, guard.dependency)
        self._guards[key] = guard

    async def close(self, exc: BaseException | None) -> None:
        """Closes the dependencies in the reverse order and raises the closing errors."""
        self._is_closed = True
        if self._guards:
            errors: MutableSequence[BaseException] = []
            for guard in reversed(tuple(self._guards.values())):
                try:
                    await guard.close(exc)
                except BaseException as close_exc:  # noqa: BLE001 - the remaining dependencies must be closed too
                    errors.append(close_exc)
            self._guards.clear()
            raise_close_errors(exc, errors)


type ClosableScopes = Sequence[ClosableScope]
"""Scopes indexed by lifetime."""


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
class EffectResolutionStep(ResolutionStep[EffectDependencyResolver[Any]]):
    # Middlewares may run the handler concurrently, so the first call resolves a message dependency for all of them.
    _LAZY_LIFETIMES: ClassVar[Collection[IntEnum]] = (EffectDependencyLifetime.MESSAGE,)

    async def run(self, scopes: ClosableScopes) -> None:
        scope = scopes[self.lifetime]
        if self.lifetime in self._LAZY_LIFETIMES:
            async with scope.lock:
                if self.key not in scope:
                    await self._resolve(scope, scopes)
        else:
            await self._resolve(scope, scopes)

    async def _resolve(self, scope: ClosableScope, scopes: ClosableScopes) -> None:
        guard = await self.resolver.resolve(get_arguments(self.arguments, scopes))
        await scope.add(self.key, guard)


@dataclass(slots=True, frozen=True)
class PreparedEffectHandler:
    handler: Callable[..., Awaitable[object]]
    steps: Sequence[EffectResolutionStep]
    arguments: Sequence[DependencyArgument]

    async def resolve(self, scopes: ClosableScopes) -> None:
        for step in self.steps:
            await step.run(scopes)

    async def call(self, scopes: ClosableScopes) -> object:
        return await self.handler(**get_arguments(self.arguments, scopes))


type _EffectDependencyGraph = DependencyGraph[EffectDependencyDef[Any]]


class EffectDependencyContainer:
    """Validates the dependencies of effect handlers and prepares everything needed to resolve them."""

    _EFFECT_NODE: ClassVar[Node] = Node(key=EFFECT_KEY, lifetime=EffectDependencyLifetime.EFFECT)
    # The message that yields an effect is unknown in advance, so its type is checked at runtime.
    _BUILTIN_NODES: ClassVar[Collection[Node]] = (
        Node(key=MESSAGE_KEY, lifetime=EffectDependencyLifetime.MESSAGE, narrowable=True),
        Node(key=MESSAGE_CONTEXT_KEY, lifetime=EffectDependencyLifetime.MESSAGE),
        Node(key=EFFECT_CONTEXT_KEY, lifetime=EffectDependencyLifetime.EFFECT),
    )

    def __init__(
        self,
        dependency_defs: Collection[EffectDependencyDef[Any]] = (),
        effect_types: Collection[type[Effect[Any]]] = (),
    ) -> None:
        self._graph: _EffectDependencyGraph = DependencyGraph(
            dependency_defs=dependency_defs,
            application_lifetime=EffectDependencyLifetime.APPLICATION,
            subject_node=self._EFFECT_NODE,
            subject_types=effect_types,
            builtin_nodes=self._BUILTIN_NODES,
        )
        self._application_steps: Mapping[DependencyKey, EffectResolutionStep] = {
            bound.dependency_def.key: EffectResolutionStep.make(bound.dependency_def, bound.arguments)
            for bound in self._graph.application_dependency_defs
        }

    def prepare_effect_handler(
        self,
        effect_type: type[Effect[Any]],
        handler: Callable[..., Awaitable[object]],
    ) -> PreparedEffectHandler:
        if not iscoroutinefunction(handler):
            raise UnsupportedFunctionError(handler)
        binding = self._graph.bind(handler, get_parameters(handler), effect_type)
        return PreparedEffectHandler(
            handler=handler,
            steps=tuple(
                EffectResolutionStep.make(bound.dependency_def, bound.arguments) for bound in binding.dependency_defs
            ),
            arguments=binding.arguments,
        )

    @classmethod
    def make_message_scope(cls, message: Message[Any], message_context: MessageContext) -> ClosableScope:
        scope = ClosableScope()
        put_message(scope, message, message_context)
        return scope

    @classmethod
    def make_effect_scope(cls, effect: Effect[Any], effect_context: EffectContext) -> ClosableScope:
        scope = ClosableScope()
        scope.set(EFFECT_KEY, effect)
        scope.set(EFFECT_CONTEXT_KEY, effect_context)
        return scope

    async def start(self) -> ClosableScope:
        """Resolves application dependencies, each one as soon as its dependencies are resolved."""
        scope = ClosableScope()
        events = {key: Event() for key in self._application_steps}
        try:
            async with TaskGroup() as task_group:
                for key, step in self._application_steps.items():
                    task_group.create_task(
                        self._start_dependency(
                            step=step,
                            scopes=(scope,),
                            dependency_events=[events[dependency_key] for dependency_key in self._graph.graph[key]],
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
        step: EffectResolutionStep,
        scopes: ClosableScopes,
        dependency_events: Collection[Event],
        event: Event,
    ) -> None:
        for dependency_event in dependency_events:
            await dependency_event.wait()
        await step.run(scopes)
        event.set()

    async def stop(self, scope: ClosableScope, exc: BaseException | None) -> None:
        """Closes application dependencies, each one as soon as its dependents are closed."""
        guards = dict(scope.guards)
        dependents: MutableMapping[DependencyKey, MutableSequence[DependencyKey]] = defaultdict(list)
        for key in guards:
            for dependency_key in self._graph.graph[key]:
                dependents[dependency_key].append(key)
        events = {key: Event() for key in guards}
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

        await gather(*(close(key) for key in guards))
        raise_close_errors(exc, errors)
