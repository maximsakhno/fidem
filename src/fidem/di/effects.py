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
from typing import Any, ClassVar, Final, Self

from fidem.di.core import (
    MESSAGE_CONTEXT_KEY,
    MESSAGE_KEY,
    DependencyArgument,
    DependencyGraph,
    DependencyKey,
    Node,
    Provider,
    UnsupportedFunctionError,
    get_parameters,
    make_key,
    make_message_dependencies,
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


type DependencyFactory[DependencyT] = Callable[..., AsyncIterator[DependencyT]]


@dataclass(slots=True, frozen=True)
class EffectDependencyDef[DependencyT]:
    factory: DependencyFactory[DependencyT]
    lifetime: EffectDependencyLifetime = EffectDependencyLifetime.APPLICATION
    name: str = ""
    type: object = None
    """The type the factory yields is used when the type is not set."""


class DependencyLifetimeError(Exception):
    """A dependency requires a dependency with a narrower lifetime."""

    def __init__(self, consumer: object, dependency: DependencyKey) -> None:
        super().__init__(consumer, dependency)


@dataclass(slots=True, frozen=True)
class EffectProvider(Provider):
    lifetime: EffectDependencyLifetime
    factory: Callable[..., AbstractAsyncContextManager[object]]

    _RETURN_ORIGINS: ClassVar[Collection[object]] = (AsyncGenerator, AsyncIterator)

    @classmethod
    def make(cls, dependency_def: EffectDependencyDef[Any]) -> "EffectProvider":
        factory = dependency_def.factory
        if not isasyncgenfunction(factory):
            raise UnsupportedFunctionError(factory)
        return cls(
            key=make_key(
                function=factory,
                return_origins=cls._RETURN_ORIGINS,
                name=dependency_def.name,
                dependency_type=dependency_def.type,
            ),
            parameters=get_parameters(factory),
            lifetime=dependency_def.lifetime,
            factory=asynccontextmanager(factory),
        )


class ScopeClosedError(Exception):
    def __init__(self, key: DependencyKey) -> None:
        super().__init__(key)


async def exit_context_manager(context_manager: AbstractAsyncContextManager[object], exc: BaseException | None) -> None:
    if exc is None:
        await context_manager.__aexit__(None, None, None)
    else:
        # The context manager may suppress the error, but the scope exits with it anyway.
        await context_manager.__aexit__(type(exc), exc, exc.__traceback__)


class Scope:
    """Dependencies of one application, message or effect, closes them on exit."""

    def __init__(self, dependencies: MutableMapping[DependencyKey, object]) -> None:
        self._dependencies: MutableMapping[DependencyKey, object] = dependencies
        self._context_managers: MutableMapping[DependencyKey, AbstractAsyncContextManager[object]] = {}
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
    def dependencies(self) -> Mapping[DependencyKey, object]:
        return self._dependencies

    @property
    def context_managers(self) -> Mapping[DependencyKey, AbstractAsyncContextManager[object]]:
        """Context managers of the resolved dependencies in the order the dependencies were resolved."""
        return self._context_managers

    @property
    def lock(self) -> Lock:
        """Serializes lazy resolution, created on first use because most scopes never need it."""
        if self._lock is None:
            self._lock = Lock()
        return self._lock

    async def add(
        self,
        key: DependencyKey,
        dependency: object,
        context_manager: AbstractAsyncContextManager[object],
    ) -> None:
        if self._is_closed:
            # Nobody would close a dependency resolved after its scope had been closed.
            error = ScopeClosedError(key)
            await exit_context_manager(context_manager, error)
            raise error
        self._dependencies[key] = dependency
        self._context_managers[key] = context_manager

    async def close(self, exc: BaseException | None) -> None:
        """Closes the dependencies in the reverse order and raises the closing errors."""
        self._is_closed = True
        if self._context_managers:
            errors: MutableSequence[BaseException] = []
            for context_manager in reversed(tuple(self._context_managers.values())):
                try:
                    await exit_context_manager(context_manager, exc)
                except BaseException as close_exc:  # noqa: BLE001 - the remaining dependencies must be closed too
                    errors.append(close_exc)
            self._context_managers.clear()
            raise_close_errors(exc, errors)


type Scopes = Sequence[Scope]
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
class ScopedDependencyArgument:
    argument: DependencyArgument
    lifetime: EffectDependencyLifetime

    def get(self, scopes: Scopes) -> object:
        return self.argument.get(scopes[self.lifetime].dependencies)


def get_scoped_arguments(arguments: Sequence[ScopedDependencyArgument], scopes: Scopes) -> Mapping[str, object]:
    return {scoped.argument.parameter: scoped.get(scopes) for scoped in arguments}


@dataclass(slots=True, frozen=True)
class EffectResolutionStep:
    """Resolves one dependency from the dependencies in the scopes and puts it into its scope."""

    # Middlewares may run the handler concurrently, so the first call resolves a message dependency for all of them.
    _LAZY_LIFETIMES: ClassVar[Collection[EffectDependencyLifetime]] = (EffectDependencyLifetime.MESSAGE,)

    provider: EffectProvider
    arguments: Sequence[ScopedDependencyArgument]

    async def run(self, scopes: Scopes) -> None:
        scope = scopes[self.provider.lifetime]
        if self.provider.lifetime not in self._LAZY_LIFETIMES:
            await self._resolve(scope, scopes)
        elif self.provider.key not in scope.dependencies:
            async with scope.lock:
                if self.provider.key not in scope.dependencies:
                    await self._resolve(scope, scopes)

    async def _resolve(self, scope: Scope, scopes: Scopes) -> None:
        context_manager = self.provider.factory(**get_scoped_arguments(self.arguments, scopes))
        dependency = await context_manager.__aenter__()
        await scope.add(self.provider.key, dependency, context_manager)


@dataclass(slots=True, frozen=True)
class PreparedEffectHandler:
    handler: Callable[..., Awaitable[object]]
    steps: Sequence[EffectResolutionStep]
    arguments: Sequence[ScopedDependencyArgument]

    async def resolve(self, scopes: Scopes) -> None:
        for step in self.steps:
            await step.run(scopes)

    async def call(self, scopes: Scopes) -> object:
        return await self.handler(**get_scoped_arguments(self.arguments, scopes))


class EffectDependencyContainer:
    """Validates the dependencies of effect handlers and prepares everything needed to resolve them."""

    # The message that yields an effect is unknown in advance, so its type is checked at runtime.
    _BUILTIN_NODES: ClassVar[Mapping[Node, EffectDependencyLifetime]] = {
        Node(key=MESSAGE_KEY, narrowable=True): EffectDependencyLifetime.MESSAGE,
        Node(key=MESSAGE_CONTEXT_KEY): EffectDependencyLifetime.MESSAGE,
        Node(key=EFFECT_CONTEXT_KEY): EffectDependencyLifetime.EFFECT,
    }

    def __init__(
        self,
        dependency_defs: Collection[EffectDependencyDef[Any]] = (),
        effect_types: Collection[type[Effect[Any]]] = (),
    ) -> None:
        self._graph: DependencyGraph[EffectProvider] = DependencyGraph(
            providers=tuple(EffectProvider.make(dependency_def) for dependency_def in dependency_defs),
            subject_key=EFFECT_KEY,
            subject_types=effect_types,
            builtin_nodes=tuple(self._BUILTIN_NODES),
        )
        self._lifetimes: Mapping[DependencyKey, EffectDependencyLifetime] = {
            **{node.key: lifetime for node, lifetime in self._BUILTIN_NODES.items()},
            EFFECT_KEY: EffectDependencyLifetime.EFFECT,
            **{key: provider.lifetime for key, provider in self._graph.providers.items()},
        }
        self._check_lifetimes()
        self._application_steps: Mapping[DependencyKey, EffectResolutionStep] = {
            key: self._make_step(self._graph.providers[key], self._graph.arguments[key])
            for key in self._graph.order
            if self._graph.providers[key].lifetime == EffectDependencyLifetime.APPLICATION
        }

    def _check_lifetimes(self) -> None:
        for key, arguments in self._graph.arguments.items():
            for argument in arguments:
                if self._lifetimes[argument.key] > self._lifetimes[key]:
                    raise DependencyLifetimeError(key, argument.key)

    def _make_step(self, provider: EffectProvider, arguments: Sequence[DependencyArgument]) -> EffectResolutionStep:
        return EffectResolutionStep(provider=provider, arguments=self._scope_arguments(arguments))

    def _scope_arguments(self, arguments: Sequence[DependencyArgument]) -> Sequence[ScopedDependencyArgument]:
        return tuple(
            ScopedDependencyArgument(argument=argument, lifetime=self._lifetimes[argument.key])
            for argument in arguments
        )

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
            # Application dependencies are resolved on start.
            steps=tuple(
                self._make_step(bound.provider, bound.arguments)
                for bound in binding.providers
                if bound.provider.lifetime != EffectDependencyLifetime.APPLICATION
            ),
            arguments=self._scope_arguments(binding.arguments),
        )

    @classmethod
    def make_message_scope(cls, message: Message[Any], message_context: MessageContext) -> Scope:
        return Scope(make_message_dependencies(message, message_context))

    @classmethod
    def make_effect_scope(cls, effect: Effect[Any], effect_context: EffectContext) -> Scope:
        return Scope({EFFECT_KEY: effect, EFFECT_CONTEXT_KEY: effect_context})

    async def start(self) -> Scope:
        """Resolves application dependencies, each one as soon as its dependencies are resolved."""
        scope = Scope({})
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
        scopes: Scopes,
        dependency_events: Collection[Event],
        event: Event,
    ) -> None:
        for dependency_event in dependency_events:
            await dependency_event.wait()
        await step.run(scopes)
        event.set()

    async def stop(self, scope: Scope, exc: BaseException | None) -> None:
        """Closes application dependencies, each one as soon as its dependents are closed."""
        context_managers = dict(scope.context_managers)
        dependents: MutableMapping[DependencyKey, MutableSequence[DependencyKey]] = defaultdict(list)
        for key in context_managers:
            for dependency_key in self._graph.graph[key]:
                dependents[dependency_key].append(key)
        events = {key: Event() for key in context_managers}
        errors: MutableSequence[BaseException] = []

        async def close(key: DependencyKey) -> None:
            try:
                for dependent_key in dependents[key]:
                    await events[dependent_key].wait()
                await exit_context_manager(context_managers[key], exc)
            except BaseException as close_exc:  # noqa: BLE001 - the remaining dependencies must be closed too
                errors.append(close_exc)
            finally:
                events[key].set()

        await gather(*(close(key) for key in context_managers))
        raise_close_errors(exc, errors)
