"""Decorators of the user API: they mark functions and classes, and `Fidem` reduces the marks to the definitions
of the core.

A decorator returns the function or the class itself and reads no annotations, so a handler may refer to a class
declared below it. The annotations are read when `Fidem` resolves the mark into a definition.
"""

from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, Final, TypeAliasType, get_origin, overload

from fidem._functions import as_effectful, as_factory, is_plain_function
from fidem.core.di.core import (
    DependencyName,
    UnsupportedFunctionError,
    UnsupportedReturnTypeError,
    get_parameters,
    get_return_type,
)
from fidem.core.di.effects import EffectDependencyDef, EffectDependencyLifetime
from fidem.core.effects import Effect, EffectName, EffectVersion
from fidem.core.environment import EffectDef, EffectMiddleware, EffectMiddlewareDef
from fidem.core.guards import GuardDef, GuardInputDef
from fidem.core.messages import (
    ATOMIC_WRITE_TYPES,
    AtomicEffectful,
    Command,
    Event,
    EventHandlerName,
    Message,
    MessageName,
    MessageVersion,
    Query,
    StagedEffectful,
)
from fidem.core.operation_engine.definitions import (
    AtomicCommandDef,
    AtomicEventHandlerDef,
    CommandDef,
    EventDef,
    ExecutionMode,
    MessageMiddleware,
    MessageMiddlewareDef,
    MessageMiddlewareEffectful,
    MiddlewareName,
    QueryDef,
    StagedCommandDef,
    StagedEventHandlerDef,
)
from fidem.errors import AlreadyDeclaredError, HandledTypeError, UndeclaredClassError

__all__ = [
    "CoreDefinition",
    "EffectMiddlewares",
    "MessageMiddlewares",
    "command",
    "command_handler",
    "declared_definition",
    "dependency",
    "effect",
    "effect_handler",
    "effect_middleware",
    "effect_middleware_def",
    "effect_middleware_defs",
    "event",
    "event_handler",
    "guard",
    "guard_input",
    "is_declared",
    "message_middleware_def",
    "message_middleware_defs",
    "middleware",
    "query_handler",
]

type CoreDefinition = (
    CommandDef[Any]
    | QueryDef[Any]
    | EventDef[Any]
    | GuardDef[Any]
    | GuardInputDef[Any]
    | EffectDef[Any]
    | EffectDependencyDef[Any]
)
"""A definition of the core, `Fidem` accepts it as is."""
type MessageMiddlewares = Sequence[MessageMiddleware | MessageMiddlewareDef]
"""Middlewares of messages in their order, a function is named after itself unless `@middleware` names it."""
type EffectMiddlewares = Sequence[EffectMiddleware | EffectMiddlewareDef]
"""Middlewares of effects in their order."""


# ── Marks ─────────────────────────────────────────────────
# A decorator keeps a mark in the function or the class and returns it as is. A mark of a class holds no definition
# with the class: `@dataclass(slots=True)` below the decorator would replace the class.
_MARK_ATTRIBUTE: Final = "_fidem_mark"


class _Mark:
    """What a decorator declares about a function or a class."""


class _DefinitionMark(_Mark, ABC):
    """A mark that makes a definition of the core, discovery finds what it marks."""

    @abstractmethod
    def resolve(self, declared: Any, /) -> CoreDefinition:
        """Reads the annotations of the marked function or class and makes its definition."""
        raise NotImplementedError


def _marked[DeclaredT: Callable[..., object]](declared: DeclaredT, mark: _Mark) -> DeclaredT:
    if _mark_of(declared) is not None:
        raise AlreadyDeclaredError(declared)
    try:
        setattr(declared, _MARK_ATTRIBUTE, mark)
    except AttributeError as exc:
        # A bound method or a builtin keeps no attributes.
        raise UnsupportedFunctionError(declared) from exc
    return declared


def _mark_of(value: object) -> _Mark | None:
    """Reads the own mark, so a subclass of a declared class is another message or effect and declares its own."""
    mark: _Mark | None = getattr(value, "__dict__", {}).get(_MARK_ATTRIBUTE)
    return mark


def is_declared(value: object) -> bool:
    """Tells a function or a class that declares a definition, a middleware or a named class declares none."""
    return isinstance(_mark_of(value), _DefinitionMark)


def declared_definition(value: object) -> CoreDefinition | None:
    """Returns the definition a marked function or class declares."""
    mark = _mark_of(value)
    return mark.resolve(value) if isinstance(mark, _DefinitionMark) else None


# ── Classes ───────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class _NameMark(_Mark):
    """The stable name and the version a message or an effect is serialized and stored with."""

    name: str
    version: int


@dataclass(frozen=True, slots=True)
class _EventMark(_NameMark, _DefinitionMark):
    def resolve(self, declared: Any, /) -> EventDef[Any]:
        return EventDef(name=self.name, event_type=declared, version=self.version)


@dataclass(frozen=True, slots=True)
class _GuardInputMark(_DefinitionMark):
    def resolve(self, declared: Any, /) -> GuardInputDef[Any]:
        return GuardInputDef(declared)


def command[CommandT: type[Command[Any]]](
    name: MessageName,
    *,
    version: MessageVersion = 1,
) -> Callable[[CommandT], CommandT]:
    """Declares the stable name and the version of a command class for serialization and storage."""
    return lambda command_type: _marked(command_type, _NameMark(name=name, version=version))


def event[EventT: type[Event]](
    name: MessageName,
    *,
    version: MessageVersion = 1,
) -> Callable[[EventT], EventT]:
    """Declares an event: its stable name and version are stored in the outbox with each delivery."""
    return lambda event_type: _marked(event_type, _EventMark(name=name, version=version))


def effect[EffectT: type[Effect[Any]]](
    name: EffectName,
    *,
    version: EffectVersion = 1,
) -> Callable[[EffectT], EffectT]:
    """Declares the stable name and the version of an effect class, the journal of an operation stores them."""
    return lambda effect_type: _marked(effect_type, _NameMark(name=name, version=version))


def guard_input[InputT: type[Any]](input_type: InputT) -> InputT:
    """Declares the class of a value the caller passes into `handle` along with the message for the guards."""
    return _marked(input_type, _GuardInputMark())


def _required_name(function: Callable[..., object], named_type: type[Any]) -> _NameMark:
    mark = _mark_of(named_type)
    if not isinstance(mark, _NameMark):
        raise UndeclaredClassError(function, named_type)
    return mark


# ── Annotations ───────────────────────────────────────────
def _annotated_class(annotation: object) -> type[Any] | None:
    """Returns the class of an annotation, a generic class such as `Command[User]` gives its origin."""
    origin = get_origin(annotation) or annotation
    return origin if isinstance(origin, type) else None


def _handled_type[HandledT](
    function: Callable[..., object],
    base: type[HandledT],
    excluded: tuple[type[Any], ...] = (),
) -> type[HandledT]:
    """Returns the type of the only parameter annotated with a subclass of the base."""
    handled_types = [
        annotated
        for annotation in get_parameters(function).values()
        if (annotated := _annotated_class(annotation)) is not None
        and issubclass(annotated, base)
        and not issubclass(annotated, excluded)
    ]
    if len(handled_types) != 1:
        raise HandledTypeError(function, handled_types)
    return handled_types[0]


def _is_atomic(function: Callable[..., object]) -> bool:
    """Tells an atomic handler from a staged one by its return type.

    A generator function returns `AtomicEffectful` or `StagedEffectful`, aliases of them count too. A plain function
    returns the write of an atomic handler, a write transaction or an external write, or the result of a staged one.
    """
    return_type = get_return_type(function)
    if is_plain_function(function):
        returned = _annotated_class(return_type)
        return returned is not None and issubclass(returned, ATOMIC_WRITE_TYPES)
    alias = get_origin(return_type) or return_type
    while isinstance(alias, TypeAliasType):
        if alias is AtomicEffectful:
            return True
        if alias is StagedEffectful:
            return False
        alias = get_origin(alias.__value__) or alias.__value__
    raise UnsupportedReturnTypeError(function, return_type)


# ── Marks of functions ────────────────────────────────────
@dataclass(frozen=True, slots=True)
class _MiddlewareMark(_Mark):
    name: MiddlewareName


@dataclass(frozen=True, slots=True)
class _EffectMiddlewareMark(_Mark):
    pass


def message_middleware_def(middleware: MessageMiddleware | MessageMiddlewareDef) -> MessageMiddlewareDef:
    """Names a middleware function after itself unless `@middleware` names it."""
    if isinstance(middleware, MessageMiddlewareDef):
        return middleware
    mark = _mark_of(middleware)
    name = mark.name if isinstance(mark, _MiddlewareMark) and mark.name else middleware.__name__
    return MessageMiddlewareDef(name=name, middleware=middleware)


def message_middleware_defs(middlewares: MessageMiddlewares) -> Sequence[MessageMiddlewareDef]:
    return tuple(message_middleware_def(middleware) for middleware in middlewares)


def _optional_message_middleware_defs(
    middlewares: MessageMiddlewares | None,
) -> Sequence[MessageMiddlewareDef] | None:
    """`None` keeps the middlewares of `Fidem`."""
    return None if middlewares is None else message_middleware_defs(middlewares)


def effect_middleware_def(middleware: EffectMiddleware | EffectMiddlewareDef) -> EffectMiddlewareDef:
    return middleware if isinstance(middleware, EffectMiddlewareDef) else EffectMiddlewareDef(middleware)


def effect_middleware_defs(middlewares: EffectMiddlewares) -> Sequence[EffectMiddlewareDef]:
    return tuple(effect_middleware_def(middleware) for middleware in middlewares)


@dataclass(frozen=True, slots=True)
class _CommandHandlerMark(_DefinitionMark):
    middlewares: MessageMiddlewares | None
    mode: ExecutionMode

    def resolve(self, function: Any, /) -> CommandDef[Any]:
        command_type = _handled_type(function, Command)
        declared = _required_name(function, command_type)
        definition_type = AtomicCommandDef if _is_atomic(function) else StagedCommandDef
        return definition_type(
            name=declared.name,
            message_type=command_type,
            handler=as_effectful(function),
            version=declared.version,
            middleware_defs=_optional_message_middleware_defs(self.middlewares),
            execution_mode=self.mode,
        )


@dataclass(frozen=True, slots=True)
class _QueryHandlerMark(_DefinitionMark):
    middlewares: MessageMiddlewares | None

    def resolve(self, function: Any, /) -> QueryDef[Any]:
        return QueryDef(
            message_type=_handled_type(function, Query),
            handler=as_effectful(function),
            middleware_defs=_optional_message_middleware_defs(self.middlewares),
        )


@dataclass(frozen=True, slots=True)
class _EventHandlerMark(_DefinitionMark):
    name: EventHandlerName
    middlewares: MessageMiddlewares | None
    mode: ExecutionMode

    def resolve(self, function: Any, /) -> EventDef[Any]:
        """Makes the definition of the event with this handler only, `Fidem` merges the handlers of one event."""
        definition_type = AtomicEventHandlerDef if _is_atomic(function) else StagedEventHandlerDef
        handler_def = definition_type(
            name=self.name or function.__name__,
            handler=as_effectful(function),
            middleware_defs=_optional_message_middleware_defs(self.middlewares),
            execution_mode=self.mode,
        )
        event_type = _handled_type(function, Event)
        declared = _required_name(function, event_type)
        return EventDef(
            name=declared.name,
            event_type=event_type,
            version=declared.version,
            handler_defs=(handler_def,),
        )


@dataclass(frozen=True, slots=True)
class _GuardMark(_DefinitionMark):
    name: DependencyName
    type: object

    def resolve(self, function: Any, /) -> GuardDef[Any]:
        return GuardDef(guard=as_effectful(function), name=self.name, type=self.type)


@dataclass(frozen=True, slots=True)
class _EffectHandlerMark(_DefinitionMark):
    middlewares: EffectMiddlewares | None

    def resolve(self, function: Any, /) -> EffectDef[Any]:
        # A handler may get the message too, and a message is an effect.
        effect_type = _handled_type(function, Effect, excluded=(Message,))
        declared = _required_name(function, effect_type)
        return EffectDef(
            name=declared.name,
            effect_type=effect_type,
            handler=function,
            version=declared.version,
            middleware_defs=None if self.middlewares is None else effect_middleware_defs(self.middlewares),
        )


@dataclass(frozen=True, slots=True)
class _DependencyMark(_DefinitionMark):
    lifetime: EffectDependencyLifetime
    name: DependencyName
    type: object

    def resolve(self, function: Any, /) -> EffectDependencyDef[Any]:
        return EffectDependencyDef(
            factory=as_factory(function),
            lifetime=self.lifetime,
            name=self.name,
            type=self.type,
        )


# ── Decorators ────────────────────────────────────────────
type _Handler = Callable[..., object]
"""A generator function or a plain function, which `Fidem` makes a generator function of."""


@overload
def command_handler[FunctionT: _Handler](function: FunctionT, /) -> FunctionT: ...
@overload
def command_handler[FunctionT: _Handler](
    *,
    middlewares: MessageMiddlewares | None = None,
    mode: ExecutionMode = ExecutionMode.EPHEMERAL,
) -> Callable[[FunctionT], FunctionT]: ...
def command_handler[FunctionT: _Handler](
    function: FunctionT | None = None,
    /,
    *,
    middlewares: MessageMiddlewares | None = None,
    mode: ExecutionMode = ExecutionMode.EPHEMERAL,
) -> FunctionT | Callable[[FunctionT], FunctionT]:
    """Declares the handler of the command its parameter is annotated with, atomic or staged by its return type.

    A plain function yields no effects: an atomic one returns its write, a staged one returns its result.
    `middlewares` replace the middlewares of `Fidem`, `()` turns them off.
    """
    return _decorate(function, _CommandHandlerMark(middlewares=middlewares, mode=mode))


@overload
def query_handler[FunctionT: _Handler](function: FunctionT, /) -> FunctionT: ...
@overload
def query_handler[FunctionT: _Handler](
    *,
    middlewares: MessageMiddlewares | None = None,
) -> Callable[[FunctionT], FunctionT]: ...
def query_handler[FunctionT: _Handler](
    function: FunctionT | None = None,
    /,
    *,
    middlewares: MessageMiddlewares | None = None,
) -> FunctionT | Callable[[FunctionT], FunctionT]:
    """Declares the handler of the query its parameter is annotated with, a plain function yields no effects."""
    return _decorate(function, _QueryHandlerMark(middlewares=middlewares))


@overload
def event_handler[FunctionT: _Handler](function: FunctionT, /) -> FunctionT: ...
@overload
def event_handler[FunctionT: _Handler](
    *,
    name: EventHandlerName = "",
    middlewares: MessageMiddlewares | None = None,
    mode: ExecutionMode = ExecutionMode.EPHEMERAL,
) -> Callable[[FunctionT], FunctionT]: ...
def event_handler[FunctionT: _Handler](
    function: FunctionT | None = None,
    /,
    *,
    name: EventHandlerName = "",
    middlewares: MessageMiddlewares | None = None,
    mode: ExecutionMode = ExecutionMode.EPHEMERAL,
) -> FunctionT | Callable[[FunctionT], FunctionT]:
    """Declares a handler of the event its parameter is annotated with, atomic or staged by its return type.

    A plain function yields no effects as with `command_handler`. The name is stored in the outbox with each
    delivery, so renaming the function without `name` loses deliveries.
    """
    return _decorate(function, _EventHandlerMark(name=name, middlewares=middlewares, mode=mode))


@overload
def guard[FunctionT: _Handler](function: FunctionT, /) -> FunctionT: ...
@overload
def guard[FunctionT: _Handler](
    *,
    name: DependencyName = "",
    type: object = None,
) -> Callable[[FunctionT], FunctionT]: ...
def guard[FunctionT: _Handler](
    function: FunctionT | None = None,
    /,
    *,
    name: DependencyName = "",
    type: object = None,
) -> FunctionT | Callable[[FunctionT], FunctionT]:
    """Declares a guard, the handlers and the middlewares that take its value run only after it.

    A plain function checks without effects. The type of the value the guard returns is used when `type` is not set.
    """
    return _decorate(function, _GuardMark(name=name, type=type))


type _AsyncFunction = Callable[..., Awaitable[object]]


@overload
def effect_handler[FunctionT: _AsyncFunction](function: FunctionT, /) -> FunctionT: ...
@overload
def effect_handler[FunctionT: _AsyncFunction](
    *,
    middlewares: EffectMiddlewares | None = None,
) -> Callable[[FunctionT], FunctionT]: ...
def effect_handler[FunctionT: _AsyncFunction](
    function: FunctionT | None = None,
    /,
    *,
    middlewares: EffectMiddlewares | None = None,
) -> FunctionT | Callable[[FunctionT], FunctionT]:
    """Declares the handler of the effect its parameter is annotated with.

    `middlewares` replace the effect middlewares of `Fidem`, `()` turns them off.
    """
    return _decorate(function, _EffectHandlerMark(middlewares=middlewares))


type _Factory = Callable[..., AsyncGenerator[Any]] | Callable[..., Awaitable[Any]]
"""An async generator function or an async function, which `Fidem` makes a factory of."""


@overload
def dependency[FunctionT: _Factory](function: FunctionT, /) -> FunctionT: ...
@overload
def dependency[FunctionT: _Factory](
    *,
    lifetime: EffectDependencyLifetime = EffectDependencyLifetime.APPLICATION,
    name: DependencyName = "",
    type: object = None,
) -> Callable[[FunctionT], FunctionT]: ...
def dependency[FunctionT: _Factory](
    function: FunctionT | None = None,
    /,
    *,
    lifetime: EffectDependencyLifetime = EffectDependencyLifetime.APPLICATION,
    name: DependencyName = "",
    type: object = None,
) -> FunctionT | Callable[[FunctionT], FunctionT]:
    """Declares the factory of a dependency of effect handlers, a service has the `SERVICE` lifetime.

    An async generator yields the dependency and closes it after, an async function returns a dependency that needs
    no closing. The type the factory yields or returns is used when `type` is not set.
    """
    return _decorate(function, _DependencyMark(lifetime=lifetime, name=name, type=type))


type _Middleware = Callable[..., MessageMiddlewareEffectful[Any]]


@overload
def middleware[FunctionT: _Middleware](function: FunctionT, /) -> FunctionT: ...
@overload
def middleware[FunctionT: _Middleware](*, name: MiddlewareName = "") -> Callable[[FunctionT], FunctionT]: ...
def middleware[FunctionT: _Middleware](
    function: FunctionT | None = None,
    /,
    *,
    name: MiddlewareName = "",
) -> FunctionT | Callable[[FunctionT], FunctionT]:
    """Declares a middleware of messages with a stable name, the effect ids of the middleware depend on it.

    A plain function works as a middleware too and is named after itself. A middleware goes into an ordered list,
    so discovery does not find it.
    """
    return _decorate(function, _MiddlewareMark(name=name))


def effect_middleware[FunctionT: EffectMiddleware](function: FunctionT) -> FunctionT:
    """Declares a middleware of effects, a plain function works as one too."""
    return _marked(function, _EffectMiddlewareMark())


def _decorate[FunctionT: Callable[..., object]](
    function: FunctionT | None,
    mark: _Mark,
) -> FunctionT | Callable[[FunctionT], FunctionT]:
    """Marks the function, or returns the decorator that marks it when the decorator is called with options."""
    if function is None:
        return lambda decorated: _marked(decorated, mark)
    return _marked(function, mark)
