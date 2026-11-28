from collections.abc import Collection, Iterable, MutableMapping, MutableSequence, Sequence
from dataclasses import dataclass, field, replace
from types import ModuleType, TracebackType
from typing import Any, Final, Self

from fidem._bundle import Definition, flatten
from fidem._decorators import (
    EffectMiddlewares,
    MessageMiddlewares,
    declared_definition,
    effect_middleware_defs,
    message_middleware_defs,
)
from fidem._discovery import discover
from fidem.core.di.effects import EffectDependencyDef
from fidem.core.environment import EffectDef, EnvironmentImpl
from fidem.core.guards import GuardDef, GuardInputDef
from fidem.core.messages import Command, MessageId, Query
from fidem.core.operation_engine.definitions import (
    AtomicCommandDef,
    CommandDef,
    EventDef,
    QueryDef,
    StagedCommandDef,
)
from fidem.core.operation_engine.engine import OperationEngineImpl
from fidem.core.operation_engine.errors import DuplicateEventDefError
from fidem.core.stdlib import STDLIB_EFFECT_DEFS
from fidem.errors import UnsupportedDefinitionError

__all__ = [
    "Fidem",
]


class Fidem:
    """An application on fidem: it runs commands and queries by the definitions it was given and discovered.

    `modules` are scanned with the submodules of their packages, `definitions` are given by hand, a bundle stands
    for its contents, an object found twice counts once. Middlewares run in the order of the list, the first one is the outermost.
    """

    def __init__(
        self,
        *,
        modules: Collection[ModuleType] = (),
        definitions: Collection[Definition] = (),
        middlewares: MessageMiddlewares = (),
        effect_middlewares: EffectMiddlewares = (),
        stdlib: bool = True,
    ) -> None:
        """`stdlib` adds the handlers of the effects of `fidem.core.stdlib`."""
        collected = _Definitions()
        collected.add_all(_unique(flatten([*definitions, *discover(modules)])))
        if stdlib:
            collected.add_all(STDLIB_EFFECT_DEFS)
        self._environment: Final = EnvironmentImpl(
            operation_engine=OperationEngineImpl(
                command_defs=collected.command_defs,
                query_defs=collected.query_defs,
                event_defs=tuple(collected.type_to_event_def.values()),
                guard_defs=collected.guard_defs,
                middleware_defs=message_middleware_defs(middlewares),
            ),
            effect_defs=collected.effect_defs,
            dependency_defs=collected.dependency_defs,
            effect_middleware_defs=effect_middleware_defs(effect_middlewares),
        )

    async def __aenter__(self) -> Self:
        await self._environment.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self._environment.__aexit__(exc_type, exc, traceback)

    async def handle[ResultT](
        self,
        message: Command[ResultT] | Query[ResultT],
        *inputs: object,
        message_id: MessageId | None = None,
    ) -> ResultT:
        """Runs the operation of the command or the query and returns its result, the guards get the inputs."""
        return await self._environment.handle(message, *inputs, message_id=message_id)


def _unique(definitions: Iterable[object]) -> Sequence[object]:
    return tuple({id(definition): definition for definition in definitions}.values())


@dataclass(slots=True)
class _Definitions:
    """Definitions of the core sorted by kind, the definitions of one event are merged."""

    command_defs: MutableSequence[CommandDef[Any]] = field(default_factory=list)
    query_defs: MutableSequence[QueryDef[Any]] = field(default_factory=list)
    type_to_event_def: MutableMapping[type[Any], EventDef[Any]] = field(default_factory=dict)
    guard_defs: MutableSequence[GuardDef[Any] | GuardInputDef[Any]] = field(default_factory=list)
    effect_defs: MutableSequence[EffectDef[Any]] = field(default_factory=list)
    dependency_defs: MutableSequence[EffectDependencyDef[Any]] = field(default_factory=list)

    def add_all(self, definitions: Iterable[object]) -> None:
        for definition in definitions:
            self._add(definition)

    def _add(self, definition: object) -> None:
        match definition:
            case StagedCommandDef() | AtomicCommandDef():
                self.command_defs.append(definition)
            case QueryDef():
                self.query_defs.append(definition)
            case EventDef():
                self._add_event_def(definition)
            case GuardDef() | GuardInputDef():
                self.guard_defs.append(definition)
            case EffectDef():
                self.effect_defs.append(definition)
            case EffectDependencyDef():
                self.dependency_defs.append(definition)
            case _:
                declared = declared_definition(definition)
                if declared is None:
                    raise UnsupportedDefinitionError(definition)
                self._add(declared)

    def _add_event_def(self, event_def: EventDef[Any]) -> None:
        """Each handler declares the event anew, so the definitions of one event merge their handlers."""
        known_def = self.type_to_event_def.get(event_def.event_type)
        if known_def is None:
            self.type_to_event_def[event_def.event_type] = event_def
            return
        if (known_def.name, known_def.version) != (event_def.name, event_def.version):
            raise DuplicateEventDefError([known_def, event_def])
        self.type_to_event_def[event_def.event_type] = replace(
            known_def,
            handler_defs=(*known_def.handler_defs, *event_def.handler_defs),
        )
