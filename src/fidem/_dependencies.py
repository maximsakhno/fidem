from collections.abc import AsyncGenerator

from fidem.core.di.core import DependencyName
from fidem.core.di.effects import EffectDependencyDef

__all__ = [
    "instance",
]


def instance[DependencyT](
    value: DependencyT,
    *,
    name: DependencyName = "",
    type: object = None,
) -> EffectDependencyDef[DependencyT]:
    """Declares a ready object as a dependency of the application, nothing closes it.

    The class of the object is the type of the dependency when `type` is not set.
    """

    async def factory() -> AsyncGenerator[DependencyT]:
        yield value

    return EffectDependencyDef(factory=factory, name=name, type=value.__class__ if type is None else type)
