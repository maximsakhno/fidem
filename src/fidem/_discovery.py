from collections.abc import Collection, Iterator, Mapping
from importlib import import_module
from pkgutil import iter_modules
from types import ModuleType

from fidem._decorators import is_declared

__all__ = [
    "discover",
]


def discover(modules: Collection[ModuleType]) -> Iterator[object]:
    """Yields the marked functions and classes of the modules and of the submodules of their packages.

    A function or a class imported from a module that is not scanned is skipped, so an import does not bring in
    a handler.
    """
    scanned = _import_modules(modules)
    for module in scanned.values():
        for value in vars(module).values():
            # The decorator marks the function or the class in the module that defines it.
            if getattr(value, "__module__", None) in scanned and is_declared(value):
                yield value


def _import_modules(modules: Collection[ModuleType]) -> Mapping[str, ModuleType]:
    """Imports the submodules of the packages, an import error propagates."""
    return {submodule.__name__: submodule for module in modules for submodule in _walk(module)}


def _walk(module: ModuleType) -> Iterator[ModuleType]:
    yield module
    # Only a package has a path.
    path: list[str] | None = getattr(module, "__path__", None)
    if path is None:
        return
    for submodule_info in iter_modules(path, prefix=f"{module.__name__}."):
        yield from _walk(import_module(submodule_info.name))
