from ast import Import, ImportFrom, parse, walk
from collections.abc import Iterator
from pathlib import Path

import fidem.core

CORE_DIR = Path(fidem.core.__file__).parent


def imported_modules(path: Path) -> Iterator[str]:
    for node in walk(parse(path.read_text(encoding="utf-8"))):
        match node:
            case Import(names=names):
                yield from (alias.name for alias in names)
            case ImportFrom(module=str(module), level=0):
                yield module
            case _:
                pass


def is_outside_core(module: str) -> bool:
    return module == "fidem" or (module.startswith("fidem.") and not module.startswith("fidem.core"))


class TestCore:
    def test_imports_nothing_from_fidem_outside_core(self) -> None:
        violations = [
            f"{path.relative_to(CORE_DIR)}: {module}"
            for path in sorted(CORE_DIR.rglob("*.py"))
            for module in imported_modules(path)
            if is_outside_core(module)
        ]

        assert violations == []
