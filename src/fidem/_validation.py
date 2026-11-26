from collections import defaultdict
from collections.abc import Callable, Hashable, Iterable, MutableMapping, MutableSequence, Sequence

__all__ = [
    "check_unique",
]


def check_unique[DefT](
    defs: Iterable[DefT],
    get_key: Callable[[DefT], Hashable],
    duplicate_error_type: Callable[[Sequence[DefT]], Exception],
) -> None:
    key_to_defs: MutableMapping[Hashable, MutableSequence[DefT]] = defaultdict(list)
    for definition in defs:
        key_to_defs[get_key(definition)].append(definition)
    for same_key_defs in key_to_defs.values():
        if len(same_key_defs) > 1:
            raise duplicate_error_type(same_key_defs)
