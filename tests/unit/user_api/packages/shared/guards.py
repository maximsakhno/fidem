from fidem import Effectful, QueryEffect, guard


@guard
def shared_guard() -> Effectful[int, QueryEffect]:
    yield from ()
    return 1
