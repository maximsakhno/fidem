from collections.abc import Generator, Sequence
from dataclasses import dataclass
from typing import Any

__all__ = [
    "RaiseStep",
    "YieldStep",
    "assert_generator",
]


@dataclass(frozen=True, slots=True)
class YieldStep:
    yield_value: Any
    send_value: Any


@dataclass(frozen=True, slots=True)
class RaiseStep:
    exception: Exception


def assert_generator(
    generator: Generator[Any, Any, Any],
    steps: Sequence[YieldStep | RaiseStep] = (),
    return_value: Any | None = None,
) -> None:
    send_value: Any | None = None
    for step in steps:
        if isinstance(step, RaiseStep):
            try:
                generator.send(send_value)
            except type(step.exception) as exc:
                assert exc == step.exception
                return
            else:
                raise AssertionError(f"Expected {type(step.exception)} to be raised")
        else:
            assert generator.send(send_value) == step.yield_value
            send_value = step.send_value

    try:
        generator.send(send_value)
    except StopIteration as exc:
        assert exc.value == return_value
    else:
        raise AssertionError("Expected generator to finish")
