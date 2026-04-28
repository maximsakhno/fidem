from collections.abc import Generator, Sequence
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class YeildStep:
    yeild_value: Any
    send_value: Any


@dataclass(frozen=True, slots=True)
class RaiseStep:
    exception: Exception


def assert_generator(
    generator: Generator[Any, Any, Any],
    steps: Sequence[YeildStep | RaiseStep] = (),
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
            assert generator.send(send_value) == step.yeild_value
            send_value = step.send_value

    try:
        generator.send(send_value)
    except StopIteration as exc:
        assert exc.value == return_value
