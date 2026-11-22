from collections.abc import Callable, Generator, Sequence
from typing import Any

import pytest

from fidem.di.messages import (
    GeneratorFunctionMessageDependencyResolver,
    MessageDependencyDef,
    MessageDependencyLifetime,
)
from fidem.testing import RaiseStep, YeildStep


class GeneratorFunctionStub:
    def __init__(
        self,
        steps: Sequence[YeildStep | RaiseStep] = (),
        return_value: object = None,
    ) -> None:
        self._steps: Sequence[YeildStep | RaiseStep] = steps
        self._return_value: object = return_value

    def __call__(self, *args: Any, **kwargs: Any) -> Generator[Any, Any, Any]:
        for step in self._steps:
            if isinstance(step, RaiseStep):
                raise step.exception
            else:
                send_value = yield step.yeild_value
                assert send_value == step.send_value

        return self._return_value


def make_message_dependency_def(factory: Callable[..., Any]) -> MessageDependencyDef[Any]:
    return MessageDependencyDef(
        resolver=GeneratorFunctionMessageDependencyResolver(factory),
        lifetime=MessageDependencyLifetime.MESSAGE,
    )


def run[ResultT](generator: Generator[Any, Any, ResultT]) -> ResultT:
    """Runs a generator that yields nothing and returns its result."""
    with pytest.raises(StopIteration) as exc_info:
        generator.send(None)
    return exc_info.value.value
