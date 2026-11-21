from collections.abc import Generator, Sequence
from typing import Any

from fidem.testing import RaiseStep, YeildStep


class GeneratorFunctionStub:
    def __init__(
        self,
        args: tuple[object, ...] | None = None,
        steps: Sequence[YeildStep | RaiseStep] = (),
        return_value: object = None,
    ) -> None:
        self._args: tuple[object, ...] | None = args
        self._steps: Sequence[YeildStep | RaiseStep] = steps
        self._return_value: object = return_value

    def __call__(self, *args: Any) -> Generator[Any, Any, Any]:
        if self._args is not None:
            assert args == self._args

        for step in self._steps:
            if isinstance(step, RaiseStep):
                raise step.exception
            else:
                send_value = yield step.yeild_value
                assert send_value == step.send_value

        return self._return_value
