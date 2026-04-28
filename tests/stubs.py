from collections.abc import Generator, Sequence
from typing import Any

from fidem.intents import IntentContext, IntentHandler, ReadIntent, WriteIntent
from fidem.operations import Command, CommandHandler, Event
from fidem.testing import RaiseStep, YeildStep


class GeneratorFunctionStub:
    def __init__(
        self,
        args: tuple[Any, ...] | None = None,
        steps: Sequence[YeildStep | RaiseStep] = (),
        return_value: Any | None = None,
    ) -> None:
        self._args = args
        self._steps = steps
        self._return_value = return_value

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


def command_middleware_stub[
    CommandT: Command[Any],
    ResultT,
    EventOutT: Event | None,
](handler: CommandHandler[CommandT, ResultT, EventOutT]) -> CommandHandler[CommandT, ResultT, EventOutT]:
    return handler


def intent_middleware_stub[
    IntentContextT: IntentContext,
    IntentT: ReadIntent[Any] | WriteIntent[Any],
    ResultT,
](handler: IntentHandler[IntentContextT, IntentT, ResultT]) -> IntentHandler[IntentContextT, IntentT, ResultT]:
    return handler
