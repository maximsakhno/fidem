from dataclasses import dataclass

from fidem import Command, Event, command, event


@command("charge")
@dataclass(frozen=True, slots=True)
class ChargeCommand(Command[str]):
    amount: int


@event("charged")
@dataclass(frozen=True, slots=True)
class ChargedEvent(Event):
    amount: int


@event("refunded")
@dataclass(frozen=True, slots=True)
class RefundedEvent(Event):
    """An event without handlers."""
