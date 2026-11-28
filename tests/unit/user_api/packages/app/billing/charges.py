from fidem import StagedEffectful, command_handler
from tests.unit.user_api.packages.app.messages import ChargeCommand, ChargedEvent
from tests.unit.user_api.packages.shared.guards import shared_guard

# The guard of another package is re-exported, discovery of this package skips it.
__all__ = [
    "charge",
    "shared_guard",
]


@command_handler
def charge(command: ChargeCommand) -> StagedEffectful[str, ChargedEvent]:
    yield from ChargedEvent(command.amount)
    return f"charged {command.amount}"
