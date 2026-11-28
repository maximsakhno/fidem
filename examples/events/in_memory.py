"""Events delivered through the outbox in memory, which needs no database.

    uv run python -m examples.events.in_memory

The business logic is in `common.py`, here are only the storage of users and activity in dictionaries
and the outbox in memory. Its dispatcher runs as a service of the application.
"""

from asyncio import run
from collections.abc import Callable, Coroutine, MutableMapping, Sequence
from typing import Any

from examples.events import common
from examples.events.common import (
    DISPATCHER_OPTIONS,
    Delivery,
    GetDeliveriesEffect,
    RecordActivityEffect,
    SaveUserEffect,
    register_and_report,
)
from fidem import EffectContext, Fidem, effect_handler
from fidem.memory import MemoryOutbox

USERS: MutableMapping[str, str] = {}
ACTIVITY: MutableMapping[str, tuple[str, str]] = {}


@effect_handler
async def handle_save_user(effect: SaveUserEffect) -> None:
    USERS[effect.user_id] = effect.email


@effect_handler
async def handle_record_activity(effect: RecordActivityEffect, context: EffectContext) -> None:
    if context.effect_id not in ACTIVITY:
        ACTIVITY[context.effect_id] = (effect.user_id, effect.action)
        print(f"recorded {effect.action} of {effect.user_id}")


def make_get_deliveries_handler(outbox: MemoryOutbox) -> Callable[..., Coroutine[Any, Any, Sequence[Delivery]]]:
    async def handle_get_deliveries(effect: GetDeliveriesEffect) -> Sequence[Delivery]:
        return [
            Delivery(event_name=row.event_name, handler=row.handler, status=row.status.value, attempts=row.attempts)
            for row in outbox.rows
        ]

    return handle_get_deliveries


async def main() -> None:
    outbox = MemoryOutbox(DISPATCHER_OPTIONS)
    fidem = Fidem(
        modules=[common],
        definitions=[
            handle_save_user,
            handle_record_activity,
            # The handler reads the outbox it closes over, so it is declared here and not at the module level.
            effect_handler(make_get_deliveries_handler(outbox)),
            # The outbox is a bundle of the write of events and the service of the dispatcher.
            outbox,
        ],
    )
    async with fidem:
        await register_and_report(fidem)


if __name__ == "__main__":
    run(main())
