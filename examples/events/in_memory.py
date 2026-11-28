"""Events delivered through the outbox in memory, which needs no database.

    uv run python -m examples.events.in_memory

The business logic is in `common.py`, here are only the storage of users and activity in dictionaries
and the outbox in memory. Its dispatcher runs as a service of the environment.
"""

from asyncio import run
from collections.abc import Callable, Coroutine, MutableMapping, Sequence
from typing import Any

from examples.events.common import (
    DISPATCHER_OPTIONS,
    Delivery,
    GetDeliveriesEffect,
    RecordActivityEffect,
    SaveUserEffect,
    make_mailer_def,
    make_operation_engine,
    register_and_report,
)
from fidem.core import EffectContext, EffectDef, EnvironmentImpl
from fidem.memory import MemoryOutbox

USERS: MutableMapping[str, str] = {}
ACTIVITY: MutableMapping[str, tuple[str, str]] = {}


async def handle_save_user(effect: SaveUserEffect) -> None:
    USERS[effect.user_id] = effect.email


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
    environment = EnvironmentImpl(
        operation_engine=make_operation_engine(),
        effect_defs=[
            EffectDef(SaveUserEffect, handle_save_user),
            EffectDef(RecordActivityEffect, handle_record_activity),
            EffectDef(GetDeliveriesEffect, make_get_deliveries_handler(outbox)),
            make_mailer_def(),
            *outbox.effect_defs,
        ],
        dependency_defs=outbox.dependency_defs,
    )
    async with environment:
        await register_and_report(environment)


if __name__ == "__main__":
    run(main())
