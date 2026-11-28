"""Events delivered through the outbox in PostgreSQL.

Start PostgreSQL and run the example:

    docker run --rm -d -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgres:17-alpine
    uv run python -m examples.events.in_postgres

Another server is set with `DATABASE_URL`, the example recreates its tables there and empties `fidem.outbox`.

The business logic is in `common.py`, here are only the tables and the outbox in PostgreSQL. The user and its event
are saved in one transaction. The dispatcher of the outbox runs as a service of the environment, and a notification
on commit wakes it up.
"""

from asyncio import run
from collections.abc import AsyncGenerator, Sequence
from os import environ

from asyncpg import Connection, Pool, create_pool

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
from fidem import EffectContext, EffectDef, EffectDependencyDef, EnvironmentImpl, Serializer
from fidem.asyncpg import (
    OUTBOX_SECTION,
    AsyncpgConnectionDef,
    AsyncpgOutboxDispatcherDef,
    AsyncpgOutboxEffectDef,
    migrate,
)
from fidem.pydantic import PydanticSerializer

DATABASE_URL = environ.get("DATABASE_URL", "postgresql://postgres@localhost:5432/postgres")
SCHEMA = """
DROP TABLE IF EXISTS activity, users;
CREATE TABLE users (id text PRIMARY KEY, email text NOT NULL);
CREATE TABLE activity (id text PRIMARY KEY, user_id text NOT NULL, action text NOT NULL);
TRUNCATE fidem.outbox;
"""


# ── Dependencies ──────────────────────────────────────────
# The pool lives while the environment runs. The dispatcher holds one of its connections to listen to notifications,
# the others serve the queries.
async def pool() -> AsyncGenerator[Pool]:
    async with create_pool(DATABASE_URL) as pool:
        # Migrations are a separate step of the deploy in an application, the example applies them on start.
        await migrate(pool, [OUTBOX_SECTION])
        await pool.execute(SCHEMA)
        yield pool


# The outbox stores events as JSON, so the driver needs a serializer.
async def serializer() -> AsyncGenerator[Serializer]:
    yield PydanticSerializer()


# ── Effect handlers ───────────────────────────────────────
# Handlers get a plain asyncpg connection, it is in the transaction of the effect.
async def handle_save_user(connection: Connection, effect: SaveUserEffect) -> None:
    await connection.execute("INSERT INTO users (id, email) VALUES ($1, $2)", effect.user_id, effect.email)


async def handle_record_activity(connection: Connection, effect: RecordActivityEffect, context: EffectContext) -> None:
    recorded_id = await connection.fetchval(
        "INSERT INTO activity (id, user_id, action) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING RETURNING id",
        context.effect_id,
        effect.user_id,
        effect.action,
    )
    if recorded_id is not None:
        print(f"recorded {effect.action} of {effect.user_id}")


async def handle_get_deliveries(connection: Connection, effect: GetDeliveriesEffect) -> Sequence[Delivery]:
    rows = await connection.fetch("SELECT event_name, handler, status::text, attempts FROM fidem.outbox")
    return [Delivery(**row) for row in rows]


async def main() -> None:
    environment = EnvironmentImpl(
        operation_engine=make_operation_engine(),
        effect_defs=[
            EffectDef(SaveUserEffect, handle_save_user),
            EffectDef(RecordActivityEffect, handle_record_activity),
            EffectDef(GetDeliveriesEffect, handle_get_deliveries),
            make_mailer_def(),
            AsyncpgOutboxEffectDef(),
        ],
        dependency_defs=[
            EffectDependencyDef(pool),
            EffectDependencyDef(serializer),
            AsyncpgConnectionDef(),
            AsyncpgOutboxDispatcherDef(DISPATCHER_OPTIONS),
        ],
    )
    async with environment:
        await register_and_report(environment)


if __name__ == "__main__":
    run(main())
