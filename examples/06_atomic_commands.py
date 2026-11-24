"""Atomic commands in PostgreSQL through asyncpg.

Start PostgreSQL and run the example:

    docker run --rm -d -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgres:17-alpine
    uv run python examples/06_atomic_commands.py

Another server is set with `DATABASE_URL`, the example recreates its tables there.
"""

from asyncio import run
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from decimal import Decimal
from os import environ
from typing import Any

from asyncpg import Connection, Pool, create_pool

from fidem.asyncpg import AsyncpgConnectionDef
from fidem.di.effects import EffectDependencyDef
from fidem.effects import DatabaseReadEffect, DatabaseWriteEffect, EffectContext, Effectful
from fidem.environment import EffectDef, EnvironmentImpl
from fidem.messages import AtomicEffectful, Command, TransactionEffectful
from fidem.operation_engine import AtomicCommandDef, OperationEngineImpl, StagedCommandDef

DATABASE_URL = environ.get("DATABASE_URL", "postgresql://postgres@localhost:5432/postgres")
SCHEMA = """
DROP TABLE IF EXISTS transfers, accounts;
CREATE TABLE accounts (id text PRIMARY KEY, balance numeric NOT NULL);
CREATE TABLE transfers (id text PRIMARY KEY, source_id text NOT NULL, target_id text NOT NULL, amount numeric NOT NULL);
INSERT INTO accounts (id, balance) VALUES ('alice', 100), ('bob', 0);
"""


# ── Domain types ──────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class Account:
    id: str
    balance: Decimal


@dataclass(frozen=True, slots=True)
class Transfer:
    source_id: str
    target_id: str
    amount: Decimal


class AccountNotFoundError(Exception):
    pass


class InsufficientFundsError(Exception):
    pass


# ── Dependencies ──────────────────────────────────────────
# The pool lives while the environment runs, and each transaction takes its own connection from it.
async def pool() -> AsyncGenerator[Pool]:
    async with create_pool(DATABASE_URL) as pool:
        await pool.execute(SCHEMA)
        yield pool


# ── Effects ───────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class GetAccountEffect(DatabaseReadEffect[Account]):
    account_id: str


@dataclass(frozen=True, slots=True)
class ChangeBalanceEffect(DatabaseWriteEffect[Account]):
    account_id: str
    amount: Decimal


@dataclass(frozen=True, slots=True)
class RecordTransferEffect(DatabaseWriteEffect[bool]):
    source_id: str
    target_id: str
    amount: Decimal


# Handlers get a plain asyncpg connection, it is in the transaction of the effect.
async def handle_get_account(connection: Connection, effect: GetAccountEffect) -> Account:
    row = await connection.fetchrow("SELECT id, balance FROM accounts WHERE id = $1", effect.account_id)
    if row is None:
        raise AccountNotFoundError(effect.account_id)
    return Account(id=row["id"], balance=row["balance"])


# The condition guards against concurrent transfers that passed the check in the read phase together.
async def handle_change_balance(connection: Connection, effect: ChangeBalanceEffect) -> Account:
    row = await connection.fetchrow(
        "UPDATE accounts SET balance = balance + $2 WHERE id = $1 AND balance + $2 >= 0 RETURNING id, balance",
        effect.account_id,
        effect.amount,
    )
    if row is None:
        if await connection.fetchval("SELECT 1 FROM accounts WHERE id = $1", effect.account_id) is None:
            raise AccountNotFoundError(effect.account_id)
        raise InsufficientFundsError(effect.account_id)
    print(f"  {effect.account_id}: {row['balance'] - effect.amount} -> {row['balance']}")
    return Account(id=row["id"], balance=row["balance"])


# A retried command repeats the effect with the same id, so the result tells whether an earlier attempt recorded it.
async def handle_record_transfer(
    connection: Connection,
    effect: RecordTransferEffect,
    context: EffectContext,
) -> bool:
    recorded_id = await connection.fetchval(
        "INSERT INTO transfers (id, source_id, target_id, amount) VALUES ($1, $2, $3, $4)"
        " ON CONFLICT (id) DO NOTHING RETURNING id",
        context.effect_id,
        effect.source_id,
        effect.target_id,
        effect.amount,
    )
    return recorded_id is not None


# ── Commands ──────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class TransferCommand(Command[Transfer]):
    source_id: str
    target_id: str
    amount: Decimal


@dataclass(frozen=True, slots=True)
class GetAccountCommand(Command[Account]):
    account_id: str


# First the handler reads outside the transaction, then it returns the writes to run in one transaction.
def transfer(command: TransferCommand) -> AtomicEffectful[Transfer]:
    source = yield from GetAccountEffect(command.source_id)
    if source.balance < command.amount:
        raise InsufficientFundsError(source.id)

    def transaction() -> TransactionEffectful[Transfer]:
        if (yield from RecordTransferEffect(command.source_id, command.target_id, command.amount)):
            yield from ChangeBalanceEffect(command.source_id, -command.amount)
            yield from ChangeBalanceEffect(command.target_id, command.amount)
        return Transfer(source_id=command.source_id, target_id=command.target_id, amount=command.amount)

    return transaction


def get_account(command: GetAccountCommand) -> Effectful[Account, DatabaseReadEffect[Any]]:
    return (yield from GetAccountEffect(command.account_id))


async def main() -> None:
    environment = EnvironmentImpl(
        operation_engine=OperationEngineImpl(
            command_defs=[
                AtomicCommandDef("transfer", TransferCommand, transfer),
                StagedCommandDef("get_account", GetAccountCommand, get_account),
            ],
        ),
        effect_defs=[
            EffectDef(GetAccountEffect, handle_get_account),
            EffectDef(ChangeBalanceEffect, handle_change_balance),
            EffectDef(RecordTransferEffect, handle_record_transfer),
        ],
        dependency_defs=[EffectDependencyDef(pool), AsyncpgConnectionDef()],
    )

    async with environment:
        print("\n=== Transfer commits both balances and the transfer together ===")
        result = await environment.execute(TransferCommand("alice", "bob", Decimal(30)), "transfer-1")
        print(f"Result: {result}")

        print("\n=== A retry with the same message id does not move the money again ===")
        result = await environment.execute(TransferCommand("alice", "bob", Decimal(30)), "transfer-1")
        print(f"Result: {result}")

        print("\n=== Insufficient funds stop the command before the transaction ===")
        try:
            await environment.execute(TransferCommand("alice", "bob", Decimal(1000)))
        except InsufficientFundsError as exc:
            print(f"Error: insufficient funds on {exc}")

        print("\n=== A failed write rolls back the whole transaction ===")
        try:
            await environment.execute(TransferCommand("alice", "carol", Decimal(10)))
        except AccountNotFoundError as exc:
            print(f"Error: account {exc} not found")
        alice = await environment.execute(GetAccountCommand("alice"))
        print(f"Alice still has {alice.balance}")
        print()


if __name__ == "__main__":
    run(main())
