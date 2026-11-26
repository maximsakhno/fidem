"""Queries with consistent reads in PostgreSQL through asyncpg.

Start PostgreSQL and run the example:

    docker run --rm -d -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgres:17-alpine
    uv run python examples/07_queries.py

Another server is set with `DATABASE_URL`, the example recreates its tables there.
"""

from asyncio import gather, run
from collections.abc import AsyncGenerator, Sequence
from dataclasses import dataclass
from decimal import Decimal
from os import environ

from asyncpg import Connection, Pool, create_pool

from fidem import (
    AtomicCommandDef,
    AtomicEffectful,
    Command,
    DatabaseReadEffect,
    DatabaseWriteEffect,
    EffectContext,
    EffectDef,
    EffectDependencyDef,
    EnvironmentImpl,
    OperationEngineImpl,
    Query,
    QueryDef,
    QueryEffectful,
    ReadTransaction,
    ReadTransactionEffectful,
    WriteTransaction,
    WriteTransactionEffectful,
)
from fidem.asyncpg import AsyncpgConnectionDef, AsyncpgTransactionOptions

DATABASE_URL = environ.get("DATABASE_URL", "postgresql://postgres@localhost:5432/postgres")
SCHEMA = """
DROP TABLE IF EXISTS transfers, accounts;
CREATE TABLE accounts (id text PRIMARY KEY, balance numeric NOT NULL);
CREATE TABLE transfers (
    position bigint GENERATED ALWAYS AS IDENTITY,
    id text PRIMARY KEY,
    source_id text NOT NULL,
    target_id text NOT NULL,
    amount numeric NOT NULL
);
INSERT INTO accounts (id, balance) VALUES ('alice', 100), ('bob', 0);
"""


# ── Domain types ──────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class Transfer:
    source_id: str
    target_id: str
    amount: Decimal


class AccountNotFoundError(Exception):
    pass


class InsufficientFundsError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class Statement:
    account_id: str
    balance: Decimal
    transfers: Sequence[Transfer]


# ── Dependencies ──────────────────────────────────────────
async def pool() -> AsyncGenerator[Pool]:
    async with create_pool(DATABASE_URL) as pool:
        await pool.execute(SCHEMA)
        yield pool


# ── Effects ───────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class GetBalanceEffect(DatabaseReadEffect[Decimal]):
    account_id: str


@dataclass(frozen=True, slots=True)
class ListTransfersEffect(DatabaseReadEffect[Sequence[Transfer]]):
    account_id: str


@dataclass(frozen=True, slots=True)
class RecordTransferEffect(DatabaseWriteEffect[None]):
    transfer: Transfer


async def handle_get_balance(connection: Connection, effect: GetBalanceEffect) -> Decimal:
    balance = await connection.fetchval("SELECT balance FROM accounts WHERE id = $1", effect.account_id)
    if balance is None:
        raise AccountNotFoundError(effect.account_id)
    return balance


async def handle_list_transfers(connection: Connection, effect: ListTransfersEffect) -> Sequence[Transfer]:
    rows = await connection.fetch(
        "SELECT source_id, target_id, amount FROM transfers WHERE $1 IN (source_id, target_id) ORDER BY position",
        effect.account_id,
    )
    return [Transfer(source_id=row["source_id"], target_id=row["target_id"], amount=row["amount"]) for row in rows]


# A retried command repeats the effect with the same id, so the balances change only with the first record.
async def handle_record_transfer(connection: Connection, effect: RecordTransferEffect, context: EffectContext) -> None:
    transfer = effect.transfer
    recorded_id = await connection.fetchval(
        "INSERT INTO transfers (id, source_id, target_id, amount) VALUES ($1, $2, $3, $4)"
        " ON CONFLICT (id) DO NOTHING RETURNING id",
        context.effect_id,
        transfer.source_id,
        transfer.target_id,
        transfer.amount,
    )
    if recorded_id is None:
        return
    await connection.execute(
        "UPDATE accounts SET balance = balance + CASE id WHEN $1 THEN -$3::numeric ELSE $3::numeric END"
        " WHERE id IN ($1, $2)",
        transfer.source_id,
        transfer.target_id,
        transfer.amount,
    )


# ── Messages ──────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class TransferCommand(Command[Transfer]):
    source_id: str
    target_id: str
    amount: Decimal


@dataclass(frozen=True, slots=True)
class GetStatementQuery(Query[Statement]):
    account_id: str


# The check and the writes must not interleave with a concurrent transfer, so the options make it serializable.
@WriteTransaction
def record_transfer(transfer: Transfer) -> WriteTransactionEffectful[Transfer]:
    if (yield from GetBalanceEffect(transfer.source_id)) < transfer.amount:
        raise InsufficientFundsError(transfer.source_id)
    yield from RecordTransferEffect(transfer)
    return transfer


# Both reads see one snapshot, so the balance always matches the transfers, even while transfers run.
@ReadTransaction
def read_statement(account_id: str) -> ReadTransactionEffectful[Statement]:
    balance = yield from GetBalanceEffect(account_id)
    transfers = yield from ListTransfersEffect(account_id)
    return Statement(account_id=account_id, balance=balance, transfers=transfers)


def transfer(command: TransferCommand) -> AtomicEffectful[Transfer]:
    yield from ()
    return record_transfer(Transfer(source_id=command.source_id, target_id=command.target_id, amount=command.amount))


def get_statement(query: GetStatementQuery) -> QueryEffectful[Statement]:
    return (yield from read_statement(query.account_id))


async def main() -> None:
    environment = EnvironmentImpl(
        operation_engine=OperationEngineImpl(
            command_defs=[AtomicCommandDef("transfer", TransferCommand, transfer)],
            query_defs=[QueryDef("get_statement", GetStatementQuery, get_statement)],
        ),
        effect_defs=[
            EffectDef(GetBalanceEffect, handle_get_balance),
            EffectDef(ListTransfersEffect, handle_list_transfers),
            EffectDef(RecordTransferEffect, handle_record_transfer),
        ],
        dependency_defs=[
            EffectDependencyDef(pool),
            # Read transactions are repeatable read and write transactions are read committed unless set otherwise.
            AsyncpgConnectionDef(
                transaction_options={record_transfer: AsyncpgTransactionOptions(isolation="serializable")},
            ),
        ],
    )

    async def transfer_to_bob() -> None:
        for _ in range(5):
            await environment.execute(TransferCommand("alice", "bob", Decimal(10)))

    async def read_statements_of_bob() -> Sequence[Statement]:
        return [await environment.query(GetStatementQuery("bob")) for _ in range(5)]

    async with environment:
        print("\n=== Statements read while transfers run always add up ===")
        _, statements = await gather(transfer_to_bob(), read_statements_of_bob())
        for statement in statements:
            received = sum(transfer.amount for transfer in statement.transfers)
            print(f"Bob has {statement.balance}, {len(statement.transfers)} transfers bring {received}")

        print("\n=== Insufficient funds stop the transfer inside the transaction ===")
        try:
            await environment.execute(TransferCommand("alice", "bob", Decimal(1000)))
        except InsufficientFundsError as exc:
            print(f"Error: insufficient funds on {exc}")
        print()


if __name__ == "__main__":
    run(main())
