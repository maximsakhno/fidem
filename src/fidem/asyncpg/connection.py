from collections.abc import AsyncGenerator, Callable, Mapping
from dataclasses import dataclass
from typing import Literal, cast

from asyncpg import Connection, Pool

from fidem.di.core import DependencyName
from fidem.di.effects import EffectDependencyDef, EffectDependencyLifetime
from fidem.errors import Error
from fidem.transactions import IMPLICIT_READ_TRANSACTION, AnyTransaction, TransactionContext

__all__ = [
    "AsyncpgConnectionDef",
    "AsyncpgTransactionOptions",
    "IsolationLevel",
    "TransactionRolledBackError",
]

type IsolationLevel = Literal["read_committed", "repeatable_read", "serializable"]


class TransactionRolledBackError(Error):
    """PostgreSQL rolled the transaction back on commit because a query in it had failed."""


@dataclass(frozen=True, slots=True)
class AsyncpgTransactionOptions:
    isolation: IsolationLevel = "read_committed"


class AsyncpgConnectionDef(EffectDependencyDef[Connection]):
    """A connection from the `asyncpg.Pool` dependency in the transaction of the effect."""

    def __init__(
        self,
        # Each query of a read committed transaction sees its own snapshot, so consistent reads need more.
        read_options: AsyncpgTransactionOptions = AsyncpgTransactionOptions(isolation="repeatable_read"),
        write_options: AsyncpgTransactionOptions = AsyncpgTransactionOptions(),
        transaction_options: Mapping[AnyTransaction, AsyncpgTransactionOptions] | None = None,
        name: DependencyName = "",
    ) -> None:
        """Options of a transaction replace the options of its kind."""
        super().__init__(
            factory=self._make_factory(
                read_options=read_options,
                write_options=write_options,
                transaction_options={
                    # A single read sees one snapshot anyway, so it needs no transaction.
                    IMPLICIT_READ_TRANSACTION: AsyncpgTransactionOptions(isolation="read_committed"),
                    **({} if transaction_options is None else transaction_options),
                },
            ),
            lifetime=EffectDependencyLifetime.TRANSACTION,
            name=name,
            type=Connection,
        )

    @staticmethod
    def _make_factory(
        read_options: AsyncpgTransactionOptions,
        write_options: AsyncpgTransactionOptions,
        transaction_options: Mapping[AnyTransaction, AsyncpgTransactionOptions],
    ) -> Callable[..., AsyncGenerator[Connection]]:
        async def connection(pool: Pool, transaction_context: TransactionContext) -> AsyncGenerator[Connection]:
            read_only = transaction_context.read_only
            options = transaction_options.get(
                transaction_context.transaction,
                read_options if read_only else write_options,
            )
            async with pool.acquire() as proxy:
                # The pool proxy forwards everything to the connection, and asyncpg documents it as one.
                connection = cast("Connection", proxy)
                # Each query of a read committed transaction sees its own snapshot, so reads need no transaction.
                if read_only and options.isolation == "read_committed":
                    yield connection
                else:
                    isolation = options.isolation.replace("_", " ").upper()
                    access_mode = " READ ONLY" if read_only else ""
                    await connection.execute(f"BEGIN ISOLATION LEVEL {isolation}{access_mode}")
                    try:
                        yield connection
                    except BaseException:
                        await connection.execute("ROLLBACK")
                        raise
                    # Unlike `Connection.transaction()`, this sees that the commit of a failed transaction rolled it back.
                    if await connection.execute("COMMIT") == "ROLLBACK":
                        raise TransactionRolledBackError

        return connection
