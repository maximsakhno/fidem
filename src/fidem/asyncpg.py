from collections.abc import AsyncGenerator, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, cast

from asyncpg import Connection, Pool

from fidem.di.effects import EffectDependencyDef, EffectDependencyLifetime
from fidem.effects import TransactionContext
from fidem.messages import Message

type IsolationLevel = Literal["read_committed", "repeatable_read", "serializable"]


class TransactionRolledBackError(Exception):
    """PostgreSQL rolled the transaction back on commit because a query in it had failed."""


@dataclass(frozen=True, slots=True)
class AsyncpgTransactionOptions:
    isolation: IsolationLevel = "read_committed"


class AsyncpgConnectionDef(EffectDependencyDef[Connection]):
    """A connection from the `asyncpg.Pool` dependency in the transaction of the effect."""

    def __init__(
        self,
        options: AsyncpgTransactionOptions = AsyncpgTransactionOptions(),
        message_options: Mapping[type[Message[Any]], AsyncpgTransactionOptions] | None = None,
        name: str = "",
    ) -> None:
        """Message options replace the options in the transactions of the message except the read-only ones."""
        super().__init__(
            factory=self._make_factory(options, {} if message_options is None else dict(message_options)),
            lifetime=EffectDependencyLifetime.TRANSACTION,
            name=name,
            type=Connection,
        )

    @staticmethod
    def _make_factory(
        options: AsyncpgTransactionOptions,
        message_options: Mapping[type[Message[Any]], AsyncpgTransactionOptions],
    ) -> Callable[..., AsyncGenerator[Connection]]:
        async def connection(
            pool: Pool,
            transaction_context: TransactionContext,
            message: Message[Any],
        ) -> AsyncGenerator[Connection]:
            # The options of a message are for its writes, and a read-only transaction wraps a single read.
            transaction_options = (
                options if transaction_context.read_only else message_options.get(type(message), options)
            )
            async with pool.acquire() as proxy:
                # The pool proxy forwards everything to the connection, and asyncpg documents it as one.
                connection = cast("Connection", proxy)
                # Each query of a read committed transaction sees its own snapshot anyway.
                if transaction_context.read_only and transaction_options.isolation == "read_committed":
                    yield connection
                else:
                    isolation = transaction_options.isolation.replace("_", " ").upper()
                    await connection.execute(f"BEGIN ISOLATION LEVEL {isolation}")
                    try:
                        yield connection
                    except BaseException:
                        await connection.execute("ROLLBACK")
                        raise
                    # Unlike `Connection.transaction()`, this sees that the commit of a failed transaction rolled it back.
                    if await connection.execute("COMMIT") == "ROLLBACK":
                        raise TransactionRolledBackError

        return connection
