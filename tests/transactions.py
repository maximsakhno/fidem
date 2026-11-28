from fidem.core.transactions import (
    ReadTransaction,
    ReadTransactionEffectful,
    WriteTransaction,
    WriteTransactionEffectful,
)
from tests.effects import TestDatabaseReadEffect, TestDatabaseWriteEffect


@ReadTransaction
def read_transaction() -> ReadTransactionEffectful[str]:
    return (yield from TestDatabaseReadEffect())


@WriteTransaction
def write_transaction() -> WriteTransactionEffectful[str]:
    return (yield from TestDatabaseWriteEffect())
