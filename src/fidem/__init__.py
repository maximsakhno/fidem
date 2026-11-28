"""The API of an application on fidem: messages, effects, transactions and the errors it catches.

The internals and the declarations the user API builds on are in `fidem.core`.
"""

from fidem.core.di.effects import (
    ScopeClosedError,
)
from fidem.core.effects import (
    DatabaseEffect,
    DatabaseReadEffect,
    DatabaseWriteEffect,
    Effect,
    EffectContext,
    Effectful,
    EffectId,
    ExternalEffect,
    ExternalReadEffect,
    ExternalWriteEffect,
    InternalEffect,
    ReadEffect,
    WriteEffect,
)
from fidem.core.environment import (
    CallNext,
    EffectMiddleware,
    EffectNotDefinedError,
    EnvironmentAlreadyStartedError,
    EnvironmentNotStartedError,
)
from fidem.core.errors import (
    Error,
)
from fidem.core.guards import (
    AmbiguousGuardInputError,
    GuardInputNotDefinedError,
    MissingGuardInputError,
)
from fidem.core.messages import (
    AtomicEffectful,
    Command,
    Event,
    Message,
    MessageContext,
    MessageId,
    Query,
    QueryEffect,
    QueryEffectful,
    StagedEffectful,
)
from fidem.core.operation_engine.definitions import (
    MessageMiddlewareEffectful,
    Proceed,
)
from fidem.core.operation_engine.errors import (
    EventNotDefinedError,
    MessageNotDefinedError,
    UnsupportedAtomicResultError,
    UnsupportedEffectError,
)
from fidem.core.operation_engine.instructions import (
    EffectCall,
)
from fidem.core.outbox import (
    OutboxDispatcherOptions,
    RetryPolicy,
)
from fidem.core.stdlib import (
    NewUuidEffect,
    NowEffect,
    RandomBytesEffect,
    RandomIntEffect,
)
from fidem.core.transactions import (
    ReadTransaction,
    ReadTransactionEffect,
    ReadTransactionEffectful,
    TransactionContext,
    WriteTransaction,
    WriteTransactionEffect,
    WriteTransactionEffectful,
)

__all__ = [
    "AmbiguousGuardInputError",
    "AtomicEffectful",
    "CallNext",
    "Command",
    "DatabaseEffect",
    "DatabaseReadEffect",
    "DatabaseWriteEffect",
    "Effect",
    "EffectCall",
    "EffectContext",
    "EffectId",
    "EffectMiddleware",
    "EffectNotDefinedError",
    "Effectful",
    "EnvironmentAlreadyStartedError",
    "EnvironmentNotStartedError",
    "Error",
    "Event",
    "EventNotDefinedError",
    "ExternalEffect",
    "ExternalReadEffect",
    "ExternalWriteEffect",
    "GuardInputNotDefinedError",
    "InternalEffect",
    "Message",
    "MessageContext",
    "MessageId",
    "MessageMiddlewareEffectful",
    "MessageNotDefinedError",
    "MissingGuardInputError",
    "NewUuidEffect",
    "NowEffect",
    "OutboxDispatcherOptions",
    "Proceed",
    "Query",
    "QueryEffect",
    "QueryEffectful",
    "RandomBytesEffect",
    "RandomIntEffect",
    "ReadEffect",
    "ReadTransaction",
    "ReadTransactionEffect",
    "ReadTransactionEffectful",
    "RetryPolicy",
    "ScopeClosedError",
    "StagedEffectful",
    "TransactionContext",
    "UnsupportedAtomicResultError",
    "UnsupportedEffectError",
    "WriteEffect",
    "WriteTransaction",
    "WriteTransactionEffect",
    "WriteTransactionEffectful",
]
