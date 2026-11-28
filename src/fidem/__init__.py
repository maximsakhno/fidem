"""The API of an application on fidem: `Fidem`, decorators, messages, effects, transactions and runtime errors.

The internals and the definitions the decorators reduce to are in `fidem.core`.
"""

from fidem._bundle import (
    Bundle,
    Definition,
)
from fidem._decorators import (
    command,
    command_handler,
    dependency,
    effect,
    effect_handler,
    effect_middleware,
    event,
    event_handler,
    guard,
    guard_input,
    middleware,
    query_handler,
)
from fidem._fidem import (
    Fidem,
)
from fidem.core.di.effects import (
    EffectDependencyLifetime,
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
    EffectName,
    EffectVersion,
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
    ExecutionMode,
    MessageMiddleware,
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
    "Bundle",
    "CallNext",
    "Command",
    "DatabaseEffect",
    "DatabaseReadEffect",
    "DatabaseWriteEffect",
    "Definition",
    "Effect",
    "EffectCall",
    "EffectContext",
    "EffectDependencyLifetime",
    "EffectId",
    "EffectMiddleware",
    "EffectName",
    "EffectNotDefinedError",
    "EffectVersion",
    "Effectful",
    "EnvironmentAlreadyStartedError",
    "EnvironmentNotStartedError",
    "Error",
    "Event",
    "EventNotDefinedError",
    "ExecutionMode",
    "ExternalEffect",
    "ExternalReadEffect",
    "ExternalWriteEffect",
    "Fidem",
    "GuardInputNotDefinedError",
    "InternalEffect",
    "Message",
    "MessageContext",
    "MessageId",
    "MessageMiddleware",
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
    "command",
    "command_handler",
    "dependency",
    "effect",
    "effect_handler",
    "effect_middleware",
    "event",
    "event_handler",
    "guard",
    "guard_input",
    "middleware",
    "query_handler",
]
