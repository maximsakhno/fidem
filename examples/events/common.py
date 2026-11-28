"""The business logic of the events example, the same for the outbox in memory and in PostgreSQL.

The command saves the user and publishes its event in one transaction. The dispatcher of the outbox delivers
the event to each handler in the background. Sending the welcome email fails once and is retried, and the event
it publishes is delivered too. Each variant gives only the handlers of the effects and the dependencies.
"""

from asyncio import sleep, timeout
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta

from fidem.core import (
    AtomicCommandDef,
    AtomicEffectful,
    AtomicEventHandlerDef,
    Command,
    DatabaseReadEffect,
    DatabaseWriteEffect,
    EffectDef,
    Environment,
    Event,
    EventDef,
    ExternalWriteEffect,
    OperationEngineImpl,
    OutboxDispatcherOptions,
    Query,
    QueryDef,
    QueryEffectful,
    RetryPolicy,
    StagedEffectful,
    StagedEventHandlerDef,
    WriteTransaction,
    WriteTransactionEffectful,
)

# A short delay shows the retry without waiting.
DISPATCHER_OPTIONS = OutboxDispatcherOptions(retry_policy=RetryPolicy(initial_delay=timedelta(milliseconds=100)))


# ── Messages ──────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class RegisterUserCommand(Command[str]):
    user_id: str
    email: str


@dataclass(frozen=True, slots=True)
class UserRegistered(Event):
    user_id: str
    email: str


@dataclass(frozen=True, slots=True)
class WelcomeSent(Event):
    user_id: str


@dataclass(frozen=True, slots=True)
class Delivery:
    event_name: str
    handler: str
    status: str
    attempts: int


@dataclass(frozen=True, slots=True)
class GetDeliveriesQuery(Query[Sequence[Delivery]]):
    pass


# ── Effects ───────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class SaveUserEffect(DatabaseWriteEffect[None]):
    user_id: str
    email: str


@dataclass(frozen=True, slots=True)
class RecordActivityEffect(DatabaseWriteEffect[None]):
    """A repeated delivery repeats the effect with the same id, so its handler skips the write."""

    user_id: str
    action: str


@dataclass(frozen=True, slots=True)
class GetDeliveriesEffect(DatabaseReadEffect[Sequence[Delivery]]):
    """Reads the deliveries of the outbox, so the example sees when the dispatcher has delivered everything."""


@dataclass(frozen=True, slots=True)
class SendEmailEffect(ExternalWriteEffect[None]):
    email: str
    text: str


class Mailer:
    """Fails the first email to show a retry of the delivery."""

    def __init__(self) -> None:
        self._failed: bool = False

    async def send(self, effect: SendEmailEffect) -> None:
        if not self._failed:
            self._failed = True
            raise ConnectionError("mail server is down")
        print(f"email to {effect.email}: {effect.text}")


# ── Handlers ──────────────────────────────────────────────
# The event goes to the outbox in the transaction that saves the user.
@WriteTransaction
def save_user(command: RegisterUserCommand) -> WriteTransactionEffectful[str, UserRegistered]:
    yield from SaveUserEffect(command.user_id, command.email)
    yield from UserRegistered(command.user_id, command.email)
    return command.user_id


def register_user(command: RegisterUserCommand) -> AtomicEffectful[str, UserRegistered]:
    yield from ()
    return save_user(command)


def send_welcome(event: UserRegistered) -> StagedEffectful[None, WelcomeSent]:
    yield from SendEmailEffect(event.email, "Welcome!")
    yield from WelcomeSent(event.user_id)


@WriteTransaction
def record_activity(user_id: str, action: str) -> WriteTransactionEffectful[None]:
    yield from RecordActivityEffect(user_id, action)


def record_registration(event: UserRegistered) -> AtomicEffectful[None]:
    yield from ()
    return record_activity(event.user_id, "registration")


def record_welcome(event: WelcomeSent) -> AtomicEffectful[None]:
    yield from ()
    return record_activity(event.user_id, "welcome")


def get_deliveries(query: GetDeliveriesQuery) -> QueryEffectful[Sequence[Delivery]]:
    return (yield from GetDeliveriesEffect())


def make_operation_engine() -> OperationEngineImpl:
    return OperationEngineImpl(
        command_defs=[AtomicCommandDef("register_user", RegisterUserCommand, register_user)],
        query_defs=[QueryDef(GetDeliveriesQuery, get_deliveries)],
        event_defs=[
            EventDef(
                name="user_registered",
                event_type=UserRegistered,
                handler_defs=[
                    StagedEventHandlerDef("send_welcome", send_welcome),
                    AtomicEventHandlerDef("record", record_registration),
                ],
            ),
            EventDef(
                name="welcome_sent",
                event_type=WelcomeSent,
                handler_defs=[AtomicEventHandlerDef("record", record_welcome)],
            ),
        ],
    )


def make_mailer_def() -> EffectDef[SendEmailEffect]:
    return EffectDef(SendEmailEffect, Mailer().send)


# ── Scenario ──────────────────────────────────────────────
async def register_and_report(environment: Environment) -> None:
    print("registered", await environment.handle(RegisterUserCommand("u1", "alice@example.com")))
    deliveries = await _wait_for_deliveries(environment)
    for delivery in sorted(deliveries, key=lambda delivery: (delivery.event_name, delivery.handler)):
        print(f"{delivery.event_name} → {delivery.handler}: {delivery.status} after {delivery.attempts} attempts")


async def _wait_for_deliveries(environment: Environment) -> Sequence[Delivery]:
    """A handler writes the events it publishes before its delivery is done, so no pending one means all are done."""
    async with timeout(10):
        while True:
            deliveries = await environment.handle(GetDeliveriesQuery())
            if all(delivery.status != "pending" for delivery in deliveries):
                return deliveries
            await sleep(0.05)
