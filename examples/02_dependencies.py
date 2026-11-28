from abc import ABC, abstractmethod
from asyncio import run
from collections.abc import AsyncGenerator, MutableMapping
from dataclasses import dataclass, replace
from typing import Any, Final

from fidem import (
    Command,
    DatabaseReadEffect,
    DatabaseWriteEffect,
    Effect,
    EffectContext,
    EffectDef,
    EffectDependencyDef,
    EffectDependencyLifetime,
    EnvironmentImpl,
    MessageContext,
    OperationEngineImpl,
    StagedCommandDef,
    StagedEffectful,
)


# ── Domain types ──────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class User:
    id: str
    name: str


# ── Dependencies ──────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str
    replica_url: str


class Database(ABC):
    @abstractmethod
    async def get_user(self, user_id: str) -> User: ...

    @abstractmethod
    async def save_user(self, user: User) -> None: ...


class InMemoryDatabase(Database):
    def __init__(self, url: str, users: MutableMapping[str, User]) -> None:
        self.url: Final[str] = url
        self._users: MutableMapping[str, User] = users

    async def get_user(self, user_id: str) -> User:
        return self._users[user_id]

    async def save_user(self, user: User) -> None:
        self._users[user.id] = user


class Transaction:
    def __init__(self, database: Database) -> None:
        self._database: Database = database
        self._pending: MutableMapping[str, User] = {}

    def save_user(self, user: User) -> None:
        self._pending[user.id] = user

    async def commit(self) -> None:
        for user in self._pending.values():
            await self._database.save_user(user)


class Log:
    def __init__(self, prefix: str) -> None:
        self._prefix: str = prefix

    def write(self, message: str) -> None:
        print(f"{self._prefix} {message}")


class MessageLog(Log):
    pass


class EffectLog(Log):
    pass


USERS: MutableMapping[str, User] = {"u1": User(id="u1", name="Alice")}


# A factory of a dependency that needs no closing yields it and ends.
async def settings() -> AsyncGenerator[Settings]:
    yield Settings(database_url="db://primary", replica_url="db://replica")


# Both databases have the type `Database`, handlers choose one by the parameter name.
async def primary(settings: Settings) -> AsyncGenerator[Database]:
    print(f"[app] connecting to {settings.database_url}")
    yield InMemoryDatabase(settings.database_url, USERS)
    print(f"[app] disconnecting from {settings.database_url}")


async def replica(settings: Settings) -> AsyncGenerator[Database]:
    print(f"[app] connecting to {settings.replica_url}")
    yield InMemoryDatabase(settings.replica_url, USERS)
    print(f"[app] disconnecting from {settings.replica_url}")


# The log lives while the message is handled.
async def message_log(message_context: MessageContext) -> AsyncGenerator[MessageLog]:
    log = MessageLog(f"[message {message_context.message_id}]")
    log.write("start")
    try:
        yield log
    finally:
        log.write("end")


# The transaction lives while its database effects run and sees the error that ended it.
async def transaction(primary: Database, log: MessageLog) -> AsyncGenerator[Transaction]:
    log.write("begin")
    transaction = Transaction(primary)
    try:
        yield transaction
    except Exception:
        log.write("rollback")
        raise
    await transaction.commit()
    log.write("commit")


# A factory may depend on the effect through any of its base classes.
async def effect_log(effect: Effect[Any], effect_context: EffectContext) -> AsyncGenerator[EffectLog]:
    yield EffectLog(f"[effect {type(effect).__name__} {effect_context.effect_id[:8]}]")


# ── Effects ───────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class GetUserEffect(DatabaseReadEffect[User]):
    user_id: str


@dataclass(frozen=True, slots=True)
class SaveUserEffect(DatabaseWriteEffect[User]):
    user: User


# Handler parameters are dependencies too, the order and the set of parameters are free.
async def handle_get_user(replica: Database, effect: GetUserEffect, log: EffectLog) -> User:
    log.write(f"reading user {effect.user_id} from the replica")
    return await replica.get_user(effect.user_id)


# Only handlers of database effects may depend on the transaction.
async def handle_save_user(effect: SaveUserEffect, transaction: Transaction, log: EffectLog) -> User:
    log.write(f"saving user {effect.user.id} in the transaction")
    transaction.save_user(effect.user)
    if not effect.user.name:
        raise ValueError("The name must not be empty")
    return effect.user


# ── Command ───────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class RenameUserCommand(Command[User]):
    user_id: str
    name: str


def rename_user(context: MessageContext, command: RenameUserCommand) -> StagedEffectful[User]:
    user = yield from GetUserEffect(command.user_id)
    return (yield from SaveUserEffect(replace(user, name=command.name)))


async def main() -> None:
    environment = EnvironmentImpl(
        operation_engine=OperationEngineImpl(
            command_defs=[StagedCommandDef("rename_user", RenameUserCommand, rename_user)],
        ),
        effect_defs=[
            EffectDef(GetUserEffect, handle_get_user),
            EffectDef(SaveUserEffect, handle_save_user),
        ],
        dependency_defs=[
            EffectDependencyDef(settings),
            EffectDependencyDef(primary, name="primary"),
            EffectDependencyDef(replica, name="replica"),
            EffectDependencyDef(message_log, lifetime=EffectDependencyLifetime.MESSAGE),
            EffectDependencyDef(transaction, lifetime=EffectDependencyLifetime.TRANSACTION),
            EffectDependencyDef(effect_log, lifetime=EffectDependencyLifetime.EFFECT),
        ],
    )

    async with environment:
        print("\n=== Successful rename ===")
        user = await environment.handle(RenameUserCommand(user_id="u1", name="Bob"), "rename-ok")
        print(f"Result: {user}, stored: {USERS['u1']}")

        print("\n=== Failed rename rolls the transaction back ===")
        try:
            await environment.handle(RenameUserCommand(user_id="u1", name=""), "rename-failed")
        except ValueError as exc:
            print(f"Error: {exc}, stored: {USERS['u1']}")
        print()


if __name__ == "__main__":
    run(main())
