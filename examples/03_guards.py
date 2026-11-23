from asyncio import run
from collections.abc import MutableMapping
from dataclasses import dataclass, replace
from typing import Any

from fidem.effects import DatabaseReadEffect, DatabaseWriteEffect, Effect, Effectful
from fidem.environment import EffectDef, EnvironmentImpl
from fidem.guards import Guard, GuardDef, GuardEffect
from fidem.messages import Command
from fidem.operation_engine import OperationEngineImpl, StagedCommandDef


# ── Domain types ──────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class User:
    id: str
    name: str
    is_admin: bool


class AuthenticationError(Exception):
    pass


class PermissionDeniedError(Exception):
    pass


TOKEN_SECRET = "secret"

USERS: MutableMapping[str, User] = {
    "u1": User(id="u1", name="Alice", is_admin=True),
    "u2": User(id="u2", name="Bob", is_admin=False),
}


# ── Effects ───────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class GetUserEffect(DatabaseReadEffect[User]):
    user_id: str


@dataclass(frozen=True, slots=True)
class SaveUserEffect(DatabaseWriteEffect[User]):
    user: User


async def handle_get_user(effect: GetUserEffect) -> User:
    return USERS[effect.user_id]


async def handle_save_user(effect: SaveUserEffect) -> User:
    USERS[effect.user.id] = effect.user
    return effect.user


# ── Commands ──────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class AuthenticatedCommand[ResultT](Command[ResultT]):
    token: str


@dataclass(frozen=True, slots=True)
class RenameUserCommand(AuthenticatedCommand[User]):
    user_id: str
    name: str


# ── Guards ────────────────────────────────────────────────
class TokenVerifier:
    def __init__(self, secret: str) -> None:
        self._secret: str = secret

    def verify(self, token: str) -> str:
        user_id, _, secret = token.partition(":")
        if secret != self._secret:
            raise AuthenticationError("Invalid token")
        return user_id


@dataclass(frozen=True, slots=True)
class CurrentUser:
    user: User


@dataclass(frozen=True, slots=True)
class Admin:
    user: User


# Guards get their settings through a closure.
def make_current_user(verifier: TokenVerifier) -> Guard[CurrentUser]:
    # The guard gets the command by its base class, so it serves every authenticated command.
    def current_user(command: AuthenticatedCommand[Any]) -> Effectful[CurrentUser, GuardEffect]:
        user_id = verifier.verify(command.token)
        return CurrentUser((yield from GetUserEffect(user_id)))

    return current_user


# A handler that takes `Admin` runs only for admins.
def admin(current_user: CurrentUser) -> Effectful[Admin, GuardEffect]:
    yield from ()
    if not current_user.user.is_admin:
        raise PermissionDeniedError(f"{current_user.user.name} is not an admin")
    return Admin(current_user.user)


# ── Command handler ───────────────────────────────────────
# The handler runs only when the guards have authenticated and authorized the command.
def rename_user(admin: Admin, command: RenameUserCommand) -> Effectful[User, Effect[Any]]:
    print(f"[handler] {admin.user.name} renames {command.user_id} to {command.name}")
    user = yield from GetUserEffect(command.user_id)
    return (yield from SaveUserEffect(replace(user, name=command.name)))


async def main() -> None:
    environment = EnvironmentImpl(
        operation_engine=OperationEngineImpl(
            command_defs=[StagedCommandDef("rename_user", RenameUserCommand, rename_user)],
            guard_defs=[
                GuardDef(make_current_user(TokenVerifier(TOKEN_SECRET))),
                GuardDef(admin),
            ],
        ),
        effect_defs=[
            EffectDef(GetUserEffect, handle_get_user),
            EffectDef(SaveUserEffect, handle_save_user),
        ],
    )

    async with environment:
        print("\n=== Admin renames a user ===")
        user = await environment.execute(RenameUserCommand(token="u1:secret", user_id="u2", name="Robert"))
        print(f"Result: {user}")

        print("\n=== Not an admin ===")
        try:
            await environment.execute(RenameUserCommand(token="u2:secret", user_id="u1", name="Alicia"))
        except PermissionDeniedError as exc:
            print(f"Error: {exc}")

        print("\n=== Invalid token ===")
        try:
            await environment.execute(RenameUserCommand(token="u1:wrong", user_id="u2", name="Bobby"))
        except AuthenticationError as exc:
            print(f"Error: {exc}")
        print()


if __name__ == "__main__":
    run(main())
