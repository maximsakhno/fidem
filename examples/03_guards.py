from asyncio import run
from collections.abc import MutableMapping
from dataclasses import dataclass, field, replace

from fidem import (
    Command,
    DatabaseReadEffect,
    DatabaseWriteEffect,
    EffectDef,
    Effectful,
    EnvironmentImpl,
    Guard,
    GuardDef,
    GuardInputDef,
    OperationEngineImpl,
    QueryEffect,
    StagedCommandDef,
    StagedEffectful,
)


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
# The command holds only business data, the token comes along with it as a guard input.
@dataclass(frozen=True, slots=True)
class RenameUserCommand(Command[User]):
    user_id: str
    name: str


# ── Guards ────────────────────────────────────────────────
# A guard input is matched by its class, so the token gets a class of its own.
@dataclass(frozen=True, slots=True)
class AccessToken:
    raw: str = field(repr=False)
    """Hidden from repr, so the token does not get into logs with an error."""


class TokenVerifier:
    def __init__(self, secret: str) -> None:
        self._secret: str = secret

    def verify(self, token: AccessToken) -> str:
        user_id, _, secret = token.raw.partition(":")
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
    def current_user(token: AccessToken) -> Effectful[CurrentUser, QueryEffect]:
        user_id = verifier.verify(token)
        return CurrentUser((yield from GetUserEffect(user_id)))

    return current_user


# A handler that takes `Admin` runs only for admins.
def admin(current_user: CurrentUser) -> Effectful[Admin, QueryEffect]:
    yield from ()
    if not current_user.user.is_admin:
        raise PermissionDeniedError(f"{current_user.user.name} is not an admin")
    return Admin(current_user.user)


# ── Command handler ───────────────────────────────────────
# The handler runs only when the guards have authenticated and authorized the command.
def rename_user(admin: Admin, command: RenameUserCommand) -> StagedEffectful[User]:
    print(f"[handler] {admin.user.name} renames {command.user_id} to {command.name}")
    user = yield from GetUserEffect(command.user_id)
    return (yield from SaveUserEffect(replace(user, name=command.name)))


async def main() -> None:
    environment = EnvironmentImpl(
        operation_engine=OperationEngineImpl(
            command_defs=[StagedCommandDef("rename_user", RenameUserCommand, rename_user)],
            guard_defs=[
                GuardInputDef(AccessToken),
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
        user = await environment.handle(RenameUserCommand(user_id="u2", name="Robert"), AccessToken("u1:secret"))
        print(f"Result: {user}")

        print("\n=== Not an admin ===")
        try:
            await environment.handle(RenameUserCommand(user_id="u1", name="Alicia"), AccessToken("u2:secret"))
        except PermissionDeniedError as exc:
            print(f"Error: {exc}")

        print("\n=== Invalid token ===")
        try:
            await environment.handle(RenameUserCommand(user_id="u2", name="Bobby"), AccessToken("u1:wrong"))
        except AuthenticationError as exc:
            print(f"Error: {exc}")
        print()


if __name__ == "__main__":
    run(main())
