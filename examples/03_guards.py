from asyncio import run
from collections.abc import AsyncGenerator, MutableMapping
from dataclasses import dataclass, field, replace

from fidem import (
    Command,
    DatabaseReadEffect,
    DatabaseWriteEffect,
    Effectful,
    Fidem,
    InternalEffect,
    QueryEffect,
    StagedEffectful,
    command,
    command_handler,
    dependency,
    effect,
    effect_handler,
    guard,
    guard_input,
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


# ── Guard inputs ──────────────────────────────────────────
# A guard input is matched by its class, so the token gets a class of its own.
@guard_input
@dataclass(frozen=True, slots=True)
class AccessToken:
    raw: str = field(repr=False)
    """Hidden from repr, so the token does not get into logs with an error."""


# ── Dependencies ──────────────────────────────────────────
class TokenVerifier:
    def __init__(self, secret: str) -> None:
        self._secret: str = secret

    def verify(self, token: AccessToken) -> str:
        user_id, _, secret = token.raw.partition(":")
        if secret != self._secret:
            raise AuthenticationError("Invalid token")
        return user_id


# The verifier is made from the settings on start, and a guard reaches it through an effect.
@dependency
async def token_verifier() -> AsyncGenerator[TokenVerifier]:
    yield TokenVerifier(TOKEN_SECRET)


# ── Effects ───────────────────────────────────────────────
@effect("verify_token")
@dataclass(frozen=True, slots=True)
class VerifyTokenEffect(InternalEffect[str]):
    token: AccessToken


@effect("get_user")
@dataclass(frozen=True, slots=True)
class GetUserEffect(DatabaseReadEffect[User]):
    user_id: str


@effect("save_user")
@dataclass(frozen=True, slots=True)
class SaveUserEffect(DatabaseWriteEffect[User]):
    user: User


@effect_handler
async def handle_verify_token(effect: VerifyTokenEffect, verifier: TokenVerifier) -> str:
    return verifier.verify(effect.token)


@effect_handler
async def handle_get_user(effect: GetUserEffect) -> User:
    return USERS[effect.user_id]


@effect_handler
async def handle_save_user(effect: SaveUserEffect) -> User:
    USERS[effect.user.id] = effect.user
    return effect.user


# ── Commands ──────────────────────────────────────────────
# The command holds only business data, the token comes along with it as a guard input.
@command("rename_user")
@dataclass(frozen=True, slots=True)
class RenameUserCommand(Command[User]):
    user_id: str
    name: str


# ── Guards ────────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class CurrentUser:
    user: User


@dataclass(frozen=True, slots=True)
class Admin:
    user: User


@guard
def current_user(token: AccessToken) -> Effectful[CurrentUser, QueryEffect]:
    user_id = yield from VerifyTokenEffect(token)
    return CurrentUser((yield from GetUserEffect(user_id)))


# A handler that takes `Admin` runs only for admins.
@guard
def admin(current_user: CurrentUser) -> Effectful[Admin, QueryEffect]:
    yield from ()
    if not current_user.user.is_admin:
        raise PermissionDeniedError(f"{current_user.user.name} is not an admin")
    return Admin(current_user.user)


# ── Command handler ───────────────────────────────────────
# The handler runs only when the guards have authenticated and authorized the command.
@command_handler
def rename_user(admin: Admin, command: RenameUserCommand) -> StagedEffectful[User]:
    print(f"[handler] {admin.user.name} renames {command.user_id} to {command.name}")
    user = yield from GetUserEffect(command.user_id)
    return (yield from SaveUserEffect(replace(user, name=command.name)))


async def main() -> None:
    fidem = Fidem(
        definitions=[
            rename_user,
            AccessToken,
            current_user,
            admin,
            token_verifier,
            handle_verify_token,
            handle_get_user,
            handle_save_user,
        ],
    )

    async with fidem:
        print("\n=== Admin renames a user ===")
        user = await fidem.handle(RenameUserCommand(user_id="u2", name="Robert"), AccessToken("u1:secret"))
        print(f"Result: {user}")

        print("\n=== Not an admin ===")
        try:
            await fidem.handle(RenameUserCommand(user_id="u1", name="Alicia"), AccessToken("u2:secret"))
        except PermissionDeniedError as exc:
            print(f"Error: {exc}")

        print("\n=== Invalid token ===")
        try:
            await fidem.handle(RenameUserCommand(user_id="u2", name="Bobby"), AccessToken("u1:wrong"))
        except AuthenticationError as exc:
            print(f"Error: {exc}")
        print()


if __name__ == "__main__":
    run(main())
