from asyncio import run
from dataclasses import dataclass

from fidem import (
    Command,
    DatabaseReadEffect,
    DatabaseWriteEffect,
    EffectContext,
    EffectDef,
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
    balance: float


@dataclass(frozen=True, slots=True)
class Product:
    id: str
    name: str
    stock: int
    price: float


@dataclass(frozen=True, slots=True)
class Order:
    id: str
    user_id: str
    product_id: str
    quantity: int
    total: float


@dataclass(frozen=True, slots=True)
class Reservation:
    reservation_id: str
    product_id: str
    quantity: int


# ── Effects ───────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class GetUserEffect(DatabaseReadEffect[User]):
    user_id: str


@dataclass(frozen=True, slots=True)
class CheckStockEffect(DatabaseReadEffect[Product]):
    product_id: str


@dataclass(frozen=True, slots=True)
class ReserveStockEffect(DatabaseWriteEffect[Reservation]):
    product_id: str
    quantity: int


@dataclass(frozen=True, slots=True)
class CreateOrderEffect(DatabaseWriteEffect[Order]):
    user_id: str
    product_id: str
    quantity: int
    total: float


@dataclass(frozen=True, slots=True)
class DebitUserEffect(DatabaseWriteEffect[User]):
    user_id: str
    amount: float


# ── Dummy effect handlers ─────────────────────────────────
async def handle_get_user(ctx: EffectContext, effect: GetUserEffect) -> User:
    print(f"[DB] {ctx.effect_id}: fetching user {effect.user_id}")
    return User(id=effect.user_id, name="Alice", balance=150.0)


async def handle_check_stock(ctx: EffectContext, effect: CheckStockEffect) -> Product:
    print(f"[DB] {ctx.effect_id}: checking stock for product {effect.product_id}")
    return Product(id=effect.product_id, name="Awesome Gadget", stock=10, price=49.99)


async def handle_reserve_stock(ctx: EffectContext, effect: ReserveStockEffect) -> Reservation:
    print(f"[DB] {ctx.effect_id}: reserving {effect.quantity} units of {effect.product_id}")
    return Reservation(reservation_id="res-001", product_id=effect.product_id, quantity=effect.quantity)


async def handle_create_order(ctx: EffectContext, effect: CreateOrderEffect) -> Order:
    print(f"[DB] {ctx.effect_id}: inserting order for user {effect.user_id}")
    return Order(
        id="order-123",
        user_id=effect.user_id,
        product_id=effect.product_id,
        quantity=effect.quantity,
        total=effect.total,
    )


async def handle_debit_user(ctx: EffectContext, effect: DebitUserEffect) -> User:
    print(f"[DB] {ctx.effect_id}: debiting {effect.amount} from user {effect.user_id}")
    return User(id=effect.user_id, name="Alice", balance=150.0 - effect.amount)


# ── Command ───────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class PlaceOrderCommand(Command[Order]):
    user_id: str
    product_id: str
    quantity: int


def place_order_handler(ctx: MessageContext, cmd: PlaceOrderCommand) -> StagedEffectful[Order]:
    user = yield from GetUserEffect(cmd.user_id)
    product = yield from CheckStockEffect(cmd.product_id)

    if product.stock < cmd.quantity:
        raise RuntimeError(f"Insufficient stock (available {product.stock}, requested {cmd.quantity})")

    total = product.price * cmd.quantity
    if user.balance < total:
        raise RuntimeError(f"Insufficient balance (balance {user.balance}, needed {total})")

    yield from ReserveStockEffect(cmd.product_id, cmd.quantity)
    order = yield from CreateOrderEffect(
        user_id=cmd.user_id,
        product_id=cmd.product_id,
        quantity=cmd.quantity,
        total=total,
    )
    yield from DebitUserEffect(cmd.user_id, total)
    return order


async def main() -> None:
    env = EnvironmentImpl(
        operation_engine=OperationEngineImpl(
            command_defs=[
                StagedCommandDef("place_order", PlaceOrderCommand, place_order_handler),
            ],
        ),
        effect_defs=[
            EffectDef(GetUserEffect, handle_get_user),
            EffectDef(CheckStockEffect, handle_check_stock),
            EffectDef(ReserveStockEffect, handle_reserve_stock),
            EffectDef(CreateOrderEffect, handle_create_order),
            EffectDef(DebitUserEffect, handle_debit_user),
        ],
    )

    async with env:
        print("=== Successful order placement ===")
        result = await env.handle(PlaceOrderCommand(user_id="u1", product_id="p1", quantity=2), message_id="order-ok")
        print(f"Result: {result}")

        # Effect ids repeat on replay, but ephemeral mode runs every effect again, so real handlers must be idempotent by effect_id.
        print("\n=== Replay with the same message id ===")
        await env.handle(PlaceOrderCommand(user_id="u1", product_id="p1", quantity=2), message_id="order-ok")

        print("\n=== Order with insufficient balance ===")
        try:
            await env.handle(PlaceOrderCommand(user_id="u1", product_id="p1", quantity=10))
        except RuntimeError as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    run(main())
