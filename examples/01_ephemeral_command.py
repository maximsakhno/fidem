from asyncio import run
from dataclasses import dataclass

from fidem import (
    Command,
    DatabaseReadEffect,
    DatabaseWriteEffect,
    EffectContext,
    Fidem,
    MessageContext,
    StagedEffectful,
    command,
    command_handler,
    effect,
    effect_handler,
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
@effect("get_user")
@dataclass(frozen=True, slots=True)
class GetUserEffect(DatabaseReadEffect[User]):
    user_id: str


@effect("check_stock")
@dataclass(frozen=True, slots=True)
class CheckStockEffect(DatabaseReadEffect[Product]):
    product_id: str


@effect("reserve_stock")
@dataclass(frozen=True, slots=True)
class ReserveStockEffect(DatabaseWriteEffect[Reservation]):
    product_id: str
    quantity: int


@effect("create_order")
@dataclass(frozen=True, slots=True)
class CreateOrderEffect(DatabaseWriteEffect[Order]):
    user_id: str
    product_id: str
    quantity: int
    total: float


@effect("debit_user")
@dataclass(frozen=True, slots=True)
class DebitUserEffect(DatabaseWriteEffect[User]):
    user_id: str
    amount: float


# ── Dummy effect handlers ─────────────────────────────────
@effect_handler
async def handle_get_user(ctx: EffectContext, effect: GetUserEffect) -> User:
    print(f"[DB] {ctx.effect_id}: fetching user {effect.user_id}")
    return User(id=effect.user_id, name="Alice", balance=150.0)


@effect_handler
async def handle_check_stock(ctx: EffectContext, effect: CheckStockEffect) -> Product:
    print(f"[DB] {ctx.effect_id}: checking stock for product {effect.product_id}")
    return Product(id=effect.product_id, name="Awesome Gadget", stock=10, price=49.99)


@effect_handler
async def handle_reserve_stock(ctx: EffectContext, effect: ReserveStockEffect) -> Reservation:
    print(f"[DB] {ctx.effect_id}: reserving {effect.quantity} units of {effect.product_id}")
    return Reservation(reservation_id="res-001", product_id=effect.product_id, quantity=effect.quantity)


@effect_handler
async def handle_create_order(ctx: EffectContext, effect: CreateOrderEffect) -> Order:
    print(f"[DB] {ctx.effect_id}: inserting order for user {effect.user_id}")
    return Order(
        id="order-123",
        user_id=effect.user_id,
        product_id=effect.product_id,
        quantity=effect.quantity,
        total=effect.total,
    )


@effect_handler
async def handle_debit_user(ctx: EffectContext, effect: DebitUserEffect) -> User:
    print(f"[DB] {ctx.effect_id}: debiting {effect.amount} from user {effect.user_id}")
    return User(id=effect.user_id, name="Alice", balance=150.0 - effect.amount)


# ── Command ───────────────────────────────────────────────
@command("place_order")
@dataclass(frozen=True, slots=True)
class PlaceOrderCommand(Command[Order]):
    user_id: str
    product_id: str
    quantity: int


@command_handler
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
    fidem = Fidem(
        definitions=[
            place_order_handler,
            handle_get_user,
            handle_check_stock,
            handle_reserve_stock,
            handle_create_order,
            handle_debit_user,
        ],
    )

    async with fidem:
        print("=== Successful order placement ===")
        result = await fidem.handle(PlaceOrderCommand(user_id="u1", product_id="p1", quantity=2), message_id="order-ok")
        print(f"Result: {result}")

        # Effect ids repeat on replay, but ephemeral mode runs every effect again, so real handlers must be idempotent by effect_id.
        print("\n=== Replay with the same message id ===")
        await fidem.handle(PlaceOrderCommand(user_id="u1", product_id="p1", quantity=2), message_id="order-ok")

        print("\n=== Order with insufficient balance ===")
        try:
            await fidem.handle(PlaceOrderCommand(user_id="u1", product_id="p1", quantity=10))
        except RuntimeError as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    run(main())
