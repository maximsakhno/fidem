import asyncio
from dataclasses import dataclass

from fidem.environment import EnvironmentImpl, ReadIntentDef, WriteIntentDef
from fidem.intents import IntentCompensationContext, IntentContext, ReadIntent, WriteIntent, ask
from fidem.operation_engine import CommandContextIn, CommandDef, OperationEngineImpl
from fidem.operations import Command, CommandContext, CommandGenerator


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


# ── Intents ───────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class GetUserIntent(ReadIntent[User]):
    user_id: str


@dataclass(frozen=True, slots=True)
class CheckStockIntent(ReadIntent[Product]):
    product_id: str


@dataclass(frozen=True, slots=True)
class ReserveStockIntent(WriteIntent[Reservation]):
    product_id: str
    quantity: int


@dataclass(frozen=True, slots=True)
class CreateOrderIntent(WriteIntent[Order]):
    user_id: str
    product_id: str
    quantity: int
    total: float


@dataclass(frozen=True, slots=True)
class DebitUserIntent(WriteIntent[User]):
    user_id: str
    amount: float


# ── Dummy intent handlers ─────────────────────────────────
async def handle_get_user(ctx: IntentContext, intent: GetUserIntent) -> User:
    print(f"[DB] Fetching user {intent.user_id}")
    return User(id=intent.user_id, name="Alice", balance=150.0)


async def handle_check_stock(ctx: IntentContext, intent: CheckStockIntent) -> Product:
    print(f"[DB] Checking stock for product {intent.product_id}")
    return Product(id=intent.product_id, name="Awesome Gadget", stock=10, price=49.99)


async def handle_reserve_stock(ctx: IntentContext, intent: ReserveStockIntent) -> Reservation:
    print(f"[DB] Reserving {intent.quantity} units of {intent.product_id} …")
    return Reservation(reservation_id="res-001", product_id=intent.product_id, quantity=intent.quantity)


async def handle_create_order(ctx: IntentContext, intent: CreateOrderIntent) -> Order:
    print(f"[DB] Inserting order for user {intent.user_id} …")
    return Order(
        id="order-123",
        user_id=intent.user_id,
        product_id=intent.product_id,
        quantity=intent.quantity,
        total=intent.total,
    )


async def handle_debit_user(ctx: IntentContext, intent: DebitUserIntent) -> User:
    print(f"[DB] Debiting {intent.amount} from user {intent.user_id} …")
    return User(id=intent.user_id, name="Alice", balance=150.0 - intent.amount)


# ── Compensation handlers ─────────────────────────────────
async def compensate_reserve_stock(ctx: IntentCompensationContext[Reservation], intent: ReserveStockIntent) -> None:
    print(f"[DB] COMPENSATING: releasing reservation {ctx.original_result.reservation_id} …")


async def compensate_create_order(ctx: IntentCompensationContext[Order], intent: CreateOrderIntent) -> None:
    print(f"[DB] COMPENSATING: deleting order {ctx.original_result.id} and restoring stock …")


async def compensate_debit_user(ctx: IntentCompensationContext[User], intent: DebitUserIntent) -> None:
    print(f"[DB] COMPENSATING: refunding {intent.amount} to user {intent.user_id} …")


# ── Command ───────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class PlaceOrderCommand(Command[Order]):
    user_id: str
    product_id: str
    quantity: int


def place_order_handler(ctx: CommandContext, cmd: PlaceOrderCommand) -> CommandGenerator[Order]:
    # 1. Read data
    user = yield from ask(GetUserIntent(cmd.user_id))
    product = yield from ask(CheckStockIntent(cmd.product_id))

    if product.stock < cmd.quantity:
        raise RuntimeError(f"Insufficient stock (available {product.stock}, requested {cmd.quantity})")

    total = product.price * cmd.quantity

    # 2. Reserve stock (Write)
    yield from ask(ReserveStockIntent(cmd.product_id, cmd.quantity))

    # 3. Create order (Write)
    order = yield from ask(CreateOrderIntent(cmd.user_id, cmd.product_id, cmd.quantity, total))

    # 4. Debit user (Write)
    if user.balance < total:
        raise RuntimeError(f"Insufficient balance (balance {user.balance}, needed {total})")
    yield from ask(DebitUserIntent(cmd.user_id, total))

    return order


async def main() -> None:
    env = EnvironmentImpl(
        operation_engine=OperationEngineImpl(
            command_defs=[
                CommandDef(PlaceOrderCommand, place_order_handler),
            ],
        ),
        intent_defs=[
            ReadIntentDef(GetUserIntent, handle_get_user),
            ReadIntentDef(CheckStockIntent, handle_check_stock),
            WriteIntentDef(ReserveStockIntent, handle_reserve_stock, compensate_reserve_stock),
            WriteIntentDef(CreateOrderIntent, handle_create_order, compensate_create_order),
            WriteIntentDef(DebitUserIntent, handle_debit_user, compensate_debit_user),
        ],
    )

    print("=== Successful order placement ===")
    try:
        result = await env.execute(
            PlaceOrderCommand(user_id="u1", product_id="p1", quantity=2),
            CommandContextIn(operation_id="order-ok"),
        )
        print(f"Result: {result}")
    except RuntimeError as e:
        print(f"Error: {e}")

    print("\n=== Order with insufficient balance (rollback) ===")
    try:
        # 10 units * 49.99 = 499.90, balance 150 → error
        await env.execute(
            PlaceOrderCommand(user_id="u1", product_id="p1", quantity=10),
            CommandContextIn(operation_id="order-fail-balance"),
        )
    except RuntimeError as e:
        print(f"Error: {e}")
        print("Engine should have rolled back the transaction, compensating all successful write intents.")

    print("\nFinished!")


if __name__ == "__main__":
    asyncio.run(main())
