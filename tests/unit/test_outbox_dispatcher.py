from asyncio import CancelledError, Event as AsyncioEvent, create_task, get_running_loop, sleep
from collections.abc import Collection, MutableSequence, Sequence
from datetime import timedelta
from typing import Final
from unittest.mock import Mock

import pytest

from fidem.core.environment import Environment
from fidem.core.messages import Event, MessageId
from fidem.core.outbox import OutboxDelivery, OutboxDispatcher, OutboxDispatcherOptions, OutboxStore, RetryPolicy
from tests.messages import TestEvent


class TestRetryPolicy:
    def test_multiplies_delay_after_each_failed_attempt_up_to_max_delay(self) -> None:
        policy = RetryPolicy(
            max_attempts=10,
            initial_delay=timedelta(seconds=1),
            multiplier=3,
            max_delay=timedelta(seconds=20),
        )

        delays = [policy.retry_delay(attempts) for attempts in range(1, 6)]

        bounds = [timedelta(seconds=seconds) for seconds in (1, 3, 9, 20, 20)]
        for delay, bound in zip(delays, bounds, strict=True):
            assert delay is not None
            assert bound / 2 <= delay <= bound

    def test_returns_no_delay_after_last_attempt(self) -> None:
        policy = RetryPolicy(max_attempts=3)

        assert policy.retry_delay(2) is not None
        assert policy.retry_delay(3) is None

    def test_does_not_overflow_with_many_attempts(self) -> None:
        policy = RetryPolicy(max_attempts=10_000, max_delay=timedelta(hours=1))

        delay = policy.retry_delay(9_999)

        assert delay is not None
        assert delay <= timedelta(hours=1)

    @pytest.mark.parametrize(
        "arguments",
        [
            {"max_attempts": 0},
            {"initial_delay": timedelta(seconds=-1)},
            {"initial_delay": timedelta(seconds=2), "max_delay": timedelta(seconds=1)},
            {"multiplier": 0.5},
        ],
    )
    def test_raises_value_error_when_policy_is_invalid(self, arguments: dict[str, object]) -> None:
        with pytest.raises(ValueError):  # noqa: PT011 - the message is for people
            RetryPolicy(**arguments)  # pyright: ignore[reportArgumentType] - the arguments are checked at runtime


class TestOutboxDispatcherOptions:
    @pytest.mark.parametrize(
        "arguments",
        [
            {"concurrency": 0},
            {"lease": timedelta(0)},
            {"poll_interval": timedelta(0)},
            {"stop_timeout": timedelta(seconds=-1)},
        ],
    )
    def test_raises_value_error_when_options_are_invalid(self, arguments: dict[str, object]) -> None:
        with pytest.raises(ValueError):  # noqa: PT011 - the message is for people
            OutboxDispatcherOptions(**arguments)  # pyright: ignore[reportArgumentType] - the arguments are checked at runtime


DELIVERY = OutboxDelivery(message_id="event:handler", event_name="test", handler="handler", attempts=1, payload=None)


class StoreStub(OutboxStore[None]):
    """Gives out the deliveries once, after the gate opens, and records the releases."""

    def __init__(self, deliveries: Sequence[OutboxDelivery[None]], gate: AsyncioEvent | None = None) -> None:
        self.taking: Final[AsyncioEvent] = AsyncioEvent()
        self.released: Final[MutableSequence[OutboxDelivery[None]]] = []
        self._deliveries: Sequence[OutboxDelivery[None]] = deliveries
        self._gate: AsyncioEvent | None = gate

    async def take(
        self,
        limit: int,
        lease: timedelta,
        max_attempts: int,
        running: Collection[MessageId],
    ) -> Sequence[OutboxDelivery[None]]:
        self.taking.set()
        if self._gate is not None:
            await self._gate.wait()
        deliveries = self._deliveries
        self._deliveries = ()
        return deliveries

    async def fail_exhausted(
        self,
        max_attempts: int,
        error: str,
        running: Collection[MessageId],
    ) -> Sequence[OutboxDelivery[None]]:
        return ()

    def load_event(self, delivery: OutboxDelivery[None]) -> Event:
        return TestEvent()

    async def complete(self, delivery: OutboxDelivery[None]) -> None:
        pass

    async def fail(self, delivery: OutboxDelivery[None], error: str, retry_delay: timedelta | None) -> None:
        pass

    async def release(self, delivery: OutboxDelivery[None]) -> None:
        self.released.append(delivery)

    async def wait(self, max_wait: timedelta) -> None:
        await sleep(max_wait.total_seconds())


@pytest.mark.timeout(5)
class TestOutboxDispatcherStop:
    async def test_releases_deliveries_of_take_cancelled_by_stop(self) -> None:
        gate = AsyncioEvent()
        store = StoreStub([DELIVERY], gate=gate)
        environment = Mock(spec=Environment)
        dispatcher = OutboxDispatcher(store=store, environment=environment)

        async with dispatcher:
            await store.taking.wait()
            # The store finishes the take only after the stop has cancelled it.
            get_running_loop().call_later(0.05, gate.set)

        assert store.released == [DELIVERY]
        environment.deliver.assert_not_called()

    async def test_cancels_running_deliveries_when_stop_is_cancelled(self) -> None:
        async def deliver(event: Event, handler: str, message_id: str) -> None:
            started.set()
            try:
                await AsyncioEvent().wait()
            finally:
                finished.set()

        started = AsyncioEvent()
        finished = AsyncioEvent()
        environment = Mock(spec=Environment)
        environment.deliver.side_effect = deliver
        dispatcher = OutboxDispatcher(
            store=StoreStub([DELIVERY]),
            environment=environment,
            options=OutboxDispatcherOptions(stop_timeout=timedelta(minutes=1)),
        )
        await dispatcher.__aenter__()
        await started.wait()

        stopping = create_task(dispatcher.__aexit__(None, None, None))
        await sleep(0.01)
        stopping.cancel()
        with pytest.raises(CancelledError):
            await stopping

        assert finished.is_set()
