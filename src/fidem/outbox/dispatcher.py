from abc import ABC, abstractmethod
from asyncio import CancelledError, Event as AsyncioEvent, Task, create_task, shield, sleep, timeout, wait
from collections.abc import Collection, MutableSet, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta
from logging import ERROR, WARNING, getLogger
from random import uniform
from traceback import format_exception
from types import TracebackType
from typing import Any, Self

from fidem.environment import Environment
from fidem.messages import Event, EventHandlerName, MessageId, MessageName

__all__ = [
    "OutboxDelivery",
    "OutboxDispatcher",
    "OutboxDispatcherOptions",
    "OutboxStore",
    "RetryPolicy",
    "wait_for_change",
]

_logger = getLogger(__name__)


@dataclass(frozen=True, slots=True, kw_only=True)
class RetryPolicy:
    """Delays between the attempts of a delivery, the delay grows by the multiplier up to the maximum."""

    max_attempts: int = 10
    initial_delay: timedelta = timedelta(seconds=1)
    multiplier: float = 2
    max_delay: timedelta = timedelta(hours=1)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError(f"At least one attempt is required, got {self.max_attempts}")
        if not timedelta(0) <= self.initial_delay <= self.max_delay:
            raise ValueError(f"Delays must satisfy 0 <= {self.initial_delay} <= {self.max_delay}")
        if self.multiplier < 1:
            raise ValueError(f"The multiplier must not shrink the delay, got {self.multiplier}")

    def retry_delay(self, attempts: int) -> timedelta | None:
        """The delay after the given number of failed attempts, `None` when no attempts are left."""
        if attempts >= self.max_attempts:
            return None
        delay = self.initial_delay
        for _ in range(attempts - 1):
            delay = min(delay * self.multiplier, self.max_delay)
        # Jitter spreads the retries of deliveries that failed together.
        return delay * uniform(0.5, 1)  # noqa: S311 - jitter needs no cryptographic strength


@dataclass(frozen=True, slots=True, kw_only=True)
class OutboxDispatcherOptions:
    retry_policy: RetryPolicy = RetryPolicy()
    lease: timedelta = timedelta(minutes=5)
    """How long a delivery stays taken, after that another dispatcher takes it."""
    poll_interval: timedelta = timedelta(seconds=5)
    """The longest wait for new deliveries without a notification from the store."""
    concurrency: int = 10
    """The most deliveries a dispatcher runs at once."""
    stop_timeout: timedelta = timedelta(seconds=30)
    """How long the stop waits for running deliveries before it cancels them."""

    def __post_init__(self) -> None:
        if self.concurrency < 1:
            raise ValueError(f"At least one delivery at once is required, got {self.concurrency}")
        if self.lease <= timedelta(0) or self.poll_interval <= timedelta(0) or self.stop_timeout < timedelta(0):
            raise ValueError("The lease and the poll interval must be positive, the stop timeout must not be negative")


@dataclass(frozen=True, slots=True)
class OutboxDelivery[PayloadT]:
    """A delivery of an event to one of its handlers that a dispatcher has taken."""

    message_id: MessageId
    """The message id of the operation of the handler, which also identifies the delivery."""
    event_name: MessageName
    handler: EventHandlerName
    attempts: int
    """Attempts including the current one, a result is recorded only while they are the same."""
    payload: PayloadT
    """The stored event, which the store loads in the attempt so that a broken one fails like a handler."""


class OutboxStore[PayloadT](ABC):
    """Deliveries of events to their handlers, the result of a delivery is recorded only while it stays taken."""

    @abstractmethod
    async def take(
        self,
        limit: int,
        lease: timedelta,
        max_attempts: int,
        running: Collection[MessageId],
    ) -> Sequence[OutboxDelivery[PayloadT]]:
        """Takes up to `limit` pending deliveries available now and known to the code, counts an attempt of each.

        It skips the deliveries that have used up their attempts and the ones that the dispatcher still runs even
        when their lease has ended.
        """
        raise NotImplementedError

    @abstractmethod
    async def fail_exhausted(
        self,
        max_attempts: int,
        error: str,
        running: Collection[MessageId],
    ) -> Sequence[OutboxDelivery[PayloadT]]:
        """Marks failed the available deliveries that have used up their attempts and returns them.

        Such a delivery is available again only because its last attempt ended without a result.
        """
        raise NotImplementedError

    @abstractmethod
    def load_event(self, delivery: OutboxDelivery[PayloadT]) -> Event:
        raise NotImplementedError

    @abstractmethod
    async def complete(self, delivery: OutboxDelivery[PayloadT]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def fail(self, delivery: OutboxDelivery[PayloadT], error: str, retry_delay: timedelta | None) -> None:
        """Makes the delivery available after the delay, or marks it failed when the delay is `None`."""
        raise NotImplementedError

    @abstractmethod
    async def release(self, delivery: OutboxDelivery[PayloadT]) -> None:
        """Makes the delivery available now and does not count its attempt."""
        raise NotImplementedError

    @abstractmethod
    async def wait(self, max_wait: timedelta) -> None:
        """Waits until a delivery may be available, but no longer than `max_wait`."""
        raise NotImplementedError


class OutboxDispatcher:
    """Delivers events from the store to their handlers in the background while it is entered."""

    def __init__(
        self,
        store: OutboxStore[Any],
        environment: Environment,
        options: OutboxDispatcherOptions = OutboxDispatcherOptions(),
    ) -> None:
        self._store: OutboxStore[Any] = store
        self._environment: Environment = environment
        self._options: OutboxDispatcherOptions = options
        self._deliveries: MutableSet[Task[None]] = set()
        self._handling: MutableSet[MessageId] = set()
        """Deliveries whose handlers run, the store skips them even when their lease has ended."""
        self._delivery_finished: AsyncioEvent = AsyncioEvent()
        self._dispatching: Task[None] | None = None

    async def __aenter__(self) -> Self:
        self._dispatching = create_task(self._dispatch(), name="fidem.outbox.dispatcher")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Stops taking deliveries, waits for the running ones until the stop timeout and cancels the rest."""
        if self._dispatching is not None:
            self._dispatching.cancel()
            # `wait` does not raise, so a cancellation of the caller is not mistaken for the one of the task.
            await wait([self._dispatching])
            self._dispatching = None
        try:
            if self._deliveries:
                await wait(self._deliveries, timeout=self._options.stop_timeout.total_seconds())
        finally:
            # Also when the stop itself is cancelled, because the environment closes the dependencies next.
            running = [task for task in self._deliveries if not task.done()]
            for task in running:
                task.cancel()
            if running:
                await wait(running)

    async def _dispatch(self) -> None:
        poll_interval = self._options.poll_interval
        while True:
            try:
                await self._dispatch_available()
            except Exception:
                _logger.exception("Outbox dispatcher failed to take deliveries, retrying in %s", poll_interval)
                await sleep(poll_interval.total_seconds())

    async def _dispatch_available(self) -> None:
        free_slots = self._options.concurrency - len(self._deliveries)
        if free_slots == 0:
            self._delivery_finished.clear()
            await self._delivery_finished.wait()
            return
        await self._fail_exhausted()
        deliveries = await self._take(free_slots)
        for delivery in deliveries:
            self._start(delivery)
        if len(deliveries) < free_slots:
            await self._store.wait(max_wait=self._options.poll_interval)

    async def _fail_exhausted(self) -> None:
        max_attempts = self._options.retry_policy.max_attempts
        exhausted = await self._store.fail_exhausted(
            max_attempts=max_attempts,
            error=f"The lease of attempt {max_attempts} ended without a result",
            running=tuple(self._handling),
        )
        for delivery in exhausted:
            _logger.error(
                "Delivery %s of event %s to handler %s failed, the lease of its last attempt ended without a result",
                delivery.message_id,
                delivery.event_name,
                delivery.handler,
            )

    async def _take(self, limit: int) -> Sequence[OutboxDelivery[Any]]:
        """Releases what a cancelled take has taken, because the store may have taken it before the cancellation."""
        taking = create_task(
            self._store.take(
                limit=limit,
                lease=self._options.lease,
                max_attempts=self._options.retry_policy.max_attempts,
                running=tuple(self._handling),
            )
        )
        try:
            return await shield(taking)
        except CancelledError:
            for delivery in await taking:
                await self._store.release(delivery)
            raise

    def _start(self, delivery: OutboxDelivery[Any]) -> None:
        self._handling.add(delivery.message_id)
        task = create_task(self._deliver(delivery), name=f"fidem.outbox.delivery:{delivery.message_id}")
        self._deliveries.add(task)
        task.add_done_callback(self._finish)

    def _finish(self, task: Task[None]) -> None:
        self._deliveries.discard(task)
        self._delivery_finished.set()

    async def _deliver(self, delivery: OutboxDelivery[Any]) -> None:
        try:
            await self._attempt(delivery)
        except Exception:
            _logger.exception("Outbox dispatcher failed to record the result of delivery %s", delivery.message_id)

    async def _attempt(self, delivery: OutboxDelivery[Any]) -> None:
        try:
            try:
                event = self._store.load_event(delivery)
                await self._environment.deliver(event=event, handler=delivery.handler, message_id=delivery.message_id)
            finally:
                # The result may make the delivery available at once, so the next take must not skip it.
                self._handling.discard(delivery.message_id)
        except CancelledError:
            await self._store.release(delivery)
            raise
        except Exception as exc:  # noqa: BLE001 - a failed attempt is retried
            await self._fail(delivery, exc)
        else:
            await self._store.complete(delivery)

    async def _fail(self, delivery: OutboxDelivery[Any], exc: Exception) -> None:
        retry_delay = self._options.retry_policy.retry_delay(delivery.attempts)
        _logger.log(
            ERROR if retry_delay is None else WARNING,
            "Delivery %s of event %s to handler %s failed on attempt %s, %s",
            delivery.message_id,
            delivery.event_name,
            delivery.handler,
            delivery.attempts,
            "no attempts left" if retry_delay is None else f"retrying in {retry_delay}",
            exc_info=exc,
        )
        await self._store.fail(delivery, error="".join(format_exception(exc)), retry_delay=retry_delay)


async def wait_for_change(changed: AsyncioEvent, max_wait: timedelta) -> None:
    """Waits until the store sets the event or the time passes, then clears it for the next wait.

    A change during the last take leaves the event set, so the next wait returns at once.
    """
    with suppress(TimeoutError):
        async with timeout(max_wait.total_seconds()):
            await changed.wait()
    changed.clear()
