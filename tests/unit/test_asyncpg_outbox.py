from fidem.asyncpg import AsyncpgOutbox


class TestAsyncpgOutbox:
    def test_bundles_write_of_events_and_dispatcher(self) -> None:
        outbox = AsyncpgOutbox()

        assert outbox.definitions == (*outbox.effect_defs, *outbox.dependency_defs)

    def test_writer_bundles_only_write_of_events(self) -> None:
        outbox = AsyncpgOutbox()

        assert outbox.writer.definitions == tuple(outbox.effect_defs)
