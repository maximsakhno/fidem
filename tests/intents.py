from fidem.intents import ReadIntent, WriteIntent


class TestReadIntent(ReadIntent[str]):
    pass


class TestWriteIntent(WriteIntent[str]):
    pass
