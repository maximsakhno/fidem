from fidem.messages import Command, Event, Query


class TestCommand(Command[str]):
    pass


class TestQuery(Query[str]):
    pass


class TestEvent(Event):
    pass
