from fidem.messages import Command, Event


class TestCommand(Command[str]):
    pass


class TestEvent(Event):
    pass
