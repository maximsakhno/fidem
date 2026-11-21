from fidem.effects import ControlEffect, DatabaseWriteEffect, ExternalReadEffect, InternalEffect


class TestInternalEffect(InternalEffect[str]):
    pass


class TestExternalReadEffect(ExternalReadEffect[str]):
    pass


class TestDatabaseWriteEffect(DatabaseWriteEffect[str]):
    pass


class TestControlEffect(ControlEffect[str]):
    pass
