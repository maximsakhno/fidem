from fidem.core.effects import (
    ControlEffect,
    DatabaseReadEffect,
    DatabaseWriteEffect,
    ExternalReadEffect,
    ExternalWriteEffect,
    InternalEffect,
)


class TestInternalEffect(InternalEffect[str]):
    pass


class TestExternalReadEffect(ExternalReadEffect[str]):
    pass


class TestExternalWriteEffect(ExternalWriteEffect[str]):
    pass


class TestDatabaseReadEffect(DatabaseReadEffect[str]):
    pass


class TestDatabaseWriteEffect(DatabaseWriteEffect[str]):
    pass


class TestControlEffect(ControlEffect[str]):
    pass
