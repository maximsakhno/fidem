from importlib.resources import files
from typing import Final

from fidem.asyncpg.migrations import MigrationSection, load_section

__all__ = ["OUTBOX_SECTION"]

OUTBOX_SECTION: Final[MigrationSection] = load_section(
    name="fidem.outbox", directory=files(__package__) / "sql" / "outbox"
)
"""The table that events are written to in the transaction that publishes them."""
