__all__ = [
    "Error",
]


class Error(Exception):
    """The base of the errors that fidem raises, except `ValueError` and `TypeError` from its own bugs."""
