from inspect import isfunction


def type_name(value: object) -> str | None:
    """Names a parameter by its type, a class or function by its name, and leaves plain values to pytest."""
    if value is None or isinstance(value, str | int | float):
        return None
    if isinstance(value, type) or isfunction(value):
        return value.__name__
    return type(value).__name__
