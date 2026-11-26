from abc import ABC, abstractmethod
from collections.abc import Mapping, MutableMapping, Sequence
from typing import Any, final, overload

from fidem.errors import Error

__all__ = [
    "Codec",
    "DeserializationError",
    "JsonSchema",
    "JsonValue",
    "SerializationError",
    "Serializer",
    "UnsupportedTypeError",
]

type JsonValue = bool | int | float | str | Sequence[JsonValue] | Mapping[str, JsonValue] | None
"""A value that maps to JSON as is."""
type JsonSchema = Mapping[str, JsonValue]


class Codec[T](ABC):
    """Converts values of one type to JSON and back."""

    @abstractmethod
    def to_json_value(self, value: T) -> JsonValue:
        raise NotImplementedError

    @abstractmethod
    def from_json_value(self, data: JsonValue) -> T:
        raise NotImplementedError

    @abstractmethod
    def to_json(self, value: T) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def from_json(self, data: bytes | str) -> T:
        raise NotImplementedError

    @abstractmethod
    def json_schema(self) -> JsonSchema:
        """JSON Schema 2020-12 of the input with nested types in `$defs`."""
        raise NotImplementedError


class Serializer(ABC):
    """Prepares codecs for the types that its implementation supports, once per type."""

    # Created on first use so that implementations need not call the constructor of the base class.
    _codecs: MutableMapping[object, Codec[Any]] | None = None

    # Not `TypeForm`, because it is not in the standard library yet, so the type of a union codec is written explicitly.
    @overload
    def codec[T](self, type_: type[T]) -> Codec[T]: ...

    @overload
    def codec(self, type_: object) -> Codec[Any]: ...

    @final
    def codec(self, type_: object) -> Codec[Any]:
        if self._codecs is None:
            self._codecs = {}
        try:
            codec = self._codecs.get(type_)
        except TypeError:
            # A type with unhashable metadata, such as `Annotated`, cannot be cached.
            return self._make_codec(type_)
        if codec is None:
            codec = self._make_codec(type_)
            self._codecs[type_] = codec
        return codec

    @abstractmethod
    def _make_codec(self, type_: object) -> Codec[Any]:
        """Raises `UnsupportedTypeError` here rather than on the first use of the codec."""
        raise NotImplementedError


class UnsupportedTypeError(Error):
    def __init__(self, type_: object) -> None:
        super().__init__(type_)


class SerializationError(Error):
    pass


class DeserializationError(Error):
    pass
