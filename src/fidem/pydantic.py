from typing import Any, ClassVar, cast, override

from pydantic import PydanticUserError, TypeAdapter, ValidationError
from pydantic_core import to_json

from fidem.core.serialization import (
    Codec,
    DeserializationError,
    JsonSchema,
    JsonValue,
    SerializationError,
    Serializer,
    UnsupportedTypeError,
)

__all__ = [
    "PydanticSerializer",
]


class _PydanticCodec[T](Codec[T]):
    # Not only `PydanticSerializationError`, because a circular reference or bytes that are not UTF-8 fail with a bare `ValueError`.
    SERIALIZATION_ERRORS: ClassVar[type[ValueError]] = ValueError

    def __init__(self, adapter: TypeAdapter[T], *, strict: bool) -> None:
        self._adapter: TypeAdapter[T] = adapter
        self._strict: bool = strict
        self._json_schema: JsonSchema = adapter.json_schema()

    @override
    def to_json_value(self, value: T) -> JsonValue:
        try:
            return cast(
                JsonValue,
                self._adapter.dump_python(
                    value,
                    mode="json",
                    by_alias=True,
                    warnings="error",
                ),
            )
        except self.SERIALIZATION_ERRORS as e:
            raise SerializationError from e

    @override
    def from_json_value(self, data: JsonValue) -> T:
        # Through JSON, because in strict mode `validate_python` requires Python objects, such as a dataclass instead of a dict.
        try:
            json = to_json(data)
        except ValueError as e:
            raise DeserializationError from e
        return self.from_json(json)

    @override
    def to_json(self, value: T) -> bytes:
        try:
            return self._adapter.dump_json(value, by_alias=True, warnings="error")
        except self.SERIALIZATION_ERRORS as e:
            raise SerializationError from e

    @override
    def from_json(self, data: bytes | str) -> T:
        try:
            return self._adapter.validate_json(data, strict=self._strict)
        except ValidationError as e:
            raise DeserializationError from e

    @override
    def json_schema(self) -> JsonSchema:
        return self._json_schema


class PydanticSerializer(Serializer):
    """Supports the types that pydantic validates."""

    def __init__(self, *, strict: bool = False) -> None:
        self._strict: bool = strict

    @override
    def _make_codec(self, type_: object) -> Codec[Any]:
        try:
            return _PydanticCodec(adapter=TypeAdapter[Any](cast(type[Any], type_)), strict=self._strict)
        except PydanticUserError as e:
            raise UnsupportedTypeError(type_) from e
