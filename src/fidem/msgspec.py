from typing import Any, ClassVar, cast, override

from msgspec import convert, to_builtins
from msgspec.json import Decoder, Encoder, schema

from fidem.serialization import (
    Codec,
    DeserializationError,
    JsonSchema,
    JsonValue,
    SerializationError,
    Serializer,
    UnsupportedTypeError,
)

__all__ = [
    "MsgspecSerializer",
]


class _MsgspecCodec[T](Codec[T]):
    # The encoder does not depend on the type, it encodes a value by its runtime type.
    ENCODER: ClassVar[Encoder] = Encoder()
    # Not only `DecodeError`, because bytes that are not UTF-8 fail with `UnicodeDecodeError`.
    DESERIALIZATION_ERRORS: ClassVar[type[ValueError]] = ValueError

    def __init__(self, type_: type[T], *, strict: bool) -> None:
        self._json_schema: JsonSchema = schema(type_)
        self._decoder: Decoder[T] = Decoder(type_, strict=strict)

    @override
    def to_json_value(self, value: T) -> JsonValue:
        try:
            return cast(JsonValue, to_builtins(value, str_keys=True))
        except TypeError as e:
            raise SerializationError from e

    @override
    def from_json_value(self, data: JsonValue) -> T:
        try:
            return convert(
                data,
                self._decoder.type,
                strict=self._decoder.strict,
                str_keys=True,
            )
        except self.DESERIALIZATION_ERRORS as e:
            raise DeserializationError from e

    @override
    def to_json(self, value: T) -> bytes:
        try:
            return self.ENCODER.encode(value)
        except TypeError as e:
            raise SerializationError from e

    @override
    def from_json(self, data: bytes | str) -> T:
        try:
            return self._decoder.decode(data)
        except self.DESERIALIZATION_ERRORS as e:
            raise DeserializationError from e

    @override
    def json_schema(self) -> JsonSchema:
        return self._json_schema


class MsgspecSerializer(Serializer):
    """Supports the types that msgspec decodes."""

    def __init__(self, *, strict: bool = True) -> None:
        self._strict: bool = strict

    @override
    def _make_codec(self, type_: object) -> Codec[Any]:
        try:
            return _MsgspecCodec(type_=cast(type[Any], type_), strict=self._strict)
        # `NameError` comes from an unresolved forward reference.
        except (TypeError, NameError) as e:
            raise UnsupportedTypeError(type_) from e
