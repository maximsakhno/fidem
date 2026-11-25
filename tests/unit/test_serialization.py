from collections.abc import Mapping
from dataclasses import dataclass, make_dataclass
from datetime import UTC, datetime
from json import dumps, loads
from typing import Any
from uuid import UUID

import pytest
from msgspec import Struct
from pydantic import BaseModel, Field

from fidem.messages import Command
from fidem.msgspec import MsgspecSerializer
from fidem.pydantic import PydanticSerializer
from fidem.serialization import (
    Codec,
    DeserializationError,
    JsonValue,
    SerializationError,
    Serializer,
    UnsupportedTypeError,
)


@dataclass(frozen=True, slots=True)
class Address:
    city: str


@dataclass(frozen=True, slots=True)
class RegisterUser(Command[None]):
    user_id: UUID
    registered_at: datetime
    address: Address
    tags: tuple[str, ...]


class Opaque:
    pass


Unresolved = make_dataclass("Unresolved", [("address", "MissingAddress")])


USER = RegisterUser(
    user_id=UUID("8f5497e0-abee-4852-836b-faeacb6708eb"),
    registered_at=datetime(2026, 11, 25, 12, 30, tzinfo=UTC),
    address=Address(city="Yerevan"),
    tags=("admin",),
)


@pytest.fixture(params=[PydanticSerializer, MsgspecSerializer])
def serializer(request: pytest.FixtureRequest) -> Serializer:
    return request.param()


@pytest.mark.parametrize(
    ("type_", "value"),
    [
        (RegisterUser, USER),
        (list[Address], [Address(city="Yerevan"), Address(city="Gyumri")]),
        (Address | None, Address(city="Yerevan")),
        (Address | None, None),
        (int, 42),
    ],
)
def test_value_survives_round_trip_through_both_forms(serializer: Serializer, type_: object, value: object) -> None:
    codec = serializer.codec(type_)

    assert codec.from_json(codec.to_json(value)) == value
    assert codec.from_json_value(codec.to_json_value(value)) == value


def test_json_value_matches_json(serializer: Serializer) -> None:
    codec = serializer.codec(RegisterUser)

    # Through `json.dumps`, because a JSON value may contain tuples where the parsed JSON has lists.
    json_value = loads(dumps(codec.to_json_value(USER)))
    assert loads(codec.to_json(USER)) == json_value
    assert json_value == {
        "user_id": "8f5497e0-abee-4852-836b-faeacb6708eb",
        "registered_at": "2026-11-25T12:30:00Z",
        "address": {"city": "Yerevan"},
        "tags": ["admin"],
    }


def test_json_is_decoded_from_str(serializer: Serializer) -> None:
    codec = serializer.codec(Address)

    assert codec.from_json('{"city": "Yerevan"}') == Address(city="Yerevan")


def test_codec_is_typed_by_class(serializer: Serializer) -> None:
    codec: Codec[list[Address]] = serializer.codec(list[Address])

    assert codec.from_json(b'[{"city": "Yerevan"}]') == [Address(city="Yerevan")]


def test_unsupported_type_fails_when_codec_is_prepared(serializer: Serializer) -> None:
    with pytest.raises(UnsupportedTypeError):
        serializer.codec(Opaque)


def test_type_with_unresolved_forward_reference_is_unsupported(serializer: Serializer) -> None:
    with pytest.raises(UnsupportedTypeError):
        serializer.codec(Unresolved)


def test_dict_keys_become_strings(serializer: Serializer) -> None:
    codec = serializer.codec(dict[int, str])

    assert codec.to_json_value({1: "a"}) == {"1": "a"}
    assert codec.from_json_value({"1": "a"}) == {1: "a"}


def test_unserializable_value_fails(serializer: Serializer) -> None:
    codec = serializer.codec(Any)

    with pytest.raises(SerializationError):
        codec.to_json(Opaque())
    with pytest.raises(SerializationError):
        codec.to_json_value(Opaque())


@pytest.mark.parametrize(
    "data",
    [{"city": 1}, {}, ["Yerevan"]],
)
def test_data_of_another_shape_fails(serializer: Serializer, data: JsonValue) -> None:
    codec = serializer.codec(Address)

    with pytest.raises(DeserializationError):
        codec.from_json_value(data)
    with pytest.raises(DeserializationError):
        codec.from_json(dumps(data))


def test_malformed_json_fails(serializer: Serializer) -> None:
    codec = serializer.codec(Address)

    with pytest.raises(DeserializationError):
        codec.from_json(b'{"city": ')


def test_json_that_is_not_utf8_fails(serializer: Serializer) -> None:
    codec = serializer.codec(str)

    with pytest.raises(DeserializationError):
        codec.from_json(b'"\xff"')


def test_data_that_is_not_json_value_fails(serializer: Serializer) -> None:
    codec = serializer.codec(str)
    data: Any = Opaque()

    with pytest.raises(DeserializationError):
        codec.from_json_value(data)


def test_json_schema_keeps_nested_types_in_defs(serializer: Serializer) -> None:
    schema = serializer.codec(RegisterUser).json_schema()

    defs = schema["$defs"]
    assert isinstance(defs, Mapping)
    assert "Address" in defs
    dumps(schema)


@pytest.mark.parametrize("serializer", [PydanticSerializer(strict=True), MsgspecSerializer()])
def test_strict_serializer_does_not_coerce(serializer: Serializer) -> None:
    codec = serializer.codec(int)

    with pytest.raises(DeserializationError):
        codec.from_json(b'"1"')
    with pytest.raises(DeserializationError):
        codec.from_json_value("1")


@pytest.mark.parametrize("serializer", [PydanticSerializer(), MsgspecSerializer(strict=False)])
def test_lax_serializer_coerces(serializer: Serializer) -> None:
    codec = serializer.codec(int)

    assert codec.from_json(b'"1"') == 1
    assert codec.from_json_value("1") == 1


def test_strict_pydantic_serializer_decodes_json_values() -> None:
    codec = PydanticSerializer(strict=True).codec(RegisterUser)

    assert codec.from_json_value(codec.to_json_value(USER)) == USER


class Profile(BaseModel):
    display_name: str = Field(alias="displayName")


def test_pydantic_serializer_writes_aliases_it_reads() -> None:
    codec = PydanticSerializer().codec(Profile)
    profile = Profile(displayName="Max")

    assert codec.to_json_value(profile) == {"displayName": "Max"}
    assert codec.from_json(codec.to_json(profile)) == profile


def test_pydantic_serializer_rejects_value_of_another_type() -> None:
    codec: Codec[Any] = PydanticSerializer().codec(Address)

    with pytest.raises(SerializationError):
        codec.to_json(42)
    with pytest.raises(SerializationError):
        codec.to_json_value(42)


def test_pydantic_serializer_rejects_circular_reference() -> None:
    codec = PydanticSerializer().codec(list[Any])
    value: list[Any] = []
    value.append(value)

    with pytest.raises(SerializationError):
        codec.to_json_value(value)
    with pytest.raises(SerializationError):
        codec.to_json(value)


class Point(Struct, frozen=True):
    x: int
    y: int


def test_msgspec_serializer_supports_structs() -> None:
    codec = MsgspecSerializer().codec(Point)

    assert codec.to_json_value(Point(x=1, y=2)) == {"x": 1, "y": 2}
    assert codec.from_json(b'{"x": 1, "y": 2}') == Point(x=1, y=2)
