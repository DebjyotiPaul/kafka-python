import io
import typing
import uuid

__all__ = [
    "decode_int8",
    "decode_int16",
    "decode_int32",
    "decode_int64",
    "decode_varint",
    "decode_uuid",
    "decode_compact_string",
    "decode_nullable_string",
    "decode_compact_array",
    "decode_tagged_fields",
]

DecodeFunction = typing.Callable[[io.BytesIO], typing.Any]


def decode_int8(byte_stream: io.BytesIO):
    return int.from_bytes(byte_stream.read(1), signed=True)


def decode_int16(byte_stream: io.BytesIO):
    return int.from_bytes(byte_stream.read(2), signed=True)


def decode_int32(byte_stream: io.BytesIO):
    return int.from_bytes(byte_stream.read(4), signed=True)


def decode_int64(byte_stream: io.BytesIO):
    return int.from_bytes(byte_stream.read(8), signed=True)


def decode_varint(byte_stream: io.BytesIO):
    BASE = 128
    integer, multiplier = 0, 1
    while True:
        n = int.from_bytes(byte_stream.read(1), signed=False)
        integer += (n % BASE) * multiplier
        if n < BASE:
            return integer
        multiplier *= BASE


def decode_uuid(byte_stream: io.BytesIO):
    return uuid.UUID(bytes=byte_stream.read(16))


def decode_compact_string(byte_stream: io.BytesIO):
    n = decode_varint(byte_stream) - 1
    return byte_stream.read(n).decode()


def decode_nullable_string(byte_stream: io.BytesIO):
    n = decode_int16(byte_stream)
    return "" if n < 0 else byte_stream.read(n).decode()


def decode_compact_array(byte_stream: io.BytesIO, decode_function: DecodeFunction):
    n = decode_varint(byte_stream) - 1
    return [decode_function(byte_stream) for _ in range(n)]


def decode_tagged_fields(byte_stream: io.BytesIO):
    # There are no tagged fields in this challenge. TAG_BUFFER is always a null byte.
    assert byte_stream.read(1) == b"\x00", "Unexpected tagged fields."
