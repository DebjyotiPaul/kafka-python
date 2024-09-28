import uuid

__all__ = [
    "encode_int8",
    "encode_int16",
    "encode_int32",
    "encode_int64",
    "encode_varint",
    "encode_uuid",
    "encode_compact_array",
    "encode_tagged_fields",
]


def encode_int8(integer: int):
    return integer.to_bytes(1, signed=True)


def encode_int16(integer: int):
    return integer.to_bytes(2, signed=True)


def encode_int32(integer: int):
    return integer.to_bytes(4, signed=True)


def encode_int64(integer: int):
    return integer.to_bytes(8, signed=True)


def encode_varint(integer: int):
    BASE = 128
    encoding = b""
    while True:
        integer, n = divmod(integer, BASE)
        if integer > 0:
            n += BASE
        encoding += encode_int8(n)
        if integer == 0:
            return encoding


def encode_uuid(uuid: uuid.UUID):
    return uuid.bytes


def encode_compact_array(array: list):
    return encode_varint(len(array) + 1) + b"".join(item.encode() for item in array)


def encode_tagged_fields():
    return b"\x00"
