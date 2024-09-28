def get_int8(int8: bytes) -> int:
    assert len(int8) == 1
    return int.from_bytes(int8, "big", signed=True)
def to_int8(int8: int) -> bytes:
    return int.to_bytes(int8, 1, "big", signed=True)
def get_int16(int16: bytes) -> int:
    assert len(int16) == 2
    return int.from_bytes(int16, "big", signed=True)
def to_int16(int16: int) -> bytes:
    return int.to_bytes(int16, 2, "big", signed=True)
def get_int32(int32: bytes) -> int:
    assert len(int32) == 4
    return int.from_bytes(int32, "big", signed=True)
def to_int32(int32: int) -> bytes:
    return int.to_bytes(int32, 4, "big", signed=True)
def get_int64(int64: bytes) -> int:
    assert len(int64) == 8
    return int.from_bytes(int64, "big", signed=True)
def to_int64(int64: int) -> bytes:
    return int.to_bytes(int64, 8, "big", signed=True)
def parse_string(req: bytes) -> tuple[str, bytes]:
    str_len = get_int16(req[:2])
    string = req[2 : 2 + str_len].decode()
    return (string, req[2 + str_len :])
def get_varint(buffer: bytes) -> tuple[int, bytes]:
    result = 0
    shift = 0
    for i, byte in enumerate(buffer):
        # Extract the 7 least significant bits from the current byte.
        result |= (byte & 0x7F) << shift
        # If the most significant bit is not set, we are done.
        if not (byte & 0x80):
            # Return the decoded value and the remaining buffer.
            return result, buffer[i + 1 :]
        # Otherwise, move to the next group of 7 bits.
        shift += 7
def to_varint(value: int) -> bytes:
    result = bytearray()
    while value >= 0x80:
        # Take the lowest 7 bits of value and append them to the result, with the MSB set to 1.
        result.append((value & 0x7F) | 0x80)
        value >>= 7  # Shift the value 7 bits to the right.
    # Append the last byte, with MSB set to 0 (indicating this is the last byte).
    result.append(value & 0x7F)
    return bytes(result)
def parse_compact_string(req: bytes) -> tuple[str, bytes]:
    strlen, req = get_varint(req)
    string = req[: strlen - 1].decode()
    print(strlen, string)
    return string, req[strlen - 1 :]
def to_array(array: list[bytes]) -> bytes:  # compact array
    return to_varint(len(array) + 1) + b"".join(array)
def to_tagbuffer(fields: dict) -> bytes:  # todo
    return to_varint(0)
def get_uuid(req: bytes) -> tuple[bytes, bytes]:
    return req[:16], req[16:]