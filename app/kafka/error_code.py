import enum

from .encode_functions import encode_int16


@enum.unique
class ErrorCode(enum.IntEnum):
    NONE = 0
    UNSUPPORTED_VERSION = 35
    UNKNOWN_TOPIC_ID = 100

    def encode(self):
        return encode_int16(self)
