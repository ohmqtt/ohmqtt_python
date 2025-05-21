"""Primitives for encoding and decoding MQTT packet fields."""

import struct
from typing import Final

from .error import MQTTError
from .mqtt_spec import MQTTReasonCode

# Maximum variable integer value.
MAX_VARINT: Final = 268435455


UInt8Packer: Final = struct.Struct("B")
UInt16Packer: Final = struct.Struct(">H")
UInt32Packer: Final = struct.Struct(">L")


def encode_bool(x: bool) -> bytes:
    """Encode a boolean to a buffer."""
    # MQTT booleans are encoded as a single byte with a value of 0 or 1.
    return b"\x01" if x else b"\x00"


def decode_bool(data: memoryview) -> tuple[bool, int]:
    """Decode a boolean from a buffer.

    Returns a tuple of the decoded boolean and the number of bytes consumed."""
    try:
        if data[0] not in (0, 1):
            raise MQTTError("Invalid boolean value", MQTTReasonCode.ProtocolError)
        return bool(data[0]), 1
    except MQTTError:
        raise
    except Exception as e:
        raise MQTTError("Failed to decode boolean from buffer", MQTTReasonCode.MalformedPacket) from e


def encode_uint8(x: int) -> bytes:
    """Encode an 8-bit integer to a buffer."""
    return UInt8Packer.pack(x)


def decode_uint8(data: memoryview) -> tuple[int, int]:
    """Decode an 8-bit integer from a buffer.

    Returns a tuple of the decoded integer and the number of bytes consumed."""
    try:
        return UInt8Packer.unpack_from(data)[0], 1
    except Exception as e:
        raise MQTTError("Failed to decode byte from buffer", MQTTReasonCode.MalformedPacket) from e


def encode_uint16(x: int) -> bytes:
    """Encode a 16-bit integer to a buffer."""
    return UInt16Packer.pack(x)


def decode_uint16(data: memoryview) -> tuple[int, int]:
    """Decode a 16-bit integer from a buffer.

    Returns a tuple of the decoded integer and the number of bytes consumed."""
    try:
        return UInt16Packer.unpack_from(data)[0], 2
    except Exception as e:
        raise MQTTError("Failed to decode 16-bit integer from buffer", MQTTReasonCode.MalformedPacket) from e


def encode_uint32(x: int) -> bytes:
    """Encode a 32-bit integer to a buffer."""
    return UInt32Packer.pack(x)


def decode_uint32(data: memoryview) -> tuple[int, int]:
    """Decode a 32-bit integer from a buffer.

    Returns a tuple of the decoded integer and the number of bytes consumed."""
    try:
        return UInt32Packer.unpack_from(data)[0], 4
    except Exception as e:
        raise MQTTError("Failed to decode 32-bit integer from buffer", MQTTReasonCode.MalformedPacket) from e


def encode_string(s: str) -> bytes:
    """Encode a UTF-8 string to a buffer."""
    data = s.encode("utf-8")
    return UInt16Packer.pack(len(data)) + data


def decode_string(data: memoryview) -> tuple[str, int]:
    """Decode a UTF-8 string from a buffer.

    Returns a tuple of the decoded string and the number of bytes consumed."""
    try:
        length = UInt16Packer.unpack_from(data)[0]
        if length > len(data) - 2:
            raise ValueError("String data underrun")
        # Strict UTF-8 decoding will catch any invalid UTF-8 sequences.
        # This is important for MQTT, as invalid sequences are not allowed.
        s = str(data[2:2 + length], encoding="utf-8", errors="strict")
        # The only other invalid character is the null character.
        if "\u0000" in s:
            raise ValueError("Unicode null character in string")
        return s, length + 2
    except Exception as e:
        raise MQTTError("Failed to decode string from buffer", MQTTReasonCode.MalformedPacket) from e


def encode_string_pair(values: tuple[str, str]) -> bytes:
    """Encode a UTF-8 string pair to a buffer."""
    left = values[0].encode("utf-8")
    right = values[1].encode("utf-8")
    return UInt16Packer.pack(len(left)) + left + UInt16Packer.pack(len(right)) + right


def decode_string_pair(data: memoryview) -> tuple[tuple[str, str], int]:
    """Decode a UTF-8 string pair from a buffer.

    Returns a tuple of the decoded string pair and the number of bytes consumed."""
    try:
        key, key_length = decode_string(data)
        value, value_length = decode_string(data[key_length:])
        return (key, value), key_length + value_length
    except Exception as e:
        raise MQTTError("Failed to decode string pair from buffer", MQTTReasonCode.MalformedPacket) from e


def encode_binary(data: bytes) -> bytes:
    """Encode binary data to a buffer."""
    return UInt16Packer.pack(len(data)) + data


def decode_binary(data: memoryview) -> tuple[bytes, int]:
    """Decode binary data from a buffer.

    Returns a tuple of the decoded data and the number of bytes consumed."""
    try:
        length = UInt16Packer.unpack_from(data)[0]
        if length > len(data) - 2:
            raise ValueError("Binary data underrun")
        return data[2:2 + length].tobytes("A"), length + 2
    except Exception as e:
        raise MQTTError("Failed to decode binary data from buffer", MQTTReasonCode.MalformedPacket) from e


def encode_varint(x: int) -> bytes:
    """Encode a variable length integer to a buffer."""
    if not 0 <= x <= MAX_VARINT:
        raise ValueError(f"Varint out of range (0 <= x <= {MAX_VARINT})")
    if x < 0x7F:
        # Fast path for single byte value.
        return x.to_bytes(1, byteorder="big")
    packed = bytearray()
    while True:
        b = x % 0x80
        x //= 0x80
        if x > 0:
            b += 0x80
        packed.append(b)
        if x == 0:
            return bytes(packed)


def decode_varint(data: memoryview) -> tuple[int, int]:
    """Decode a variable length integer from a buffer.

    Returns a tuple of the decoded integer and the number of bytes consumed."""
    try:
        result = 0
        mult = 1
        sz = 0
        for byte in data:
            sz += 1
            result += byte % 0x80 * mult
            if byte < 0x80:
                return result, sz
            if sz >= 4:
                raise ValueError("Varint overflow")
            mult *= 0x80
        raise ValueError("Varint underrun")
    except Exception as e:
        raise MQTTError("Failed to decode varint from buffer", MQTTReasonCode.MalformedPacket) from e
