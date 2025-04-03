"""Primitives for encoding and decoding MQTT packet fields."""

from typing import Final

from .error import MQTTError
from .mqtt_spec import MQTTReasonCode

# Maximum variable integer value.
MAX_VARINT: Final = 268435455


def encode_bool(x: bool) -> bytes:
    """Encode a boolean to a buffer."""
    # MQTT booleans are encoded as a single byte with a value of 0 or 1.
    return b"\x01" if x else b"\x00"


def decode_bool(data: bytes) -> tuple[bool, int]:
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
    return x.to_bytes(length=1, byteorder="big")


def decode_uint8(data: bytes) -> tuple[int, int]:
    """Decode an 8-bit integer from a buffer.
    
    Returns a tuple of the decoded integer and the number of bytes consumed."""
    try:
        return int(data[0]), 1
    except Exception as e:
        raise MQTTError("Failed to decode byte from buffer", MQTTReasonCode.MalformedPacket) from e


def encode_uint16(x: int) -> bytes:
    """Encode a 16-bit integer to a buffer."""
    return x.to_bytes(length=2, byteorder="big")


def decode_uint16(data: bytes) -> tuple[int, int]:
    """Decode a 16-bit integer from a buffer.
    
    Returns a tuple of the decoded integer and the number of bytes consumed."""
    try:
        if len(data) < 2:
            raise ValueError("Integer underrun")
        return int.from_bytes(data[:2], byteorder="big"), 2
    except Exception as e:
        raise MQTTError("Failed to decode 16-bit integer from buffer", MQTTReasonCode.MalformedPacket) from e


def encode_uint32(x: int) -> bytes:
    """Encode a 32-bit integer to a buffer."""
    return x.to_bytes(length=4, byteorder="big")


def decode_uint32(data: bytes) -> tuple[int, int]:
    """Decode a 32-bit integer from a buffer.
    
    Returns a tuple of the decoded integer and the number of bytes consumed."""
    try:
        if len(data) < 4:
            raise ValueError("Integer underrun")
        return int.from_bytes(data[:4], byteorder="big"), 4
    except Exception as e:
        raise MQTTError("Failed to decode 32-bit integer from buffer", MQTTReasonCode.MalformedPacket) from e


def encode_string(s: str) -> bytes:
    """Encode a UTF-8 string to a buffer."""
    data = s.encode("utf-8")
    return len(data).to_bytes(2, byteorder="big") + data


def decode_string(data: bytes) -> tuple[str, int]:
    """Decode a UTF-8 string from a buffer.
    
    Returns a tuple of the decoded string and the number of bytes consumed."""
    try:
        if len(data) < 2:
            raise ValueError("String length underrun")
        length = int.from_bytes(data[:2], byteorder="big")
        if length > len(data) - 2:
            raise ValueError("String data underrun")
        # Strict UTF-8 decoding should catch any invalid UTF-8 sequences.
        # This is important for MQTT, as invalid sequences are not allowed.
        s = data[2:2 + length].decode("utf-8", errors="strict")
        # Re-encoding the string to UTF-8 makes extra sure there are no surrogates or invalid sequences.
        s.encode("utf-8", errors="strict")
        # The only other invalid character is the null character.
        if s.find("\u0000") != -1:
            raise ValueError("Unicode null character in string")
        return s, length + 2
    except Exception as e:
        raise MQTTError("Failed to decode string from buffer", MQTTReasonCode.MalformedPacket) from e


def encode_string_pair(values: tuple[str, str]) -> bytes:
    """Encode a UTF-8 string pair to a buffer."""
    return encode_string(values[0]) + encode_string(values[1])


def decode_string_pair(data: bytes) -> tuple[tuple[str, str], int]:
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
    return len(data).to_bytes(2, byteorder="big") + data


def decode_binary(data: bytes) -> tuple[bytes, int]:
    """Decode binary data from a buffer.
    
    Returns a tuple of the decoded data and the number of bytes consumed."""
    try:
        if len(data) < 2:
            raise ValueError("Binary length underrun")
        length = int.from_bytes(data[:2], byteorder="big")
        if length > len(data) - 2:
            raise ValueError("Binary data underrun")
        return bytes(data[2:2 + length]), length + 2
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


def decode_varint(data: bytes) -> tuple[int, int]:
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
