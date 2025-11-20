"""RFC 6455 Websocket protocol library."""

import base64
from enum import IntEnum
import hashlib
import secrets
import sys
from typing import Final


GUID: Final = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class OpCode(IntEnum):
    """Websocket OpCode values."""
    CONT = 0x00
    TEXT = 0x01
    BINARY = 0x02
    CLOSE = 0x08
    PING = 0x09
    PONG = 0x0A


def generate_nonce() -> str:
    """Generate a random base64-encoded nonce for Websocket handshake."""
    random_bytes = secrets.token_bytes(16)
    return base64.b64encode(random_bytes).decode("utf-8")


def validate_handshake_key(nonce: str, accept_key: str) -> bool:
    """Validate the Sec-WebSocket-Accept key from the server."""
    expected_key = nonce + GUID
    expected_key_digest = hashlib.sha1(expected_key.encode("utf-8")).digest()
    expected_key_b64 = base64.b64encode(expected_key_digest).decode("utf-8")
    return accept_key == expected_key_b64


def generate_mask() -> bytes:
    """Generate a random 4-byte mask for Websocket frames."""
    # Per RFC 6455, Section 5.3, the mask must come from a strong source of entropy.
    return secrets.token_bytes(4)


def apply_mask(mask: bytes, data: bytes) -> bytes:
    """Apply a WebSocket mask to the input data."""
    # This is a performance-critical method.
    # Do not loop over the payload data.
    # In fact, just use the pure Python implementation from the websockets library,
    #   which is based on Will McGugan's implementation.
    # See: https://github.com/python-websockets/websockets/commit/c7fc0d36bd8ea2aeb7c4321f53d208fb1297db85
    assert len(mask) == 4, "Mask must be 4 bytes"
    data_int = int.from_bytes(data, sys.byteorder)
    mask_repeated = mask * (len(data) // 4) + mask[:len(data) % 4]
    mask_int = int.from_bytes(mask_repeated, sys.byteorder)
    return (data_int ^ mask_int).to_bytes(len(data), sys.byteorder)


def frame_ws_data(opcode: OpCode, data: bytes) -> bytes:
    """Frame data for WebSocket transport.

    All data frames sent to the broker will be singular, final frames."""
    header = bytes([0x80 | opcode.value])
    length = len(data)
    if length <= 125:
        length_encoded = bytes([length | 0x80])
    elif length <= 0xffff:
        length_encoded = bytes([126 | 0x80]) + length.to_bytes(2, "big")
    else:
        length_encoded = bytes([127 | 0x80]) + length.to_bytes(8, "big")
    mask = generate_mask()
    masked_data = apply_mask(mask, data)
    return header + length_encoded + mask + masked_data
