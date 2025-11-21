"""RFC 6455 Websocket protocol library."""

import base64
from enum import IntEnum
import hashlib
import secrets
import sys
from typing import Final

from ..logger import get_logger


logger: Final = get_logger("ohmqtt.connection.wslib")
GUID: Final = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class WebsocketError(Exception):
    """Exception raised for WebSocket protocol errors."""


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


def generate_handshake_key(nonce: str) -> str:
    """Generate the expected Sec-WebSocket-Accept key from the server."""
    expected_key = nonce + GUID
    expected_key_digest = hashlib.sha1(expected_key.encode("utf-8")).digest()
    return base64.b64encode(expected_key_digest).decode("utf-8")


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


def frame_ws_data(opcode: OpCode, payload: bytes, do_mask: bool = True) -> bytes:
    """Frame data for WebSocket transport.

    All data frames sent to the broker will be singular, final frames."""
    header = bytes([0x80 | opcode.value])
    length = len(payload)
    mask_bit = 0x80 if do_mask else 0x00
    if length <= 125:
        length_encoded = bytes([length | mask_bit])
    elif length <= 0xffff:
        length_encoded = bytes([126 | mask_bit]) + length.to_bytes(2, "big")
    else:
        length_encoded = bytes([127 | mask_bit]) + length.to_bytes(8, "big")
    if do_mask:
        mask = generate_mask()
        payload = apply_mask(mask, payload)
        return header + length_encoded + mask + payload
    return header + length_encoded + payload


def deframe_ws_data(buffer: bytearray | bytes) -> tuple[OpCode, bytes, bool, int] | None:
    """Deframe data from WebSocket transport.

    Returns a tuple of (OpCode, payload, masked) if a complete frame is available,
        or None if more data is needed."""
    if len(buffer) < 2:
        logger.debug("Deframe needs at least 2 bytes, has %d", len(buffer))
        return None

    fin = (buffer[0] & 0x80) != 0
    if not fin:
        raise WebsocketError("Fragmented WebSocket frames are not supported")
    opcode = OpCode(buffer[0] & 0x0F)
    masked = (buffer[1] & 0x80) != 0
    payload_length = buffer[1] & 0x7F

    index = 2
    if payload_length == 126:
        if len(buffer) < index + 2:
            logger.debug("Deframe needs at least %d bytes for extended payload length, has %d", index + 2, len(buffer))
            return None
        payload_length = int.from_bytes(buffer[index:index + 2], "big")
        index += 2
    elif payload_length == 127:
        if len(buffer) < index + 8:
            logger.debug("Deframe needs at least %d bytes for extended payload length, has %d", index + 8, len(buffer))
            return None
        payload_length = int.from_bytes(buffer[index:index + 8], "big")
        index += 8

    frame_length = index + masked * 4 + payload_length
    if len(buffer) < frame_length:
        logger.debug("Deframe needs at least %d bytes for complete frame, has %d", frame_length, len(buffer))
        return None

    mask: bytes | None = None
    if masked:
        mask = bytes(buffer[index:index + 4])
        index += 4

    payload = bytes(buffer[index:frame_length])

    if masked and mask is not None:
        payload = apply_mask(mask, payload)

    return opcode, payload, masked, frame_length
