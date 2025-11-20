import pytest

from ohmqtt.connection.wslib import (
    generate_nonce,
    validate_handshake_key,
    generate_mask,
    apply_mask,
    frame_ws_data,
    OpCode,
)


def test_wslib_generate_nonce() -> None:
    nonce1 = generate_nonce()
    nonce2 = generate_nonce()
    assert isinstance(nonce1, str)
    assert isinstance(nonce2, str)
    assert nonce1 != nonce2
    assert len(nonce1) == 24  # 16 bytes base64-encoded is 24 characters


def test_wslib_validate_handshake_key() -> None:
    nonce = "dGhlIHNhbXBsZSBub25jZQ=="
    accept_key = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
    assert validate_handshake_key(nonce, accept_key) is True
    assert validate_handshake_key(nonce, "invalid_key") is False


def test_wslib_generate_mask() -> None:
    mask1 = generate_mask()
    mask2 = generate_mask()
    assert isinstance(mask1, bytes)
    assert isinstance(mask2, bytes)
    assert len(mask1) == 4
    assert len(mask2) == 4
    assert mask1 != mask2


def test_wslib_apply_mask() -> None:
    mask = b"\x01\x02\x03\x04"
    data = b"Hello, WebSocket!"
    masked_data = apply_mask(mask, data)
    unmasked_data = apply_mask(mask, masked_data)
    assert unmasked_data == data


@pytest.mark.parametrize("data_length", [0, 1, 2, 3, 4, 5])
def test_wslib_apply_mask_varied_lengths(data_length: int) -> None:
    mask = b"\x0F\x0E\x0D\x0C"
    data = bytes(range(data_length))
    masked_data = apply_mask(mask, data)
    unmasked_data = apply_mask(mask, masked_data)
    assert unmasked_data == data


def test_wslib_apply_mask_large_data() -> None:
    mask = b"\xAA\xBB\xCC\xDD"
    data = b"\x88" * 0xffffff  # 16MB
    masked_data = apply_mask(mask, data)
    unmasked_data = apply_mask(mask, masked_data)
    assert unmasked_data == data


@pytest.mark.parametrize("opcode", [OpCode.CONT, OpCode.TEXT, OpCode.BINARY, OpCode.CLOSE, OpCode.PING, OpCode.PONG])
@pytest.mark.parametrize("data_length", [0, 125, 126, 127, 65535, 65536])
def test_wslib_frame_ws_data(opcode: OpCode, data_length: int) -> None:
    data = b"\x55" * data_length
    framed_data = frame_ws_data(opcode, data)

    assert framed_data[0] == 0x80 | opcode.value  # FIN + opcode
    length_byte = framed_data[1]
    assert length_byte & 0x80  # Mask bit should be set
    if data_length <= 125:
        assert (length_byte & 0x7F) == data_length
        mask_start = 2
    elif data_length <= 0xFFFF:
        assert (length_byte & 0x7F) == 126
        extended_length = int.from_bytes(framed_data[2:4], "big")
        assert extended_length == data_length
        mask_start = 4
    else:
        assert (length_byte & 0x7F) == 127
        extended_length = int.from_bytes(framed_data[2:10], "big")
        assert extended_length == data_length
        mask_start = 10
    mask = framed_data[mask_start:mask_start+4]
    masked_payload = framed_data[mask_start+4:]
    unmasked_payload = apply_mask(mask, masked_payload)
    assert unmasked_payload == data
