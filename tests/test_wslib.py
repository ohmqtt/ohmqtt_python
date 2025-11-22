import pytest

from ohmqtt.connection.wslib import (
    generate_nonce,
    generate_handshake_key,
    generate_mask,
    apply_mask,
    frame_ws_data,
    deframe_ws_data,
    OpCode,
    WebsocketError,
)


def test_wslib_generate_nonce() -> None:
    nonce1 = generate_nonce()
    nonce2 = generate_nonce()
    assert isinstance(nonce1, str)
    assert isinstance(nonce2, str)
    assert nonce1 != nonce2
    assert len(nonce1) == 24  # 16 bytes base64-encoded is 24 characters
    assert len(nonce2) == 24


def test_wslib_validate_handshake_key() -> None:
    nonce = "dGhlIHNhbXBsZSBub25jZQ=="
    accept_key = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
    assert generate_handshake_key(nonce) == accept_key


def test_wslib_generate_mask() -> None:
    mask1 = generate_mask()
    mask2 = generate_mask()
    assert isinstance(mask1, bytes)
    assert isinstance(mask2, bytes)
    assert len(mask1) == 4
    assert len(mask2) == 4
    assert mask1 != mask2


def test_wslib_apply_mask() -> None:
    mask = b"\xFF\x00\xFF\x00"
    data = b"Hello, WebSocket!"
    masked_data = apply_mask(mask, data)
    unmasked_data = apply_mask(mask, masked_data)
    assert masked_data != data
    assert unmasked_data == data


@pytest.mark.parametrize("data_length", [0, 1, 2, 3, 4, 5])
def test_wslib_apply_mask_varied_lengths(data_length: int) -> None:
    mask = b"\xFF\x00\xFF\x00"
    data = bytes(range(data_length))
    masked_data = apply_mask(mask, data)
    unmasked_data = apply_mask(mask, masked_data)
    if data_length > 0:
        assert masked_data != data
    assert unmasked_data == data


@pytest.mark.parametrize("opcode", [OpCode.CONT, OpCode.TEXT, OpCode.BINARY, OpCode.CLOSE, OpCode.PING, OpCode.PONG])
@pytest.mark.parametrize("data_length", [0, 125, 126, 127, 65535, 65536])
@pytest.mark.parametrize("do_mask", [True, False])
def test_wslib_frame_ws_data(opcode: OpCode, data_length: int, do_mask: bool) -> None:
    data = b"\x88" * data_length
    framed_data = frame_ws_data(opcode, data, do_mask=do_mask)

    assert framed_data[0] == 0x80 | opcode.value  # FIN + opcode
    length_byte = framed_data[1]
    mask_bit = 0x80 if do_mask else 0x00
    assert (length_byte & 0x80) == mask_bit
    if data_length <= 125:
        assert (length_byte & 0x7F) == data_length
        index = 2
    elif data_length <= 0xFFFF:
        assert (length_byte & 0x7F) == 126
        extended_length = int.from_bytes(framed_data[2:4], "big")
        assert extended_length == data_length
        index = 4
    else:
        assert (length_byte & 0x7F) == 127
        extended_length = int.from_bytes(framed_data[2:10], "big")
        assert extended_length == data_length
        index = 10
    if do_mask:
        mask = framed_data[index:index+4]
        masked_payload = framed_data[index+4:]
        payload = apply_mask(mask, masked_payload)
    else:
        payload = framed_data[index:]
    assert payload == data


@pytest.mark.parametrize("opcode", [OpCode.CONT, OpCode.TEXT, OpCode.BINARY, OpCode.CLOSE, OpCode.PING, OpCode.PONG])
@pytest.mark.parametrize("data_length", [0, 125, 126, 127, 65535, 65536])
@pytest.mark.parametrize("do_mask", [True, False])
def test_wslib_deframe_ws_data(opcode: OpCode, data_length: int, do_mask: bool) -> None:
    data = b"\x88" * data_length
    framed_data = frame_ws_data(opcode, data, do_mask=do_mask)

    frame = deframe_ws_data(bytearray(framed_data))
    assert frame is not None
    opcode, payload, was_masked, length = frame
    assert opcode == opcode
    assert payload == data
    assert was_masked == do_mask
    assert length == len(framed_data)


@pytest.mark.parametrize("opcode", [OpCode.CONT, OpCode.TEXT, OpCode.BINARY, OpCode.CLOSE, OpCode.PING, OpCode.PONG])
@pytest.mark.parametrize("data_length", [0, 125, 126, 127, 65535, 65536])
@pytest.mark.parametrize("do_mask", [True, False])
def test_wslib_deframe_ws_data_drip_feed(opcode: OpCode, data_length: int, do_mask: bool) -> None:
    data = b"\x88" * data_length
    framed_data = frame_ws_data(opcode, data, do_mask=do_mask)

    # Only drip feed enough for confidence that we've covered all the decoding branches.
    feed_length = min(len(framed_data) - 1, 16)
    for i in range(feed_length):
        result = deframe_ws_data(bytearray(framed_data[:i]))
        assert result is None


def test_wslib_deframe_ws_data_fragment() -> None:
    framed_data = bytearray([OpCode.BINARY, 0x00])  # FIN=0, opcode=BINARY, no payload
    with pytest.raises(WebsocketError, match="Fragmented WebSocket frames are not supported"):
        deframe_ws_data(framed_data)


@pytest.mark.parametrize("do_mask", [True, False])
def test_wslib_frame_large_data(do_mask: bool) -> None:
    data_length = 256 * 1024 * 1024  # 256MB, maximum MQTT packet size
    data = b"\x88" * data_length
    framed_data = frame_ws_data(OpCode.BINARY, data, do_mask=do_mask)
    frame = deframe_ws_data(bytearray(framed_data))
    assert frame is not None
    opcode, payload, was_masked, length = frame
    assert opcode == OpCode.BINARY
    assert payload == data
    assert was_masked is do_mask
    assert length == len(framed_data)
