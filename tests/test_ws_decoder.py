import socket
import ssl
from unittest.mock import Mock

import pytest
from pytest_mock import MockerFixture

from ohmqtt.connection.decoder import ClosedSocketError
from ohmqtt.connection.ws_decoder import WebsocketDecoder
from ohmqtt.connection.wslib import WebsocketError, OpCode


@pytest.fixture
def mock_socket(mocker: MockerFixture) -> Mock:
    """Return a mock socket for testing."""
    return mocker.Mock(spec=socket.socket)  # type: ignore[no-any-return]


def test_ws_decoder_defaults() -> None:
    """reset() should set the decoder to its default state."""
    decoder = WebsocketDecoder()
    decoder2 = WebsocketDecoder()
    decoder2.reset()
    assert decoder.opcode == decoder2.opcode
    assert decoder.length == decoder2.length
    assert decoder.data == decoder2.data


@pytest.mark.parametrize("payload_length", [0, 125, 126, 127, 255, 65535, 65536])
def test_ws_decoder_payload_lengths(payload_length: int, mock_socket: Mock) -> None:
    """Test decoding frames with various payload lengths."""
    decoder = WebsocketDecoder()

    payload = b"x" * payload_length
    if payload_length <= 125:
        header = bytes([0x80 | OpCode.BINARY, payload_length])
    elif payload_length <= 0xFFFF:
        header = bytes([0x80 | OpCode.BINARY, 126]) + payload_length.to_bytes(2, "big")
    else:
        header = bytes([0x80 | OpCode.BINARY, 127]) + payload_length.to_bytes(8, "big")

    mock_socket.recv.side_effect = [bytes([b]) for b in header] + [payload]
    out = decoder.decode(mock_socket)
    assert out == (OpCode.BINARY, payload)


@pytest.mark.parametrize("exc", [BlockingIOError, ssl.SSLWantReadError, ssl.SSLWantWriteError])
def test_ws_decoder_drip_feed(exc: Exception, mock_socket: Mock) -> None:
    """Feed the decoder one byte at a time."""
    decoder = WebsocketDecoder()
    # Use a packet with a payload of 255 bytes to ensure we have to read
    # multiple bytes for the frame length.
    payload = b"x" * 255
    frame = bytes([0x80 | OpCode.BINARY]) + bytes([126, 0, 255]) + payload

    # Setup mock to return one byte at a time
    for i in range(len(frame) - 1):
        mock_socket.recv.side_effect = [frame[i:i+1], exc]
        assert decoder.decode(mock_socket) is None

    # Final byte completes the packet
    mock_socket.recv.side_effect = [frame[-1:]]
    out = decoder.decode(mock_socket)
    assert out == (OpCode.BINARY, payload)


@pytest.mark.parametrize("exc", [BlockingIOError, ssl.SSLWantReadError, ssl.SSLWantWriteError])
@pytest.mark.parametrize("closer", [b"", OSError])
@pytest.mark.parametrize("available_bytes", [n for n in range(6)])
def test_ws_decoder_drip_partial_closures(exc: Exception, closer: bytes | Exception,
                                          available_bytes: int, mock_socket: Mock) -> None:
    """Feed the decoder partial data, then close the socket."""
    decoder = WebsocketDecoder()
    # Use a packet with a payload of 255 bytes to ensure we have to read
    # multiple bytes for the frame length.
    payload = b"x" * 255
    frame = bytes([0x80 | OpCode.BINARY]) + bytes([126, 0, 255]) + payload

    # Send a few bytes then simulate closed socket
    if available_bytes > 0:
        mock_socket.recv.side_effect = [bytes([b]) for b in frame[:available_bytes]] + [exc]
    else:
        mock_socket.recv.side_effect = [exc]

    assert decoder.decode(mock_socket) is None

    mock_socket.recv.side_effect = [closer]
    with pytest.raises(ClosedSocketError):
        decoder.decode(mock_socket)


def test_ws_decoder_protocol_errors(mock_socket: Mock) -> None:
    """Test that protocol errors raise WebsocketError."""
    decoder = WebsocketDecoder()

    # Fragmented frame (FIN bit not set)
    mock_socket.recv.side_effect = [bytes([OpCode.BINARY])]
    with pytest.raises(WebsocketError, match="Fragmented WebSocket frames are not supported"):
        decoder.decode(mock_socket)

    decoder = WebsocketDecoder()

    # Reserved bits set
    mock_socket.recv.side_effect = [bytes([0x80 | 0x70 | OpCode.BINARY])]
    with pytest.raises(WebsocketError, match="Reserved WebSocket bits are set"):
        decoder.decode(mock_socket)

    decoder = WebsocketDecoder()

    # Invalid opcode
    mock_socket.recv.side_effect = [bytes([0x80 | 0x03])]
    with pytest.raises(WebsocketError, match="Invalid WebSocket opcode"):
        decoder.decode(mock_socket)

    decoder = WebsocketDecoder()

    # Masked frame from server
    mock_socket.recv.side_effect = [bytes([0x80 | OpCode.BINARY]), b"\x80"]
    with pytest.raises(WebsocketError, match="Masked frames from server are not allowed"):
        decoder.decode(mock_socket)
