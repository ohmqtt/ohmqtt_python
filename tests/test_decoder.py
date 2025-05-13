import socket
import ssl
from unittest.mock import Mock

import pytest
from pytest_mock import MockerFixture

from ohmqtt.connection.decoder import IncrementalDecoder, ClosedSocketError
from ohmqtt.error import MQTTError
from ohmqtt.mqtt_spec import MQTTReasonCode
from ohmqtt.packet import MQTTPublishPacket


@pytest.fixture
def mock_socket(mocker: MockerFixture) -> Mock:
    """Return a mock socket for testing."""
    return mocker.Mock(spec=socket.socket)  # type: ignore[no-any-return]


def test_decoder_defaults() -> None:
    """reset() should set the decoder to its default state."""
    decoder = IncrementalDecoder()
    decoder2 = IncrementalDecoder()
    decoder2.reset()
    assert decoder.head == decoder2.head
    assert decoder.length == decoder2.length
    assert decoder.data == decoder2.data


@pytest.mark.parametrize("exc", [BlockingIOError, ssl.SSLWantReadError, ssl.SSLWantWriteError])
def test_decoder_drip_feed(exc: Exception, mock_socket: Mock) -> None:
    """Feed the decoder one byte at a time."""
    decoder = IncrementalDecoder()
    # Use a packet with a payload of 255 bytes to ensure we have to read
    # multiple bytes for the length varint.
    in_packet = MQTTPublishPacket(topic="topic", payload=b"x" * 255, qos=1, packet_id=66)
    data = in_packet.encode()

    # Setup mock to return one byte at a time
    for i in range(len(data) - 1):
        mock_socket.recv.side_effect = [data[i:i+1], exc]
        assert decoder.decode(mock_socket) is None

    # Final byte completes the packet
    mock_socket.recv.side_effect = [data[-1:]]
    out_packet = decoder.decode(mock_socket)
    assert out_packet == in_packet


@pytest.mark.parametrize("exc", [BlockingIOError, ssl.SSLWantReadError, ssl.SSLWantWriteError])
@pytest.mark.parametrize("closer", [b"", OSError])
@pytest.mark.parametrize("available_bytes", [n for n in range(6)])
def test_decoder_drip_partial_closures(exc: Exception, closer: bytes | Exception,
                                       available_bytes: int, mock_socket: Mock) -> None:
    """Feed the decoder partial data, then close the socket."""
    decoder = IncrementalDecoder()
    # Use a packet with a payload of 255 bytes to ensure we have to read
    # multiple bytes for the length varint.
    in_packet = MQTTPublishPacket(topic="topic", payload=b"x" * 255, qos=1, packet_id=66)
    data = in_packet.encode()

    # Send a few bytes then simulate closed socket
    if available_bytes > 0:
        mock_socket.recv.side_effect = [data[:available_bytes], exc]
    else:
        mock_socket.recv.side_effect = [exc]

    assert decoder.decode(mock_socket) is None

    mock_socket.recv.side_effect = [closer]
    with pytest.raises(ClosedSocketError):
        decoder.decode(mock_socket)


@pytest.mark.parametrize("bad_data", [
    b"\xf0\x80\x80\x80\x80",
    b"\xf0\xff\xff\xff\xff",
])
def test_decoder_bad_length(bad_data: bytes, mock_socket: Mock) -> None:
    """Feed the decoder a bad varint length."""
    decoder = IncrementalDecoder()
    mock_socket.recv.return_value = bad_data
    with pytest.raises(MQTTError, match=r"Varint overflow") as excinfo:
        decoder.decode(mock_socket)
    assert excinfo.value.reason_code == MQTTReasonCode.MalformedPacket


def test_decoder_empty_reads(mock_socket: Mock) -> None:
    """Feed the decoder empty reads."""
    decoder = IncrementalDecoder()
    # Use a packet with a payload of 255 bytes to ensure we have to read
    # multiple bytes for the length varint.
    in_packet = MQTTPublishPacket(topic="topic", payload=b"x" * 255, qos=1, packet_id=66)
    data = in_packet.encode()

    # Empty read on first byte.
    mock_socket.recv.side_effect = [b""]
    with pytest.raises(ClosedSocketError):
        decoder.decode(mock_socket)
    decoder.reset()

    # Empty read on first varint byte.
    mock_socket.recv.side_effect = [data[:1], b""]
    with pytest.raises(ClosedSocketError):
        decoder.decode(mock_socket)
    decoder.reset()

    # Empty read on second varint byte.
    mock_socket.recv.side_effect = [data[:1], data[1:2], b""]
    with pytest.raises(ClosedSocketError):
        decoder.decode(mock_socket)
    decoder.reset()

    # Empty read on first byte of content.
    mock_socket.recv.side_effect = [data[:1], data[1:2], data[2:3], b""]
    with pytest.raises(ClosedSocketError):
        decoder.decode(mock_socket)
    decoder.reset()

    # Empty reads on each content byte.
    for n in range(3, len(data)):
        mock_socket.recv.side_effect = [data[:1], data[1:2], data[2:3], data[3:n], b""]
        with pytest.raises(ClosedSocketError):
            decoder.decode(mock_socket)
        decoder.reset()

    mock_socket.recv.side_effect = [data[:1], data[1:2], data[2:3], data[3:]]
    assert decoder.decode(mock_socket) == in_packet
    decoder.reset()
