import io
import socket
import ssl
from unittest.mock import Mock

import pytest
from pytest_mock import MockerFixture

from ohmqtt.connection.decoder import IncrementalDecoder, ClosedSocketError
from ohmqtt.error import MQTTError
from ohmqtt.mqtt_spec import MQTTReasonCode, MQTTQoS
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
    in_packet = MQTTPublishPacket(topic="topic", payload=b"x" * 255, qos=MQTTQoS.Q1, packet_id=66)
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
    in_packet = MQTTPublishPacket(topic="topic", payload=b"x" * 255, qos=MQTTQoS.Q1, packet_id=66)
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


@pytest.mark.parametrize("payload_size", [2_097_152, 3_000_000])
def test_decoder_four_byte_length(payload_size: int, mock_socket: Mock) -> None:
    decoder = IncrementalDecoder()
    in_packet = MQTTPublishPacket(topic="topic", payload=b"x" * payload_size, qos=MQTTQoS.Q1, packet_id=66)
    data = in_packet.encode()
    assert data[4] < 0x80 <= data[3]
    mock_socket.recv.side_effect = io.BytesIO(data).read
    assert decoder.decode(mock_socket) == in_packet


@pytest.mark.parametrize("raw", [
    pytest.param(b"\xd0\x80\x00", id="zero_in_two_bytes"),
    pytest.param(b"\xd0\x80\x80\x00", id="zero_in_three_bytes"),
    pytest.param(b"\x30\xff\x00", id="127_in_two_bytes"),
])
def test_decoder_non_minimal_length(raw: bytes, loopback_socket: socket.socket) -> None:
    decoder = IncrementalDecoder()
    loopback_socket.setblocking(False)
    loopback_socket.test_sendall(raw)  # type: ignore[attr-defined]
    with pytest.raises(MQTTError, match="not minimally encoded") as excinfo:
        decoder.decode(loopback_socket)
    assert excinfo.value.reason_code == MQTTReasonCode.MalformedPacket


def test_decoder_empty_reads(mock_socket: Mock) -> None:
    """Feed the decoder empty reads."""
    decoder = IncrementalDecoder()
    # Use a packet with a payload of 255 bytes to ensure we have to read
    # multiple bytes for the length varint.
    in_packet = MQTTPublishPacket(topic="topic", payload=b"x" * 255, qos=MQTTQoS.Q1, packet_id=66)
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


@pytest.mark.parametrize("bad_packet", [
    pytest.param(b"\x40\x04\x00\x01\x00\x05", id="puback_bad_props_length"),
    pytest.param(b"\x90\x03\x00\x01\x05", id="suback_bad_props_length"),
    pytest.param(b"\xb0\x01\x00", id="unsuback_truncated"),
    pytest.param(b"\xe0\x02\x00\x05", id="disconnect_bad_props_length"),
    pytest.param(b"\x20\x01\x00", id="connack_truncated"),
    pytest.param(b"\x40\x0b\x00\x01\x00\x07\x26\x00\x01k\x00\x05a", id="puback_truncated_user_property"),
])
def test_decoder_malformed_packet(bad_packet: bytes, loopback_socket: socket.socket) -> None:
    decoder = IncrementalDecoder()
    good_packet = MQTTPublishPacket(topic="topic", payload=b"payload")
    loopback_socket.test_sendall(bad_packet + good_packet.encode())  # type: ignore[attr-defined]
    with pytest.raises(MQTTError) as excinfo:
        decoder.decode(loopback_socket)
    assert excinfo.value.reason_code == MQTTReasonCode.MalformedPacket
    assert decoder.decode(loopback_socket) == good_packet


def test_decoder_readonly_view(mocker: MockerFixture, loopback_socket: socket.socket) -> None:
    decoder = IncrementalDecoder()
    packet = MQTTPublishPacket(topic="topic", payload=b"payload")
    checked: list[bool] = []

    def decode_packet_from_parts(head: int, data: memoryview) -> MQTTPublishPacket:
        assert isinstance(data, memoryview)
        with pytest.raises(TypeError):
            data[0] = 0
        checked.append(True)
        return packet

    mocker.patch("ohmqtt.connection.decoder.decode_packet_from_parts", side_effect=decode_packet_from_parts)
    loopback_socket.test_sendall(packet.encode())  # type: ignore[attr-defined]
    assert decoder.decode(loopback_socket) == packet
    assert checked == [True]


@pytest.mark.parametrize("payload_size", [0, 200, 20000])
def test_decoder_max_packet_size(payload_size: int, mock_socket: Mock) -> None:
    data = MQTTPublishPacket(topic="topic", payload=b"x" * payload_size).encode()
    header_size = data.index(b"\x00\x05topic")

    decoder = IncrementalDecoder()
    mock_socket.recv.side_effect = [data[i:i + 1] for i in range(header_size)] + [data[header_size:]]
    assert decoder.decode(mock_socket, len(data)) is not None

    # The packet must be refused before reading the rest of it.
    decoder = IncrementalDecoder()
    mock_socket.recv.reset_mock()
    mock_socket.recv.side_effect = [data[i:i + 1] for i in range(header_size)]
    with pytest.raises(MQTTError) as excinfo:
        decoder.decode(mock_socket, len(data) - 1)
    assert excinfo.value.reason_code == MQTTReasonCode.PacketTooLarge
    assert mock_socket.recv.call_count == header_size
