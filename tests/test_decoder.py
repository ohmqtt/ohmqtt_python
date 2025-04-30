import pytest

from ohmqtt.connection.decoder import IncrementalDecoder, ClosedSocketError
from ohmqtt.error import MQTTError
from ohmqtt.mqtt_spec import MQTTReasonCode
from ohmqtt.packet import MQTTPublishPacket


def test_decoder_drip_feed(loopback_socket):
    """Feed the decoder one byte at a time."""
    loopback_socket.setblocking(False)
    decoder = IncrementalDecoder()
    # Use a packet with a payload of 255 bytes to ensure we have to read
    # multiple bytes for the length varint.
    in_packet = MQTTPublishPacket(topic="topic", payload=b"x" * 255, qos=1, packet_id=66)
    data = in_packet.encode()
    for i in range(len(data) - 1):
        loopback_socket.test_sendall(data[i:i+1])
        assert decoder.decode(loopback_socket) is None

    loopback_socket.test_sendall(data[-1:])
    out_packet = decoder.decode(loopback_socket)
    assert out_packet == in_packet


@pytest.mark.parametrize("available_bytes", [n for n in range(0, 6)])
def test_decoder_drip_partial_closures(available_bytes, loopback_socket):
    """Feed the decoder partial data, then close the socket."""
    loopback_socket.setblocking(False)
    decoder = IncrementalDecoder()
    # Use a packet with a payload of 255 bytes to ensure we have to read
    # multiple bytes for the length varint.
    in_packet = MQTTPublishPacket(topic="topic", payload=b"x" * 255, qos=1, packet_id=66)
    data = in_packet.encode()

    # Send a few bytes.
    loopback_socket.test_sendall(data[:available_bytes])
    assert decoder.decode(loopback_socket) is None

    loopback_socket.close()
    with pytest.raises(ClosedSocketError):
        decoder.decode(loopback_socket)


def test_decoder_bad_length(loopback_socket):
    """Feed the decoder a bad varint length."""
    loopback_socket.setblocking(False)
    decoder = IncrementalDecoder()
    loopback_socket.test_sendall(b"\xf0\xff\xff\xff\xff\xff")
    try:
        decoder.decode(loopback_socket)
    except MQTTError as exc:
        assert exc.reason_code == MQTTReasonCode.MalformedPacket
    else:
        pytest.fail("Expected MQTTError")


def test_decoder_empty_reads(mocker):
    """Feed the decoder empty reads."""
    mock_socket = mocker.Mock()
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
