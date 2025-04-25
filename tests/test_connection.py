import logging
import pytest
import time
import weakref

from ohmqtt.connection import (
    Connection,
    ConnectionCloseCallback,
    ConnectionOpenCallback,
    ConnectionReadCallback,
)
from ohmqtt.error import MQTTError
from ohmqtt.mqtt_spec import MQTTReasonCode
from ohmqtt.packet import (
    MQTTConnAckPacket,
    MQTTPublishPacket,
    PING,
    PONG,
)
from ohmqtt.socket_wrapper import SocketWrapper


class Callbacks:
    """Container for Connection callbacks."""
    def __init__(self, mocker):
        self.close_callback = mocker.Mock(spec=ConnectionCloseCallback)
        self.open_callback = mocker.Mock(spec=ConnectionOpenCallback)
        self.read_callback = mocker.Mock(spec=ConnectionReadCallback)


@pytest.fixture
def callbacks(mocker):
    return Callbacks(mocker)


def wait_for(callback, timeout=1.0):
    """Wait for a condition to be true, or raise a TimeoutError."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        if callback():
            return
        time.sleep(0.01)
    raise TimeoutError()


def test_connection_happy_path(callbacks, mocker, loopback_socket):
    loopback_socket.setblocking(False)
    mock_socket_wrapper = mocker.Mock(spec=SocketWrapper)
    MockSocketWrapper = mocker.patch("ohmqtt.connection.SocketWrapper", return_value=mock_socket_wrapper)
    connection = Connection(
        "localhost",
        1883,
        close_callback=callbacks.close_callback,
        open_callback=callbacks.open_callback,
        read_callback=callbacks.read_callback,
        keepalive_interval=3,
        use_tls=True,
        tls_context=None,
        tls_hostname="localhost",
    )

    MockSocketWrapper.assert_called_once_with(
        "localhost",
        1883,
        close_callback=callbacks.close_callback,
        keepalive_callback=connection._keepalive_callback,
        open_callback=callbacks.open_callback,
        read_callback=connection._read_packet,
        keepalive_interval=3,
        use_tls=True,
        tls_context=None,
        tls_hostname="localhost",
    )
    mock_socket_wrapper.start.assert_called_once()

    connection._open_callback()
    callbacks.open_callback.assert_called_once_with()
    callbacks.open_callback.reset_mock()

    connection.send(b"hello")
    mock_socket_wrapper.send.assert_called_once_with(b"hello")
    mock_socket_wrapper.send.reset_mock()

    # Receiving a CONNACK
    packet = MQTTConnAckPacket(
        session_present=False,
        reason_code=MQTTReasonCode["Success"],
        properties={"ServerKeepAlive": 60},
    )
    loopback_socket.test_sendall(packet.encode())
    connection._read_packet(loopback_socket)
    callbacks.read_callback.assert_called_once_with(packet)
    callbacks.read_callback.reset_mock()
    mock_socket_wrapper.set_keepalive_interval.assert_called_once_with(60)
    mock_socket_wrapper.set_keepalive_interval.reset_mock()

    # Receiving a PINGREQ
    loopback_socket.test_sendall(PING)
    connection._read_packet(loopback_socket)
    mock_socket_wrapper.send.assert_called_once_with(PONG)
    mock_socket_wrapper.send.reset_mock()

    # Receiving a PINGRESP
    loopback_socket.test_sendall(PONG)
    connection._read_packet(loopback_socket)
    mock_socket_wrapper.pong_received.assert_called_once_with()
    mock_socket_wrapper.pong_received.reset_mock()

    # Receiving a PUBLISH
    packet = MQTTPublishPacket(
        topic="test/topic",
        payload=b"test_payload",
        qos=0,
    )
    loopback_socket.test_sendall(packet.encode())
    connection._read_packet(loopback_socket)
    callbacks.read_callback.assert_called_once_with(packet)
    callbacks.read_callback.reset_mock()

    # Time for a keepalive
    connection._keepalive_callback(mock_socket_wrapper)
    mock_socket_wrapper.send.assert_called_once_with(PING)
    mock_socket_wrapper.ping_sent.assert_called_once_with()
    mock_socket_wrapper.send.reset_mock()
    mock_socket_wrapper.ping_sent.reset_mock()

    connection.close()
    mock_socket_wrapper.close.assert_called_once_with()
    mock_socket_wrapper.close.reset_mock()
    connection._close_callback()
    callbacks.close_callback.assert_called_once_with()
    callbacks.close_callback.reset_mock()


def test_connection_partial_read(callbacks, mocker, loopback_socket):
    loopback_socket.setblocking(False)
    mock_socket_wrapper = mocker.Mock(spec=SocketWrapper)
    mocker.patch("ohmqtt.connection.SocketWrapper", return_value=mock_socket_wrapper)
    connection = Connection(
        "localhost",
        1883,
        close_callback=callbacks.close_callback,
        open_callback=callbacks.open_callback,
        read_callback=callbacks.read_callback,
    )

    packet = MQTTPublishPacket(
        topic="test/topic",
        payload=b"x" * 255,  # Length of packet must be long enough that the length varint is split.
        qos=1,
        packet_id=66,
    )
    encoded = packet.encode()
    print(str(packet))

    for n in range(1, len(encoded)):
        loopback_socket.test_sendall(encoded[:n])
        connection._read_packet(loopback_socket)
        assert not callbacks.read_callback.called
        loopback_socket.test_sendall(encoded[n:])
        connection._read_packet(loopback_socket)
        assert callbacks.read_callback.called
        recvd = callbacks.read_callback.call_args[0][0]
        print(str(recvd))
        assert recvd == packet
        callbacks.read_callback.reset_mock()


def test_connection_garbage_read(callbacks, mocker, loopback_socket):
    loopback_socket.setblocking(False)
    mock_socket_wrapper = mocker.Mock(spec=SocketWrapper)
    mocker.patch("ohmqtt.connection.SocketWrapper", return_value=mock_socket_wrapper)
    connection = Connection(
        "localhost",
        1883,
        close_callback=callbacks.close_callback,
        open_callback=callbacks.open_callback,
        read_callback=callbacks.read_callback,
    )
    encoded = b"\xff\xff\xff\xff\xffThis is not a valid MQTT packet."
    loopback_socket.test_sendall(encoded)
    with pytest.raises(MQTTError):
        connection._read_packet(loopback_socket)
