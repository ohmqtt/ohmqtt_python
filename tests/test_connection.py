import os
import pytest
import socket
import ssl
import time

from ohmqtt.connection import (
    Connection,
    ConnectParams,
    ConnectionCloseCallback,
    ConnectionOpenCallback,
    ConnectionReadCallback,
)
from ohmqtt.mqtt_spec import MQTTReasonCode
from ohmqtt.packet import (
    MQTTConnectPacket,
    MQTTConnAckPacket,
    MQTTDisconnectPacket,
    MQTTPublishPacket,
    PING,
    PONG,
)


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
        time.sleep(0.001)
    raise TimeoutError()


@pytest.mark.parametrize(
    "use_tls, tls_hostname", [(False, ""), (True, "localhost")]
)
def test_connection_happy_path(callbacks, mocker, loopback_socket, loopback_tls_socket, ssl_client_context, use_tls, tls_hostname):
    """Test the happy path of the Connection class."""
    if use_tls:
        tls_context = ssl_client_context(loopback_tls_socket.cert_pem)
        loopback_tls_socket.test_do_handshake()
        loop = loopback_tls_socket
    else:
        tls_context = ssl.create_default_context()
        loop = loopback_socket
    mocker.patch("ohmqtt.connection._get_socket", return_value=loop)
    with Connection(
        callbacks.close_callback,
        callbacks.open_callback,
        callbacks.read_callback,
    ) as connection:
        connection.connect(ConnectParams(
            "localhost",
            1883,
            use_tls=use_tls,
            tls_hostname=tls_hostname,
            tls_context=tls_context,
            tcp_nodelay=False,
        ))

        time.sleep(0.1)  # Wait for potential TLS handshake.
        connect_packet = MQTTConnectPacket()
        assert loop.test_recv(512) == connect_packet.encode()
        assert loop.connect_calls == [("localhost", 1883)]

        connack_packet = MQTTConnAckPacket()
        loop.test_sendall(connack_packet.encode())

        connection.wait_for_connect(timeout=0.1)
        callbacks.open_callback.assert_called_once_with(connack_packet)
        callbacks.open_callback.reset_mock()

        # DISCONNECT
        connection.disconnect()
        disconnect_packet = MQTTDisconnectPacket()
        assert loop.test_recv(512) == disconnect_packet.encode()

        connection.join(timeout=0.1)
        assert connection.is_alive()

        callbacks.close_callback.assert_called_once_with()
        callbacks.close_callback.reset_mock()

    # Exiting the context manager should call shutdown.
    connection.join(timeout=0.1)
    assert not connection.is_alive()
    callbacks.close_callback.assert_not_called()


def test_connection_partial_read(callbacks, mocker, loopback_socket):
    loopback_socket.setblocking(False)
    mocker.patch("ohmqtt.connection._get_socket", return_value=loopback_socket)
    with Connection(
        close_callback=callbacks.close_callback,
        open_callback=callbacks.open_callback,
        read_callback=callbacks.read_callback,
    ) as connection:
        connection.connect(ConnectParams("localhost", 1883, tcp_nodelay=False))

        connect_packet = MQTTConnectPacket()
        assert loopback_socket.test_recv(512) == connect_packet.encode()

        connack_packet = MQTTConnAckPacket()
        loopback_socket.test_sendall(connack_packet.encode())
        connection.wait_for_connect(timeout=0.1)

        packet = MQTTPublishPacket(
            topic="test/topic",
            payload=b"x" * 255,  # Length of packet must be long enough that the length varint is split.
            qos=1,
            packet_id=66,
        )
        encoded = packet.encode()

        for n in range(1, len(encoded)):
            loopback_socket.test_sendall(encoded[:n])
            time.sleep(0.002)
            assert not callbacks.read_callback.called
            loopback_socket.test_sendall(encoded[n:])
            time.sleep(0.002)
            assert callbacks.read_callback.called
            recvd = callbacks.read_callback.call_args[0][0]
            assert recvd == packet
            callbacks.read_callback.reset_mock()


def test_connection_garbage_read(callbacks, mocker, loopback_socket):
    mocker.patch("ohmqtt.connection._get_socket", return_value=loopback_socket)
    with Connection(
        close_callback=callbacks.close_callback,
        open_callback=callbacks.open_callback,
        read_callback=callbacks.read_callback,
    ) as connection:
        connection.connect(ConnectParams("localhost", 1883, tcp_nodelay=False))

        connect_packet = MQTTConnectPacket()
        assert loopback_socket.test_recv(512) == connect_packet.encode()

        connack_packet = MQTTConnAckPacket()
        loopback_socket.test_sendall(connack_packet.encode())
        connection.wait_for_connect(timeout=0.1)

        encoded = b"\xff\xff\xff\xff\xffThis is not a valid MQTT packet."
        loopback_socket.test_sendall(encoded)

        # Should be closed, but not shut down.
        expected = MQTTDisconnectPacket(reason_code=MQTTReasonCode["MalformedPacket"]).encode()
        assert loopback_socket.test_recv(512) == expected
        connection.wait_for_disconnect(timeout=0.1)
        callbacks.close_callback.assert_called_once_with()
        connection.join(timeout=0.1)
        assert connection.is_alive()


def test_connection_slots(callbacks):
    connection = Connection(
        close_callback=callbacks.close_callback,
        open_callback=callbacks.open_callback,
        read_callback=callbacks.read_callback,
    )
    # Ensure that all slots are initialized in constructor.
    assert all(hasattr(connection, attr) for attr in connection.__slots__), \
        [attr for attr in connection.__slots__ if not hasattr(connection, attr)]



def test_connection_nodelay(callbacks, mocker):
    """Test that the Connection class sets TCP_NODELAY."""
    mock_socket = mocker.Mock(spec=socket.socket)
    devnull = open(os.devnull, "rb")
    mock_socket.fileno.return_value = devnull.fileno()
    mock_socket.recv.return_value = b""
    mocker.patch("ohmqtt.connection._get_socket", return_value=mock_socket)
    with Connection(
        callbacks.close_callback,
        callbacks.open_callback,
        callbacks.read_callback,
    ) as connection:
        connection.connect(ConnectParams("localhost", 1883, tcp_nodelay=True))
        time.sleep(0.1)
        mock_socket.setsockopt.assert_called_once_with(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)


def test_connection_ping_pong(callbacks, mocker, loopback_socket):
    """Test that the Connection class sends pings and handles pongs."""
    mocker.patch("ohmqtt.connection._get_socket", return_value=loopback_socket)
    with Connection(
        callbacks.close_callback,
        callbacks.open_callback,
        callbacks.read_callback,
    ) as connection:
        connection.connect(ConnectParams("localhost", 1883, keepalive_interval=1, tcp_nodelay=False))

        connect_packet = MQTTConnectPacket(keep_alive=1)
        assert loopback_socket.test_recv(512) == connect_packet.encode()

        connack_packet = MQTTConnAckPacket()
        loopback_socket.test_sendall(connack_packet.encode())
        connection.wait_for_connect(timeout=0.1)

        assert loopback_socket.test_recv(512) == PING
        loopback_socket.test_sendall(PONG)
        assert loopback_socket.test_recv(512) == PING
        time.sleep(0.9)
        callbacks.close_callback.assert_not_called()
        time.sleep(0.7)
        callbacks.close_callback.assert_called_once_with()
        callbacks.close_callback.reset_mock()
        # Should not have received a DISCONNECT packet.
        assert not loopback_socket.test_recv(512)


def test_connection_set_keepalive_interval(callbacks, mocker, loopback_socket):
    """Test that CONNACK can set the keepalive interval after starting the thread."""
    mocker.patch("ohmqtt.connection._get_socket", return_value=loopback_socket)
    with Connection(
        callbacks.close_callback,
        callbacks.open_callback,
        callbacks.read_callback,
    ) as connection:
        connection.connect(ConnectParams("localhost", 1883, tcp_nodelay=False))

        connect_packet = MQTTConnectPacket()
        assert loopback_socket.test_recv(512) == connect_packet.encode()

        connack_packet = MQTTConnAckPacket(
            session_present=False,
            reason_code=MQTTReasonCode["Success"],
            properties={"ServerKeepAlive": 1},
        )
        loopback_socket.test_sendall(connack_packet.encode())
        connection.wait_for_connect(timeout=0.1)
        callbacks.open_callback.assert_called_once_with(connack_packet)
        callbacks.open_callback.reset_mock()

        assert loopback_socket.test_recv(512) == PING
        time.sleep(1.6)
        callbacks.close_callback.assert_called_once_with()
        callbacks.close_callback.reset_mock()
        # Should not have received a DISCONNECT packet.
        assert not loopback_socket.test_recv(512)
