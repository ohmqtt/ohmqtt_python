import os
import pytest
import socket
import ssl
import time

from ohmqtt.connection import (
    Address,
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
from ohmqtt.property import MQTTConnAckProps


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
    "address, tls_hostname", [
    ("mqtt://localhost", ""),
    ("mqtts://localhost", "localhost")
])
def test_connection_happy_path(callbacks, mocker, loopback_socket, loopback_tls_socket, ssl_client_context, address, tls_hostname):
    """Test the happy path of the Connection class."""
    addr = Address(address)
    if addr.use_tls:
        tls_context = ssl_client_context(loopback_tls_socket.cert_pem)
        loopback_tls_socket.test_do_handshake()
        loop = loopback_tls_socket
    else:
        tls_context = ssl.create_default_context()
        loop = loopback_socket
    mocker.patch("ohmqtt.connection.states._get_socket", return_value=loop)
    with Connection(
        callbacks.close_callback,
        callbacks.open_callback,
        callbacks.read_callback,
    ) as connection:
        connection.connect(ConnectParams(
            addr,
            tls_hostname=tls_hostname,
            tls_context=tls_context,
            tcp_nodelay=False,
        ))

        time.sleep(0.1)  # Wait for potential TLS handshake.
        connect_packet = MQTTConnectPacket()
        assert loop.test_recv(512) == connect_packet.encode()
        assert loop.connect_calls == [(addr.host, addr.port)]

        connack_packet = MQTTConnAckPacket()
        loop.test_sendall(connack_packet.encode())

        connection.wait_for_connect(timeout=0.1)
        callbacks.open_callback.assert_called_once_with(connack_packet)
        callbacks.open_callback.reset_mock()

        pub_packet = MQTTPublishPacket(
            topic="test/topic",
            payload=b"test payload",
            qos=0,
        )
        connection.send(pub_packet.encode())
        assert loop.test_recv(512) == pub_packet.encode()

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


def test_connection_garbage_read(callbacks, mocker, loopback_socket):
    mocker.patch("ohmqtt.connection.states._get_socket", return_value=loopback_socket)
    with Connection(
        close_callback=callbacks.close_callback,
        open_callback=callbacks.open_callback,
        read_callback=callbacks.read_callback,
    ) as connection:
        connection.connect(ConnectParams(Address("localhost"), tcp_nodelay=False))

        connect_packet = MQTTConnectPacket()
        assert loopback_socket.test_recv(512) == connect_packet.encode()

        connack_packet = MQTTConnAckPacket()
        loopback_socket.test_sendall(connack_packet.encode())
        connection.wait_for_connect(timeout=0.1)

        encoded = b"\xff\xff\xff\xff\xffThis is not a valid MQTT packet."
        loopback_socket.test_sendall(encoded)

        # Should be closed, but not shut down.
        expected = MQTTDisconnectPacket(reason_code=MQTTReasonCode.MalformedPacket).encode()
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
    mock_socket.send.return_value = 0
    mocker.patch("ohmqtt.connection.states._get_socket", return_value=mock_socket)
    with Connection(
        callbacks.close_callback,
        callbacks.open_callback,
        callbacks.read_callback,
    ) as connection:
        connection.connect(ConnectParams(Address("localhost"), tcp_nodelay=True))
        time.sleep(0.1)
        mock_socket.setsockopt.assert_called_once_with(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)


def test_connection_ping_pong(callbacks, mocker, loopback_socket):
    """Test that the Connection class sends pings and handles pongs."""
    mocker.patch("ohmqtt.connection.states._get_socket", return_value=loopback_socket)
    with Connection(
        callbacks.close_callback,
        callbacks.open_callback,
        callbacks.read_callback,
    ) as connection:
        connection.connect(ConnectParams(Address("localhost"), keepalive_interval=1, tcp_nodelay=False))

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
    mocker.patch("ohmqtt.connection.states._get_socket", return_value=loopback_socket)
    with Connection(
        callbacks.close_callback,
        callbacks.open_callback,
        callbacks.read_callback,
    ) as connection:
        connection.connect(ConnectParams(Address("localhost"), tcp_nodelay=False))

        connect_packet = MQTTConnectPacket()
        assert loopback_socket.test_recv(512) == connect_packet.encode()

        connack_packet = MQTTConnAckPacket(
            session_present=False,
            reason_code=MQTTReasonCode.Success,
            properties=MQTTConnAckProps(ServerKeepAlive=1),
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


def test_connection_reconnect(callbacks, mocker, loopback_socket):
    mocker.patch("ohmqtt.connection.states._get_socket", return_value=loopback_socket)
    with Connection(
        callbacks.close_callback,
        callbacks.open_callback,
        callbacks.read_callback,
    ) as connection:
        connection.connect(ConnectParams(Address("localhost"), tcp_nodelay=False, keepalive_interval=1, reconnect_delay=0.5))

        connect_packet = MQTTConnectPacket(keep_alive=1)
        assert loopback_socket.test_recv(512) == connect_packet.encode()
        assert loopback_socket.connect_calls == [("localhost", 1883)]
        loopback_socket.connect_calls.clear()

        connack_packet = MQTTConnAckPacket(
            session_present=False,
            reason_code=MQTTReasonCode.Success,
        )
        loopback_socket.test_sendall(connack_packet.encode())
        connection.wait_for_connect(timeout=0.1)
        callbacks.open_callback.assert_called_once_with(connack_packet)
        callbacks.open_callback.reset_mock()

        # Simulate a disconnect.
        disconnect_packet = MQTTDisconnectPacket()
        loopback_socket.test_sendall(disconnect_packet.encode())
        time.sleep(0.1)
        loopback_socket.test_close()
        connection.wait_for_disconnect(timeout=2.0)  # Should close on keepalive timeout.
        loopback_socket.reset()

        assert loopback_socket.test_recv(512) == connect_packet.encode()
        assert loopback_socket.connect_calls == [("localhost", 1883)]
        loopback_socket.connect_calls.clear()

        connack_packet = MQTTConnAckPacket(
            session_present=False,
            reason_code=MQTTReasonCode.Success,
        )
        loopback_socket.test_sendall(connack_packet.encode())
        connection.wait_for_connect(timeout=0.1)
        callbacks.open_callback.assert_called_once_with(connack_packet)
        callbacks.open_callback.reset_mock()
