import pytest
import socket
import time

from ohmqtt.connection import Connection
from ohmqtt.mqtt_spec import MQTTReasonCode
from ohmqtt.packet import (
    MQTTConnectPacket,
    MQTTConnAckPacket,
    MQTTPublishPacket,
    MQTTPingReqPacket,
    MQTTPingRespPacket,
    MQTTDisconnectPacket,
)
from ohmqtt.property import MQTTPropertyDict


class Callbacks:
    """Container for Connection callbacks."""
    def __init__(self):
        self.close_calls = []
        self.connect_calls = []
        self.read_calls = []

    def close_cb(self, conn, exc):
        self.close_calls.append((conn, exc))

    def connect_cb(self, conn):
        self.connect_calls.append(conn)

    def read_cb(self, conn, packet):
        self.read_calls.append((conn, packet))


@pytest.fixture
def callbacks():
    return Callbacks()


def wait_for(callback, timeout=1.0):
    """Wait for a condition to be true, or raise a TimeoutError."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        if callback():
            return
        time.sleep(0.01)
    raise TimeoutError()


def test_connection_connect(callbacks, mocker, loopback_socket):
    mocker.patch("socket.create_connection", return_value=loopback_socket)
    with Connection(
        "localhost",
        1883,
        0,
        callbacks.close_cb,
        callbacks.connect_cb,
        callbacks.read_cb,
    ) as conn:
        wait_for(lambda: len(callbacks.connect_calls) > 0)
        assert len(callbacks.connect_calls) == 1
        assert callbacks.connect_calls[0] == conn

        connect_packet = MQTTConnectPacket("foo", 1)
        conn.send(connect_packet.encode())

        recvd = loopback_socket.test_recv(1024)
        assert recvd == connect_packet.encode()

        connack_packet = MQTTConnAckPacket()
        loopback_socket.test_sendall(connack_packet.encode())

        wait_for(lambda: len(callbacks.read_calls) > 0)
        assert len(callbacks.read_calls) == 1
        assert callbacks.read_calls[0][0] == conn
        assert callbacks.read_calls[0][1] == connack_packet

    assert len(callbacks.close_calls) == 1
    assert callbacks.close_calls[0][0] == conn
    assert callbacks.close_calls[0][1] is None
    assert socket.create_connection.call_count == 1
    assert socket.create_connection.call_args[0] == (("localhost", 1883),)


def test_connection_keepalive(callbacks, mocker, loopback_socket):
    # We must periodically send PINGREQ packets to the server to keep the connection alive.
    mocker.patch("socket.create_connection", return_value=loopback_socket)
    with Connection(
        "localhost",
        1883,
        1,
        callbacks.close_cb,
        callbacks.connect_cb,
        callbacks.read_cb,
    ) as conn:
        wait_for(lambda: len(callbacks.connect_calls) > 0)

        for n in range(2):
            t0 = time.monotonic()
            recvd = loopback_socket.test_recv(1024)
            assert time.monotonic() - t0 < 1.5
            assert recvd == MQTTPingReqPacket().encode()

            loopback_socket.test_sendall(MQTTPingRespPacket().encode())
    assert len(callbacks.close_calls) == 1
    assert callbacks.close_calls[0][1] is None


def test_connection_server_keepalive(callbacks, mocker, loopback_socket):
    # The client must use the server's keepalive interval as specified in the CONNACK packet.
    mocker.patch("socket.create_connection", return_value=loopback_socket)
    with Connection(
        "localhost",
        1883,
        0,
        callbacks.close_cb,
        callbacks.connect_cb,
        callbacks.read_cb,
    ) as conn:
        wait_for(lambda: len(callbacks.connect_calls) > 0)

        conn.send(MQTTConnectPacket().encode())
        recvd = loopback_socket.test_recv(1024)
        loopback_socket.test_sendall(MQTTConnAckPacket(properties={"ServerKeepAlive": 1}).encode())
        wait_for(lambda: len(callbacks.read_calls) > 0)

        t0 = time.monotonic()
        recvd = loopback_socket.test_recv(1024)
        assert time.monotonic() - t0 < 1.5
        assert recvd == b"\xc0\x00"

        loopback_socket.test_sendall(b"\xd0\x00")
    assert len(callbacks.close_calls) == 1
    assert callbacks.close_calls[0][1] is None


def test_connection_server_ping(callbacks, mocker, loopback_socket):
    # The server sends a PINGREQ, we respond with PINGRESP.
    mocker.patch("socket.create_connection", return_value=loopback_socket)
    with Connection(
        "localhost",
        1883,
        0,
        callbacks.close_cb,
        callbacks.connect_cb,
        callbacks.read_cb,
    ) as conn:
        wait_for(lambda: len(callbacks.connect_calls) > 0)

        conn.send(MQTTConnectPacket().encode())
        recvd = loopback_socket.test_recv(1024)
        loopback_socket.test_sendall(MQTTConnAckPacket().encode())
        wait_for(lambda: len(callbacks.read_calls) > 0)

        t0 = time.monotonic()
        loopback_socket.test_sendall(MQTTPingReqPacket().encode())
        recvd = loopback_socket.test_recv(1024)
        assert time.monotonic() - t0 < 0.5
        assert recvd == MQTTPingRespPacket().encode()


def test_connection_recv_timeout(callbacks, mocker, loopback_socket):
    # If the server does not send any data within a factor of the keepalive interval,
    #   we should close the connection.
    mocker.patch("socket.create_connection", return_value=loopback_socket)
    with Connection(
        "localhost",
        1883,
        1,
        callbacks.close_cb,
        callbacks.connect_cb,
        callbacks.read_cb,
    ) as conn:
        wait_for(lambda: len(callbacks.connect_calls) > 0)
        t0 = time.monotonic()
        wait_for(lambda: len(callbacks.close_calls) > 0, timeout=1.75)
        assert time.monotonic() - t0 > 1.25
    assert len(callbacks.close_calls) == 1
    assert callbacks.close_calls[0][1].reason_code == MQTTReasonCode.KeepAliveTimeout


def test_connection_ping_timeout(callbacks, mocker, loopback_socket):
    # If server does not respond to PINGREQ in time, but does send other data, we should close the connection.
    mocker.patch("socket.create_connection", return_value=loopback_socket)
    with Connection(
        "localhost",
        1883,
        1,
        callbacks.close_cb,
        callbacks.connect_cb,
        callbacks.read_cb,
    ) as conn:
        wait_for(lambda: len(callbacks.connect_calls) > 0)

        conn.send(MQTTConnectPacket().encode())
        recvd = loopback_socket.test_recv(1024)
        loopback_socket.test_sendall(MQTTConnAckPacket().encode())
        wait_for(lambda: len(callbacks.read_calls) > 0)

        t0 = time.monotonic()
        time.sleep(0.75)
        loopback_socket.test_sendall(MQTTPublishPacket("foo", b"bar").encode())
        time.sleep(0.75)
        loopback_socket.test_sendall(MQTTPublishPacket("foo", b"bar").encode())

        wait_for(lambda: len(callbacks.close_calls) > 0, timeout=1.75)
        assert time.monotonic() - t0 > 1.25
    assert len(callbacks.close_calls) == 1
    assert callbacks.close_calls[0][1].reason_code == MQTTReasonCode.KeepAliveTimeout


def test_connection_recv_partial(callbacks, mocker, loopback_socket):
    # We must be able to reconstruct a partial packet, split at any point in the frame.
    mocker.patch("socket.create_connection", return_value=loopback_socket)
    with Connection(
        "localhost",
        1883,
        0,
        callbacks.close_cb,
        callbacks.connect_cb,
        callbacks.read_cb,
    ) as conn:
        wait_for(lambda: len(callbacks.connect_calls) > 0)

        conn.send(MQTTConnectPacket().encode())
        recvd = loopback_socket.test_recv(1024)
        loopback_socket.test_sendall(MQTTConnAckPacket().encode())
        wait_for(lambda: len(callbacks.read_calls) > 0)

        # Make a packet with a size of at least 128 bytes,
        # so we can test a fixed header split across two reads.
        pub_packet = MQTTPublishPacket("foo", b"z" * 192)
        encoded = pub_packet.encode()

        for n in range(1, len(encoded) - 1):
            callbacks.read_calls.clear()
            loopback_socket.test_sendall(encoded[:n])
            time.sleep(0.01)
            loopback_socket.test_sendall(encoded[n:])
            wait_for(lambda: len(callbacks.read_calls) > 0)
            assert len(callbacks.read_calls) == 1
            assert callbacks.read_calls[0][0] == conn
            assert callbacks.read_calls[0][1] == pub_packet
            time.sleep(0.01)


def test_connection_bad_length(callbacks, mocker, loopback_socket):
    # Handle a packet with a bad length in the fixed header.
    mocker.patch("socket.create_connection", return_value=loopback_socket)
    with Connection(
        "localhost",
        1883,
        0,
        callbacks.close_cb,
        callbacks.connect_cb,
        callbacks.read_cb,
    ) as conn:
        wait_for(lambda: len(callbacks.connect_calls) > 0)

        conn.send(MQTTConnectPacket().encode())
        recvd = loopback_socket.test_recv(1024)
        loopback_socket.test_sendall(MQTTConnAckPacket().encode())
        wait_for(lambda: len(callbacks.read_calls) > 0)

        loopback_socket.test_sendall(bytes.fromhex("30ffffffffff"))
        recvd = loopback_socket.test_recv(1024)
        assert recvd == MQTTDisconnectPacket(reason_code=MQTTReasonCode.MalformedPacket).encode()
        wait_for(lambda: len(callbacks.close_calls) > 0)
    assert len(callbacks.close_calls) == 1
    assert callbacks.close_calls[0][1].reason_code == MQTTReasonCode.MalformedPacket


def test_connection_error_disconnect(callbacks, mocker, loopback_socket):
    # Send DISCONNECT to the server and disconnect when a bad packet is received.
    mocker.patch("socket.create_connection", return_value=loopback_socket)
    with Connection(
        "localhost",
        1883,
        0,
        callbacks.close_cb,
        callbacks.connect_cb,
        callbacks.read_cb,
    ) as conn:
        wait_for(lambda: len(callbacks.connect_calls) > 0)

        loopback_socket.test_sendall(b"\xff\xff\xff\xff\xff")
        recvd = loopback_socket.test_recv(1024)
        assert recvd == MQTTDisconnectPacket(reason_code=MQTTReasonCode.MalformedPacket).encode()
        wait_for(lambda: len(callbacks.close_calls) > 0)
    assert len(callbacks.close_calls) == 1
    assert callbacks.close_calls[0][1].reason_code == MQTTReasonCode.MalformedPacket


def test_connection_pre_send(callbacks, mocker):
    # Sending a packet before the connection is established should raise an error.
    mocker.patch("socket.create_connection")
    conn = Connection(
        "localhost",
        1883,
        1,
        callbacks.close_cb,
        callbacks.connect_cb,
        callbacks.read_cb,
    )
    with pytest.raises(RuntimeError):
        conn.send(b"foo")


def test_connection_remote_close(callbacks, mocker, loopback_socket):
    # Remote host closes the connection cleanly without sending DISCONNECT.
    mocker.patch("socket.create_connection", return_value=loopback_socket)
    with Connection(
        "localhost",
        1883,
        0,
        callbacks.close_cb,
        callbacks.connect_cb,
        callbacks.read_cb,
    ) as conn:
        wait_for(lambda: len(callbacks.connect_calls) > 0)

        conn.send(MQTTConnectPacket().encode())
        loopback_socket.test_recv(1024)
        loopback_socket.test_sendall(MQTTConnAckPacket().encode())
        wait_for(lambda: len(callbacks.read_calls) > 0)

        loopback_socket.test_shutdown(socket.SHUT_RDWR)
        wait_for(lambda: len(callbacks.close_calls) > 0)
    assert len(callbacks.close_calls) == 1
    assert callbacks.close_calls[0][1] is None


def test_connection_disconnect_error(callbacks, mocker, loopback_socket):
    # The niche case of an error while sending DISCONNECT to the broker.
    # We simulate this by sending a malformed packet then immediately shutting down the socket.
    mocker.patch("socket.create_connection", return_value=loopback_socket)
    with Connection(
        "localhost",
        1883,
        0,
        callbacks.close_cb,
        callbacks.connect_cb,
        callbacks.read_cb,
    ) as conn:
        wait_for(lambda: len(callbacks.connect_calls) > 0)

        conn.send(MQTTConnectPacket().encode())
        loopback_socket.test_recv(1024)
        time.sleep(0)  # Yield the thread before this next bit.
        loopback_socket.test_sendall(b"\xff\xff\xff\xff\xff\xff\xff")

        loopback_socket.test_close()
        wait_for(lambda: len(callbacks.close_calls) > 0)
        assert callbacks.close_calls[0][1].reason_code == MQTTReasonCode.MalformedPacket


def test_connection_write_error(callbacks, mocker, loopback_socket):
    # Connection should close upon a write error.
    mocker.patch("socket.create_connection", return_value=loopback_socket)
    with Connection(
        "localhost",
        1883,
        0,
        callbacks.close_cb,
        callbacks.connect_cb,
        callbacks.read_cb,
    ) as conn:
        wait_for(lambda: len(callbacks.connect_calls) > 0)

        conn.send(MQTTConnectPacket().encode())
        conn.handle_write_error(OSError("Test error"))
        wait_for(lambda: len(callbacks.close_calls) > 0)
    assert len(callbacks.close_calls) == 1
    assert isinstance(callbacks.close_calls[0][1], OSError)
    assert callbacks.close_calls[0][1].args[0] == "Test error"


def test_connection_connect_error(callbacks, mocker):
    # Handling socket errors during connection setup.
    mocker.patch("socket.create_connection", side_effect=socket.error)
    with Connection(
        "localhost",
        1883,
        0,
        callbacks.close_cb,
        callbacks.connect_cb,
        callbacks.read_cb,
    ) as conn:
        pass
    assert len(callbacks.close_calls) == 1
    assert isinstance(callbacks.close_calls[0][1], socket.error)
    assert socket.create_connection.call_count == 1
    assert socket.create_connection.call_args[0] == (("localhost", 1883),)


def test_connection_tls_connect(callbacks, mocker, loopback_tls_socket, ssl_client_context):
    # Test connecting with a TLS socket and exchanging packets.
    mocker.patch("socket.create_connection", return_value=loopback_tls_socket)
    context = ssl_client_context(loopback_tls_socket.cert_pem)
    with Connection(
        "localhost",
        8883,
        0,
        callbacks.close_cb,
        callbacks.connect_cb,
        callbacks.read_cb,
        tls=True,
        tls_context=context,
        tls_hostname="localhost",
    ) as conn:
        loopback_tls_socket.test_do_handshake()
        wait_for(lambda: len(callbacks.connect_calls) > 0)
        assert len(callbacks.connect_calls) == 1
        assert callbacks.connect_calls[0] == conn

        connect_packet = MQTTConnectPacket("foo", 1)
        conn.send(connect_packet.encode())

        recvd = loopback_tls_socket.test_recv(1024)
        assert recvd == connect_packet.encode()

        connack_packet = MQTTConnAckPacket()
        loopback_tls_socket.test_sendall(connack_packet.encode())

        wait_for(lambda: len(callbacks.read_calls) > 0)
        assert len(callbacks.read_calls) == 1
        assert callbacks.read_calls[0][0] == conn
        assert callbacks.read_calls[0][1] == connack_packet

    assert len(callbacks.close_calls) == 1
    assert callbacks.close_calls[0][0] == conn
    assert callbacks.close_calls[0][1] is None
    assert socket.create_connection.call_count == 1
    assert socket.create_connection.call_args[0] == (("localhost", 8883),)


def test_connection_tls_handshake_close(callbacks, mocker, loopback_tls_socket, ssl_client_context):
    # Edge case of closing the connection during the handshake.
    mocker.patch("socket.create_connection", return_value=loopback_tls_socket)
    context = ssl_client_context(loopback_tls_socket.cert_pem)
    with Connection(
        "localhost",
        8883,
        0,
        callbacks.close_cb,
        callbacks.connect_cb,
        callbacks.read_cb,
        tls=True,
        tls_context=context,
        tls_hostname="localhost",
    ) as conn:
        pass

    assert len(callbacks.close_calls) == 1
    assert callbacks.close_calls[0][0] == conn
    assert callbacks.close_calls[0][1] is None
