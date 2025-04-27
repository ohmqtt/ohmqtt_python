import socket
import time

import pytest

from ohmqtt.packet import PING, PONG
from ohmqtt.socket_wrapper import (
    SocketCloseCallback,
    SocketKeepaliveCallback,
    SocketOpenCallback,
    SocketReadCallback,
    SocketWrapper,
)


@pytest.mark.parametrize(
    "use_tls, tls_hostname", [(False, ""), (True, "localhost")]
)
def test_socket_wrapper_happy_path(mocker, loopback_socket, loopback_tls_socket, ssl_client_context, use_tls, tls_hostname):
    """Test the happy path of the SocketWrapper class."""
    if use_tls:
        tls_context = ssl_client_context(loopback_tls_socket.cert_pem)
        loopback_tls_socket.test_do_handshake()
        loop = loopback_tls_socket
    else:
        tls_context = None
        loop = loopback_socket
    mocker.patch("ohmqtt.socket_wrapper._get_socket", return_value=loop)
    close_callback = mocker.Mock(spec=SocketCloseCallback)
    keepalive_callback = mocker.Mock(spec=SocketKeepaliveCallback)
    open_callback = mocker.Mock(spec=SocketOpenCallback)
    reads = []
    read_callback = lambda sock: reads.append(sock.recv(512))
    with SocketWrapper(
        close_callback,
        keepalive_callback,
        open_callback,
        read_callback,
    ) as socket_wrapper:
        socket_wrapper.connect(
            "localhost",
            1883,
            use_tls=use_tls,
            tls_hostname=tls_hostname,
            tls_context=tls_context,
            tcp_nodelay=False,
        )
        time.sleep(0.1)
        assert loop.connect_calls == [("localhost", 1883)]
        open_callback.assert_called_once_with()
        open_callback.reset_mock()

        loop.test_sendall(b"hello")
        time.sleep(0.1)
        assert reads == [b"hello"]

        socket_wrapper.send(b"world")
        assert loop.test_recv(512) == b"world"

        socket_wrapper.disconnect()
        socket_wrapper.join(timeout=0.1)
        assert socket_wrapper.is_alive()

        close_callback.assert_called_once_with()
        close_callback.reset_mock()

    # Exiting the context manager should call shutdown.
    socket_wrapper.join(timeout=0.1)
    assert not socket_wrapper.is_alive()
    close_callback.assert_not_called()


def test_socket_wrapper_nodelay(mocker):
    """Test that the SocketWrapper class sets TCP_NODELAY."""
    mock_socket = mocker.Mock(spec=socket.socket)
    mock_socket.fileno.return_value = 1  # We can select on stdin without explosions right?
    mocker.patch("ohmqtt.socket_wrapper._get_socket", return_value=mock_socket)
    close_callback = mocker.Mock(spec=SocketCloseCallback)
    keepalive_callback = mocker.Mock(spec=SocketKeepaliveCallback)
    open_callback = mocker.Mock(spec=SocketOpenCallback)
    read_callback = mocker.Mock(spec=SocketReadCallback)
    with SocketWrapper(
        close_callback,
        keepalive_callback,
        open_callback,
        read_callback,
    ) as socket_wrapper:
        socket_wrapper.connect("localhost", 1883, tcp_nodelay=True)
        time.sleep(0.1)
        mock_socket.setsockopt.assert_called_once_with(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)


def test_socket_wrapper_ping_no_response(mocker, loopback_socket):
    """Test that the SocketWrapper class sends pings and disconnects on no receipt."""
    mocker.patch("ohmqtt.socket_wrapper._get_socket", return_value=loopback_socket)
    close_callback = mocker.Mock(spec=SocketCloseCallback)
    open_callback = mocker.Mock(spec=SocketOpenCallback)
    read_callback = mocker.Mock(spec=SocketReadCallback)
    keepalive_calls = []
    def keepalive_callback(s):
        s.send(PING)
        s.ping_sent()
        keepalive_calls.append(s)
    with SocketWrapper(
        close_callback,
        keepalive_callback,
        open_callback,
        read_callback,
    ) as socket_wrapper:
        socket_wrapper.connect("localhost", 1883, keepalive_interval=1, tcp_nodelay=False)
        assert loopback_socket.test_recv(512) == PING
        assert len(keepalive_calls) == 1
        time.sleep(1.6)
        close_callback.assert_called_once_with()
        close_callback.reset_mock()


def test_socket_wrapper_ping_no_pong(mocker, loopback_socket):
    """Test that the SocketWrapper class sends pings and disconnects on no receipt."""
    mocker.patch("ohmqtt.socket_wrapper._get_socket", return_value=loopback_socket)
    close_callback = mocker.Mock(spec=SocketCloseCallback)
    open_callback = mocker.Mock(spec=SocketOpenCallback)
    reads = []
    read_callback = lambda sock: reads.append(sock.recv(512))
    keepalive_calls = []
    def keepalive_callback(s):
        s.send(PING)
        s.ping_sent()
        keepalive_calls.append(s)
    with SocketWrapper(
        close_callback,
        keepalive_callback,
        open_callback,
        read_callback,
    ) as socket_wrapper:
        socket_wrapper.connect("localhost", 1883, keepalive_interval=1, tcp_nodelay=False)
        assert loopback_socket.test_recv(512) == PING
        assert len(keepalive_calls) == 1
        loopback_socket.test_sendall(b"\x00\x00")  # Not a pong, we won't acknowledge it anyway.
        time.sleep(0.1)
        assert reads == [b"\x00\x00"]
        time.sleep(1.6)
        close_callback.assert_called_once_with()
        close_callback.reset_mock()


def test_socket_wrapper_ping_pong(mocker, loopback_socket):
    """Test that the SocketWrapper class sends pings and handles pongs."""
    mocker.patch("ohmqtt.socket_wrapper._get_socket", return_value=loopback_socket)
    close_callback = mocker.Mock(spec=SocketCloseCallback)
    open_callback = mocker.Mock(spec=SocketOpenCallback)
    reads = []
    read_callback = lambda sock: reads.append(sock.recv(512))
    keepalive_calls = []
    def keepalive_callback(s):
        s.send(PING)
        s.ping_sent()
        keepalive_calls.append(s)
    with SocketWrapper(
        close_callback,
        keepalive_callback,
        open_callback,
        read_callback,
    ) as socket_wrapper:
        socket_wrapper.connect("localhost", 1883, keepalive_interval=1, tcp_nodelay=False)
        assert loopback_socket.test_recv(512) == PING
        assert len(keepalive_calls) == 1
        loopback_socket.test_sendall(PONG)
        time.sleep(0.1)
        assert reads == [PONG]
        socket_wrapper.pong_received()
        time.sleep(1.6)
        close_callback.assert_called_once_with()
        close_callback.reset_mock()


def test_socket_wrapper_set_keepalive_interval(mocker, loopback_socket):
    """Test that we can set the keepalive interval after starting the thread."""
    mocker.patch("ohmqtt.socket_wrapper._get_socket", return_value=loopback_socket)
    close_callback = mocker.Mock(spec=SocketCloseCallback)
    open_callback = mocker.Mock(spec=SocketOpenCallback)
    reads = []
    read_callback = lambda sock: reads.append(sock.recv(512))
    keepalive_calls = []
    def keepalive_callback(s):
        s.send(PING)
        s.ping_sent()
        keepalive_calls.append(s)
    with SocketWrapper(
        close_callback,
        keepalive_callback,
        open_callback,
        read_callback,
    ) as socket_wrapper:
        socket_wrapper.connect("localhost", 1883, tcp_nodelay=False)
        socket_wrapper.send(b"foo")  # Would be CONNECT
        assert loopback_socket.test_recv(512) == b"foo"
        loopback_socket.test_sendall(b"bar")  # Would be CONNACK
        time.sleep(0.1)
        assert reads == [b"bar"]
        socket_wrapper.set_keepalive_interval(1)
        assert loopback_socket.test_recv(512) == PING
        assert len(keepalive_calls) == 1
        time.sleep(1.6)
        close_callback.assert_called_once_with()
        close_callback.reset_mock()
