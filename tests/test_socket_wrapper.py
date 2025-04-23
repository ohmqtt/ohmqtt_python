import socket
import time
import weakref

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
    close_callback = mocker.Mock(spec=SocketCloseCallback)
    keepalive_callback = mocker.Mock(spec=SocketKeepaliveCallback)
    open_callback = mocker.Mock(spec=SocketOpenCallback)
    read_callback = mocker.Mock(spec=SocketReadCallback)
    socket_wrapper = SocketWrapper(
        "localhost",
        1883,
        close_callback,
        keepalive_callback,
        open_callback,
        read_callback,
        use_tls=use_tls,
        tls_hostname=tls_hostname,
        tls_context=tls_context,
    )

    socket_wrapper.sock = loop

    socket_wrapper.start()
    time.sleep(0.1)
    assert loop.connect_calls == [("localhost", 1883)]
    open_callback.assert_called_once_with()
    open_callback.reset_mock()

    loop.test_sendall(b"hello")
    time.sleep(0.1)
    read_callback.assert_called_once_with(b"hello")
    read_callback.reset_mock()

    socket_wrapper.send(b"world")
    assert loop.test_recv(512) == b"world"

    socket_wrapper.close()
    socket_wrapper.join(timeout=1.0)
    assert not socket_wrapper.is_alive()

    close_callback.assert_called_once_with()
    close_callback.reset_mock()


def test_socket_wrapper_refs(loopback_socket):
    """Test that the SocketWrapper class does not have internal circular references."""
    close_callback = lambda: None
    keepalive_callback = lambda _: None
    open_callback = lambda: None
    read_callback = lambda _: None
    socket_wrapper = SocketWrapper(
        "localhost",
        1883,
        close_callback,
        keepalive_callback,
        open_callback,
        read_callback,
    )
    socket_wrapper.sock = loopback_socket
    socket_wrapper.start()
    time.sleep(0.1)
    socket_wrapper.close()
    socket_wrapper.join(timeout=1.0)
    assert not socket_wrapper.is_alive()

    ref = weakref.ref(socket_wrapper)
    del socket_wrapper
    assert ref() is None


def test_socket_wrapper_nodelay(mocker):
    """Test that the SocketWrapper class sets TCP_NODELAY."""
    mock_socket = mocker.Mock(spec=socket.socket)
    mocker.patch("socket.socket", return_value=mock_socket)
    close_callback = mocker.Mock(spec=SocketCloseCallback)
    keepalive_callback = mocker.Mock(spec=SocketKeepaliveCallback)
    open_callback = mocker.Mock(spec=SocketOpenCallback)
    read_callback = mocker.Mock(spec=SocketReadCallback)
    socket_wrapper = SocketWrapper(
        "localhost",
        1883,
        close_callback,
        keepalive_callback,
        open_callback,
        read_callback,
        tcp_nodelay=True,
    )
    mock_socket.setsockopt.assert_called_once_with(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)


def test_socket_wrapper_ping_no_response(mocker, loopback_socket):
    """Test that the SocketWrapper class sends pings and disconnects on no receipt."""
    close_callback = mocker.Mock(spec=SocketCloseCallback)
    open_callback = mocker.Mock(spec=SocketOpenCallback)
    read_callback = mocker.Mock(spec=SocketReadCallback)
    keepalive_calls = []
    def keepalive_callback(s):
        s.send(PING)
        s.ping_sent()
        keepalive_calls.append(s)
    socket_wrapper = SocketWrapper(
        "localhost",
        1883,
        close_callback,
        keepalive_callback,
        open_callback,
        read_callback,
        keepalive_interval=1,
    )
    socket_wrapper.sock = loopback_socket

    socket_wrapper.start()
    time.sleep(1.1)
    assert len(keepalive_calls) == 1
    socket_wrapper.join(timeout=1.6)
    assert not socket_wrapper.is_alive()
    close_callback.assert_called_once_with()
    close_callback.reset_mock()


def test_socket_wrapper_ping_no_pong(mocker, loopback_socket):
    """Test that the SocketWrapper class sends pings and disconnects on no receipt."""
    close_callback = mocker.Mock(spec=SocketCloseCallback)
    open_callback = mocker.Mock(spec=SocketOpenCallback)
    read_callback = mocker.Mock(spec=SocketReadCallback)
    keepalive_calls = []
    def keepalive_callback(s):
        s.send(PING)
        s.ping_sent()
        keepalive_calls.append(s)
    socket_wrapper = SocketWrapper(
        "localhost",
        1883,
        close_callback,
        keepalive_callback,
        open_callback,
        read_callback,
        keepalive_interval=1,
    )
    socket_wrapper.sock = loopback_socket

    socket_wrapper.start()
    time.sleep(1.1)
    assert len(keepalive_calls) == 1
    loopback_socket.test_sendall(b"\x00\x00")  # Not a pong, we won't acknowledge it anyway.
    time.sleep(0.1)
    read_callback.assert_called_once_with(b"\x00\x00")
    read_callback.reset_mock()
    socket_wrapper.join(timeout=2.0)
    assert not socket_wrapper.is_alive()
    close_callback.assert_called_once_with()
    close_callback.reset_mock()


def test_socket_wrapper_ping_pong(mocker, loopback_socket):
    """Test that the SocketWrapper class sends pings and handles pongs."""
    close_callback = mocker.Mock(spec=SocketCloseCallback)
    open_callback = mocker.Mock(spec=SocketOpenCallback)
    read_callback = mocker.Mock(spec=SocketReadCallback)
    keepalive_calls = []
    def keepalive_callback(s):
        s.send(PING)
        s.ping_sent()
        keepalive_calls.append(s)
    socket_wrapper = SocketWrapper(
        "localhost",
        1883,
        close_callback,
        keepalive_callback,
        open_callback,
        read_callback,
        keepalive_interval=1,
    )
    socket_wrapper.sock = loopback_socket

    socket_wrapper.start()
    time.sleep(1.1)
    assert len(keepalive_calls) == 1
    loopback_socket.test_sendall(PONG)
    time.sleep(0.1)
    read_callback.assert_called_once_with(PONG)
    read_callback.reset_mock()
    socket_wrapper.pong_received()
    socket_wrapper.join(timeout=2.0)
    assert not socket_wrapper.is_alive()
    close_callback.assert_called_once_with()
    close_callback.reset_mock()


def test_socket_wrapper_set_keepalive_interval(mocker, loopback_socket):
    """Test that we can set the keepalive interval after starting the thread."""
    close_callback = mocker.Mock(spec=SocketCloseCallback)
    open_callback = mocker.Mock(spec=SocketOpenCallback)
    read_callback = mocker.Mock(spec=SocketReadCallback)
    keepalive_calls = []
    def keepalive_callback(s):
        s.send(PING)
        s.ping_sent()
        keepalive_calls.append(s)
    socket_wrapper = SocketWrapper(
        "localhost",
        1883,
        close_callback,
        keepalive_callback,
        open_callback,
        read_callback,
    )
    socket_wrapper.sock = loopback_socket

    socket_wrapper.start()
    time.sleep(1.1)
    socket_wrapper.send(b"foo")  # Would be CONNECT
    loopback_socket.test_sendall(b"bar")  # Would be CONNACK
    socket_wrapper.set_keepalive_interval(1)
    time.sleep(1.1)
    assert len(keepalive_calls) == 1
    socket_wrapper.join(timeout=1.6)
    assert not socket_wrapper.is_alive()
    close_callback.assert_called_once_with()
    close_callback.reset_mock()
