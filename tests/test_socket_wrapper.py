import socket
import time
import weakref

import pytest

from ohmqtt.socket_wrapper import CloseCallback, OpenCallback, ReadCallback, SocketWrapper


@pytest.mark.parametrize(
    "use_tls, tls_hostname", [(False, ""), (True, ""), (True, "localhost")]
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
    close_callback = mocker.Mock(spec=CloseCallback)
    open_callback = mocker.Mock(spec=OpenCallback)
    read_callback = mocker.Mock(spec=ReadCallback)
    socket_wrapper = SocketWrapper(
        "localhost",
        1883,
        close_callback,
        open_callback,
        read_callback,
        use_tls=use_tls,
        tls_hostname=tls_hostname,
        tls_context=tls_context,
    )

    socket_wrapper._sock = loop

    socket_wrapper.start()
    time.sleep(0.1)
    assert loop.connect_calls == [("localhost", 1883)]
    open_callback.assert_called_once_with(socket_wrapper)
    open_callback.reset_mock()

    loop.test_sendall(b"hello")
    time.sleep(0.1)
    read_callback.assert_called_once_with(socket_wrapper, b"hello")
    read_callback.reset_mock()

    socket_wrapper.send(b"world")
    assert loop.test_recv(512) == b"world"

    socket_wrapper.close()
    socket_wrapper.join(timeout=1.0)
    assert not socket_wrapper.is_alive()

    close_callback.assert_called_once_with(socket_wrapper)
    close_callback.reset_mock()


def test_socket_wrapper_refs():
    """Test that the SocketWrapper class does not cause circular references."""
    close_callback = lambda _: None
    open_callback = lambda _: None
    read_callback = lambda _, __: None
    socket_wrapper = SocketWrapper(
        "localhost",
        1883,
        close_callback,
        open_callback,
        read_callback,
    )

    # Expect that SocketWrapper does not have strong references
    #   to the callbacks, so they can be freed when dereferenced.
    refs = [weakref.ref(cb) for cb in (close_callback, open_callback, read_callback)]
    del [close_callback, open_callback, read_callback]
    for ref in refs:
        assert ref() is None

    # Also check for internal circular references.
    ref = weakref.ref(socket_wrapper)
    del socket_wrapper
    assert ref() is None


def test_socket_wrapper_nodelay(mocker):
    """Test that the SocketWrapper class sets TCP_NODELAY."""
    mock_socket = mocker.Mock(spec=socket.socket)
    mocker.patch("socket.socket", return_value=mock_socket)
    close_callback = mocker.Mock(spec=CloseCallback)
    open_callback = mocker.Mock(spec=OpenCallback)
    read_callback = mocker.Mock(spec=ReadCallback)
    socket_wrapper = SocketWrapper(
        "localhost",
        1883,
        close_callback,
        open_callback,
        read_callback,
        tcp_nodelay=True,
    )
    mock_socket.setsockopt.assert_called_once_with(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)


def test_socket_wrapper_ping_no_response(mocker, loopback_socket):
    """Test that the SocketWrapper class sends pings and disconnects on no receipt."""
    close_callback = mocker.Mock(spec=CloseCallback)
    open_callback = mocker.Mock(spec=OpenCallback)
    read_callback = mocker.Mock(spec=ReadCallback)
    socket_wrapper = SocketWrapper(
        "localhost",
        1883,
        close_callback,
        open_callback,
        read_callback,
        keepalive_interval=1,
    )
    socket_wrapper._sock = loopback_socket

    socket_wrapper.start()
    time.sleep(1.5)
    assert loopback_socket.test_recv(512) == b"\xc0\x00"
    socket_wrapper.join(timeout=1.5)
    assert not socket_wrapper.is_alive()
    close_callback.assert_called_once_with(socket_wrapper)
    close_callback.reset_mock()


def test_socket_wrapper_ping_no_pong(mocker, loopback_socket):
    """Test that the SocketWrapper class sends pings and disconnects on no receipt."""
    close_callback = mocker.Mock(spec=CloseCallback)
    open_callback = mocker.Mock(spec=OpenCallback)
    read_callback = mocker.Mock(spec=ReadCallback)
    socket_wrapper = SocketWrapper(
        "localhost",
        1883,
        close_callback,
        open_callback,
        read_callback,
        keepalive_interval=1,
    )
    socket_wrapper._sock = loopback_socket

    socket_wrapper.start()
    time.sleep(1)
    assert loopback_socket.test_recv(512) == b"\xc0\x00"
    loopback_socket.test_sendall(b"\x00\x00")  # Not a pong, we won't acknowledge it anyway.
    socket_wrapper.join(timeout=3.0)
    assert not socket_wrapper.is_alive()
    close_callback.assert_called_once_with(socket_wrapper)
    close_callback.reset_mock()


def test_socket_wrapper_ping_pong(mocker, loopback_socket):
    """Test that the SocketWrapper class sends pings and disconnects on no receipt."""
    close_callback = mocker.Mock(spec=CloseCallback)
    open_callback = mocker.Mock(spec=OpenCallback)
    read_callback = mocker.Mock(spec=ReadCallback)
    socket_wrapper = SocketWrapper(
        "localhost",
        1883,
        close_callback,
        open_callback,
        read_callback,
        keepalive_interval=1,
    )
    socket_wrapper._sock = loopback_socket

    socket_wrapper.start()
    time.sleep(1)
    assert loopback_socket.test_recv(512) == b"\xc0\x00"
    loopback_socket.test_sendall(b"\xd0\x00")
    socket_wrapper.pong_received()
    time.sleep(1)
    assert loopback_socket.test_recv(512) == b"\xc0\x00"
    loopback_socket.test_sendall(b"\xd0\x00")
    socket_wrapper.pong_received()
    socket_wrapper.close()
    socket_wrapper.join(timeout=3.0)
    assert not socket_wrapper.is_alive()
    close_callback.assert_called_once_with(socket_wrapper)
    close_callback.reset_mock()
