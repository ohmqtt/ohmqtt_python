import socket
import time

import pytest

from ohmqtt.socket_wrapper import CloseCallback, OpenCallback, ReadCallback, SocketWrapper


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
    mocker.patch("socket.socket", return_value=loop)
    close_callback = mocker.Mock(spec=CloseCallback)
    open_callback = mocker.Mock(spec=OpenCallback)
    read_callback = mocker.Mock(spec=ReadCallback)
    context = ssl_client_context(loopback_tls_socket.cert_pem)
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
    assert loop.test_recv(5) == b"world"

    socket_wrapper.close()
    socket_wrapper.join(timeout=1.0)
    assert not socket_wrapper.is_alive()

    close_callback.assert_called_once_with(socket_wrapper)
    close_callback.reset_mock()
