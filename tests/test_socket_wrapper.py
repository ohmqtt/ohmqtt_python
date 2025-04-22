import socket
import time

import pytest

from ohmqtt.socket_wrapper import OpenCallback, ReadCallback, SocketWrapper


def test_socket_wrapper_happy_path(mocker, loopback_socket):
    """Test the happy path of the SocketWrapper class."""
    mocker.patch("socket.socket", return_value=loopback_socket)
    open_callback = mocker.Mock(spec=OpenCallback)
    read_callback = mocker.Mock(spec=ReadCallback)
    socket_wrapper = SocketWrapper("localhost", 1883, open_callback, read_callback)

    socket_wrapper.start()
    time.sleep(0.1)
    assert loopback_socket.connect_calls == [("localhost", 1883)]
    open_callback.assert_called_once_with(socket_wrapper)
    open_callback.reset_mock()

    loopback_socket.test_sendall(b"hello")
    time.sleep(0.1)
    read_callback.assert_called_once_with(socket_wrapper, b"hello")
    read_callback.reset_mock()

    socket_wrapper.send(b"world")
    assert loopback_socket.test_recv(5) == b"world"

    socket_wrapper.close()
    socket_wrapper.join(timeout=1.0)
    assert not socket_wrapper.is_alive()
