import socket
import time

import pytest

from ohmqtt.socket_writer import SocketWriter
from ohmqtt.socket_writer import logger as socket_writer_logger


def test_socket_writer(loopback_socket):
    loopback_socket.setblocking(False)

    # Set the socket send buffer size to a small value for testing.
    # We expect the OS may not use exactly this size, but it should be close enough for testing.
    sndbuf_sz = 2048
    loopback_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, sndbuf_sz)
    # Set the socket receive buffer size to a larger value for testing.
    rcvbuf_sz = 65535
    loopback_socket.testsock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf_sz)

    with SocketWriter(loopback_socket) as writer:
        # We should be able to send under the buffer size without blocking or deferring. 
        for n in range(1, sndbuf_sz + 1):
            data = b"X" * n
            assert writer.send(data) is True, f"Failed to send {n} bytes {sndbuf_sz=}"
            assert loopback_socket.test_recv(sndbuf_sz) == data

        # Sending a large amount of data should defer.
        data = b"X" * rcvbuf_sz
        assert writer.send(data) is False
        recvd = b""
        t0 = time.monotonic()
        while len(recvd) < len(data) and time.monotonic() - t0 < 5:
            recvd += loopback_socket.test_recv(rcvbuf_sz)
        assert recvd == data, f"{len(recvd)} != {len(data)}"

        # Back to back large sends should both defer.
        assert writer.send(data) is False
        assert writer.send(data) is False
        recvd = b""
        t0 = time.monotonic()
        while len(recvd) < len(data) * 2 and time.monotonic() - t0 < 5:
            recvd += loopback_socket.test_recv(rcvbuf_sz)
        assert recvd == data + data, f"{len(recvd)} != {len(data) * 2}"

        # Closing the writer should not flush the buffer.
        assert writer.send(data) is False
        time.sleep(0)  # Yield to the thread.
    # Now the writer is closed, and the buffer should not be flushed.
    time.sleep(0.1)  # Yield again to wait for OS to move things around.
    recvd = loopback_socket.test_recv(rcvbuf_sz)
    assert recvd == data[:len(recvd)], f"{len(recvd)} xx {len(data)}"


def test_socket_writer_blocking_socket(loopback_socket):
    loopback_socket.setblocking(True)

    with pytest.raises(AssertionError):
        SocketWriter(loopback_socket)


def test_socket_writer_error_with_callback(loopback_socket):
    loopback_socket.setblocking(False)
    loopback_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)
    callbacks = []

    with SocketWriter(loopback_socket, callbacks.append) as writer:
        assert writer.send(b"X" * 65535) is False
        # Simulate a socket error by closing the socket.
        loopback_socket.testsock.close()
        time.sleep(0.1)  # Wait before closing the writer to ensure the error is handled.
    assert len(callbacks) == 1
    assert isinstance(callbacks[0], OSError)


def test_socket_writer_error_no_callback(loopback_socket, mocker):
    mocker.patch("ohmqtt.socket_writer.logger.exception")
    loopback_socket.setblocking(False)
    loopback_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)

    with SocketWriter(loopback_socket) as writer:
        assert writer.send(b"X" * 65535) is False
        # Simulate a socket error by closing the socket.
        loopback_socket.testsock.close()
        time.sleep(0.1)  # Wait before closing the writer to ensure the error is handled.
    assert socket_writer_logger.exception.call_count == 1
    assert socket_writer_logger.exception.call_args[0][0] == "Unhandled exception in SocketWriter thread"
