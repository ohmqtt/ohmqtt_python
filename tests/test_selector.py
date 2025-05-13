import socket
import threading

import pytest

from ohmqtt.connection.selector import InterruptibleSelector


def test_selector_protection() -> None:
    selector = InterruptibleSelector()

    with pytest.raises(RuntimeError):
        selector.select()
    with pytest.raises(RuntimeError):
        selector.interrupt()


def test_selector_interrupt(loopback_socket: socket.socket) -> None:
    selector = InterruptibleSelector()

    start = threading.Event()
    def wait_for_interrupt() -> None:
        with selector:
            selector.change_sock(loopback_socket)
            start.set()
            selector.select(read=True)

    thread = threading.Thread(target=wait_for_interrupt, daemon=True)
    thread.start()

    start.wait(1.0)
    with selector:
        selector.interrupt()

    thread.join(1.0)
    assert not thread.is_alive()


def test_selector_change_sock() -> None:
    selector = InterruptibleSelector()
    sock1 = socket.socket()
    sock2 = socket.socket()

    with selector:
        selector.change_sock(sock1)
        selector.change_sock(sock2)
        selector.change_sock(sock2)  # Redundant change, should not raise


def test_selector_no_sock() -> None:
    selector = InterruptibleSelector()

    with selector:
        with pytest.raises(RuntimeError):
            selector.select()


def test_selector_close(loopback_socket: socket.socket) -> None:
    selector = InterruptibleSelector()

    with selector:
        selector.change_sock(loopback_socket)
        selector.close()
        with pytest.raises(RuntimeError):
            selector.change_sock(loopback_socket)
        with pytest.raises(RuntimeError):
            selector.select()
        with pytest.raises(RuntimeError):
            selector.interrupt()


def test_selector_select(loopback_socket: socket.socket) -> None:
    selector = InterruptibleSelector()

    with selector:
        selector.change_sock(loopback_socket)
        assert selector.select(read=True, timeout=0.1) == (False, False)
        assert selector.select(write=True, timeout=0.1) == (False, True)
        loopback_socket.test_sendall(b"test")  # type: ignore[attr-defined]
        assert selector.select(read=True, timeout=0.1) == (True, False)
        with pytest.raises(ValueError):
            selector.select(timeout=0.1)
