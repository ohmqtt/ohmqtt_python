import threading

import pytest

from ohmqtt.connection.selector import InterruptibleSelector


def test_selector_protection():
    selector = InterruptibleSelector()

    with pytest.raises(RuntimeError):
        selector.select([], [], [], None)
    with pytest.raises(RuntimeError):
        selector.interrupt()


def test_selector_interrupt():
    selector = InterruptibleSelector()

    start = threading.Event()
    def wait_for_interrupt():
        with selector:
            start.set()
            selector.select([], [], [], None)

    thread = threading.Thread(target=wait_for_interrupt, daemon=True)
    thread.start()

    start.wait(1.0)
    with selector:
        selector.interrupt()

    thread.join(1.0)
    assert not thread.is_alive()


def test_selector_close():
    selector = InterruptibleSelector()

    with selector:
        selector.close()
        with pytest.raises(RuntimeError):
            selector.select([], [], [], None)
        with pytest.raises(RuntimeError):
            selector.interrupt()
