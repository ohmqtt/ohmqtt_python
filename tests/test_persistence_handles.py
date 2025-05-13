import threading
import time

from ohmqtt.persistence.base import ReliablePublishHandle, UnreliablePublishHandle


def test_persistence_handles_unreliable_publish() -> None:
    """Test the UnreliablePublishHandle class."""
    handle = UnreliablePublishHandle()
    assert handle.is_acked() is False
    assert handle.wait_for_ack() is False
    assert handle.wait_for_ack(timeout=1) is False

    assert not hasattr(handle, "__dict__")
    assert all(hasattr(handle, attr) for attr in handle.__slots__)


def test_persistence_handles_reliable_publish() -> None:
    """Test the ReliablePublishHandle class."""
    cond = threading.Condition()
    handle = ReliablePublishHandle(cond)
    assert handle.is_acked() is False
    assert handle.wait_for_ack(timeout=0.001) is False

    def do_ack() -> None:
        time.sleep(0.1)
        with cond:
            handle.acked = True
            cond.notify_all()
    thread = threading.Thread(target=do_ack)
    thread.start()

    assert handle.wait_for_ack(timeout=1) is True
    assert handle.is_acked() is True
    thread.join()

    assert not hasattr(handle, "__dict__")
    assert all(hasattr(handle, attr) for attr in handle.__slots__)
