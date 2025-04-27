import threading
import time

from ohmqtt.retention import ReliablePublishHandle, UnreliablePublishHandle


def test_retention_unreliable_publish_handle():
    """Test the UnreliablePublishHandle class."""
    handle = UnreliablePublishHandle()
    assert handle.is_acked() is False
    assert handle.wait_for_ack() is False
    assert handle.wait_for_ack(timeout=1) is False


def test_retention_reliable_publish_handle():
    """Test the ReliablePublishHandle class."""
    cond = threading.Condition()
    handle = ReliablePublishHandle(cond)
    assert handle.is_acked() is False
    assert handle.wait_for_ack(timeout=0.001) is False

    def do_ack():
        time.sleep(0.1)
        with cond:
            handle.acked = True
            cond.notify_all()
    thread = threading.Thread(target=do_ack)
    thread.start()

    assert handle.wait_for_ack(timeout=1) is True
    assert handle.is_acked() is True
    thread.join()
