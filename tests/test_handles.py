import threading
import time

import pytest

from ohmqtt.handles import (
    PublishHandle,
    SubscribeHandle,
    UnsubscribeHandle,
)
from ohmqtt.packet import (
    MQTTPubAckPacket,
    MQTTPubRecPacket,
    MQTTPubCompPacket,
    MQTTSubAckPacket,
    MQTTUnsubAckPacket,
)


@pytest.mark.parametrize("handle_cls", [PublishHandle, SubscribeHandle, UnsubscribeHandle])
def test_handles_slots(handle_cls: type[PublishHandle | SubscribeHandle | UnsubscribeHandle]) -> None:
    """Test the UnreliablePublishHandle class."""
    cond = threading.Condition()
    handle = handle_cls(cond)

    assert not hasattr(handle, "__dict__")
    assert all(hasattr(handle, attr) for attr in handle.__slots__)


def _test_ack(
    handle_cls: type[PublishHandle | SubscribeHandle | UnsubscribeHandle],
    ack_cls: type[MQTTPubAckPacket | MQTTPubRecPacket | MQTTPubCompPacket | MQTTSubAckPacket | MQTTUnsubAckPacket],
) -> None:
    start = threading.Event()
    cond = threading.Condition()
    handle = PublishHandle(cond)
    ack = ack_cls(packet_id=1)
    with pytest.raises(TimeoutError):
        handle.wait_for_ack(timeout=0.001)

    def do_ack() -> None:
        start.wait()
        time.sleep(0.001)
        with cond:
            handle.ack = ack  # type: ignore[assignment]
            cond.notify_all()
    thread = threading.Thread(target=do_ack)
    thread.start()

    start.set()
    assert handle.wait_for_ack(timeout=0.01) == ack

    thread.join()


def _test_exc(
    handle_cls: type[PublishHandle | SubscribeHandle | UnsubscribeHandle],
    ack_cls: type[MQTTPubAckPacket | MQTTPubRecPacket | MQTTPubCompPacket | MQTTSubAckPacket | MQTTUnsubAckPacket],
) -> None:
    start = threading.Event()
    cond = threading.Condition()
    handle = PublishHandle(cond)
    with pytest.raises(TimeoutError):
        handle.wait_for_ack(timeout=0.001)

    def do_ack() -> None:
        start.wait()
        time.sleep(0.001)
        with cond:
            handle.exc = ValueError("TEST")
            cond.notify_all()
    thread = threading.Thread(target=do_ack)
    thread.start()

    start.set()
    with pytest.raises(ValueError, match="TEST"):
        handle.wait_for_ack(timeout=0.01)

    thread.join()


@pytest.mark.parametrize("ack_cls", [MQTTPubAckPacket, MQTTPubRecPacket, MQTTPubCompPacket])
def test_handles_publish_ack(ack_cls: type[MQTTPubAckPacket | MQTTPubRecPacket | MQTTPubCompPacket]) -> None:
    _test_ack(PublishHandle, ack_cls)


@pytest.mark.parametrize("ack_cls", [MQTTPubAckPacket, MQTTPubRecPacket, MQTTPubCompPacket])
def test_handles_publish_exc(ack_cls: type[MQTTPubAckPacket | MQTTPubRecPacket | MQTTPubCompPacket]) -> None:
    _test_exc(PublishHandle, ack_cls)


def test_handles_subscribe_ack() -> None:
    _test_ack(SubscribeHandle, MQTTSubAckPacket)


def test_handles_subscribe_exc() -> None:
    _test_exc(SubscribeHandle, MQTTSubAckPacket)


def test_handles_unsubscribe_ack() -> None:
    _test_ack(UnsubscribeHandle, MQTTUnsubAckPacket)


def test_handles_unsubscribe_exc() -> None:
    _test_exc(UnsubscribeHandle, MQTTUnsubAckPacket)


@pytest.mark.parametrize("handle_cls", [PublishHandle, SubscribeHandle, UnsubscribeHandle])
def test_handles_deref(handle_cls: type[PublishHandle | SubscribeHandle | UnsubscribeHandle]) -> None:
    cond = threading.Condition()
    handle = handle_cls(cond)
    del cond
    with pytest.raises(RuntimeError, match="Condition variable is no longer available"):
        handle.wait_for_ack(timeout=0.001)
