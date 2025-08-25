"""E2E Tests"""

from __future__ import annotations

from collections import deque
import time
from typing import Final, Iterator

import pytest

from .util.fake_broker import FakeBroker
from ohmqtt.client import Client
from ohmqtt.logger import get_logger
from ohmqtt.mqtt_spec import MQTTQoS
from ohmqtt.packet import (
    MQTTPacket,
    MQTTConnectPacket,
    MQTTSubscribePacket,
    MQTTUnsubscribePacket,
    MQTTPublishPacket,
    MQTTPubAckPacket,
    MQTTPubRelPacket,
    MQTTDisconnectPacket,
)

logger: Final = get_logger("tests.test_z2z")


@pytest.fixture
def broker() -> Iterator[FakeBroker]:
    with FakeBroker() as broker:
        yield broker


def get_client(broker: FakeBroker, db_path: str) -> Client:
    client = Client(db_path)
    client.start_loop()

    client.connect(
        address=f"localhost:{broker.port}",
        client_id="test_client",
        clean_start=True,
    )
    client.wait_for_connect(timeout=0.25)
    assert client.is_connected()
    assert broker.received.popleft() == MQTTConnectPacket(
        client_id="test_client",
        clean_start=True,
    )
    return client


@pytest.mark.parametrize("db_path", ["", ":memory:"])
def test_z2z_happy_path(db_path: str, broker: FakeBroker) -> None:
    client = get_client(broker, db_path)
    delay: Final = 0.01  # seconds grace period

    client_received: deque[MQTTPacket] = deque()
    def callback(client: Client, packet: MQTTPacket) -> None:
        client_received.append(packet)

    # SUBSCRIBE
    sub_handle = client.subscribe("test/topic", callback)
    assert sub_handle is not None
    sub_handle.wait_for_ack(timeout=0.25)
    time.sleep(delay)
    assert broker.received.popleft() == MQTTSubscribePacket(
        topics=[("test/topic", 2)],
        packet_id=1,
    )

    # PUBLISH QoS 0
    for n in range(10):
        pub_handle = client.publish("test/topic", b"banana", qos=0)
        time.sleep(delay)
        assert broker.received.popleft() == client_received.popleft() == MQTTPublishPacket(
            topic="test/topic",
            payload=b"banana",
        )

    # PUBLISH QoS 1
    for n in range(1, 10):
        pub_handle = client.publish("test/topic", b"coconut", qos=1)
        pub_handle.wait_for_ack(timeout=0.25)
        time.sleep(delay)
        broker_rec = broker.received.popleft()
        client_rec = client_received.popleft()
        assert isinstance(broker_rec, MQTTPublishPacket)
        assert isinstance(client_rec, MQTTPublishPacket)
        assert broker_rec.topic == client_rec.topic == "test/topic"
        assert broker_rec.payload == client_rec.payload == b"coconut"
        assert broker_rec.qos == client_rec.qos == MQTTQoS.Q1
        assert broker.received.popleft() == MQTTPubAckPacket(packet_id=broker_rec.packet_id)

    # UNSUBSCRIBE
    unsub_handle = client.unsubscribe("test/topic")
    assert unsub_handle is not None
    unsub_handle.wait_for_ack(timeout=0.25)
    time.sleep(delay)
    assert broker.received.popleft() == MQTTUnsubscribePacket(
        topics=["test/topic"],
        packet_id=1,
    )

    # PUBLISH QoS 2
    for n in range(10, 20):
        pub_handle = client.publish("test/topic", b"pineapple", qos=2)
        pub_handle.wait_for_ack(timeout=0.25)
        time.sleep(delay)
        broker_rec = broker.received.popleft()
        assert isinstance(broker_rec, MQTTPublishPacket)
        assert broker_rec.topic == "test/topic"
        assert broker_rec.payload == b"pineapple"
        assert broker_rec.qos == MQTTQoS.Q2
        assert broker.received.popleft() == MQTTPubRelPacket(packet_id=broker_rec.packet_id)

    # DISCONNECT
    client.disconnect()
    client.wait_for_disconnect(timeout=0.25)
    time.sleep(delay)
    assert broker.received.popleft() == MQTTDisconnectPacket()

    client.shutdown()
    client.wait_for_shutdown(timeout=0.25)


@pytest.mark.parametrize("db_path", ["", ":memory:"])
def test_z2z_saturation_qos0(db_path: str, broker: FakeBroker) -> None:
    num: Final = 5000
    timeout: Final = 5.0  # seconds
    sz: Final = 1000
    delay: Final = 0.01  # seconds grace period
    client = get_client(broker, db_path)

    client_received: deque[MQTTPacket] = deque()
    def callback(client: Client, packet: MQTTPacket) -> None:
        client_received.append(packet)

    sub_handle = client.subscribe("test/topic", callback)
    assert sub_handle is not None
    sub_handle.wait_for_ack(timeout=0.25)
    time.sleep(delay)
    assert broker.received.popleft() == MQTTSubscribePacket(
        topics=[("test/topic", 2)],
        packet_id=1,
    )

    for _ in range(num):
        client.publish("test/topic", b"x" * sz)

    t0 = time.monotonic()
    while True:
        if len(broker.received) == num and len(client_received) == num:
            break
        if time.monotonic() > t0 + timeout:
            raise TimeoutError(f"Didn't publish and receive messages in time {len(broker.received)}:{len(client_received)}")
        time.sleep(delay)

    for _ in range(num):
        broker_rec = broker.received.popleft()
        client_rec = client_received.popleft()
        assert isinstance(broker_rec, MQTTPublishPacket)
        assert isinstance(client_rec, MQTTPublishPacket)
        assert broker_rec == client_rec == MQTTPublishPacket(
            topic="test/topic",
            payload=b"x" * sz,
        )


@pytest.mark.parametrize("db_path", ["", ":memory:"])
def test_z2z_saturation_qos1(db_path: str, broker: FakeBroker) -> None:
    num: Final = 2000
    timeout: Final = 5.0  # seconds
    sz: Final = 1000
    delay: Final = 0.01  # seconds grace period
    client = get_client(broker, db_path)

    client_received: deque[MQTTPacket] = deque()
    def callback(client: Client, packet: MQTTPacket) -> None:
        client_received.append(packet)

    sub_handle = client.subscribe("test/topic", callback)
    assert sub_handle is not None
    sub_handle.wait_for_ack(timeout=0.25)
    time.sleep(delay)
    assert broker.received.popleft() == MQTTSubscribePacket(
        topics=[("test/topic", 2)],
        packet_id=1,
    )

    for _ in range(num):
        client.publish("test/topic", b"x" * sz, qos=1)

    t0 = time.monotonic()
    while True:
        if len(broker.received) == num * 2 and len(client_received) == num:
            break
        if time.monotonic() > t0 + timeout:
            raise TimeoutError(f"Didn't publish and receive messages in time {len(broker.received)}:{len(client_received)}")
        time.sleep(delay)

    for _ in range(num):
        broker_rec = broker.received.popleft()
        while isinstance(broker_rec, MQTTPubAckPacket):
            broker_rec = broker.received.popleft()
        client_rec = client_received.popleft()
        assert isinstance(broker_rec, MQTTPublishPacket)
        assert isinstance(client_rec, MQTTPublishPacket)
        assert broker_rec.topic == client_rec.topic == "test/topic"
        assert broker_rec.payload == client_rec.payload == b"x" * sz
        assert broker_rec.qos == client_rec.qos == MQTTQoS.Q1


@pytest.mark.parametrize("db_path", ["", ":memory:"])
def test_z2z_saturation_qos2(db_path: str, broker: FakeBroker) -> None:
    num: Final = 1000
    timeout: Final = 5.0  # seconds
    sz: Final = 1000
    delay: Final = 0.01  # seconds grace period
    client = get_client(broker, db_path)

    client_received: deque[MQTTPacket] = deque()
    def callback(client: Client, packet: MQTTPacket) -> None:
        client_received.append(packet)

    sub_handle = client.subscribe("test/topic", callback)
    assert sub_handle is not None
    sub_handle.wait_for_ack(timeout=0.25)
    time.sleep(delay)
    assert broker.received.popleft() == MQTTSubscribePacket(
        topics=[("test/topic", 2)],
        packet_id=1,
    )

    for _ in range(num):
        client.publish("test/topic", b"x" * sz, qos=2)

    t0 = time.monotonic()
    while True:
        if len(broker.received) == num * 4 and len(client_received) == num:
            break
        if time.monotonic() > t0 + timeout:
            raise TimeoutError(f"Didn't publish and receive messages in time {len(broker.received)}:{len(client_received)}")
        time.sleep(delay)

    for _ in range(num):
        broker_rec = broker.received.popleft()
        while not isinstance(broker_rec, MQTTPublishPacket):
            broker_rec = broker.received.popleft()
        client_rec = client_received.popleft()
        assert isinstance(broker_rec, MQTTPublishPacket)
        assert isinstance(client_rec, MQTTPublishPacket)
        assert broker_rec.topic == client_rec.topic == "test/topic"
        assert broker_rec.payload == client_rec.payload == b"x" * sz
        assert broker_rec.qos == client_rec.qos == MQTTQoS.Q2
