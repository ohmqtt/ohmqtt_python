"""E2E Tests"""

from __future__ import annotations

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
def client() -> Client:
    client = Client()
    client.start_loop()
    return client


@pytest.fixture
def broker() -> Iterator[FakeBroker]:
    with FakeBroker() as broker:
        yield broker


def test_z2z_happy_path(client: Client, broker: FakeBroker) -> None:
    delay: Final = 0.005  # seconds grace period

    client_received = []
    def callback(client: Client, packet: MQTTPacket) -> None:
        client_received.append(packet)

    client.connect(
        address=f"localhost:{broker.port}",
        client_id="test_client",
        clean_start=True,
    )
    client.wait_for_connect(timeout=0.25)
    assert client.is_connected()
    assert broker.received.pop(0) == MQTTConnectPacket(
        client_id="test_client",
        clean_start=True,
    )

    # SUBSCRIBE
    sub_handle = client.subscribe("test/topic", callback)
    assert sub_handle is not None
    sub_handle.wait_for_ack(timeout=0.25)
    time.sleep(delay)
    assert broker.received.pop(0) == MQTTSubscribePacket(
        topics=[("test/topic", 2)],
        packet_id=1,
    )

    # PUBLISH QoS 0
    for n in range(50):
        pub_handle = client.publish("test/topic", b"banana", qos=0)
        time.sleep(delay)
        assert broker.received.pop(0) == client_received.pop(0) == MQTTPublishPacket(
            topic="test/topic",
            payload=b"banana",
        )

    # PUBLISH QoS 1
    for n in range(1, 50):
        pub_handle = client.publish("test/topic", b"coconut", qos=1)
        pub_handle.wait_for_ack(timeout=0.25)
        time.sleep(delay)
        assert broker.received.pop(0) == client_received.pop(0) == MQTTPublishPacket(
            topic="test/topic",
            payload=b"coconut",
            qos=MQTTQoS.Q1,
            packet_id=n,
        )
        assert broker.received.pop(0) == MQTTPubAckPacket(packet_id=n)

    # UNSUBSCRIBE
    unsub_handle = client.unsubscribe("test/topic", callback)
    assert unsub_handle is not None
    unsub_handle.wait_for_ack(timeout=0.25)
    time.sleep(delay)
    assert broker.received.pop(0) == MQTTUnsubscribePacket(
        topics=["test/topic"],
        packet_id=1,
    )

    # PUBLISH QoS 2
    for n in range(50, 100):
        pub_handle = client.publish("test/topic", b"pineapple", qos=2)
        pub_handle.wait_for_ack(timeout=0.25)
        time.sleep(delay)
        assert broker.received.pop(0) == MQTTPublishPacket(
            topic="test/topic",
            payload=b"pineapple",
            qos=MQTTQoS.Q2,
            packet_id=n,
        )
        assert broker.received.pop(0) == MQTTPubRelPacket(packet_id=n)

    # DISCONNECT
    client.disconnect()
    client.wait_for_disconnect(timeout=0.25)
    time.sleep(delay)
    assert broker.received.pop(0) == MQTTDisconnectPacket()

    client.shutdown()
    client.wait_for_shutdown(timeout=0.25)
