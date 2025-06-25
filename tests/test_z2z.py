"""E2E Tests"""

from __future__ import annotations

import time
from typing import Final

from .util.fake_broker import FakeBroker
from ohmqtt.client import Client
from ohmqtt.logger import get_logger
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


def test_z2z_happy_path() -> None:
    client_received = []
    def callback(client: Client, packet: MQTTPacket) -> None:
        client_received.append(packet)

    with FakeBroker() as broker:
        with Client() as client:
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
            logger.info("SUBSCRIBE acked")
            time.sleep(0.01)
            assert broker.received.pop(0) == MQTTSubscribePacket(
                topics=[("test/topic", 2)],
                packet_id=1,
            )

            # PUBLISH QoS 1
            pub_handle = client.publish("test/topic", b"coconut", qos=1)
            pub_handle.wait_for_ack(timeout=0.25)
            logger.info("QoS1 acked")
            time.sleep(0.01)
            assert broker.received.pop(0) == client_received.pop(0) == MQTTPublishPacket(
                topic="test/topic",
                payload=b"coconut",
                qos=1,
                packet_id=1,
            )
            assert broker.received.pop(0) == MQTTPubAckPacket(packet_id=1)

            # UNSUBSCRIBE
            unsub_handle = client.unsubscribe("test/topic", callback)
            assert unsub_handle is not None
            unsub_handle.wait_for_ack(timeout=0.25)
            logger.info("UNSUBSCRIBE acked")
            time.sleep(0.01)
            assert broker.received.pop(0) == MQTTUnsubscribePacket(
                topics=["test/topic"],
                packet_id=1,
            )

            # PUBLISH QoS 2
            pub_handle = client.publish("test/topic", b"pineapple", qos=2)
            pub_handle.wait_for_ack(timeout=0.25)
            logger.info("QoS2 acked")
            time.sleep(0.01)
            assert broker.received.pop(0) == MQTTPublishPacket(
                topic="test/topic",
                payload=b"pineapple",
                qos=2,
                packet_id=2,
            )
            assert broker.received.pop(0) == MQTTPubRelPacket(packet_id=2)

            # DISCONNECT
            client.disconnect()
            client.wait_for_disconnect(timeout=0.25)
            time.sleep(0.01)
            assert broker.received.pop(0) == MQTTDisconnectPacket()

        client.wait_for_shutdown(timeout=0.25)
