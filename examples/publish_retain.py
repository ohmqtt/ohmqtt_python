#!/usr/bin/env python3
"""This example demonstrates publishing a retained message."""

from queue import Queue

from ohmqtt.client import Client
from ohmqtt.message import MQTTMessage


def main() -> None:
    client = Client()

    client.connect("localhost", 1883)
    client.wait_for_connect(timeout=5.0)
    print("*** Connected to broker")

    pub = client.publish("ohmqtt/examples/publish_retain", b"test_payload", qos=1, retain=True)
    assert pub.wait_for_ack()

    q: Queue[MQTTMessage] = Queue()
    def callback(_: Client, msg: MQTTMessage) -> None:
        q.put(msg)
    client.subscribe("ohmqtt/examples/publish_retain", qos=1, callback=callback)
    msg = q.get(timeout=5.0)
    assert msg.topic == "ohmqtt/examples/publish_retain"
    assert msg.payload == b"test_payload"
    assert msg.qos == 1
    assert msg.retain
    print(f"*** Received retained message: {str(msg)}")

    client.disconnect()
    client.wait_for_disconnect(timeout=5.0)
    print("*** Disconnected from broker")


if __name__ == "__main__":
    main()
