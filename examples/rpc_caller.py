#!/usr/bin/env python3
"""This example demonstrates a simple RPC caller.

It sends an RPC request to a specific topic and waits for the response.

The ResponseTopic property is used to specify the topic to which the response should be sent."""

import argparse
import threading
import uuid
from typing import Callable

from ohmqtt.client import Client
from ohmqtt.packet import MQTTPublishPacket
from ohmqtt.property import MQTTPublishProps


RPCCallback = Callable[[bytes], None]


class RPCCaller:
    """A simple RPC caller."""
    def __init__(self, client: Client) -> None:
        self.client = client
        self.callbacks: dict[str, RPCCallback] = {}

    def send_request(self, payload: bytes, callback: RPCCallback) -> None:
        # Expect the response to be sent to a unique topic.
        unique_id = str(uuid.uuid4())
        response_topic = f"ohmqtt/examples/rpc/response/{unique_id}"

        # Store the callback for this request.
        self.callbacks[unique_id] = callback

        # Subscribe to the response topic.
        self.client.subscribe(response_topic, self.handle_response)

        # Publish the request with the necessary properties.
        self.client.publish(
            "ohmqtt/examples/rpc/request",
            payload,
            qos=2,
            properties=MQTTPublishProps(ResponseTopic=response_topic),
        ).wait_for_ack()

        print(f"Sent RPC request with response topic: {response_topic}")

    def handle_response(self, client: Client, msg: MQTTPublishPacket) -> None:
        """Handle incoming RPC responses."""
        try:
            callback = self.callbacks.pop(msg.topic.split("/")[-1])
        except KeyError:
            print("Couldn't find callback for response")
            return
        else:
            # Call the callback with the response payload.
            callback(msg.payload)
        finally:
            # Unsubscribe from the response topic.
            client.unsubscribe(msg.topic, self.handle_response)


def main(args: argparse.Namespace) -> None:
    with Client() as client:
        rpc_caller = RPCCaller(client)
        client.connect(args.address)
        print("*** Waiting for connection...")
        client.wait_for_connect(timeout=5.0)

        # Set up a callback which helps us block until a response is received.
        recvd = threading.Event()
        def callback(payload: bytes) -> None:
            print(f"*** Received RPC response: {payload.decode()}")
            recvd.set()  # Signal that the response has been received.

        while True:
            recvd.clear()  # Reset the event for the next request.
            try:
                payload = input("*** Enter something fun (or 'exit' to quit): ")
                if payload.lower() == "exit":
                    break
                rpc_caller.send_request(payload.encode(), callback)
                recvd.wait(timeout=5.0)  # Wait for the response to be received.
            except (EOFError, KeyboardInterrupt):
                print("\n*** Shutting down RPC caller...")
                break


if __name__ == "__main__":
    from .args import parser
    args = parser.parse_args()
    main(args)
