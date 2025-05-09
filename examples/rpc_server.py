#!/usr/bin/env python3
"""This example demonstrates a simple RPC server.

It listens for incoming RPC requests on a specific topic and responds with the result of the RPC call.

The ResponseTopic property is used by the requestor to specify the topic to which the response should be sent.

See the "rpc_caller" example for the request side of this RPC implementation.

This also demonstrates running the client loop in the main thread."""

import argparse

from ohmqtt.client import Client
from ohmqtt.packet import MQTTPublishPacket
from ohmqtt.property import MQTTPublishProps


class RPCServer:
    """A simple stateless RPC server."""
    def handle_request(self, client: Client, msg: MQTTPublishPacket) -> None:
        """Handle incoming RPC requests."""
        print(f"*** Received RPC request: {msg!s}")

        # Find the response topic in the message properties.
        if not msg.properties.ResponseTopic:
            print("Request was missing required response topic property")
            return
        response_topic = msg.properties.ResponseTopic

        response_props = MQTTPublishProps()
        # If the request includes correlation data, send it back in the response.
        if msg.properties.CorrelationData is not None:
            response_props.CorrelationData = msg.properties.CorrelationData

        # Simulate some processing.
        response = f"This is a good day for {msg.payload.decode()}"

        # Send the response back to the specified topic.
        client.publish(response_topic, response.encode(), qos=2, properties=response_props)


def main(args: argparse.Namespace) -> None:
    rpc_server = RPCServer()
    client = Client()
    client.connect(args.address)
    print("*** Waiting for connection...")
    assert client.loop_until_connected(timeout=5.0), "Timeout waiting for connection"

    client.subscribe("ohmqtt/examples/rpc/request", rpc_server.handle_request)
    print("*** Waiting for RPC requests...")

    try:
        client.loop_forever()  # Wait indefinitely for incoming messages.
    except KeyboardInterrupt:
        print("\n*** Shutting down RPC server...")


if __name__ == "__main__":
    from .args import parser
    args = parser.parse_args()
    main(args)
