#!/usr/bin/env python3
"""This example demonstrates a simple RPC server.

It listens for incoming RPC requests on a specific topic and responds with the result of the RPC call.

The ResponseTopic property is used by the requestor to specify the topic to which the response should be sent.

See the "rpc_caller" example for the request side of this RPC implementation."""

from ohmqtt.client import Client
from ohmqtt.message import MQTTMessage
from ohmqtt.property import MQTTPropertyDict


class RPCServer:
    """A simple stateless RPC server."""
    def handle_request(self, client: Client, msg: MQTTMessage) -> None:
        """Handle incoming RPC requests."""
        print(f"*** Received RPC request: {str(msg)}")

        # Find the response topic in the message properties.
        try:
            response_topic = msg.properties["ResponseTopic"]
        except KeyError:
            print("Request was missing required response topic property")
            return
        
        # If the request includes correlation data, send it back in the response.
        response_props: MQTTPropertyDict = {}
        if "CorrelationData" in msg.properties:
            response_props["CorrelationData"] = msg.properties["CorrelationData"]

        # Simulate some processing.
        response = f"This is a good day for {msg.payload.decode()}"

        # Send the response back to the specified topic.
        client.publish(response_topic, response.encode(), qos=2, properties=response_props)


def main() -> None:
    rpc_server = RPCServer()
    client = Client()
    client.connect("localhost", 1883)
    client.subscribe("ohmqtt/examples/rpc/request", qos=2, callback=rpc_server.handle_request)

    print("*** Waiting for RPC requests...")
    try:
        client.loop_forever()  # Wait indefinitely for incoming messages.
    except KeyboardInterrupt:
        print("\n*** Shutting down RPC server...")


if __name__ == "__main__":
    main()
