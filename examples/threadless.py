#!/usr/bin/env python3
"""This example demonstrates a single-threaded application which reacts to incoming messages.

Run this alongside the `examples/publish.py` example to see messages being received."""

import argparse

from ohmqtt import Client, MQTTPublishPacket


def main(args: argparse.Namespace) -> None:
    client = Client()
    client.connect(args.address)
    client.loop_until_connected(timeout=5.0)

    def callback(client: Client, packet: MQTTPublishPacket) -> None:
        print(f"Received message: {packet.payload.decode()}")

    client.subscribe("ohmqtt/examples/publish", callback=callback)
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        client.shutdown()
        client.loop_once(max_wait=0)


if __name__ == "__main__":
    from .args import parser
    args = parser.parse_args()
    main(args)
