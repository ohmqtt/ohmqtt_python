#!/usr/bin/env python3
"""This example demonstrates connecting to a broker, publishing messages to a topic, and then disconnecting."""

import argparse
import time

from ohmqtt.client import Client


def main(args: argparse.Namespace) -> None:
    with Client() as client:

        print("*** Connecting to broker...")
        client.connect(args.address)

        client.wait_for_connect(timeout=5.0)
        print("*** Connected to broker")

        for n in range(1, 9):
            client.publish("ohmqtt/examples/publish", b"test_payload: " + str(n).encode(), qos=0)
            print(f"*** Published message {n}")
            time.sleep(1.0)

        print("*** Disconnecting from broker...")
        client.disconnect()

        client.wait_for_disconnect(timeout=5.0)
        print("*** Disconnected from broker")


if __name__ == "__main__":
    from .args import parser
    args = parser.parse_args()
    main(args)
