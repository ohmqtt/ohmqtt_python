import argparse


parser = argparse.ArgumentParser(description="ohmqtt example")
parser.add_argument(
    "-a", "--address",
    type=str,
    default="localhost:1883",
    help="MQTT broker address (default: localhost:1883)",
)
