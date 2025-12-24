"""Command line interface for ohmqtt."""

import argparse

from .publish import PublishCommand
from .subscribe import SubscribeCommand
from .. import __version__


def main(args: list[str]) -> None:
    """Main entry point for the ohmqtt CLI."""
    parser = argparse.ArgumentParser(description="ohmqtt Command Line Interface")
    parser.add_argument(
        "--version",
        action="version",
        version=__version__,
        help="show the version of ohmqtt and exit",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    PublishCommand.register(subparsers)
    SubscribeCommand.register(subparsers)

    parsed = parser.parse_args(args)
    if hasattr(parsed, "func"):
        parsed.func(parsed)
    else:
        parser.print_help()
