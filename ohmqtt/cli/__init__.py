"""Command line interface for ohmqtt."""

import argparse
import logging

from .publish import PublishCommand
from .subscribe import SubscribeCommand
from .. import __version__


def main(args: list[str]) -> None:
    """Main entry point for the ohmqtt CLI."""
    parser = argparse.ArgumentParser(description="ohmqtt Command Line Interface")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="enable verbose output",
    )
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

    if parsed.verbose:
        logging.basicConfig(level=logging.DEBUG)

    if hasattr(parsed, "func"):
        parsed.func(parsed)
    else:
        parser.print_help()
