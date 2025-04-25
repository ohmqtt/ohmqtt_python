import logging
from typing import Final

ohmqtt_logger: Final = logging.getLogger("ohmqtt")


def set_log_level(level: int) -> None:
    """Set the log level for the ohmqtt logger."""
    ohmqtt_logger.setLevel(level)


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the specified name."""
    return ohmqtt_logger.getChild(name)
