import logging

from ohmqtt.logger import get_logger, set_log_level


def test_logger_set_level() -> None:
    logger = get_logger("test")
    for level in (
        logging.DEBUG,
        logging.INFO,
        logging.WARNING,
        logging.ERROR,
        logging.CRITICAL,
    ):
        set_log_level(level)
        assert logger.getEffectiveLevel() == level
