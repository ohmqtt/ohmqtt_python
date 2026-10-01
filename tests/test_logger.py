import importlib
import logging
import pkgutil

import ohmqtt
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


def test_logger_module_names() -> None:
    for module_info in pkgutil.walk_packages(ohmqtt.__path__, "ohmqtt."):
        if module_info.name.endswith("__main__"):
            continue  # Importing would run the CLI.
        module = importlib.import_module(module_info.name)
        logger = getattr(module, "logger", None)
        if isinstance(logger, logging.Logger):
            assert logger.name == module.__name__
