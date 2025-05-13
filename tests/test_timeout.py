from unittest.mock import Mock

import pytest
from pytest_mock import MockerFixture

from ohmqtt.connection.timeout import Timeout


@pytest.fixture
def mock_time(mocker: MockerFixture) -> Mock:
    return mocker.patch("ohmqtt.connection.timeout._time")


def test_timeout(mock_time: Mock) -> None:
    mock_time.return_value = 0.0
    timeout = Timeout()

    assert timeout.interval is None
    assert timeout.get_timeout() is None
    assert not timeout.exceeded()

    timeout.interval = 1.0
    assert timeout.get_timeout() == 1.0
    assert not timeout.exceeded()

    mock_time.return_value = 1.0
    assert timeout.get_timeout() == 0.0
    assert timeout.exceeded()

    mock_time.return_value = 2.0
    assert timeout.get_timeout() == 0.0
    assert timeout.exceeded()

    timeout.mark()
    assert timeout.get_timeout() == 1.0
    assert not timeout.exceeded()


def test_timeout_max_wait(mock_time: Mock) -> None:
    mock_time.return_value = 0.0
    timeout = Timeout()

    assert timeout.interval is None
    assert timeout.get_timeout(max_wait=1.0) == 1.0

    timeout.interval = 1.0
    assert timeout.get_timeout(max_wait=0.5) == 0.5
