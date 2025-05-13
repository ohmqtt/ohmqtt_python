from unittest.mock import Mock

import pytest
from pytest_mock import MockerFixture

from ohmqtt.connection.keepalive import KeepAlive, MIN_TIMEOUT


@pytest.fixture
def mock_keepalive_time(mocker: MockerFixture) -> Mock:
    """Mock the keepalive module's time method to control the passage of time."""
    return mocker.patch("ohmqtt.connection.keepalive._time")


def test_keepalive_props() -> None:
    """Test the properties of the KeepAlive class."""
    keepalive = KeepAlive()
    assert keepalive.keepalive_interval == 0
    keepalive.keepalive_interval = 10
    assert keepalive.keepalive_interval == 10
    with pytest.raises(ValueError):
        keepalive.keepalive_interval = -1
    with pytest.raises(ValueError):
        keepalive.keepalive_interval = 65536


def test_keepalive_slots() -> None:
    """Ensure slots match attributes exactly."""
    keepalive = KeepAlive()
    assert not hasattr(keepalive, "__dict__")
    assert all(hasattr(keepalive, attr) for attr in keepalive.__slots__), \
        [attr for attr in keepalive.__slots__ if not hasattr(keepalive, attr)]


def test_keepalive_no_interval(mock_keepalive_time: Mock) -> None:
    """When interval is 0, keepalive should be inert."""
    keepalive = KeepAlive()

    mock_keepalive_time.return_value = 0.0
    keepalive.mark_init()
    assert keepalive.get_next_timeout() is None

    mock_keepalive_time.return_value = 1e9
    assert not keepalive.should_send_ping()
    assert not keepalive.should_close()
    assert keepalive.get_next_timeout() is None


def test_keepalive_dark_forest(mock_keepalive_time: Mock) -> None:
    """When interval is set and no data from server, close after a ping."""
    keepalive = KeepAlive()

    mock_keepalive_time.return_value = 0.0
    keepalive.mark_init()
    keepalive.keepalive_interval = 10
    assert keepalive.get_next_timeout() == 10.0

    mock_keepalive_time.return_value = 5.0
    assert not keepalive.should_send_ping()
    assert not keepalive.should_close()
    assert keepalive.get_next_timeout() == 5.0

    mock_keepalive_time.return_value = 10.0
    assert keepalive.should_send_ping()
    assert not keepalive.should_close()
    keepalive.mark_ping()
    assert keepalive.get_next_timeout() == 10.0

    mock_keepalive_time.return_value = 20.0
    assert not keepalive.should_send_ping()
    assert keepalive.should_close()
    assert keepalive.get_next_timeout() == MIN_TIMEOUT


def test_keepalive_send(mock_keepalive_time: Mock) -> None:
    """When interval is set and data is sent, no pings should be sent."""
    keepalive = KeepAlive()

    mock_keepalive_time.return_value = 0.0
    keepalive.mark_init()
    keepalive.keepalive_interval = 10

    for n in range(0, 60, 5):
        mock_keepalive_time.return_value = float(n)
        keepalive.mark_send()
        assert not keepalive.should_send_ping()
        assert not keepalive.should_close()
        assert keepalive.get_next_timeout() == 10.0


def test_keepalive_pingpong(mock_keepalive_time: Mock) -> None:
    """When interval is set now data is sent, exchange pings and pongs."""
    keepalive = KeepAlive()

    mock_keepalive_time.return_value = 0.0
    keepalive.mark_init()
    keepalive.keepalive_interval = 10

    for n in range(10, 60, 10):
        mock_keepalive_time.return_value = float(n)
        assert keepalive.get_next_timeout() == MIN_TIMEOUT
        assert keepalive.should_send_ping()
        assert not keepalive.should_close()
        keepalive.mark_ping()
        keepalive.mark_pong()
        assert keepalive.get_next_timeout() == 10.0
