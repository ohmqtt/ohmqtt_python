import pytest

from ohmqtt.connection.timeout import Timeout


@pytest.fixture
def mock_time(mocker):
    yield mocker.patch("ohmqtt.connection.timeout._time")


def test_timeout(mock_time):
    """Test the Timeout class."""
    timeout = Timeout()

    assert timeout.interval is None
    assert timeout.get_timeout() is None
    assert not timeout.exceeded()

    timeout.interval = 1.0
    mock_time.return_value = 0.0
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
