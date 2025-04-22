import pytest

from ohmqtt.client import Client
from ohmqtt.session import Session


@pytest.fixture
def mock_session(mocker):
    """Mock the Session class."""
    return mocker.Mock(spec=Session)


@pytest.fixture
def MockSession(mocker, mock_session):
    MockSession = mocker.patch("ohmqtt.client.Session", return_value=mock_session)
    yield MockSession


def test_client_happy_path(MockSession, mock_session):
    received = []

    client = Client(client_id="test_client")
    MockSession.assert_called_once_with(
        "test_client",
        message_cb=client.on_message,
    )
    assert client.client_id == "test_client"
    assert client.session == mock_session
    assert client.session.connect.call_count == 0
    assert client.session.publish.call_count == 0
    assert client.session.subscribe.call_count == 0
    assert client.session.unsubscribe.call_count == 0
    MockSession.reset_mock()

    client.connect("localhost", 1883)
    mock_session.connect.assert_called_once_with("localhost", 1883)
    mock_session.connect.reset_mock()

    client.subscribe("test/+", lambda c, t, p, pr: received.append((c, t, p, pr)))
    mock_session.subscribe.assert_called_once_with("test/+", qos=2, properties=None)
    mock_session.subscribe.reset_mock()

    client.on_message(client.session, "test/topic", b"test_payload", None)
    assert len(received) == 1
    assert received[0][0] == client
    assert received[0][1] == "test/topic"
    assert received[0][2] == b"test_payload"
    assert received[0][3] is None
    received.clear()

    client.on_message(client.session, "foo/bar", b"test_payload", None)
    assert len(received) == 0

    client.unsubscribe("test/+")
    mock_session.unsubscribe.assert_called_once_with("test/+")
    mock_session.unsubscribe.reset_mock()

    client.on_message(client.session, "test/topic", b"test_payload", None)
    assert len(received) == 0

    client.publish("test/topic", b"test_payload")
    mock_session.publish.assert_called_once_with(
        "test/topic",
        b"test_payload",
        qos=0,
        retain=False,
        properties=None,
    )
    mock_session.publish.reset_mock()


def test_client_unsubscribe_untracked(MockSession, mock_session):
    """Test that unsubscribing from an untracked topic filter does not raise an error."""
    client = Client(client_id="test_client")
    client.connect("localhost", 1883)
    client.unsubscribe("test/topic")
    mock_session.unsubscribe.assert_called_once_with("test/topic")
    mock_session.unsubscribe.reset_mock()


def test_client_subscribe_callback_error(MockSession, mock_session):
    """Test that an error in the subscribe callback is not raised."""
    def error_callback(client, topic, payload, properties):
        raise ValueError("Test error")

    client = Client(client_id="test_client")
    client.connect("localhost", 1883)
    client.subscribe("test/+", error_callback)

    # Must not raise an Exception.
    client.on_message(client.session, "test/topic", b"test_payload", None)


def test_client_subscribe_callback_unsubscribe(MockSession, mock_session):
    """Test that unsubscribing a callback works as expected."""
    received = []

    callback1 = lambda c, t, p, pr: received.append((c, t, p, pr))
    callback2 = lambda c, t, p, pr: received.append((c, t, p, pr))

    client = Client(client_id="test_client")
    client.connect("localhost", 1883)
    client.subscribe("test/+", callback1)
    client.subscribe("test/+", callback2)

    client.on_message(client.session, "test/topic", b"test_payload", None)
    assert len(received) == 2
    received.clear()

    client.unsubscribe("test/+", callback1)
    client.on_message(client.session, "test/topic", b"test_payload", None)
    assert len(received) == 1
    received.clear()

    client.unsubscribe("test/+", callback2)
    client.on_message(client.session, "test/topic", b"test_payload", None)
    assert len(received) == 0
