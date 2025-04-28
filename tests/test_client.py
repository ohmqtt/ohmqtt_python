import pytest

from ohmqtt.client import Client
from ohmqtt.packet import MQTTPublishPacket
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

    with Client() as client:
        MockSession.assert_called_once_with(
            auth_callback=client._handle_auth,
            close_callback=client._handle_close,
            message_callback=client._handle_message,
            open_callback=client._handle_open,
        )
        assert client.session == mock_session
        assert client.session.connect.call_count == 0
        assert client.session.publish.call_count == 0
        assert client.session.subscribe.call_count == 0
        assert client.session.unsubscribe.call_count == 0
        MockSession.reset_mock()

        # Connect to the broker.
        client.connect("localhost", 1883)
        mock_session.connect.assert_called_once()
        assert mock_session.connect.call_args[0][0].host == "localhost"
        assert mock_session.connect.call_args[0][0].port == 1883
        mock_session.connect.reset_mock()

        # AUTH
        client.auth(
            reason_code=0x23,
            authentication_method="test_method",
            authentication_data=b"test_data",
            reason_string="test_reason",
            user_properties=[("key", "value")],
        )
        mock_session.auth.assert_called_once_with(
            reason_code=0x23,
            authentication_method="test_method",
            authentication_data=b"test_data",
            reason_string="test_reason",
            user_properties=[("key", "value")],
        )
        mock_session.auth.reset_mock()

        # SUBSCRIBE
        def callback(client, message):
            received.append(message)
        sub_handle = client.subscribe("test/+", callback)
        mock_session.subscribe.assert_called_once_with("test/+", qos=2, properties=None)
        mock_session.subscribe.reset_mock()

        # Incoming subscribed PUBLISH
        packet = MQTTPublishPacket(
            topic="test/topic",
            payload=b"test_payload",
        )
        client._handle_message(packet)
        assert len(received) == 1
        assert received[0].topic == packet.topic
        assert received[0].payload == packet.payload
        received.clear()

        # Incoming unsubscribed PUBLISH, discarded
        packet.topic = "foo/bar"
        client._handle_message(packet)
        assert len(received) == 0

        # UNSUBSCRIBE
        sub_handle.unsubscribe()
        mock_session.unsubscribe.assert_called_once_with("test/+")
        mock_session.unsubscribe.reset_mock()

        # Incoming unsubscribed PUBLISH, discarded
        packet.topic = "test/topic"
        client._handle_message(packet)
        assert len(received) == 0

        # PUBLISH
        client.publish("test/topic", b"test_payload")
        mock_session.publish.assert_called_once_with(
            "test/topic",
            b"test_payload",
            qos=0,
            retain=False,
            properties=None,
        )
        mock_session.publish.reset_mock()

    mock_session.shutdown.assert_called_once()
    mock_session.shutdown.reset_mock()


def test_client_unsubscribe_untracked(MockSession, mock_session):
    """Test that unsubscribing from an untracked topic filter does not raise an error."""
    with Client() as client:
        client.connect("localhost", 1883)
        client.unsubscribe("test/topic")
        mock_session.unsubscribe.assert_called_once_with("test/topic")
        mock_session.unsubscribe.reset_mock()


def test_client_subscribe_callback_error(MockSession, mock_session):
    """Test that an error in the subscribe callback is not raised."""
    def error_callback(_):
        raise ValueError("Test error")

    with Client() as client:
        client.connect("localhost", 1883)
        client.subscribe("test/+", error_callback)

        packet = MQTTPublishPacket(
            topic="test/topic",
            payload=b"test_payload",
        )

        # Must not raise an Exception.
        client._handle_message(packet)


def test_client_subscribe_callback_unsubscribe(MockSession, mock_session):
    """Test that unsubscribing a callback works as expected."""
    received = []

    callback1 = lambda c, m: received.append(m)
    callback2 = lambda c, m: received.append(m)

    with Client() as client:
        client.connect("localhost", 1883)
        client.subscribe("test/+", callback1)
        client.subscribe("test/+", callback2)

        packet = MQTTPublishPacket(
            topic="test/topic",
            payload=b"test_payload",
        )
        client._handle_message(packet)
        assert len(received) == 2
        received.clear()

        client.unsubscribe("test/+", callback1)
        client._handle_message(packet)
        assert len(received) == 1
        received.clear()

        client.unsubscribe("test/+", callback2)
        client._handle_message(packet)
        assert len(received) == 0


def test_client_slots(MockSession, mock_session):
    """Test that the client slots are set correctly."""
    with Client() as client:
        assert not hasattr(client, "__dict__")
        assert all(hasattr(client, attr) for attr in client.__slots__)
