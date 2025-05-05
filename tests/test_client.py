import pytest
import threading

from ohmqtt.client import Client
from ohmqtt.connection import Connection, MessageHandlers
from ohmqtt.mqtt_spec import MQTTReasonCode
from ohmqtt.packet import MQTTAuthPacket
from ohmqtt.property import MQTTAuthProps, MQTTPublishProps
from ohmqtt.session import Session
from ohmqtt.subscriptions import Subscriptions
from ohmqtt.topic_alias import AliasPolicy


@pytest.fixture
def mock_connection(mocker):
    """Mock the Connection class."""
    mock_connection = mocker.Mock(spec=Connection)
    mocker.patch("ohmqtt.client.Connection", return_value=mock_connection)
    yield mock_connection


@pytest.fixture
def mock_handlers(mocker):
    """Mock the MessageHandlers class."""
    mock_handlers = mocker.MagicMock(spec=MessageHandlers)
    mock_handlers.__enter__.return_value = mock_handlers
    mocker.patch("ohmqtt.connection.MessageHandlers", return_value=mock_handlers)
    yield mock_handlers


@pytest.fixture
def mock_session(mocker):
    """Mock the Session class."""
    mock_session = mocker.Mock(spec=Session)
    mocker.patch("ohmqtt.client.Session", return_value=mock_session)
    yield mock_session


@pytest.fixture
def mock_subscriptions(mocker):
    """Mock the Subscriptions class."""
    mock_subscriptions = mocker.Mock(spec=Subscriptions)
    mocker.patch("ohmqtt.client.Subscriptions", return_value=mock_subscriptions)
    yield mock_subscriptions


@pytest.fixture
def mock_thread(mocker):
    """Mock the threading.Thread class."""
    mock_thread = mocker.Mock(threading.Thread)
    mocker.patch("threading.Thread", return_value=mock_thread)
    yield mock_thread


def test_client_connect(mocker, mock_connection, mock_handlers, mock_session, mock_subscriptions):
    client = Client()
    client.connect("localhost")
    mock_connection.connect.assert_called_once()


def test_client_disconnect(mocker, mock_connection, mock_handlers, mock_session, mock_subscriptions):
    client = Client()
    client.disconnect()
    mock_connection.disconnect.assert_called_once()


def test_client_shutdown(mocker, mock_connection, mock_handlers, mock_session, mock_subscriptions):
    client = Client()
    client.shutdown()
    mock_connection.shutdown.assert_called_once()


def test_client_publish(mocker, mock_connection, mock_handlers, mock_session, mock_subscriptions):
    client = Client()

    mock_session.publish.return_value = mocker.Mock()
    publish_handle = client.publish(
        "test/topic",
        b"test_payload",
        qos=2,
        retain=True,
        properties=MQTTPublishProps(
            MessageExpiryInterval=60,
            ResponseTopic="response/topic",
            CorrelationData=b"correlation_data",
            UserProperty=[("key", "value")],
        ),
        alias_policy=AliasPolicy.ALWAYS,
    )
    assert publish_handle == mock_session.publish.return_value
    mock_session.publish.assert_called_once_with(
        "test/topic",
        b"test_payload",
        qos=2,
        retain=True,
        properties=MQTTPublishProps(
            MessageExpiryInterval=60,
            ResponseTopic="response/topic",
            CorrelationData=b"correlation_data",
            UserProperty=[("key", "value")],
        ),
        alias_policy=AliasPolicy.ALWAYS,
    )


def test_client_subscribe(mocker, mock_connection, mock_handlers, mock_session, mock_subscriptions):
    client = Client()

    callback = lambda: None
    mock_subscriptions.subscribe.return_value = mocker.Mock()
    sub_handle = client.subscribe("test/+", callback)
    assert sub_handle == mock_subscriptions.subscribe.return_value
    mock_subscriptions.subscribe.assert_called_once()
    assert mock_subscriptions.subscribe.call_args[0][0] == "test/+"
    assert mock_subscriptions.subscribe.call_args[0][1] == callback
    mock_subscriptions.subscribe.reset_mock()


def test_client_unsubscribe(mocker, mock_connection, mock_handlers, mock_session, mock_subscriptions):
    client = Client()

    callback = lambda: None
    mock_subscriptions.unsubscribe.return_value = mocker.Mock()
    unsub_handle = client.unsubscribe("test/topic", callback)
    assert unsub_handle == mock_subscriptions.unsubscribe.return_value
    mock_subscriptions.unsubscribe.assert_called_once()
    assert mock_subscriptions.unsubscribe.call_args[0][0] == "test/topic"
    assert mock_subscriptions.unsubscribe.call_args[0][1] == callback


def test_client_auth(mocker, mock_connection, mock_handlers, mock_session, mock_subscriptions):
    client = Client()

    client.auth(
        reason_code=MQTTReasonCode.ReAuthenticate,
        authentication_method="test_method",
        authentication_data=b"test_data",
        reason_string="test_reason",
        user_properties=[("key", "value")],
    )
    mock_connection.send.assert_called_once_with(MQTTAuthPacket(
        reason_code=MQTTReasonCode.ReAuthenticate,
        properties=MQTTAuthProps(
            AuthenticationMethod="test_method",
            AuthenticationData=b"test_data",
            ReasonString="test_reason",
            UserProperty=[("key", "value")],
        ),
    ).encode())


def test_client_wait_for_connect(mocker, mock_connection, mock_handlers, mock_session, mock_subscriptions):
    client = Client()

    mock_connection.wait_for_connect.return_value = True
    client.wait_for_connect(0.1)
    mock_connection.wait_for_connect.assert_called_once_with(0.1)

    mock_connection.wait_for_connect.return_value = False
    with pytest.raises(TimeoutError):
        client.wait_for_connect(0.1)


def test_client_wait_for_disconnect(mocker, mock_connection, mock_handlers, mock_session, mock_subscriptions):
    client = Client()

    mock_connection.wait_for_disconnect.return_value = True
    client.wait_for_disconnect(0.1)
    mock_connection.wait_for_disconnect.assert_called_once_with(0.1)

    mock_connection.wait_for_disconnect.return_value = False
    with pytest.raises(TimeoutError):
        client.wait_for_disconnect(0.1)


def test_client_wait_for_shutdown(mocker, mock_connection, mock_handlers, mock_session, mock_subscriptions):
    client = Client()

    mock_connection.wait_for_shutdown.return_value = True
    client.wait_for_shutdown(0.1)
    mock_connection.wait_for_shutdown.assert_called_once_with(0.1)

    mock_connection.wait_for_shutdown.return_value = False
    with pytest.raises(TimeoutError):
        client.wait_for_shutdown(0.1)


def test_client_start_loop(mocker, mock_connection, mock_handlers, mock_session, mock_subscriptions, mock_thread):
    client = Client()
    client.start_loop()
    mock_thread.start.assert_called_once()
    with pytest.raises(RuntimeError):
        client.start_loop()


def test_client_loop_once(mocker, mock_connection, mock_handlers, mock_session, mock_subscriptions):
    client = Client()
    client.loop_once(0.1)
    mock_connection.loop_once.assert_called_once_with(0.1)


def test_client_loop_forever(mocker, mock_connection, mock_handlers, mock_session, mock_subscriptions):
    client = Client()
    client.loop_forever()
    mock_connection.loop_forever.assert_called_once_with()


def test_client_loop_until_connected(mocker, mock_connection, mock_handlers, mock_session, mock_subscriptions):
    client = Client()
    client.loop_until_connected(0.1)
    mock_connection.loop_until_connected.assert_called_once_with(0.1)


def test_client_is_connected(mocker, mock_connection, mock_handlers, mock_session, mock_subscriptions):
    client = Client()
    mock_connection.is_connected.return_value = True
    assert client.is_connected() is True
    mock_connection.is_connected.assert_called_once()
    mock_connection.is_connected.reset_mock()

    mock_connection.is_connected.return_value = False
    assert client.is_connected() is False
    mock_connection.is_connected.assert_called_with()


def test_client_handle_auth(mocker, mock_connection, mock_handlers, mock_session, mock_subscriptions):
    client = Client()

    auth_packet = MQTTAuthPacket()
    client.handle_auth(auth_packet)


def test_client_slots(mocker, mock_connection, mock_handlers, mock_session, mock_subscriptions):
    """Test that the client slots are set correctly."""
    with Client() as client:
        assert not hasattr(client, "__dict__")
        assert hasattr(client, "__weakref__")
        assert all(hasattr(client, attr) for attr in client.__slots__)
