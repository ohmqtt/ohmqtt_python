from unittest.mock import Mock, MagicMock
import weakref

import pytest
from pytest_mock import MockerFixture

from ohmqtt.client import Client
from ohmqtt.connection import Connection, MessageHandlers, InvalidStateError
from ohmqtt.mqtt_spec import MQTTReasonCode, MQTTQoS
from ohmqtt.packet import (
    MQTTPublishPacket,
    MQTTSubscribePacket,
    MQTTSubAckPacket,
    MQTTUnsubAckPacket,
    MQTTConnAckPacket,
)
from ohmqtt.property import MQTTSubscribeProps
from ohmqtt.subscriptions import Subscriptions, RetainPolicy


def dummy_callback(client: Client, packet: MQTTPublishPacket) -> None:
    """Dummy SubscribeCallback for testing."""


@pytest.fixture
def mock_client(mocker: MockerFixture) -> Mock:
    return mocker.Mock(spec=Client)  # type: ignore[no-any-return]


@pytest.fixture
def mock_connection(mocker: MockerFixture) -> Mock:
    conn = mocker.MagicMock(spec=Connection)
    conn.fsm.cond.__enter__.return_value = mocker.Mock()
    conn.fsm.lock.__enter__.return_value = mocker.Mock()
    return conn  # type: ignore[no-any-return]


@pytest.fixture
def mock_handlers(mocker: MockerFixture) -> MagicMock:
    return mocker.MagicMock(spec=MessageHandlers)  # type: ignore[no-any-return]


def test_subscriptions_registration(mock_client: Mock, mock_connection: Mock) -> None:
    handlers = MessageHandlers()
    with handlers:
        subscriptions = Subscriptions(handlers, mock_connection, weakref.ref(mock_client))
    assert subscriptions.handle_suback in handlers.get_handlers(MQTTSubAckPacket)
    assert subscriptions.handle_unsuback in handlers.get_handlers(MQTTUnsubAckPacket)


@pytest.mark.parametrize("max_qos", [MQTTQoS.Q0, MQTTQoS.Q1, MQTTQoS.Q2])
@pytest.mark.parametrize("no_local", [True, False])
@pytest.mark.parametrize("retain_as_published", [True, False])
@pytest.mark.parametrize("retain_policy", [RetainPolicy.NEVER, RetainPolicy.ONCE, RetainPolicy.ALWAYS])
def test_subscriptions_subscribe_opts(
    mock_handlers: MagicMock,
    mock_client: Mock,
    mock_connection: Mock,
    max_qos: MQTTQoS,
    no_local: bool,
    retain_as_published: bool,
    retain_policy: RetainPolicy
) -> None:
    """Test subscribing with options."""
    subscriptions = Subscriptions(mock_handlers, mock_connection, weakref.ref(mock_client))

    subscriptions.subscribe(
        "test/topic",
        dummy_callback,
        max_qos=max_qos,
        share_name="test_share",
        no_local=no_local,
        retain_as_published=retain_as_published,
        retain_policy=retain_policy,
        sub_id=23,
        user_properties=[("key", "value")],
    )
    expected_opts = max_qos.value | (retain_policy << 4) | (retain_as_published << 3) | (no_local << 2)
    mock_connection.send.assert_called_once_with(MQTTSubscribePacket(
        topics=[("$share/test_share/test/topic", expected_opts)],
        packet_id=1,
        properties=MQTTSubscribeProps(
            SubscriptionIdentifier={23},
            UserProperty=(("key", "value"),),
        ),
    ))


def test_subscriptions_wait_for_suback(
    mock_handlers: MagicMock,
    mock_client: Mock,
    mock_connection: Mock
) -> None:
    """Test waiting for a SUBACK packet."""
    subscriptions = Subscriptions(mock_handlers, mock_connection, weakref.ref(mock_client))

    handle = subscriptions.subscribe("test/topic", dummy_callback)
    assert handle is not None

    # Simulate receiving a SUBACK packet
    suback_packet = MQTTSubAckPacket(
        packet_id=1,
        reason_codes=[MQTTReasonCode.GrantedQoS2],
    )
    subscriptions.handle_suback(suback_packet)

    assert handle.wait_for_ack(timeout=0.1) == suback_packet
    assert handle.ack == suback_packet


@pytest.mark.parametrize("session_present", [True, False])
def test_subscriptions_subscribe_failure(
    mock_handlers: MagicMock,
    mock_client: Mock,
    mock_connection: Mock,
    session_present: bool
) -> None:
    """Test replaying SUBSCRIBE packets on reconnection after calling in a bad state."""
    subscriptions = Subscriptions(mock_handlers, mock_connection, weakref.ref(mock_client))

    mock_connection.send.side_effect = InvalidStateError("TEST")
    handle = subscriptions.subscribe("test/topic", dummy_callback)

    assert handle is not None

    mock_connection.send.side_effect = None
    mock_connection.reset_mock()

    subscriptions.handle_connack(MQTTConnAckPacket(session_present=session_present))

    mock_connection.send.assert_called_once_with(MQTTSubscribePacket(
        topics=[("test/topic", 2)],
        packet_id=1,
    ))
    mock_connection.reset_mock()

    subscriptions.handle_connack(MQTTConnAckPacket(session_present=session_present))

    if not session_present:
        # If the session was not present, we should send the SUBSCRIBE packet again.
        mock_connection.send.assert_called_once_with(MQTTSubscribePacket(
            topics=[("test/topic", 2)],
            packet_id=1,
        ))


@pytest.mark.parametrize(("tf", "topic"), [
    ("test/topic", "test/topic"),
    ("test/+", "test/topic"),
    ("#", "test/topic"),
])
def test_subscriptions_handle_publish(
    tf: str,
    topic: str,
    mock_handlers: MagicMock,
    mock_client: Mock,
    mock_connection: Mock
) -> None:
    """Test handling a publish packet."""
    subscriptions = Subscriptions(mock_handlers, mock_connection, weakref.ref(mock_client))

    recvd = []
    def callback(client: Client, packet: MQTTPublishPacket) -> None:
        recvd.append(packet)
    subscriptions.subscribe(tf, callback)

    publish_packet = MQTTPublishPacket(
        topic=topic,
        payload=b"test payload",
    )
    subscriptions.handle_publish(publish_packet)

    assert recvd == [publish_packet]


def test_subscriptions_slots(
    mock_handlers: MagicMock,
    mock_client: Mock,
    mock_connection: Mock
) -> None:
    subscriptions = Subscriptions(mock_handlers, mock_connection, weakref.ref(mock_client))
    # Slots all the way down.
    assert not hasattr(subscriptions, "__dict__")
    assert hasattr(subscriptions, "__weakref__")
    assert all(hasattr(subscriptions, attr) for attr in subscriptions.__slots__)
