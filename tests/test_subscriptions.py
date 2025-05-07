import pytest
import weakref

from ohmqtt.client import Client
from ohmqtt.connection import Connection, MessageHandlers, InvalidStateError
from ohmqtt.packet import (
    MQTTPublishPacket,
    MQTTSubscribePacket,
    MQTTSubAckPacket,
    MQTTUnsubscribePacket,
    MQTTUnsubAckPacket,
    MQTTConnAckPacket,
)
from ohmqtt.property import MQTTSubscribeProps, MQTTUnsubscribeProps
from ohmqtt.subscriptions import Subscriptions, RetainPolicy


@pytest.fixture
def mock_client(mocker):
    return mocker.Mock(spec=Client)


@pytest.fixture
def mock_connection(mocker):
    return mocker.Mock(spec=Connection)


@pytest.fixture
def mock_handlers(mocker):
    return mocker.MagicMock(spec=MessageHandlers)


def test_subscriptions_registration(mock_client, mock_connection):
    handlers = MessageHandlers()
    with handlers:
        subscriptions = Subscriptions(handlers, mock_connection, weakref.ref(mock_client))
    assert subscriptions.handle_publish in handlers.get_handlers(MQTTPublishPacket)
    assert subscriptions.handle_suback in handlers.get_handlers(MQTTSubAckPacket)
    assert subscriptions.handle_unsuback in handlers.get_handlers(MQTTUnsubAckPacket)
    assert subscriptions.handle_connack in handlers.get_handlers(MQTTConnAckPacket)

@pytest.mark.parametrize("max_qos", [0, 1, 2])
@pytest.mark.parametrize("no_local", [True, False])
@pytest.mark.parametrize("retain_as_published", [True, False])
@pytest.mark.parametrize("retain_policy", [RetainPolicy.NEVER, RetainPolicy.ONCE, RetainPolicy.ALWAYS])
def test_subscriptions_subscribe_opts(mock_handlers, mock_client, mock_connection, max_qos, no_local, retain_as_published, retain_policy):
    """Test subscribing with options."""
    subscriptions = Subscriptions(mock_handlers, mock_connection, weakref.ref(mock_client))

    subscriptions.subscribe(
        "test/topic",
        lambda: None,
        max_qos=max_qos,
        share_name="test_share",
        no_local=no_local,
        retain_as_published=retain_as_published,
        retain_policy=retain_policy,
        sub_id=23,
        user_properties=[("key", "value")],
    )
    expected_opts = max_qos | (retain_policy << 4) | (retain_as_published << 3) | (no_local << 2)
    mock_connection.send.assert_called_once_with(MQTTSubscribePacket(
        topics=[("$share/test_share/test/topic", expected_opts)],
        packet_id=1,
        properties=MQTTSubscribeProps(
            SubscriptionIdentifier={23},
            UserProperty=(("key", "value"),),
        ),
    ))


def test_subscriptions_handle_unsubscribe(mock_handlers, mock_client, mock_connection):
    """Test using a subscription handle to unsubscribe."""
    subscriptions = Subscriptions(mock_handlers, mock_connection, weakref.ref(mock_client))

    sub_handle = subscriptions.subscribe("test/topic", lambda: None)
    mock_connection.send.assert_called_once_with(MQTTSubscribePacket(
        topics=[("test/topic", 2)],
        packet_id=1,
    ))

    unsub_handle = sub_handle.unsubscribe()
    mock_connection.send.assert_called_with(MQTTUnsubscribePacket(
        topics=["test/topic"],
        packet_id=1,
        properties=MQTTUnsubscribeProps(),
    ))

    unsuback_packet = MQTTUnsubAckPacket(packet_id=1)
    subscriptions.handle_unsuback(unsuback_packet)

    assert unsub_handle.wait_for_ack(timeout=0.1) == unsuback_packet
    assert unsub_handle.ack == unsuback_packet


def test_subscriptions_wait_for_suback(mock_handlers, mock_client, mock_connection):
    """Test waiting for a SUBACK packet."""
    subscriptions = Subscriptions(mock_handlers, mock_connection, weakref.ref(mock_client))

    handle = subscriptions.subscribe("test/topic", lambda: None)

    # Simulate receiving a SUBACK packet
    suback_packet = MQTTSubAckPacket(
        packet_id=1,
        reason_codes=[0],
        properties=MQTTSubscribeProps(),
    )
    subscriptions.handle_suback(suback_packet)

    assert handle.wait_for_ack(timeout=0.1) == suback_packet
    assert handle.ack == suback_packet


@pytest.mark.parametrize("session_present", [True, False])
def test_subscriptions_subscribe_failure(mock_handlers, mock_client, mock_connection, session_present):
    """Test replaying SUBSCRIBE packets on reconnection after calling in a bad state."""
    subscriptions = Subscriptions(mock_handlers, mock_connection, weakref.ref(mock_client))

    mock_connection.send.side_effect = InvalidStateError("TEST")
    handle = subscriptions.subscribe("test/topic", lambda: None)

    assert handle is None

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


@pytest.mark.parametrize("session_present", [True, False])
def test_subscriptions_multi_subscribe(mock_handlers, mock_client, mock_connection, session_present):
    """Test subscribing to the same topic multiple times."""
    subscriptions = Subscriptions(mock_handlers, mock_connection, weakref.ref(mock_client))

    handle1 = subscriptions.subscribe("test/topic", lambda: None)
    handle2 = subscriptions.subscribe("test/topic", lambda: None)

    assert handle1 is not None
    assert handle2 is not None
    assert handle1 != handle2

    assert mock_connection.send.call_count == 2
    assert mock_connection.send.call_args_list[0][0][0] == MQTTSubscribePacket(
        topics=[("test/topic", 2)],
        packet_id=1,
    )
    assert mock_connection.send.call_args_list[1][0][0] == MQTTSubscribePacket(
        topics=[("test/topic", 2)],
        packet_id=2,
    )
    mock_connection.reset_mock()

    suback_packet = MQTTSubAckPacket(
        packet_id=1,
        reason_codes=[0],
        properties=MQTTSubscribeProps(),
    )
    subscriptions.handle_suback(suback_packet)

    assert handle1.wait_for_ack(timeout=0.1) == suback_packet
    assert handle2.wait_for_ack(timeout=0.1) is None

    suback_packet.packet_id = 2
    subscriptions.handle_suback(suback_packet)

    assert handle2.wait_for_ack(timeout=0.1) == suback_packet

    subscriptions.handle_connack(MQTTConnAckPacket(session_present=session_present))

    if not session_present:
        # If the session was not present, we should send the SUBSCRIBE packets again.
        assert mock_connection.send.call_count == 2
        assert mock_connection.send.call_args_list[0][0][0] == MQTTSubscribePacket(
            topics=[("test/topic", 2)],
            packet_id=1,
        )
        assert mock_connection.send.call_args_list[1][0][0] == MQTTSubscribePacket(
            topics=[("test/topic", 2)],
            packet_id=2,
        )
        mock_connection.reset_mock()


def test_subscriptions_slots(mock_handlers, mock_client, mock_connection):
    subscriptions = Subscriptions(mock_handlers, mock_connection, weakref.ref(mock_client))
    # Slots all the way down.
    assert not hasattr(subscriptions, "__dict__")
    assert hasattr(subscriptions, "__weakref__")
    assert all(hasattr(subscriptions, attr) for attr in subscriptions.__slots__)
