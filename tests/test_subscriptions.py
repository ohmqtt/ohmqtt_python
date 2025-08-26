from typing import cast
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
    MQTTUnsubscribePacket,
    MQTTSubAckPacket,
    MQTTUnsubAckPacket,
    MQTTConnAckPacket,
)
from ohmqtt.property import MQTTPublishProps, MQTTSubscribeProps
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


def test_subscriptions_multi_subscribe(
    mock_handlers: MagicMock,
    mock_client: Mock,
    mock_connection: Mock
) -> None:
    """Test subscribing multiple times to the same topic."""
    subscriptions = Subscriptions(mock_handlers, mock_connection, weakref.ref(mock_client))

    handle1 = subscriptions.subscribe("test/topic", dummy_callback)
    assert handle1 is not None

    handle2 = subscriptions.subscribe("test/topic", dummy_callback)
    assert handle2 is not None
    assert handle1 != handle2

    assert handle1.wait_for_ack(timeout=0.001) is None
    assert handle1.ack is None
    assert handle1.failed

    # Simulate receiving a SUBACK packet
    suback_packet = MQTTSubAckPacket(
        packet_id=1,
        reason_codes=[MQTTReasonCode.GrantedQoS2],
    )
    subscriptions.handle_suback(suback_packet)

    assert handle2.wait_for_ack(timeout=0.1) == suback_packet
    assert handle2.ack == suback_packet


def test_subscriptions_wait_for_suback(
    mock_handlers: MagicMock,
    mock_client: Mock,
    mock_connection: Mock
) -> None:
    """Test waiting for a SUBACK packet."""
    subscriptions = Subscriptions(mock_handlers, mock_connection, weakref.ref(mock_client))

    handle = subscriptions.subscribe("test/topic", dummy_callback)
    assert handle is not None
    assert handle.wait_for_ack(timeout=0.001) is None

    # Simulate receiving a SUBACK packet
    suback_packet = MQTTSubAckPacket(
        packet_id=1,
        reason_codes=[MQTTReasonCode.GrantedQoS2],
    )
    subscriptions.handle_suback(suback_packet)

    assert handle.wait_for_ack(timeout=0.001) == suback_packet
    assert handle.ack == suback_packet

    mock_handlers.reset_mock()
    del mock_handlers
    mock_client.reset_mock()
    del mock_client
    mock_connection.reset_mock()
    del mock_connection
    del subscriptions
    with pytest.raises(RuntimeError, match="Subscriptions went out of scope"):
        handle.wait_for_ack(timeout=0.001)


def test_subscriptions_wait_for_unsuback(
    mock_handlers: MagicMock,
    mock_client: Mock,
    mock_connection: Mock
) -> None:
    """Test waiting for a SUBACK packet."""
    subscriptions = Subscriptions(mock_handlers, mock_connection, weakref.ref(mock_client))

    subscriptions.subscribe("test/topic", dummy_callback)

    # Simulate receiving a SUBACK packet
    suback_packet = MQTTSubAckPacket(
        packet_id=1,
        reason_codes=[MQTTReasonCode.GrantedQoS2],
    )
    subscriptions.handle_suback(suback_packet)

    handle = subscriptions.unsubscribe("test/topic")
    assert handle is not None
    assert handle.wait_for_ack(timeout=0.001) is None

    # Simulate receiving an UNSUBACK packet
    unsuback_packet = MQTTUnsubAckPacket(
        packet_id=1,
        reason_codes=[MQTTReasonCode.Success],
    )
    subscriptions.handle_unsuback(unsuback_packet)

    assert handle.wait_for_ack(timeout=0.001) == unsuback_packet
    assert handle.ack == unsuback_packet

    mock_handlers.reset_mock()
    del mock_handlers
    mock_client.reset_mock()
    del mock_client
    mock_connection.reset_mock()
    del mock_connection
    del subscriptions
    with pytest.raises(RuntimeError, match="Subscriptions went out of scope"):
        handle.wait_for_ack(timeout=0.001)


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


def test_subscriptions_unsubscribe_failure(
    mock_handlers: MagicMock,
    mock_client: Mock,
    mock_connection: Mock
) -> None:
    subscriptions = Subscriptions(mock_handlers, mock_connection, weakref.ref(mock_client))

    subscriptions.subscribe("test/topic", dummy_callback)
    mock_connection.send.assert_called_once_with(MQTTSubscribePacket(
        topics=[("test/topic", 2)],
        packet_id=1,
    ))
    subscriptions.handle_suback(MQTTSubAckPacket(
        packet_id=1,
        reason_codes=[MQTTReasonCode.GrantedQoS2],
    ))

    mock_connection.send.side_effect = InvalidStateError("TEST")
    handle = subscriptions.unsubscribe("test/topic")

    assert handle is not None

    mock_connection.send.side_effect = None
    mock_connection.reset_mock()

    subscriptions.handle_connack(MQTTConnAckPacket(session_present=True))

    mock_connection.send.assert_called_once_with(MQTTUnsubscribePacket(
        topics=["test/topic"],
        packet_id=1,
    ))
    mock_connection.reset_mock()


def test_subscriptions_unsubscribe_null(mock_handlers: MagicMock, mock_client: Mock, mock_connection: Mock) -> None:
    subscriptions = Subscriptions(mock_handlers, mock_connection, weakref.ref(mock_client))

    handle = subscriptions.unsubscribe("test/topic")
    assert handle is None


def test_subscriptions_unsubscribe_redundant(mock_handlers: MagicMock, mock_client: Mock, mock_connection: Mock) -> None:
    subscriptions = Subscriptions(mock_handlers, mock_connection, weakref.ref(mock_client))

    subscriptions.subscribe("test/topic", dummy_callback)
    subscriptions.handle_suback(MQTTSubAckPacket(
        packet_id=1,
        reason_codes=[MQTTReasonCode.GrantedQoS2],
    ))

    handle1 = subscriptions.unsubscribe("test/topic")
    handle2 = subscriptions.unsubscribe("test/topic")
    assert handle1 is not None
    assert handle1.failed
    subscriptions.handle_unsuback(MQTTUnsubAckPacket(
        packet_id=1,
        reason_codes=[MQTTReasonCode.GrantedQoS2],
    ))
    assert handle2 is not None
    assert handle2.ack is not None

    mock_connection.reset_mock()
    assert subscriptions.unsubscribe("test/topic") is None
    mock_connection.send.assert_not_called()


@pytest.mark.parametrize(("tf", "topic"), [
    ("test/topic", "test/topic"),
    ("test/+", "test/topic"),
    ("#", "test/topic"),
])
def test_subscriptions_handle_publish(
    tf: str,
    topic: str,
    mock_handlers: MagicMock,
    mock_connection: Mock
) -> None:
    """Test handling a publish packet."""
    mock_client = cast(Client, frozenset())
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

    del mock_client
    with pytest.raises(RuntimeError, match="Client went out of scope"):
        subscriptions.handle_publish(publish_packet)


def test_subscriptions_handle_publish_shared(
    mock_handlers: MagicMock,
    mock_client: Mock,
    mock_connection: Mock
) -> None:
    """Test handling a publish packet with shared subscriptions."""
    subscriptions = Subscriptions(mock_handlers, mock_connection, weakref.ref(mock_client))

    recvd1 = []
    recvd2 = []
    def callback1(client: Client, packet: MQTTPublishPacket) -> None:
        recvd1.append(packet)
    def callback2(client: Client, packet: MQTTPublishPacket) -> None:
        recvd2.append(packet)

    subscriptions.subscribe("test/topic", callback1)
    subscriptions.subscribe("test/topic", callback2, share_name="test_share")

    publish_packet = MQTTPublishPacket(
        topic="test/topic",
        payload=b"test payload",
    )
    subscriptions.handle_publish(publish_packet)

    assert recvd1 == [publish_packet]
    assert recvd2 == [publish_packet]


def test_subscriptions_handle_publish_sub_id(
    mock_handlers: MagicMock,
    mock_client: Mock,
    mock_connection: Mock
) -> None:
    """Test handling a publish packet with subscription identifier."""
    subscriptions = Subscriptions(mock_handlers, mock_connection, weakref.ref(mock_client))

    recvd1 = []
    recvd2 = []
    def callback1(client: Client, packet: MQTTPublishPacket) -> None:
        recvd1.append(packet)
    def callback2(client: Client, packet: MQTTPublishPacket) -> None:
        recvd2.append(packet)

    subscriptions.subscribe("test/topic", callback1)
    subscriptions.subscribe("test/+", callback2, sub_id=1)

    publish_packet = MQTTPublishPacket(
        topic="test/topic",
        payload=b"test payload",
        properties=MQTTPublishProps(SubscriptionIdentifier={1}),
    )
    subscriptions.handle_publish(publish_packet)

    assert recvd1 == []
    assert recvd2 == [publish_packet]


def test_subscriptions_handle_publish_exception(
    mock_handlers: MagicMock,
    mock_client: Mock,
    mock_connection: Mock
) -> None:
    """Test handling a publish packet with a broken callback."""
    subscriptions = Subscriptions(mock_handlers, mock_connection, weakref.ref(mock_client))

    recvd = []
    def callback1(client: Client, packet: MQTTPublishPacket) -> None:
        raise RuntimeError("TEST")
    def callback2(client: Client, packet: MQTTPublishPacket) -> None:
        recvd.append(packet)

    subscriptions.subscribe("test/topic", callback1)
    subscriptions.subscribe("test/+", callback2)

    publish_packet = MQTTPublishPacket(
        topic="test/topic",
        payload=b"test payload",
    )
    subscriptions.handle_publish(publish_packet)

    assert recvd == [publish_packet]


def test_subscriptions_packet_id(
    mock_handlers: MagicMock,
    mock_client: Mock,
    mock_connection: Mock
) -> None:
    subscriptions = Subscriptions(mock_handlers, mock_connection, weakref.ref(mock_client))

    # Test the private methods because the public interface is slow.
    for n in range(0xffff + 5):
        assert subscriptions._get_next_sub_packet_id() == (n % 0xffff) + 1  # noqa: SLF001

    for n in range(0xffff + 5):
        assert subscriptions._get_next_unsub_packet_id() == (n % 0xffff) + 1  # noqa: SLF001


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
