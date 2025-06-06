from unittest.mock import Mock

import pytest
from pytest_mock import MockerFixture

from ohmqtt.connection import Connection, ConnectParams, MessageHandlers
from ohmqtt.packet import (
    MQTTConnAckPacket,
    MQTTPublishPacket,
    MQTTPubAckPacket,
    MQTTPubRecPacket,
    MQTTPubRelPacket,
    MQTTPubCompPacket,
)
from ohmqtt.property import MQTTConnAckProps, MQTTPublishProps
from ohmqtt.session import Session
from ohmqtt.subscriptions import Subscriptions
from ohmqtt.topic_alias import AliasPolicy


@pytest.fixture
def mock_handlers(mocker: MockerFixture) -> Mock:
    return mocker.Mock(spec=MessageHandlers)  # type: ignore[no-any-return]


@pytest.fixture
def mock_connection(mocker: MockerFixture) -> Mock:
    return mocker.Mock(spec=Connection)  # type: ignore[no-any-return]


@pytest.fixture
def mock_subscriptions(mocker: MockerFixture) -> Mock:
    return mocker.Mock(spec=Subscriptions)  # type: ignore[no-any-return]


def test_session_publish_qos0(mock_handlers: Mock, mock_subscriptions: Mock, mock_connection: Mock) -> None:
    session = Session(mock_handlers, mock_subscriptions, mock_connection)
    mock_connection.can_send.return_value = True

    handle = session.publish("test/topic", b"test payload")
    mock_connection.send.assert_called_with(MQTTPublishPacket(
        topic="test/topic",
        payload=b"test payload",
    ))
    mock_connection.send.reset_mock()
    assert handle.is_acked() is False
    assert handle.wait_for_ack() is False


def test_session_publish_qos1(mock_handlers: Mock, mock_subscriptions: Mock, mock_connection: Mock) -> None:
    session = Session(mock_handlers, mock_subscriptions, mock_connection)
    session.server_receive_maximum = 20
    mock_connection.can_send.return_value = True

    handle = session.publish("test/topic", b"test payload", qos=1)
    mock_connection.send.assert_called_with(MQTTPublishPacket(
        topic="test/topic",
        payload=b"test payload",
        qos=1,
        packet_id=1,
    ))
    mock_connection.send.reset_mock()
    assert handle.is_acked() is False
    assert handle.wait_for_ack(0.001) is False

    session.handle_puback(MQTTPubAckPacket(packet_id=1))
    assert handle.is_acked() is True
    assert handle.wait_for_ack(0.001) is True


def test_session_publish_qos2(mock_handlers: Mock, mock_subscriptions: Mock, mock_connection: Mock) -> None:
    session = Session(mock_handlers, mock_subscriptions, mock_connection)
    session.server_receive_maximum = 20
    mock_connection.can_send.return_value = True

    handle = session.publish("test/topic", b"test payload", qos=2)
    mock_connection.send.assert_called_with(MQTTPublishPacket(
        topic="test/topic",
        payload=b"test payload",
        qos=2,
        packet_id=1,
    ))
    mock_connection.send.reset_mock()
    assert handle.is_acked() is False
    assert handle.wait_for_ack(0.001) is False

    session.handle_pubrec(MQTTPubRecPacket(packet_id=1))
    assert handle.is_acked() is False
    assert handle.wait_for_ack(0.001) is False

    mock_connection.send.assert_called_with(MQTTPubRelPacket(packet_id=1))
    mock_connection.send.reset_mock()

    session.handle_pubcomp(MQTTPubCompPacket(packet_id=1))
    assert handle.is_acked() is True
    assert handle.wait_for_ack(0.001) is True


@pytest.mark.parametrize("db_path", [":memory:", ""])
@pytest.mark.parametrize("qos", [0, 1, 2])
def test_session_publish_alias(db_path: str, qos: int, mock_handlers: Mock, mock_subscriptions: Mock, mock_connection: Mock) -> None:
    session = Session(mock_handlers, mock_subscriptions, mock_connection, db_path=db_path)
    session.set_params(ConnectParams(client_id="test_client", clean_start=True))
    mock_connection.can_send.return_value = True

    session.handle_connack(MQTTConnAckPacket(properties=MQTTConnAckProps(TopicAliasMaximum=255)))

    session.publish("test/topic1", b"test payload", qos=qos, alias_policy=AliasPolicy.NEVER)
    mock_connection.send.assert_called_with(MQTTPublishPacket(
        topic="test/topic1",
        payload=b"test payload",
        qos=qos,
        packet_id=1 if qos > 0 else 0,
    ))
    mock_connection.send.reset_mock()

    session.publish("test/topic2", b"test payload", qos=qos, alias_policy=AliasPolicy.TRY)
    mock_connection.send.assert_called_with(MQTTPublishPacket(
        topic="test/topic2",
        payload=b"test payload",
        qos=qos,
        packet_id=2 if qos > 0 else 0,
        properties=MQTTPublishProps(TopicAlias=1),
    ))
    mock_connection.send.reset_mock()

    if qos > 0:
        with pytest.raises(ValueError):
            session.publish("test/topic3", b"test payload", qos=qos, alias_policy=AliasPolicy.ALWAYS)
    else:
        session.publish("test/topic3", b"test payload", qos=qos, alias_policy=AliasPolicy.ALWAYS)
        mock_connection.send.assert_called_with(MQTTPublishPacket(
            topic="test/topic3",
            payload=b"test payload",
            qos=qos,
            packet_id=3 if qos > 0 else 0,
            properties=MQTTPublishProps(TopicAlias=2),
        ))
        mock_connection.send.reset_mock()


def test_session_handle_publish_qos0(mock_handlers: Mock, mock_subscriptions: Mock, mock_connection: Mock) -> None:
    session = Session(mock_handlers, mock_subscriptions, mock_connection)
    mock_connection.can_send.return_value = True

    packet = MQTTPublishPacket(
        topic="test/topic",
        payload=b"test payload",
        qos=0,
    )
    session.handle_publish(packet)
    mock_subscriptions.handle_publish.assert_called_once_with(packet)


def test_session_handle_publish_qos1(mock_handlers: Mock, mock_subscriptions: Mock, mock_connection: Mock) -> None:
    session = Session(mock_handlers, mock_subscriptions, mock_connection)
    mock_connection.can_send.return_value = True

    packet = MQTTPublishPacket(
        topic="test/topic",
        payload=b"test payload",
        packet_id=3,
        qos=1,
    )
    session.handle_publish(packet)
    mock_subscriptions.handle_publish.assert_called_once_with(packet)
    mock_connection.send.assert_called_once_with(MQTTPubAckPacket(packet_id=3))


def test_session_handle_publish_qos2(mock_handlers: Mock, mock_subscriptions: Mock, mock_connection: Mock) -> None:
    session = Session(mock_handlers, mock_subscriptions, mock_connection)
    mock_connection.can_send.return_value = True

    packet = MQTTPublishPacket(
        topic="test/topic",
        payload=b"test payload",
        packet_id=3,
        qos=2,
    )
    session.handle_publish(packet)
    mock_subscriptions.handle_publish.assert_called_once_with(packet)
    mock_subscriptions.handle_publish.reset_mock()
    mock_connection.send.assert_called_once_with(MQTTPubRecPacket(packet_id=3))
    mock_connection.send.reset_mock()

    # Filter duplicates.
    session.handle_publish(packet)
    mock_subscriptions.handle_publish.assert_not_called()
    mock_connection.send.assert_called_once_with(MQTTPubRecPacket(packet_id=3))
    mock_connection.send.reset_mock()

    session.handle_pubrel(MQTTPubRelPacket(packet_id=3))
    mock_connection.send.assert_called_once_with(MQTTPubCompPacket(packet_id=3))
    mock_connection.send.reset_mock()
    # After PUBREL, message with same packet_id should be treated as a new application message.
    session.handle_publish(packet)
    mock_subscriptions.handle_publish.assert_called_once_with(packet)
    mock_connection.send.assert_called_once_with(MQTTPubRecPacket(packet_id=3))


def test_session_slots(mock_handlers: Mock, mock_subscriptions: Mock, mock_connection: Mock) -> None:
    session = Session(mock_handlers, mock_subscriptions, mock_connection)
    assert not hasattr(session, "__dict__")
    assert all(hasattr(session, attr) for attr in session.__slots__), \
        [attr for attr in session.__slots__ if not hasattr(session, attr)]
