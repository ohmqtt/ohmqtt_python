import pytest

from ohmqtt.connection import Connection, MessageHandlers
from ohmqtt.packet import (
    MQTTPublishPacket,
    MQTTPubAckPacket,
    MQTTPubRecPacket,
    MQTTPubRelPacket,
    MQTTPubCompPacket,
)
from ohmqtt.session import Session


@pytest.fixture
def mock_handlers(mocker):
    return mocker.Mock(spec=MessageHandlers)


@pytest.fixture
def mock_connection(mocker):
    return mocker.Mock(spec=Connection)


def test_session_publish_qos0(mock_handlers, mock_connection):
    session = Session(mock_handlers, mock_connection)
    mock_connection.can_send.return_value = True

    handle = session.publish("test/topic", b"test payload")
    mock_connection.send.assert_called_with(MQTTPublishPacket(
        topic="test/topic",
        payload=b"test payload",
    ).encode())
    mock_connection.send.reset_mock()
    assert handle.is_acked() is False
    assert handle.wait_for_ack() is False


def test_session_publish_qos1(mock_handlers, mock_connection):
    session = Session(mock_handlers, mock_connection)
    session.server_receive_maximum = 20
    mock_connection.can_send.return_value = True

    handle = session.publish("test/topic", b"test payload", qos=1)
    mock_connection.send.assert_called_with(MQTTPublishPacket(
        topic="test/topic",
        payload=b"test payload",
        qos=1,
        packet_id=1,
    ).encode())
    mock_connection.send.reset_mock()
    assert handle.is_acked() is False
    assert handle.wait_for_ack(0.001) is False

    session.handle_puback(MQTTPubAckPacket(packet_id=1))
    assert handle.is_acked() is True
    assert handle.wait_for_ack(0.001) is True


def test_session_publish_qos2(mock_handlers, mock_connection):
    session = Session(mock_handlers, mock_connection)
    session.server_receive_maximum = 20
    mock_connection.can_send.return_value = True

    handle = session.publish("test/topic", b"test payload", qos=2)
    mock_connection.send.assert_called_with(MQTTPublishPacket(
        topic="test/topic",
        payload=b"test payload",
        qos=2,
        packet_id=1,
    ).encode())
    mock_connection.send.reset_mock()
    assert handle.is_acked() is False
    assert handle.wait_for_ack(0.001) is False

    session.handle_pubrec(MQTTPubRecPacket(packet_id=1))
    assert handle.is_acked() is False
    assert handle.wait_for_ack(0.001) is False

    mock_connection.send.assert_called_with(MQTTPubRelPacket(packet_id=1).encode())
    mock_connection.send.reset_mock()

    session.handle_pubcomp(MQTTPubCompPacket(packet_id=1))
    assert handle.is_acked() is True
    assert handle.wait_for_ack(0.001) is True


def test_session_slots(mock_handlers, mock_connection):
    session = Session(mock_handlers, mock_connection)
    assert not hasattr(session, "__dict__")
    assert all(hasattr(session, attr) for attr in session.__slots__), \
        [attr for attr in session.__slots__ if not hasattr(session, attr)]
