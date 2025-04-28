import pytest

from ohmqtt.connection import Connection
from ohmqtt.mqtt_spec import MQTTReasonCode
from ohmqtt.packet import (
    decode_packet,
    MQTTPacket,
    MQTTConnAckPacket,
    MQTTSubscribePacket,
    MQTTSubAckPacket,
    MQTTUnsubscribePacket,
    MQTTUnsubAckPacket,
    MQTTPublishPacket,
    MQTTPubAckPacket,
    MQTTPubRecPacket,
    MQTTPubRelPacket,
    MQTTPubCompPacket,
    MQTTAuthPacket,
)
from ohmqtt.session import Session, SessionConnectParams


@pytest.fixture
def callbacks(mocker):
    """Fixture to retain calls to Session callbacks."""
    _callbacks = {k: mocker.Mock() for k in ("auth", "close", "open", "message")}
    return _callbacks


def expect_from_session(MockConnection, mock_connection, packet_type) -> MQTTPacket:
    """Expect a packet to be sent from the session."""
    mock_connection.send.assert_called_once()
    packet_encoded = mock_connection.send.call_args[0][0]
    packet = decode_packet(packet_encoded)
    assert isinstance(packet, packet_type)
    mock_connection.send.reset_mock()
    return packet


def expect_message_from_session(callbacks, publish_packet) -> None:
    callbacks["message"].assert_called_once_with(publish_packet)
    callbacks["message"].reset_mock()


def send_to_session(MockConnection, mock_connection, packet: MQTTPacket) -> None:
    """Send a packet to the session."""
    MockConnection.call_args.kwargs["read_callback"](packet)


def test_session_happy_path(callbacks, mocker):
    mock_connection = mocker.Mock(spec=Connection)
    MockConnection = mocker.patch("ohmqtt.session.Connection", return_value=mock_connection)
    session = Session(
        auth_callback=callbacks["auth"],
        close_callback=callbacks["close"],
        open_callback=callbacks["open"],
        message_callback=callbacks["message"],
    )
    session.connect(SessionConnectParams("localhost", 1883))
    mock_connection.connect.assert_called_once()
    assert mock_connection.connect.call_args[0][0].host == "localhost"
    assert mock_connection.connect.call_args[0][0].port == 1883

    # Assert that the Connection was created.
    assert MockConnection.call_count == 1

    # Send back a CONNACK packet.
    connack_packet = MQTTConnAckPacket(properties={"AssignedClientIdentifier": "test_client"})
    MockConnection.call_args.kwargs["open_callback"](connack_packet)
    callbacks["open"].assert_called_once_with()
    callbacks["open"].reset_mock()

    # Server sends an AUTH packet to the Session.
    auth_packet = MQTTAuthPacket(
        reason_code=MQTTReasonCode.ContinueAuthentication,
        properties={"AuthenticationMethod": "test_auth"},
    )
    send_to_session(MockConnection, mock_connection, auth_packet)
    callbacks["auth"].assert_called_once_with(auth_packet.reason_code, "test_auth", None, None, None)
    callbacks["auth"].reset_mock()

    # User sends an AUTH packet to the server through the Session.
    session.auth(
        reason_code=MQTTReasonCode.Success,
        authentication_method="test_auth",
        authentication_data=b"test_auth_data",
    )
    auth_packet = expect_from_session(MockConnection, mock_connection, MQTTAuthPacket)
    assert auth_packet.reason_code == MQTTReasonCode.Success
    assert auth_packet.properties["AuthenticationMethod"] == "test_auth"
    assert auth_packet.properties["AuthenticationData"] == b"test_auth_data"

    # SUBSCRIBE to a topic.
    session.subscribe("topic", 2)
    subscribe_packet = expect_from_session(MockConnection, mock_connection, MQTTSubscribePacket)
    assert subscribe_packet.packet_id > 0
    assert subscribe_packet.topics == [("topic", 2),]

    # SUBACK the subscription.
    suback_packet = MQTTSubAckPacket(
        packet_id=subscribe_packet.packet_id,
        reason_codes=[MQTTReasonCode.Success],
    )
    send_to_session(MockConnection, mock_connection, suback_packet)

    # PUBLISH a message with qos 0.
    session.publish("topic", b"message 0", qos=0)
    publish_packet = expect_from_session(MockConnection, mock_connection, MQTTPublishPacket)
    assert publish_packet.topic == "topic"
    assert publish_packet.payload == b"message 0"
    assert publish_packet.qos == 0
    assert publish_packet.packet_id == 0

    # We should receive a message.
    send_to_session(MockConnection, mock_connection, publish_packet)
    expect_message_from_session(callbacks, publish_packet)

    # PUBLISH a message with qos 1.
    session.publish("topic", b"message 1", qos=1)
    publish_packet = expect_from_session(MockConnection, mock_connection, MQTTPublishPacket)
    assert publish_packet.topic == "topic"
    assert publish_packet.payload == b"message 1"
    assert publish_packet.qos == 1
    assert publish_packet.packet_id > 0

    # PUBACK the PUBLISH.
    puback_packet = MQTTPubAckPacket(
        packet_id=publish_packet.packet_id,
        reason_code=MQTTReasonCode.Success,
    )
    send_to_session(MockConnection, mock_connection, puback_packet)

    # We should receive a message.
    send_to_session(MockConnection, mock_connection, publish_packet)
    expect_message_from_session(callbacks, publish_packet)

    # Session should send a PUBACK packet.
    puback_packet = expect_from_session(MockConnection, mock_connection, MQTTPubAckPacket)
    assert puback_packet.packet_id == publish_packet.packet_id
    assert puback_packet.reason_code == MQTTReasonCode.Success

    # PUBLISH a message with qos 2.
    session.publish("topic", b"message 2", qos=2)
    publish_packet = expect_from_session(MockConnection, mock_connection, MQTTPublishPacket)
    assert publish_packet.topic == "topic"
    assert publish_packet.payload == b"message 2"
    assert publish_packet.qos == 2
    assert publish_packet.packet_id > 0

    # PUBREC the PUBLISH.
    pubrec_packet = MQTTPubRecPacket(
        packet_id=publish_packet.packet_id,
        reason_code=MQTTReasonCode.Success,
    )
    send_to_session(MockConnection, mock_connection, pubrec_packet)

    # Session should send a PUBREL packet.
    pubrel_packet = expect_from_session(MockConnection, mock_connection, MQTTPubRelPacket)
    assert pubrel_packet.packet_id == publish_packet.packet_id
    assert pubrel_packet.reason_code == MQTTReasonCode.Success

    # PUBCOMP the PUBREL.
    pubcomp_packet = MQTTPubCompPacket(
        packet_id=publish_packet.packet_id,
        reason_code=MQTTReasonCode.Success,
    )
    send_to_session(MockConnection, mock_connection, pubcomp_packet)

    # We should receive a message.
    send_to_session(MockConnection, mock_connection, publish_packet)
    expect_message_from_session(callbacks, publish_packet)

    # Session should send a PUBREC packet.
    pubrec_packet = expect_from_session(MockConnection, mock_connection, MQTTPubRecPacket)
    assert pubrec_packet.packet_id == publish_packet.packet_id
    assert pubrec_packet.reason_code == MQTTReasonCode.Success

    # PUBREL the PUBREC.
    pubrel_packet = MQTTPubRelPacket(
        packet_id=publish_packet.packet_id,
        reason_code=MQTTReasonCode.Success,
    )
    send_to_session(MockConnection, mock_connection, pubrel_packet)

    # Session should send a PUBCOMP packet.
    pubcomp_packet = expect_from_session(MockConnection, mock_connection, MQTTPubCompPacket)
    assert pubcomp_packet.packet_id == publish_packet.packet_id
    assert pubcomp_packet.reason_code == MQTTReasonCode.Success

    # UNSUBSCRIBE from a topic.
    session.unsubscribe("topic")
    unsubscribe_packet = expect_from_session(MockConnection, mock_connection, MQTTUnsubscribePacket)
    assert unsubscribe_packet.packet_id > 0
    assert unsubscribe_packet.topics == ["topic"]

    # UNSUBACK the unsubscription.
    unsuback_packet = MQTTUnsubAckPacket(
        packet_id=unsubscribe_packet.packet_id,
        reason_codes=[MQTTReasonCode.Success],
    )
    send_to_session(MockConnection, mock_connection, unsuback_packet)

    assert not hasattr(session, "__dict__")
    assert all(hasattr(session, attr) for attr in session.__slots__)

    session.disconnect()
    mock_connection.disconnect.assert_called_once()
