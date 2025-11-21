import socket
import time

import pytest

from .util.fake_broker import FakeBroker, FakeWebsocketBroker
from ohmqtt.connection.decoder import IncrementalDecoder
from ohmqtt.connection.states.websocket_handshake_response import WebsocketHandshakeResponseState
from ohmqtt.connection.wslib import (
    OpCode,
    frame_ws_data,
    deframe_ws_data,
    generate_nonce,
)
from ohmqtt.mqtt_spec import MQTTReasonCode, MQTTQoS
from ohmqtt.packet import (
    decode_packet,
    MQTTPacket,
    MQTTConnectPacket,
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
    MQTTPingReqPacket,
    MQTTPingRespPacket,
    MQTTDisconnectPacket,
)


def send_to_broker(sock: socket.socket, broker: FakeBroker, packet: MQTTPacket) -> None:
    sock.sendall(packet.encode())
    time.sleep(0.05)
    assert len(broker.received) == 1
    assert broker.received.popleft() == packet


def send_to_broker_ws(sock: socket.socket, broker: FakeBroker, packet: MQTTPacket) -> None:
    sock.sendall(frame_ws_data(OpCode.BINARY, packet.encode()))
    time.sleep(0.05)
    assert len(broker.received) == 1
    assert broker.received.popleft() == packet


def expect_from_broker(sock: socket.socket, decoder: IncrementalDecoder, packet: MQTTPacket) -> None:
    pkt = decoder.decode(sock)
    assert pkt == packet


def expect_from_broker_ws(sock: socket.socket, buffer: bytearray, packet: MQTTPacket) -> None:
    buffer.extend(sock.recv(0xffff))
    frame = deframe_ws_data(buffer)
    assert frame is not None
    opcode, payload, masked, frame_length = frame
    assert not masked
    assert opcode == OpCode.BINARY
    del buffer[:frame_length]
    pkt = decode_packet(payload)
    assert pkt == packet


def test_fake_broker() -> None:
    decoder = IncrementalDecoder()
    with FakeBroker() as broker:
        assert broker.port > 0
        assert broker.sock is None
        assert len(broker.received) == 0

        sock = socket.create_connection(("localhost", broker.port))
        sock.settimeout(0.05)

        # CONNECT
        connect_packet = MQTTConnectPacket(
            client_id="test_client",
            clean_start=True,
        )
        send_to_broker(sock, broker, connect_packet)
        assert broker.sock is not None
        expect_from_broker(sock, decoder, MQTTConnAckPacket())

        # SUBSCRIBE
        subscribe_packet = MQTTSubscribePacket(
            topics=[("test/topic", 2)],
            packet_id=1,
        )
        send_to_broker(sock, broker, subscribe_packet)
        expect_from_broker(sock, decoder, MQTTSubAckPacket(packet_id=1, reason_codes=[MQTTReasonCode.Success]))
        # Subscribe to another topic with no_local
        subscribe_packet = MQTTSubscribePacket(
            topics=[("test/topic2", 2 | 0x04)],
            packet_id=2,
        )
        send_to_broker(sock, broker, subscribe_packet)
        expect_from_broker(sock, decoder, MQTTSubAckPacket(packet_id=2, reason_codes=[MQTTReasonCode.Success]))

        # PUBLISH
        publish_packet_qos0 = MQTTPublishPacket(
            topic="test/topic",
            payload=b"Hello, MQTT!",
        )
        send_to_broker(sock, broker, publish_packet_qos0)
        expect_from_broker(sock, decoder, publish_packet_qos0)
        # Make sure we don't get the publish back on the no_local topic
        send_to_broker(sock, broker, MQTTPublishPacket(topic="test/topic2", payload=b"Hello, no_local!"))
        with pytest.raises(TimeoutError):
            sock.recv(1024)

        publish_packet_qos1 = MQTTPublishPacket(
            topic="test/topic",
            payload=b"Hello, MQTT QoS 1!",
            qos=MQTTQoS.Q1,
            packet_id=1,
        )
        send_to_broker(sock, broker, publish_packet_qos1)
        expect_from_broker(sock, decoder, publish_packet_qos1)
        expect_from_broker(sock, decoder, MQTTPubAckPacket(packet_id=1))

        publish_packet_qos2 = MQTTPublishPacket(
            topic="test/topic",
            payload=b"Hello, MQTT QoS 2!",
            qos=MQTTQoS.Q2,
            packet_id=2,
        )
        send_to_broker(sock, broker, publish_packet_qos2)
        expect_from_broker(sock, decoder, publish_packet_qos2)
        expect_from_broker(sock, decoder, MQTTPubRecPacket(packet_id=2))
        send_to_broker(sock, broker, MQTTPubRelPacket(packet_id=2))
        expect_from_broker(sock, decoder, MQTTPubCompPacket(packet_id=2))

        # UNSUBSCRIBE
        unsubscribe_packet = MQTTUnsubscribePacket(
            topics=["test/topic"],
            packet_id=3,
        )
        send_to_broker(sock, broker, unsubscribe_packet)
        expect_from_broker(sock, decoder, MQTTUnsubAckPacket(packet_id=3, reason_codes=[MQTTReasonCode.Success]))
        # Make sure we don't get the publish back
        send_to_broker(sock, broker, publish_packet_qos0)
        with pytest.raises(TimeoutError):
             sock.recv(1024)

        # PINGREQ
        pingreq_packet = MQTTPingReqPacket()
        send_to_broker(sock, broker, pingreq_packet)
        expect_from_broker(sock, decoder, MQTTPingRespPacket())

        # Send a message originating from the broker.
        broker.send(MQTTPingReqPacket())
        expect_from_broker(sock, decoder, MQTTPingReqPacket())

        # DISCONNECT
        disconnect_packet = MQTTDisconnectPacket(reason_code=MQTTReasonCode.NormalDisconnection)
        send_to_broker(sock, broker, disconnect_packet)
        assert sock.recv(1024) == b""


def test_fake_broker_websocket() -> None:
    with FakeWebsocketBroker() as broker:
        assert broker.port > 0
        assert broker.sock is None
        assert len(broker.received) == 0

        sock = socket.create_connection(("localhost", broker.port))
        sock.settimeout(0.05)
        nonce = generate_nonce()
        buffer = bytearray()

        # Handshake
        handshake_request = (
            "GET / HTTP/1.1\r\n"
            "Host: localhost\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {nonce}\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "Sec-WebSocket-Protocol: mqtt\r\n"
            "\r\n"
        )
        sock.sendall(handshake_request.encode("utf-8"))
        handshake_response = sock.recv(0xffff)
        assert WebsocketHandshakeResponseState.validate_response(nonce, bytearray(handshake_response)) is True

        # CONNECT
        connect_packet = MQTTConnectPacket(
            client_id="test_client",
            clean_start=True,
        )
        send_to_broker_ws(sock, broker, connect_packet)
        assert broker.sock is not None
        expect_from_broker_ws(sock, buffer, MQTTConnAckPacket())

        # PINGREQ
        pingreq_packet = MQTTPingReqPacket()
        send_to_broker_ws(sock, broker, pingreq_packet)
        expect_from_broker_ws(sock, buffer, MQTTPingRespPacket())

        # Send a message originating from the broker.
        broker.send(MQTTPingReqPacket())
        expect_from_broker_ws(sock, buffer, MQTTPingReqPacket())

        # DISCONNECT
        disconnect_packet = MQTTDisconnectPacket(reason_code=MQTTReasonCode.NormalDisconnection)
        send_to_broker_ws(sock, broker, disconnect_packet)
        assert sock.recv(1024) == b""
