"""
Fake MQTT broker for testing purposes.
"""

from __future__ import annotations

from collections import deque
import socket
import socketserver
import threading
from typing import Callable, cast, Final
import uuid

from ohmqtt.connection.decoder import ClosedSocketError, IncrementalDecoder
from ohmqtt.connection.wslib import deframe_ws_data, frame_ws_data, generate_handshake_key, OpCode
from ohmqtt.logger import get_logger
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
from ohmqtt.property import MQTTConnAckProps
from ohmqtt.topic_filter import match_topic_filter, validate_topic_filter

logger: Final = get_logger("tests.util.fake_broker")


class FakeBrokerHandler(socketserver.BaseRequestHandler):
    server: FakeBrokerServer
    subscriptions: set[str]

    def setup(self) -> None:
        self.decoder = IncrementalDecoder()
        self.subscriptions: set[str] = set()

    def handle(self) -> None:
        self.server.sock = self.request
        try:
            while (packet := self.decoder.decode(self.request)) is not None:
                self._handle_packet(packet)
        except ClosedSocketError:
            pass

    def _handle_packet(self, packet: MQTTPacket) -> None:
        outbound: list[MQTTPacket] = []
        logger.info("FakeBroker <--- %s", packet)
        self.server.received.append(packet)

        handlers = {
            MQTTConnectPacket: self._handle_connect,
            MQTTSubscribePacket: self._handle_subscribe,
            MQTTUnsubscribePacket: self._handle_unsubscribe,
            MQTTPublishPacket: self._handle_publish,
            MQTTPubRecPacket: self._handle_pubrec,
            MQTTPubRelPacket: self._handle_pubrel,
            MQTTPingReqPacket: self._handle_pingreq,
            MQTTDisconnectPacket: self._handle_disconnect,
        }

        if type(packet) in handlers:
            handler = cast(Callable[[MQTTPacket], list[MQTTPacket]], handlers[type(packet)])
            outbound.extend(handler(packet))

        for pkt in outbound:
            logger.info("FakeBroker ---> %s", pkt)
            self._send_packet(pkt)

    def _send_packet(self, packet: MQTTPacket) -> None:
        self.request.sendall(packet.encode())

    def _handle_connect(self, packet: MQTTConnectPacket) -> list[MQTTPacket]:
        if packet.client_id:
            connack = MQTTConnAckPacket()
        else:
            client_id = f"auto-{uuid.uuid4()!s}"
            connack = MQTTConnAckPacket(
                properties=MQTTConnAckProps(AssignedClientIdentifier=client_id)
            )
        return [connack]

    def _handle_subscribe(self, packet: MQTTSubscribePacket) -> list[MQTTPacket]:
        for topic_filter, opts in packet.topics:
            validate_topic_filter(topic_filter)
            if opts & 0x04:
                # no_local is set, do not mirror.
                continue
            self.subscriptions.add(topic_filter)
        return [MQTTSubAckPacket(packet_id=packet.packet_id, reason_codes=[MQTTReasonCode.Success] * len(packet.topics))]

    def _handle_unsubscribe(self, packet: MQTTUnsubscribePacket) -> list[MQTTPacket]:
        for topic in packet.topics:
            validate_topic_filter(topic)
            self.subscriptions.discard(topic)
        return [MQTTUnsubAckPacket(packet_id=packet.packet_id, reason_codes=[MQTTReasonCode.Success] * len(packet.topics))]

    def _handle_publish(self, packet: MQTTPublishPacket) -> list[MQTTPacket]:
        outbound: list[MQTTPacket] = []
        if any(match_topic_filter(topic_filter, packet.topic) for topic_filter in self.subscriptions):
            outbound.append(packet)

        if packet.qos == MQTTQoS.Q1:
            outbound.append(MQTTPubAckPacket(packet_id=packet.packet_id))
        elif packet.qos == MQTTQoS.Q2:
            outbound.append(MQTTPubRecPacket(packet_id=packet.packet_id))

        return outbound

    def _handle_pubrec(self, packet: MQTTPubRecPacket) -> list[MQTTPacket]:
        return [MQTTPubRelPacket(packet_id=packet.packet_id)]

    def _handle_pubrel(self, packet: MQTTPubRelPacket) -> list[MQTTPacket]:
        return [MQTTPubCompPacket(packet_id=packet.packet_id)]

    def _handle_pingreq(self, packet: MQTTPingReqPacket) -> list[MQTTPacket]:
        return [MQTTPingRespPacket()]

    def _handle_disconnect(self, packet: MQTTDisconnectPacket) -> list[MQTTPacket]:
        self.request.close()
        return []


class FakeWebsocketBrokerHandler(FakeBrokerHandler):
    buffer: bytearray
    handshake_done: bool

    def setup(self) -> None:
        self.buffer = bytearray()
        self.handshake_done = False
        self.subscriptions: set[str] = set()

    def handle(self) -> None:
        logger.debug("FakeWebsocketBrokerHandler handle()")
        if not self.handshake_done:
            self.server.sock = self.request
            data = self.request.recv(0xffff)
            lines = data.split(b"\r\n")
            head = lines[0]
            assert head.startswith(b"GET "), "Invalid websocket handshake method"
            assert head.endswith(b" HTTP/1.1"), "Invalid websocket handshake version"
            headers = {
                line.split(b":", 1)[0].lower().strip(): line.split(b":", 1)[1].strip()
                for line in lines[1:]
                if b": " in line
            }
            assert b"sec-websocket-key" in headers, "Invalid websocket handshake"
            assert headers.get(b"upgrade", b"").lower() == b"websocket", "Invalid websocket upgrade header"
            assert headers.get(b"connection", b"").lower() == b"upgrade", "Invalid websocket connection header"
            assert headers.get(b"sec-websocket-version", b"") == b"13", "Invalid websocket version header"
            assert headers.get(b"sec-websocket-protocol", b"") == b"mqtt", "Invalid websocket protocol header"
            nonce = headers[b"sec-websocket-key"]
            accept_key = generate_handshake_key(nonce.decode("utf-8"))

            response = (
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {accept_key}\r\n"
                "Sec-WebSocket-Protocol: mqtt\r\n"
                "\r\n"
            ).encode("utf-8")
            self.request.sendall(response)
            self.handshake_done = True
            logger.info("FakeBroker handshake responded")

        try:
            while (data := self.request.recv(0xffff)):
                self.buffer.extend(data)
                logger.debug("FakeBroker has %d bytes of websocket data", len(self.buffer))

                while (frame := deframe_ws_data(self.buffer)) is not None:
                    opcode, payload, was_masked, frame_length = frame
                    assert was_masked, "Client-to-broker frames must be masked"
                    del self.buffer[:frame_length]
                    if opcode == OpCode.BINARY:
                        packet = decode_packet(payload)
                        self._handle_packet(packet)
        except OSError:
            pass

    def _send_packet(self, packet: MQTTPacket) -> None:
        frame = frame_ws_data(OpCode.BINARY, packet.encode(), do_mask=False)
        self.request.sendall(frame)


class FakeBrokerServer(socketserver.TCPServer):
    received: deque[MQTTPacket]
    sock: socket.socket | None

    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        super().__init__(*args, **kwargs)
        self.received = deque()
        self.sock = None


class FakeBroker(threading.Thread):
    handler_class: type[socketserver.BaseRequestHandler] = FakeBrokerHandler

    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.server = FakeBrokerServer(("localhost", 0), self.handler_class)
        self.port = self.server.server_address[1]

    def __enter__(self) -> FakeBroker:
        self.start()
        return self

    def __exit__(self, *args: object) -> None:
        # Shutting down the server deadlocks here, so pass and let it be dereferenced.
        pass

    @property
    def received(self) -> deque[MQTTPacket]:
        return self.server.received

    @property
    def sock(self) -> socket.socket | None:
        return self.server.sock

    def run(self) -> None:
        with self.server:
            self.server.serve_forever()

    def shutdown(self) -> None:
        self.server.shutdown()
        self.server.server_close()

    def send(self, packet: MQTTPacket) -> None:
        """Send a packet from the broker to the client."""
        if self.sock is None:
            raise RuntimeError("Broker-to-client socket is not initialized")
        logger.info("FakeBroker ---> %s", packet)
        self.sock.sendall(packet.encode())


class FakeWebsocketBroker(FakeBroker):
    handler_class: type[socketserver.BaseRequestHandler] = FakeWebsocketBrokerHandler

    def send(self, packet: MQTTPacket) -> None:
        """Send a packet from the broker to the client."""
        if self.sock is None:
            raise RuntimeError("Broker-to-client socket is not initialized")
        logger.info("FakeBroker ---> %s", packet)
        frame = frame_ws_data(OpCode.BINARY, packet.encode(), do_mask=False)
        self.sock.sendall(frame)
