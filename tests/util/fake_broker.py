"""
Fake MQTT broker for testing purposes.
"""

from __future__ import annotations

import socket
import socketserver
import threading
from typing import Callable, cast, Final
import uuid

from ohmqtt.connection.decoder import ClosedSocketError, IncrementalDecoder
from ohmqtt.logger import get_logger
from ohmqtt.mqtt_spec import MQTTReasonCode, MQTTQoS
from ohmqtt.packet import (
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
            self.request.sendall(pkt.encode())

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

class FakeBrokerServer(socketserver.TCPServer):
    received: list[MQTTPacket]
    sock: socket.socket | None

    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        super().__init__(*args, **kwargs)
        self.received = []
        self.sock = None


class FakeBroker(threading.Thread):
    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.server = FakeBrokerServer(("localhost", 0), FakeBrokerHandler)
        self.port = self.server.server_address[1]

    def __enter__(self) -> FakeBroker:
        self.start()
        return self

    def __exit__(self, *args: object) -> None:
        # Shutting down the server deadlocks here, so pass and let it be dereferenced.
        pass

    @property
    def received(self) -> list[MQTTPacket]:
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
