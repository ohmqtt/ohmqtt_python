import logging
import ssl
import threading
from typing import Any, Callable, Final, Mapping, Sequence

from .connection import Connection
from .error import MQTTError
from .mqtt_spec import MQTTPacketType, MQTTReasonCode
from .packet import (
    MQTTPacket,
    MQTTPacketWithId,
    MQTTConnectPacket,
    MQTTConnAckPacket,
    MQTTPublishPacket,
    MQTTPubAckPacket,
    MQTTPubRecPacket,
    MQTTPubRelPacket,
    MQTTPubCompPacket,
    MQTTSubscribePacket,
    MQTTSubAckPacket,
    MQTTUnsubscribePacket,
    MQTTUnsubAckPacket,
    MQTTAuthPacket,
)
from .persistence import SessionPersistenceBackend, InMemorySessionPersistence
from .property import MQTTPropertyDict

logger: Final = logging.getLogger(__name__)

# If session expiry interval is set to 0xFFFFFFFF, the session will never expire.
MAX_SESSION_EXPIRY_INTERVAL: Final = 0xFFFFFFFF

MAX_PACKET_ID: Final = 0xFFFF

SessionAuthCallback = Callable[
    [
        "Session",
        MQTTReasonCode,
        str | None,
        bytes | None,
        str | None,
        Sequence[tuple[str, str]] | None
    ],
    None,
]
SessionCloseCallback = Callable[["Session"], None]
SessionOpenCallback = Callable[["Session"], None]
SessionMessageCallback = Callable[["Session", str, bytes, MQTTPropertyDict], None]


class Session:
    connection: Connection | None = None

    def __init__(
        self,
        client_id: str = "",
        persistence: SessionPersistenceBackend | None = None,
        *,
        auth_cb: SessionAuthCallback | None = None,
        close_cb: SessionCloseCallback | None = None,
        open_cb: SessionOpenCallback | None = None,
        message_cb: SessionMessageCallback | None = None,
    ) -> None:
        self.client_id = client_id
        self.auth_cb = auth_cb
        self.close_cb = close_cb
        self.open_cb = open_cb
        self.message_cb = message_cb
        self.server_receive_maximum = 0
        self._read_handlers: Mapping[MQTTPacketType, Callable[[Any], None]] = {
            MQTTPacketType.CONNACK: self._handle_connack,
            MQTTPacketType.PUBACK: self._handle_puback,
            MQTTPacketType.PUBREC: self._handle_pubrec,
            MQTTPacketType.PUBREL: self._handle_pubrel,
            MQTTPacketType.PUBCOMP: self._handle_pubcomp,
            MQTTPacketType.PUBLISH: self._handle_publish,
            MQTTPacketType.SUBACK: self._handle_suback,
            MQTTPacketType.UNSUBACK: self._handle_unsuback,
            MQTTPacketType.AUTH: self._handle_auth,
        }
        # This lock protects _persistence and _inflight.
        self._lock = threading.RLock()
        self._persistence = persistence if persistence is not None else InMemorySessionPersistence()
        self._inflight: set[MQTTPacketWithId] = set()

    def __enter__(self) -> "Session":
        return self
    
    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.disconnect()

    def _send_packet(self, packet: MQTTPacket) -> None:
        if self.connection is None:
            raise RuntimeError("No connection")
        logger.debug(f"---> {packet}")
        self.connection.send(packet.encode())

    def _send_retained(self, packet: MQTTPacketWithId) -> None:
        with self._lock:
            self._persistence.put(self.client_id, packet)
            if len(self._inflight) < self.server_receive_maximum:
                try:
                    self._send_packet(packet)
                    self._inflight.add(packet)
                except Exception:
                    logger.exception("Failed to send packet, deferring")
                    if packet.packet_type == MQTTPacketType.PUBLISH:
                        self._persistence.mark_dup(self.client_id, packet.packet_id)
            else:
                logger.debug("Deferring send due to server receive maximum")


    def _connection_open_callback(self, conn: Connection) -> None:
        packet = MQTTConnectPacket(
            client_id=self.client_id,
            protocol_version=self.protocol_version,
            clean_start=self.clean_start,
            properties=self.connect_properties,
        )
        self._send_packet(packet)

    def _connection_close_callback(self, conn: Connection, exc: Exception | None) -> None:
        with self._lock:
            self.server_receive_maximum = 0
            self._inflight.clear()
            if self.close_cb is not None:
                try:
                    self.close_cb(self)
                except MQTTError:
                    raise
                except Exception:
                    logger.exception("Unhandled exception in close callback")

    def _connection_read_callback(self, conn: Connection, packet: MQTTPacket) -> None:
        logger.debug(f"<--- {packet}")
        if packet.packet_type in self._read_handlers:
            self._read_handlers[packet.packet_type](packet)

    def _handle_connack(self, packet: MQTTConnAckPacket) -> None:
        if packet.reason_code.value >= 0x80:
            logger.error(f"Connection failed: {packet.reason_code}")
            raise MQTTError(f"Connection failed: {packet.reason_code}", packet.reason_code)
        with self._lock:
            if "AssignedClientIdentifier" in packet.properties:
                self.client_id = packet.properties["AssignedClientIdentifier"]
            if "ReceiveMaximum" in packet.properties:
                self.server_receive_maximum = packet.properties["ReceiveMaximum"]
            self._flush()
        try:
            if self.open_cb is not None:
                self.open_cb(self)
        except MQTTError:
            raise
        except Exception:
            logger.exception("Unhandled exception in open callback")

    def _handle_puback(self, packet: MQTTPubAckPacket) -> None:
        if packet.reason_code.value >= 0x80:
            logger.error(f"Received PUBACK with error code: {packet.reason_code}")
        with self._lock:
            try:
                ref_packet = self._persistence.ack(self.client_id, packet)
            except KeyError:
                logger.exception(f"Received PUBACK for unknown packet ID: {packet.packet_id}")
            try:
                self._inflight.remove(ref_packet)
            except KeyError:
                logger.exception(f"Received PUBACK for unknown packet: {ref_packet}")
            self._flush()

    def _handle_pubrec(self, packet: MQTTPubRecPacket) -> None:
        if packet.reason_code.value >= 0x80:
            logger.error(f"Received PUBREC with error code: {packet.reason_code}")
        reason_code: MQTTReasonCode = MQTTReasonCode.Success
        with self._lock:
            try:
                ref_packet = self._persistence.ack(self.client_id, packet)
            except KeyError:
                logger.exception(f"Received PUBREC for unknown packet ID: {packet.packet_id}")
                reason_code = MQTTReasonCode.PacketIdentifierNotFound
            try:
                self._inflight.remove(ref_packet)
            except KeyError:
                logger.exception(f"Received PUBREC for unknown packet: {ref_packet}")
            rel_packet = MQTTPubRelPacket(packet_id=packet.packet_id, reason_code=reason_code)
            self._send_retained(rel_packet)

    def _handle_pubrel(self, packet: MQTTPubRelPacket) -> None:
        if packet.reason_code.value >= 0x80:
            logger.error(f"Received PUBREL with error code: {packet.reason_code}")
        reason_code: MQTTReasonCode = MQTTReasonCode.Success
        with self._lock:
            try:
                ref_packet = self._persistence.ack(self.client_id, packet)
            except KeyError:
                logger.exception(f"Received PUBREL for unknown packet ID: {packet.packet_id}")
                reason_code = MQTTReasonCode.PacketIdentifierNotFound
            try:
                self._inflight.remove(ref_packet)
            except KeyError:
                logger.exception(f"Received PUBREL for unknown packet: {ref_packet}")
            comp_packet = MQTTPubCompPacket(packet_id=packet.packet_id, reason_code=reason_code)
            self._send_packet(comp_packet)

    def _handle_pubcomp(self, packet: MQTTPubCompPacket) -> None:
        if packet.reason_code.value >= 0x80:
            logger.error(f"Received PUBCOMP with error code: {packet.reason_code}")
        with self._lock:
            try:
                ref_packet = self._persistence.ack(self.client_id, packet)
            except KeyError:
                logger.exception(f"Received PUBCOMP for unknown packet ID: {packet.packet_id}")
            try:
                self._inflight.remove(ref_packet)
            except KeyError:
                logger.exception(f"Received PUBCOMP for unknown packet: {ref_packet}")

    def _handle_suback(self, packet: MQTTSubAckPacket) -> None:
        error_codes = [r.value for r in packet.reason_codes if r.value >= 0x80]
        if error_codes:
            logger.error(f"Received SUBACK with error codes: {packet.reason_codes}")
        with self._lock:
            try:
                ref_packet = self._persistence.ack(self.client_id, packet)
            except KeyError:
                logger.exception(f"Received SUBACK for unknown packet ID: {packet.packet_id}")
            try:
                self._inflight.remove(ref_packet)
            except KeyError:
                logger.exception(f"Received SUBACK for unknown packet: {ref_packet}")
            self._flush()

    def _handle_unsuback(self, packet: MQTTUnsubAckPacket) -> None:
        error_codes = [r.value for r in packet.reason_codes if r.value >= 0x80]
        if error_codes:
            logger.error(f"Received UNSUBACK with error codes: {packet.reason_codes}")
        with self._lock:
            try:
                ref_packet = self._persistence.ack(self.client_id, packet)
            except KeyError:
                logger.exception(f"Received UNSUBACK for unknown packet ID: {packet.packet_id}")
            try:
                self._inflight.remove(ref_packet)
            except KeyError:
                logger.exception(f"Received UNSUBACK for unknown packet: {ref_packet}")
            self._flush()

    def _handle_publish(self, packet: MQTTPublishPacket) -> None:
        try:
            assert self.connection is not None
            if packet.qos == 1:
                ack_packet = MQTTPubAckPacket(packet_id=packet.packet_id)
                self._send_packet(ack_packet)
            elif packet.qos == 2:
                rec_packet = MQTTPubRecPacket(packet_id=packet.packet_id)
                self._send_retained(rec_packet)
            # Calling the message callback must be the last thing we do with the packet.
            if self.message_cb is not None:
                logger.debug(f"Calling message callback for packet: {packet}")
                try:
                    self.message_cb(self, packet.topic, packet.payload, packet.properties)
                except Exception:
                    logger.exception("Unhandled exception in message callback")
        except MQTTError:
            raise
        except Exception:
            logger.exception("Unhandled exception in publish callback")

    def _handle_auth(self, packet: MQTTAuthPacket) -> None:
        if packet.reason_code.value >= 0x80:
            logger.error(f"Received AUTH with error code: {packet.reason_code}")
        if self.auth_cb is not None:
            try:
                self.auth_cb(
                    self,
                    packet.reason_code,
                    packet.properties.get("AuthenticationMethod"),
                    packet.properties.get("AuthenticationData"),
                    packet.properties.get("ReasonString"),
                    packet.properties.get("UserProperty"),
                )
            except Exception:
                logger.exception("Unhandled exception in auth callback")

    def connect(
        self,
        host: str,
        port: int,
        *,
        # will stuff
        protocol_version: int = 5,
        clean_start: bool = False,
        keepalive_interval: int = 0,
        recv_buffer_sz: int = 65535,
        tls: bool = False,
        tls_context: ssl.SSLContext | None = None,
        tls_hostname: str | None = None,
        connect_properties: MQTTPropertyDict | None = None,
    ) -> None:
        with self._lock:
            self.protocol_version = protocol_version
            self.clean_start = clean_start
            self.connect_properties = connect_properties if connect_properties is not None else {}
            self.connection = Connection(
                host=host,
                port=port,
                keepalive_interval=keepalive_interval,
                close_cb=self._connection_close_callback,
                connect_cb=self._connection_open_callback,
                read_cb=self._connection_read_callback,
                recv_buffer_sz=recv_buffer_sz,
                tls=tls,
                tls_context=tls_context,
                tls_hostname=tls_hostname,
            )
            self.connection.start()

    def disconnect(self) -> None:
        if self.connection is not None:
            self.connection.close()
        self.server_receive_maximum = 0
        self._inflight.clear()

    def publish(
        self,
        topic: str,
        payload: bytes,
        *,
        qos: int = 0,
        retain: bool = False,
        properties: MQTTPropertyDict | None = None,
    ) -> None:
        packet = MQTTPublishPacket(
            topic=topic,
            payload=payload,
            qos=qos,
            retain=retain,
            properties=properties,
        )
        if packet.qos > 0:
            if not self.client_id:
                raise RuntimeError("Cannot publish with QoS > 0 without a client ID, set a client ID or wait for connection")
            with self._lock:
                packet_id = self._persistence.next_packet_id(self.client_id, MQTTPacketType.PUBLISH)
                packet.packet_id = packet_id
                self._send_retained(packet)
        else:
            try:
                self._send_packet(packet)
            except Exception:
                logger.exception("Failed to send qos=0 packet, dropping it")

    def subscribe(self, topic: str, qos: int = 2, properties: MQTTPropertyDict | None = None) -> None:
        if not self.client_id:
            raise RuntimeError("Cannot subscribe without a client ID, wait for connection first")
        topics = ((topic, qos),)
        with self._lock:
            packet_id = self._persistence.next_packet_id(self.client_id, MQTTPacketType.SUBSCRIBE)
            packet = MQTTSubscribePacket(
                packet_id=packet_id,
                topics=topics,
                properties=properties,
            )
            self._send_retained(packet)

    def unsubscribe(self, topic: str | list[str], properties: MQTTPropertyDict | None = None) -> None:
        if not self.client_id:
            raise RuntimeError("Cannot unsubscribe without a client ID, wait for connection first")
        if isinstance(topic, str):
            topic = [topic]
        topics = topic
        with self._lock:
            packet_id = self._persistence.next_packet_id(self.client_id, MQTTPacketType.UNSUBSCRIBE)
            packet = MQTTUnsubscribePacket(
                packet_id=packet_id,
                topics=topics,
                properties=properties,
            )
            self._persistence.put(self.client_id, packet)
            if len(self._inflight) < self.server_receive_maximum:
                try:
                    self._send_packet(packet)
                    self._inflight.add(packet)
                except Exception:
                    logger.exception("Failed to send packet, deferring")

    def auth(
        self,
        *,
        authentication_method: str | None = None,
        authentication_data: bytes | None = None,
        reason_string: str | None = None,
        user_properties: Sequence[tuple[str, str]] | None = None,
        reason_code: MQTTReasonCode = MQTTReasonCode.Success,
    ) -> None:
        properties: MQTTPropertyDict = {}
        if authentication_method is not None:
            properties["AuthenticationMethod"] = authentication_method
        if authentication_data is not None:
            properties["AuthenticationData"] = authentication_data
        if reason_string is not None:
            properties["ReasonString"] = reason_string
        if user_properties is not None:
            properties["UserProperty"] = user_properties
        packet = MQTTAuthPacket(
            reason_code=reason_code,
            properties=properties,
        )
        self._send_packet(packet)

    def _flush(self) -> None:
        with self._lock:
            allowed_count = self.server_receive_maximum - len(self._inflight)
            pending_packets = self._persistence.get(self.client_id, self._inflight, allowed_count)
            for packet in pending_packets:
                try:
                    self._send_packet(packet)
                    self._inflight.add(packet)
                except Exception:
                    logger.exception("Unhandled exception while flushing pending packets")
                    if packet.packet_type == MQTTPacketType.PUBLISH:
                        self._persistence.mark_dup(self.client_id, packet.packet_id)
                    break


if __name__ == "__main__":
    import time

    logging.basicConfig(level=logging.DEBUG)

    def handle_message(session: Session, topic: str, payload: bytes, properties: MQTTPropertyDict) -> None:
        logger.info(f"Received message on topic '{topic}': {payload[:16]!r}... with properties: {properties}")

    session = Session(message_cb=handle_message)
    for n in range(3):
        logger.info(f"Connecting ({n+1})")
        session.connect("localhost", 1883)
        time.sleep(1)
        logger.info("Subscribing")
        session.subscribe("test", qos=2)
        time.sleep(1)
        logger.info("Publishing qos=0")
        session.publish("test", b"X0" * 65535, qos=0)
        time.sleep(1)
        logger.info("Publishing qos=1")
        session.publish("test", b"X1" * 65535, qos=1)
        time.sleep(1)
        logger.info("Publishing qos=2")
        session.publish("test", b"X2" * 65535, qos=2)
        time.sleep(1)
        logger.info("Unsubscribing")
        session.unsubscribe("test")
        time.sleep(1)
        logger.info("Publishing qos=0")
        session.publish("test", b"Y0" * 65535, qos=0)
        time.sleep(1)
        logger.info("Publishing qos=1")
        session.publish("test", b"Y1" * 65535, qos=1)
        time.sleep(1)
        logger.info("Publishing qos=2")
        session.publish("test", b"Y2" * 65535, qos=2)
        time.sleep(1)
        logger.info("Disconnecting")
        session.disconnect()
        time.sleep(1)
    logger.info("Done")
