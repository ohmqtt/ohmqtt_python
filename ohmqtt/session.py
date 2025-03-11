import logging
import ssl
import threading
from typing import Any, Callable, Final, Mapping

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
    MQTTSubscribePacket,
    MQTTSubAckPacket,
)
from .persistence import SessionPersistenceBackend, InMemorySessionPersistence
from .property import MQTTProperties

logger: Final = logging.getLogger(__name__)

# If session expiry interval is set to 0xFFFFFFFF, the session will never expire.
MAX_SESSION_EXPIRY_INTERVAL: Final = 0xFFFFFFFF

MAX_PACKET_ID: Final = 0xFFFF

SessionAuthCallback = Callable[["Session", str, bytes], None]  # TODO: Allow returning an AUTH packet.
SessionCloseCallback = Callable[["Session"], None]
SessionOpenCallback = Callable[["Session"], None]
SessionMessageCallback = Callable[["Session", str, bytes], None]


class Session:
    connection: Connection | None = None

    def __init__(self, client_id: str = "", persistence: SessionPersistenceBackend | None = None) -> None:
        self.client_id = client_id
        self.auth_cb: SessionAuthCallback | None = None
        self.close_cb: SessionCloseCallback | None = None
        self.open_cb: SessionOpenCallback | None = None
        self.message_cb: SessionMessageCallback | None = None
        self.server_receive_maximum = 0
        self._lock = threading.RLock()
        self._persistence = persistence if persistence is not None else InMemorySessionPersistence()
        self._inflight: set[MQTTPacketWithId] = set()
        self._read_handlers: Mapping[MQTTPacketType, Callable[[Any], None]] = {
            MQTTPacketType.CONNACK: self._handle_connack,
            MQTTPacketType.PUBACK: self._handle_puback,
            MQTTPacketType.PUBLISH: self._handle_publish,
            MQTTPacketType.SUBACK: self._handle_suback,
        }

    def __enter__(self) -> "Session":
        return self
    
    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.disconnect()

    def _send_packet(self, packet: MQTTPacket) -> None:
        if self.connection is None:
            raise RuntimeError("No connection")
        logger.debug(f"---> {packet}")
        self.connection.send(packet.encode())

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
                raise MQTTError("Received PUBACK for unknown packet ID", MQTTReasonCode.ProtocolError)
            try:
                self._inflight.remove(ref_packet)
            except KeyError as exc:
                logger.exception(f"Received PUBACK for unknown packet: {ref_packet}")
                raise MQTTError("Received PUBACK for unknown packet", MQTTReasonCode.ProtocolError) from exc
            self._flush()

    def _handle_suback(self, packet: MQTTSubAckPacket) -> None:
        error_codes = [r.value for r in packet.reason_codes if r.value >= 0x80]
        if error_codes:
            logger.error(f"Received SUBACK with error codes: {packet.reason_codes}")
        with self._lock:
            try:
                ref_packet = self._persistence.ack(self.client_id, packet)
            except KeyError:
                logger.exception(f"Received SUBACK for unknown packet ID: {packet.packet_id}")
                raise MQTTError("Received SUBACK for unknown packet ID", MQTTReasonCode.ProtocolError)
            try:
                self._inflight.remove(ref_packet)
            except KeyError as exc:
                logger.exception(f"Received SUBACK for unknown packet: {ref_packet}")
                raise MQTTError("Received SUBACK for unknown packet", MQTTReasonCode.ProtocolError) from exc
            self._flush()

    def _handle_publish(self, packet: MQTTPublishPacket) -> None:
        try:
            assert self.connection is not None
            if packet.qos == 1:
                ack = MQTTPubAckPacket(packet_id=packet.packet_id)
                self._send_packet(ack)
            if self.message_cb is not None:
                self.message_cb(self, packet.topic, packet.payload)
        except MQTTError:
            raise
        except Exception:
            logger.exception("Unhandled exception in message callback")

    def connect(
        self,
        host: str,
        port: int,
        *,
        # will stuff
        protocol_version: int = 5,
        clean_start: bool = False,
        keepalive_interval: int = 0,
        auth_cb: SessionAuthCallback | None = None,
        close_cb: SessionCloseCallback | None = None,
        open_cb: SessionOpenCallback | None = None,
        message_cb: SessionMessageCallback | None = None,
        recv_buffer_sz: int = 65535,
        tls: bool = False,
        tls_context: ssl.SSLContext | None = None,
        tls_hostname: str | None = None,
        connect_properties: MQTTProperties | None = None,
    ) -> None:
        with self._lock:
            self.protocol_version = protocol_version
            self.clean_start = clean_start
            self.connect_properties = connect_properties if connect_properties is not None else MQTTProperties()
            self.auth_cb = auth_cb
            self.close_cb = close_cb
            self.open_cb = open_cb
            self.message_cb = message_cb
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
        properties: MQTTProperties | None = None,
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
                raise RuntimeError("Cannot publish with QoS > 0 without a client ID, wait for connection first")
            with self._lock:
                packet_id = self._persistence.next_packet_id(self.client_id, MQTTPacketType.PUBLISH)
                packet.packet_id = packet_id
                self._persistence.put(self.client_id, packet)
                if len(self._inflight) < self.server_receive_maximum:
                    try:
                        self._send_packet(packet)
                        self._inflight.add(packet)
                    except Exception:
                        logger.exception("Failed to send packet, deferring")
                        self._persistence.mark_dup(self.client_id, packet_id)
        else:
            try:
                self._send_packet(packet)
            except Exception:
                logger.exception("Failed to send packet, dropping it")

    def subscribe(self, topic: str | list[str], qos: int = 0, properties: MQTTProperties | None = None) -> None:
        if not self.client_id:
            raise RuntimeError("Cannot subscribe without a client ID, wait for connection first")
        if isinstance(topic, str):
            topic = [topic]
        topics = [(t, qos) for t in topic]
        packet = MQTTSubscribePacket(
            topics=topics,
            properties=properties,
        )
        with self._lock:
            packet_id = self._persistence.next_packet_id(self.client_id, MQTTPacketType.SUBSCRIBE)
            packet.packet_id = packet_id
            self._persistence.put(self.client_id, packet)
            if len(self._inflight) < self.server_receive_maximum:
                try:
                    self._send_packet(packet)
                    self._inflight.add(packet)
                except Exception:
                    logger.exception("Failed to send packet, deferring")

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

    session = Session()
    #session.publish("test", "Hello, world! 1 X".encode(), qos=1)
    #for n in range(100):
    #    session.publish("test", f"Hello, world! 1 {n}".encode(), qos=1)
    session.connect("localhost", 1883)
    time.sleep(1)
    session.subscribe("test", qos=1)
    time.sleep(1)
    session.publish("test", b"Hello, world! 1 Y", qos=1)
    for n in range(3):
        session.publish("test", f"Hello, world! 0 {n}".encode(), qos=0)
        session.publish("test", f"Hello, world! 1 {n}".encode(), qos=1)
    time.sleep(3)
    session.disconnect()
    print("Done")
