import logging
import ssl
import threading
from types import TracebackType
from typing import Any, Callable, Final, Mapping, Sequence

from .connection import Connection
from .error import MQTTError
from .logger import get_logger
from .mqtt_spec import MAX_PACKET_ID, MQTTPacketType, MQTTReasonCode
from .packet import (
    MQTTPacket,
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
from .property import MQTTPropertyDict
from .retention import MessageRetention

logger: Final = get_logger("session")

# If session expiry interval is set to 0xFFFFFFFF, the session will never expire.
MAX_SESSION_EXPIRY_INTERVAL: Final = 0xFFFFFFFF

SessionAuthCallback = Callable[
    [
        "Session",
        int,
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
    __slots__ = (
        "client_id",
        "protocol_version",
        "clean_start",
        "connect_properties",
        "server_receive_maximum",
        "server_topic_alias_maximum",
        "_lock",
        "_retention",
        "_inflight",
        "_next_packet_ids",
        "_read_handlers",
        "auth_callback",
        "close_callback",
        "open_callback",
        "message_callback",
        "connection",
    )
    connection: Connection | None

    def __init__(
        self,
        client_id: str = "",
        *,
        auth_callback: SessionAuthCallback | None = None,
        close_callback: SessionCloseCallback | None = None,
        open_callback: SessionOpenCallback | None = None,
        message_callback: SessionMessageCallback | None = None,
    ) -> None:
        self.client_id = client_id
        self.auth_callback = auth_callback
        self.close_callback = close_callback
        self.open_callback = open_callback
        self.message_callback = message_callback
        self.server_receive_maximum = 0
        self.server_topic_alias_maximum = 0
        self._read_handlers: Mapping[int, Callable[[Any], None]] = {
            MQTTPacketType["CONNACK"]: self._handle_connack,
            MQTTPacketType["PUBACK"]: self._handle_puback,
            MQTTPacketType["PUBREC"]: self._handle_pubrec,
            MQTTPacketType["PUBREL"]: self._handle_pubrel,
            MQTTPacketType["PUBCOMP"]: self._handle_pubcomp,
            MQTTPacketType["PUBLISH"]: self._handle_publish,
            MQTTPacketType["SUBACK"]: self._handle_suback,
            MQTTPacketType["UNSUBACK"]: self._handle_unsuback,
            MQTTPacketType["AUTH"]: self._handle_auth,
        }
        # This lock protects _retention, _inflight and _next_packet_ids.
        self._lock = threading.RLock()
        self._retention = MessageRetention()
        self._inflight: int = 0
        self._next_packet_ids = {
            MQTTPacketType["SUBSCRIBE"]: 1,
            MQTTPacketType["UNSUBSCRIBE"]: 1,
        }
        self.connection = None

    def __enter__(self) -> "Session":
        return self
    
    def __exit__(self, exc_type: type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        self.disconnect()

    def _send_packet(self, packet: MQTTPacket) -> None:
        """Try to send a packet to the server."""
        if self.connection is None:
            raise RuntimeError("No connection")
        self.connection.send(packet.encode())
        if logging.DEBUG >= logging.root.level:
            logger.debug(f"---> {packet}")

    def _connection_open_callback(self) -> None:
        """Handle a connection open event."""
        packet = MQTTConnectPacket(
            client_id=self.client_id,
            protocol_version=self.protocol_version,
            clean_start=self.clean_start,
            properties=self.connect_properties,
        )
        self._send_packet(packet)

    def _connection_close_callback(self) -> None:
        """Handle a connection close event."""
        with self._lock:
            self.server_receive_maximum = 0
            self.server_topic_alias_maximum = 0
            self._inflight = 0
            self._retention.reset()
        if self.close_callback is not None:
            try:
                self.close_callback(self)
            except MQTTError:
                raise
            except Exception:
                logger.exception("Unhandled exception in close callback")

    def _connection_read_callback(self, packet: MQTTPacket) -> None:
        """Handle a packet read from the connection."""
        if logging.DEBUG >= logging.root.level:
            logger.debug(f"<--- {packet}")
        if packet.packet_type in self._read_handlers:
            self._read_handlers[packet.packet_type](packet)

    def _handle_connack(self, packet: MQTTConnAckPacket) -> None:
        """Handle a CONNACK packet from the server."""
        if packet.reason_code >= 0x80:
            logger.error(f"Connection failed: {packet.reason_code}")
            raise MQTTError(f"Connection failed: {packet.reason_code}", packet.reason_code)
        with self._lock:
            if "AssignedClientIdentifier" in packet.properties:
                self.client_id = packet.properties["AssignedClientIdentifier"]
            if "ReceiveMaximum" in packet.properties:
                self.server_receive_maximum = packet.properties["ReceiveMaximum"]
            else:
                self.server_receive_maximum = MAX_PACKET_ID - 1
            if "TopicAliasMaximum" in packet.properties:
                self.server_topic_alias_maximum = packet.properties["TopicAliasMaximum"]
            self._flush()
        try:
            if self.open_callback is not None:
                self.open_callback(self)
        except MQTTError:
            raise
        except Exception:
            logger.exception("Unhandled exception in open callback")

    def _handle_puback(self, packet: MQTTPubAckPacket) -> None:
        """Handle a PUBACK packet from the server."""
        if packet.reason_code >= 0x80:
            logger.error(f"Received PUBACK with error code: {packet.reason_code}")
        with self._lock:
            try:
                self._retention.ack(packet.packet_id)
            except KeyError:
                logger.exception(f"Received PUBACK for unknown packet ID: {packet.packet_id}")
            self._inflight -= 1
            self._flush()

    def _handle_pubrec(self, packet: MQTTPubRecPacket) -> None:
        """Handle a PUBREC packet from the server."""
        if packet.reason_code >= 0x80:
            logger.error(f"Received PUBREC with error code: {packet.reason_code}")
        with self._lock:
            self._retention.ack(packet.packet_id)
            self._inflight -= 1
            self._flush()

    def _handle_pubrel(self, packet: MQTTPubRelPacket) -> None:
        """Handle a PUBREL packet from the server."""
        if packet.reason_code >= 0x80:
            logger.error(f"Received PUBREL with error code: {packet.reason_code}")
        with self._lock:
            comp_packet = MQTTPubCompPacket(packet_id=packet.packet_id)
            self._send_packet(comp_packet)

    def _handle_pubcomp(self, packet: MQTTPubCompPacket) -> None:
        """Handle a PUBCOMP packet from the server."""
        if packet.reason_code >= 0x80:
            logger.error(f"Received PUBCOMP with error code: {packet.reason_code}")
        with self._lock:
            self._retention.ack(packet.packet_id)
            self._inflight -= 1
            self._flush()

    def _handle_suback(self, packet: MQTTSubAckPacket) -> None:
        """Handle a SUBACK packet from the server."""
        error_codes = [r for r in packet.reason_codes if r >= 0x80]
        if error_codes:
            logger.error(f"Received SUBACK with error codes: {packet.reason_codes}")

    def _handle_unsuback(self, packet: MQTTUnsubAckPacket) -> None:
        """Handle an UNSUBACK packet from the server."""
        error_codes = [r for r in packet.reason_codes if r >= 0x80]
        if error_codes:
            logger.error(f"Received UNSUBACK with error codes: {packet.reason_codes}")

    def _handle_publish(self, packet: MQTTPublishPacket) -> None:
        """Handle a PUBLISH packet from the server."""
        if packet.qos == 1:
            ack_packet = MQTTPubAckPacket(packet_id=packet.packet_id)
            self._send_packet(ack_packet)
        elif packet.qos == 2:
            rec_packet = MQTTPubRecPacket(packet_id=packet.packet_id)
            self._send_packet(rec_packet)
        # Calling the message callback must be the last thing we do with the packet.
        if self.message_callback is not None:
            try:
                self.message_callback(self, packet.topic, packet.payload, packet.properties)
            except Exception:
                logger.exception("Unhandled exception in message callback")

    def _handle_auth(self, packet: MQTTAuthPacket) -> None:
        """Handle an AUTH packet from the server."""
        if packet.reason_code >= 0x80:
            logger.error(f"Received AUTH with error code: {packet.reason_code}")
        if self.auth_callback is not None:
            try:
                self.auth_callback(
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
        use_tls: bool = False,
        tls_context: ssl.SSLContext | None = None,
        tls_hostname: str = "",
        connect_properties: MQTTPropertyDict | None = None,
    ) -> None:
        """Connect to a server."""
        with self._lock:
            self.protocol_version = protocol_version
            self.clean_start = clean_start
            self.connect_properties = connect_properties if connect_properties is not None else {}
            self.connection = Connection(
                host=host,
                port=port,
                keepalive_interval=keepalive_interval,
                close_callback=self._connection_close_callback,
                open_callback=self._connection_open_callback,
                read_callback=self._connection_read_callback,
                use_tls=use_tls,
                tls_context=tls_context,
                tls_hostname=tls_hostname,
            )

    def disconnect(self) -> None:
        """Disconnect from the server."""
        if self.connection is not None:
            self.connection.close()

    def publish(
        self,
        topic: str,
        payload: bytes,
        *,
        qos: int = 0,
        retain: bool = False,
        properties: MQTTPropertyDict | None = None,
    ) -> None:
        """Publish a message to a topic."""
        if qos > 0:
            if not self.client_id:
                raise RuntimeError("Cannot publish with QoS > 0 without a client ID, set a client ID or wait for connection")
            self._retention.add(
                topic=topic,
                payload=payload,
                qos=qos,
                retain=retain,
                properties=properties,
            )
            self._flush()
        else:
            packet = MQTTPublishPacket(
                topic=topic,
                payload=payload,
                qos=qos,
                retain=retain,
                properties=properties,
            )
            self._send_packet(packet)

    def _next_packet_id(self, packet_type: int) -> int:
        """Get the next packet ID for a given packet type.
        
        Should only be used for SUBSCRIBE and UNSUBSCRIBE packets. Get PUBLISH IDs from the persistence store."""
        with self._lock:
            packet_id = self._next_packet_ids[packet_type]
            self._next_packet_ids[packet_type] += 1
            if self._next_packet_ids[packet_type] > MAX_PACKET_ID:
                self._next_packet_ids[packet_type] = 1
        return packet_id

    def subscribe(self, topic: str, qos: int = 2, properties: MQTTPropertyDict | None = None) -> None:
        """Subscribe to a single topic."""
        if not self.client_id:
            raise RuntimeError("Cannot subscribe without a client ID, wait for connection first")
        topics = ((topic, qos),)
        packet_id = self._next_packet_id(MQTTPacketType["SUBSCRIBE"])
        packet = MQTTSubscribePacket(
            packet_id=packet_id,
            topics=topics,
            properties=properties,
        )
        self._send_packet(packet)

    def unsubscribe(self, topic: str, properties: MQTTPropertyDict | None = None) -> None:
        """Unsubscribe from a single topic."""
        if not self.client_id:
            raise RuntimeError("Cannot unsubscribe without a client ID, wait for connection first")
        topics = [topic]
        packet_id = self._next_packet_id(MQTTPacketType["UNSUBSCRIBE"])
        packet = MQTTUnsubscribePacket(
            packet_id=packet_id,
            topics=topics,
            properties=properties,
        )
        self._send_packet(packet)

    def auth(
        self,
        *,
        authentication_method: str | None = None,
        authentication_data: bytes | None = None,
        reason_string: str | None = None,
        user_properties: Sequence[tuple[str, str]] | None = None,
        reason_code: int = MQTTReasonCode["Success"],
    ) -> None:
        """Send an AUTH packet to the server."""
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
        """Send queued packets up to the server's receive maximum."""
        with self._lock:
            allowed_count = self.server_receive_maximum - self._inflight
            pending_messages = self._retention.get(allowed_count)
            for message in pending_messages:
                packet = message.render()
                self._inflight += 1
                try:
                    self._send_packet(packet)
                except Exception:
                    logger.exception("Unhandled exception while flushing pending packets")
                    if self.connection is not None:
                        self.connection.close()
