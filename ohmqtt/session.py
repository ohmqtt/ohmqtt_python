import logging
import ssl
import threading
from typing import Any, Callable, Final, Mapping, Sequence

from .connection import Connection
from .error import MQTTError
from .logger import get_logger
from .mqtt_spec import MAX_PACKET_ID, MQTTPacketType, MQTTReasonCode
from .packet import (
    MQTTPacket,
    MQTTConnectPacket,
    MQTTConnAckPacket,
    MQTTDisconnectPacket,
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
from .retention import MessageRetention, PublishHandle, ReliablePublishHandle, UnreliablePublishHandle

logger: Final = get_logger("session")

# If session expiry interval is set to 0xFFFFFFFF, the session will never expire.
MAX_SESSION_EXPIRY_INTERVAL: Final = 0xFFFFFFFF

SessionAuthCallback = Callable[
    [
        int,
        str | None,
        bytes | None,
        str | None,
        Sequence[tuple[str, str]] | None
    ],
    None,
]
SessionCloseCallback = Callable[[], None]
SessionOpenCallback = Callable[[], None]
SessionMessageCallback = Callable[[MQTTPublishPacket], None]


class Session:
    __slots__ = (
        "client_id",
        "protocol_version",
        "clean_start",
        "keepalive_interval",
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
    connection: Connection

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
        self.connection = Connection(
            close_callback=self._connection_close_callback,
            open_callback=self._connection_open_callback,
            read_callback=self._connection_read_callback,
        )
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

    def _send_packet(self, packet: MQTTPacket) -> None:
        """Try to send a packet to the server."""
        self.connection.send(packet.encode())
        if logging.DEBUG >= logging.root.level:
            logger.debug(f"---> {packet}")

    def _connection_open_callback(self) -> None:
        """Handle a connection open event."""
        packet = MQTTConnectPacket(
            client_id=self.client_id,
            protocol_version=self.protocol_version,
            clean_start=self.clean_start,
            keep_alive=self.keepalive_interval,
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
            self.close_callback()

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
        if self.open_callback is not None:
            self.open_callback()

    def _handle_puback(self, packet: MQTTPubAckPacket) -> None:
        """Handle a PUBACK packet from the server."""
        if packet.reason_code >= 0x80:
            logger.error(f"Received PUBACK with error code: {packet.reason_code}")
        with self._lock:
            self._retention.ack(packet.packet_id)
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
        if any(r >= 0x80 for r in packet.reason_codes):
            logger.error(f"Received SUBACK with error codes: {packet.reason_codes}")

    def _handle_unsuback(self, packet: MQTTUnsubAckPacket) -> None:
        """Handle an UNSUBACK packet from the server."""
        if any(r >= 0x80 for r in packet.reason_codes):
            logger.error(f"Received UNSUBACK with error codes: {packet.reason_codes}")

    def _handle_publish(self, packet: MQTTPublishPacket) -> None:
        """Handle a PUBLISH packet from the server."""
        if self.message_callback is not None:
            self.message_callback(packet)
        if packet.qos == 1:
            ack_packet = MQTTPubAckPacket(packet_id=packet.packet_id)
            self._send_packet(ack_packet)
        elif packet.qos == 2:
            rec_packet = MQTTPubRecPacket(packet_id=packet.packet_id)
            self._send_packet(rec_packet)

    def _handle_auth(self, packet: MQTTAuthPacket) -> None:
        """Handle an AUTH packet from the server."""
        if packet.reason_code >= 0x80:
            logger.error(f"Received AUTH with error code: {packet.reason_code}")
        if self.auth_callback is not None:
            self.auth_callback(
                packet.reason_code,
                packet.properties.get("AuthenticationMethod"),
                packet.properties.get("AuthenticationData"),
                packet.properties.get("ReasonString"),
                packet.properties.get("UserProperty"),
            )

    def connect(
        self,
        host: str,
        port: int,
        *,
        # will stuff
        protocol_version: int = 5,
        clean_start: bool = False,
        reconnect_delay: float = 0.0,
        keepalive_interval: int = 0,
        tcp_nodelay: bool = True,
        use_tls: bool = False,
        tls_context: ssl.SSLContext | None = None,
        tls_hostname: str = "",
        connect_properties: MQTTPropertyDict | None = None,
    ) -> None:
        """Connect to a server."""
        with self._lock:
            self.protocol_version = protocol_version
            self.clean_start = clean_start
            self.keepalive_interval = keepalive_interval
            self.connect_properties = connect_properties if connect_properties is not None else {}
            self.connection.connect(
                host=host,
                port=port,
                reconnect_delay=reconnect_delay,
                keepalive_interval=keepalive_interval,
                tcp_nodelay=tcp_nodelay,
                use_tls=use_tls,
                tls_context=tls_context,
                tls_hostname=tls_hostname,
            )

    def disconnect(self) -> None:
        """Disconnect from the server."""
        packet = MQTTDisconnectPacket()
        self._send_packet(packet)
        self.connection.disconnect()

    def shutdown(self) -> None:
        self.connection.shutdown()

    def wait_for_disconnect(self, timeout: float | None = None) -> None:
        """Wait for the session to disconnect from the server.

        Raises TimeoutError if the timeout is exceeded."""
        self.connection.wait_for_disconnect(timeout)

    def publish(
        self,
        topic: str,
        payload: bytes,
        *,
        qos: int = 0,
        retain: bool = False,
        properties: MQTTPropertyDict | None = None,
    ) -> PublishHandle:
        """Publish a message to a topic."""
        if qos > 0:
            with self._lock:
                handle: ReliablePublishHandle = self._retention.add(
                    topic=topic,
                    payload=payload,
                    qos=qos,
                    retain=retain,
                    properties=properties,
                )
                self._flush()
                return handle
        else:
            packet = MQTTPublishPacket(
                topic=topic,
                payload=payload,
                qos=qos,
                retain=retain,
                properties=properties if properties is not None else {},
            )
            self._send_packet(packet)
            return UnreliablePublishHandle()

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
            properties=properties if properties is not None else {},
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
            properties=properties if properties is not None else {},
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
                    self.disconnect()
