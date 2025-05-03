from __future__ import annotations

import logging
from typing import Any, Callable, Final, Mapping, Sequence

from .connection import Connection, ConnectParams, InvalidStateError
from .error import MQTTError
from .logger import get_logger
from .mqtt_spec import MAX_PACKET_ID, MQTTPacketType, MQTTReasonCode
from .packet import (
    MQTTPacket,
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
from .property import MQTTPublishProps, MQTTSubscribeProps, MQTTUnsubscribeProps, MQTTAuthProps
from .protected import Protected, protect
from .persistence.base import Persistence, PublishHandle, ReliablePublishHandle, UnreliablePublishHandle
from .persistence.in_memory import InMemoryPersistence
from .persistence.sqlite import SQLitePersistence
from .topic_alias import AliasPolicy, TopicAlias
from .topic_filter import join_share

logger: Final = get_logger("session")

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
SessionOpenCallback = Callable[[MQTTConnAckPacket], None]
SessionMessageCallback = Callable[[MQTTPublishPacket], None]


class SessionProtected(Protected):
    """Represents the thread-protected state of the session."""
    __slots__ = (
        "_inflight",
        "_next_packet_ids",
        "_params",
        "_persistence",
        "_topic_alias",
    )

    def __init__(self, persistence: Persistence) -> None:
        super().__init__()
        self._persistence = persistence
        self._topic_alias = TopicAlias()
        self._inflight = 0
        self._next_packet_ids = {
            MQTTPacketType.SUBSCRIBE: 1,
            MQTTPacketType.UNSUBSCRIBE: 1,
        }
        # _params intentionally left blank.

    @property
    @protect
    def params(self) -> ConnectParams:
        return self._params
    @params.setter
    @protect
    def params(self, value: ConnectParams) -> None:
        self._params = value

    @property
    @protect
    def persistence(self) -> Persistence:
        return self._persistence

    @property
    @protect
    def topic_alias(self) -> TopicAlias:
        return self._topic_alias

    @property
    @protect
    def inflight(self) -> int:
        return self._inflight
    @inflight.setter
    @protect
    def inflight(self, value: int) -> None:
        self._inflight = value

    @protect
    def get_next_packet_id(self, packet_type: int) -> int:
        """Get the next packet ID for a given packet type.

        Should only be used for SUBSCRIBE and UNSUBSCRIBE packets. Get PUBLISH IDs from the persistence store."""
        packet_id = self._next_packet_ids[packet_type]
        self._next_packet_ids[packet_type] += 1
        if self._next_packet_ids[packet_type] > MAX_PACKET_ID:
            self._next_packet_ids[packet_type] = 1
        return packet_id


class Session:
    __slots__ = (
        "server_receive_maximum",
        "_read_handlers",
        "auth_callback",
        "close_callback",
        "open_callback",
        "message_callback",
        "connection",
        "protected",
    )

    def __init__(
        self,
        *,
        auth_callback: SessionAuthCallback | None = None,
        close_callback: SessionCloseCallback | None = None,
        open_callback: SessionOpenCallback | None = None,
        message_callback: SessionMessageCallback | None = None,
        db_path: str = "",
        db_fast: bool = False,
    ) -> None:
        self.auth_callback = auth_callback
        self.close_callback = close_callback
        self.open_callback = open_callback
        self.message_callback = message_callback
        self.server_receive_maximum = 0
        self.connection = Connection(
            close_callback=self._connection_close_callback,
            open_callback=self._connection_open_callback,
            read_callback=self._connection_read_callback,
        )
        self._read_handlers: Mapping[int, Callable[[Any], None]] = {
            MQTTPacketType.PUBACK: self._handle_puback,
            MQTTPacketType.PUBREC: self._handle_pubrec,
            MQTTPacketType.PUBREL: self._handle_pubrel,
            MQTTPacketType.PUBCOMP: self._handle_pubcomp,
            MQTTPacketType.PUBLISH: self._handle_publish,
            MQTTPacketType.SUBACK: self._handle_suback,
            MQTTPacketType.UNSUBACK: self._handle_unsuback,
            MQTTPacketType.AUTH: self._handle_auth,
        }
        persistence: Persistence
        if db_path:
            persistence = SQLitePersistence(db_path, db_fast=db_fast)
        else:
            persistence = InMemoryPersistence()
        self.protected = SessionProtected(persistence)

    def _send_packet(self, packet: MQTTPacket) -> None:
        """Try to send a packet to the server."""
        if logging.DEBUG >= logging.root.level:
            logger.debug(f"---> {packet}")
        self.connection.send(packet.encode())

    def _send_aliased(
        self,
        packet: MQTTPublishPacket,
        alias_policy: AliasPolicy,
    ) -> None:
        """Send a potentially topic aliased PUBLISH packet to the server.

        This will handle topic aliasing if the server supports it."""
        with self.protected as protected:
            topic = packet.topic
            lookup = protected.topic_alias.lookup_outbound(topic, alias_policy)
            if lookup.alias > 0:
                packet.properties.TopicAlias = lookup.alias
                if lookup.existed:
                    packet.topic = ""
            try:
                self._send_packet(packet)
            except InvalidStateError:
                if lookup.alias > 0 and not lookup.existed:
                    protected.topic_alias.remove_outbound(topic)
                raise

    def _connection_open_callback(self, packet: MQTTConnAckPacket) -> None:
        """Handle a connection open event."""
        if packet.reason_code >= 0x80:
            logger.error(f"Connection failed: {packet.reason_code}")
            raise MQTTError(f"Connection failed: {packet.reason_code}", packet.reason_code)
        with self.protected as protected:
            protected.inflight = 0
            protected.topic_alias.reset()
            if packet.properties.ReceiveMaximum is not None:
                self.server_receive_maximum = packet.properties.ReceiveMaximum
            else:
                self.server_receive_maximum = MAX_PACKET_ID - 1
            if packet.properties.TopicAliasMaximum is not None:
                protected.topic_alias.max_out_alias = packet.properties.TopicAliasMaximum
            else:
                protected.topic_alias.max_out_alias = 0
            if packet.properties.AssignedClientIdentifier:
                client_id = packet.properties.AssignedClientIdentifier
            else:
                client_id = protected.params.client_id
            if not client_id:
                raise MQTTError("No client ID provided", MQTTReasonCode.ProtocolError)
            clear_persistence = not packet.session_present
            protected.persistence.open(client_id, clear=clear_persistence)
            if self.open_callback is not None:
                self.open_callback(packet)
            self._flush()

    def _connection_close_callback(self) -> None:
        """Handle a connection close event."""
        with self.protected as protected:
            protected.inflight = 0
            protected.topic_alias.reset()
            self.server_receive_maximum = 0
            if self.close_callback is not None:
                self.close_callback()

    def _connection_read_callback(self, packet: MQTTPacket) -> None:
        """Handle a packet read from the connection."""
        if logging.DEBUG >= logging.root.level:
            logger.debug(f"<--- {packet}")
        if packet.packet_type in self._read_handlers:
            self._read_handlers[packet.packet_type](packet)

    def _handle_puback(self, packet: MQTTPubAckPacket) -> None:
        """Handle a PUBACK packet from the server."""
        if packet.reason_code >= 0x80:
            logger.error(f"Received PUBACK with error code: {packet.reason_code}")
        with self.protected as protected:
            protected.persistence.ack(packet.packet_id)
            protected.inflight -= 1
            self._flush()

    def _handle_pubrec(self, packet: MQTTPubRecPacket) -> None:
        """Handle a PUBREC packet from the server."""
        if packet.reason_code >= 0x80:
            logger.error(f"Received PUBREC with error code: {packet.reason_code}")
        with self.protected as protected:
            protected.persistence.ack(packet.packet_id)
            protected.inflight -= 1
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
        with self.protected as protected:
            protected.persistence.ack(packet.packet_id)
            protected.inflight -= 1
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
            if packet.properties.TopicAlias is not None:
                with self.protected as protected:
                    if packet.topic:
                        protected.topic_alias.store_inbound(packet.properties.TopicAlias, packet.topic)
                    else:
                        packet.topic = protected.topic_alias.lookup_inbound(packet.properties.TopicAlias)
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
                packet.properties.AuthenticationMethod,
                packet.properties.AuthenticationData,
                packet.properties.ReasonString,
                packet.properties.UserProperty,
            )

    def connect(self, params: ConnectParams) -> None:
        """Connect to the broker."""
        with self.protected as protected:
            protected.params = params
            if params.connect_properties.TopicAliasMaximum is not None:
                protected.topic_alias.max_in_alias = params.connect_properties.TopicAliasMaximum
            else:
                protected.topic_alias.max_in_alias = 0
            self.connection.connect(params)

    def disconnect(self) -> None:
        """Disconnect from the server."""
        self.connection.disconnect()

    def shutdown(self) -> None:
        self.connection.shutdown()

    def publish(
        self,
        topic: str,
        payload: bytes,
        *,
        qos: int = 0,
        retain: bool = False,
        properties: MQTTPublishProps | None = None,
        alias_policy: AliasPolicy = AliasPolicy.NEVER,
    ) -> PublishHandle:
        """Publish a message to a topic."""
        properties = properties if properties is not None else MQTTPublishProps()
        if qos > 0:
            if alias_policy == AliasPolicy.ALWAYS:
                raise ValueError("AliasPolicy.ALWAYS is not allowed for QoS > 0")
            with self.protected as protected:
                handle: ReliablePublishHandle = protected.persistence.add(
                    topic=topic,
                    payload=payload,
                    qos=qos,
                    retain=retain,
                    properties=properties,
                    alias_policy=alias_policy,
                )
                if self.connection.can_send():
                    self._flush()
                else:
                    logger.debug("Connection is not ready to send, deferring flush")
                return handle
        else:
            packet = MQTTPublishPacket(
                topic=topic,
                payload=payload,
                qos=qos,
                retain=retain,
                properties=properties if properties is not None else {},
            )
            try:
                self._send_aliased(packet, alias_policy)
            except InvalidStateError:
                # Not being able to send a packet is not an error for QoS 0.
                logger.debug("Failed to send QoS 0 packet (invalid connection state), ignoring")
            return UnreliablePublishHandle()

    def subscribe(self, topic: str, share_name: str | None, qos: int = 2, properties: MQTTSubscribeProps | None = None) -> None:
        """Subscribe to a single topic."""
        if share_name is not None:
            topic = join_share(topic, share_name)
        topics = ((topic, qos),)
        with self.protected as protected:
            packet_id = protected.get_next_packet_id(MQTTPacketType.SUBSCRIBE)
        packet = MQTTSubscribePacket(
            packet_id=packet_id,
            topics=topics,
            properties=properties if properties is not None else MQTTSubscribeProps(),
        )
        self._send_packet(packet)

    def unsubscribe(self, topic: str, share_name: str | None, properties: MQTTUnsubscribeProps | None = None) -> None:
        """Unsubscribe from a single topic."""
        if share_name is not None:
            topic = join_share(topic, share_name)
        topics = [topic]
        with self.protected as protected:
            packet_id = protected.get_next_packet_id(MQTTPacketType.UNSUBSCRIBE)
        packet = MQTTUnsubscribePacket(
            packet_id=packet_id,
            topics=topics,
            properties=properties if properties is not None else MQTTUnsubscribeProps(),
        )
        self._send_packet(packet)

    def auth(
        self,
        *,
        authentication_method: str | None = None,
        authentication_data: bytes | None = None,
        reason_string: str | None = None,
        user_properties: Sequence[tuple[str, str]] | None = None,
        reason_code: int = MQTTReasonCode.Success,
    ) -> None:
        """Send an AUTH packet to the server."""
        properties = MQTTAuthProps()
        if authentication_method is not None:
            properties.AuthenticationMethod = authentication_method
        if authentication_data is not None:
            properties.AuthenticationData = authentication_data
        if reason_string is not None:
            properties.ReasonString = reason_string
        if user_properties is not None:
            properties.UserProperty = user_properties
        packet = MQTTAuthPacket(
            reason_code=reason_code,
            properties=properties,
        )
        self._send_packet(packet)

    def _flush(self) -> None:
        """Send queued packets up to the server's receive maximum."""
        with self.protected as protected:
            allowed_count = self.server_receive_maximum - protected.inflight
            pending_message_ids = protected.persistence.get(allowed_count)
            for message_id in pending_message_ids:
                packet, alias_policy = protected.persistence.render(message_id)
                protected.inflight += 1
                try:
                    if type(packet) is MQTTPublishPacket:
                        self._send_aliased(packet, alias_policy)
                    else:
                        self._send_packet(packet)
                except InvalidStateError:
                    # A race during transition out of ConnectedState can cause this.
                    # The rest of the machinery should be able to handle it.
                    logger.debug("Failed to send reliable packet (invalid connection state), ignoring")
