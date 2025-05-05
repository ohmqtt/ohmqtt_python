from __future__ import annotations

import logging
from typing import Callable, Final, Sequence

from .connection import Connection, ConnectParams, InvalidStateError, MessageHandlers
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
)
from .property import MQTTPublishProps
from .protected import Protected, protect
from .persistence.base import Persistence, PublishHandle, ReliablePublishHandle, UnreliablePublishHandle
from .persistence.in_memory import InMemoryPersistence
from .persistence.sqlite import SQLitePersistence
from .topic_alias import AliasPolicy, OutboundTopicAlias

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
        self._topic_alias = OutboundTopicAlias()
        self._inflight = 0
        self._next_packet_ids = {
            MQTTPacketType.SUBSCRIBE.value: 1,
            MQTTPacketType.UNSUBSCRIBE.value: 1,
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
    def topic_alias(self) -> OutboundTopicAlias:
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
        "connection",
        "protected",
    )

    def __init__(
        self,
        handlers: MessageHandlers,
        connection: Connection,
        *,
        db_path: str = "",
        db_fast: bool = False,
    ) -> None:
        self.connection = connection
        persistence: Persistence
        if db_path:
            persistence = SQLitePersistence(db_path, db_fast=db_fast)
        else:
            persistence = InMemoryPersistence()
        self.protected = SessionProtected(persistence)
        self.server_receive_maximum = 0

        handlers.register(MQTTConnAckPacket, self.handle_connack)
        handlers.register(MQTTPubAckPacket, self.handle_puback)
        handlers.register(MQTTPubRecPacket, self.handle_pubrec)
        handlers.register(MQTTPubRelPacket, self.handle_pubrel)
        handlers.register(MQTTPubCompPacket, self.handle_pubcomp)

    def set_params(self, params: ConnectParams) -> None:
        with self.protected as protected:
            protected.params = params

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
            lookup = protected.topic_alias.lookup(topic, alias_policy)
            if lookup.alias > 0:
                packet.properties.TopicAlias = lookup.alias
                if lookup.existed:
                    packet.topic = ""
            try:
                self._send_packet(packet)
            except InvalidStateError:
                if lookup.alias > 0 and not lookup.existed:
                    protected.topic_alias.pop()
                raise

    def handle_connack(self, packet: MQTTConnAckPacket) -> None:
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
                protected.topic_alias.max_alias = packet.properties.TopicAliasMaximum
            else:
                protected.topic_alias.max_alias = 0
            if packet.properties.AssignedClientIdentifier:
                client_id = packet.properties.AssignedClientIdentifier
            else:
                client_id = protected.params.client_id
            if not client_id:
                raise MQTTError("No client ID provided", MQTTReasonCode.ProtocolError)
            clear_persistence = not packet.session_present
            protected.persistence.open(client_id, clear=clear_persistence)
            self._flush()

    def handle_puback(self, packet: MQTTPubAckPacket) -> None:
        """Handle a PUBACK packet from the server."""
        if packet.reason_code >= 0x80:
            logger.error(f"Received PUBACK with error code: {packet.reason_code}")
        with self.protected as protected:
            protected.persistence.ack(packet.packet_id)
            protected.inflight -= 1
            self._flush()

    def handle_pubrec(self, packet: MQTTPubRecPacket) -> None:
        """Handle a PUBREC packet from the server."""
        if packet.reason_code >= 0x80:
            logger.error(f"Received PUBREC with error code: {packet.reason_code}")
        with self.protected as protected:
            protected.persistence.ack(packet.packet_id)
            protected.inflight -= 1
            self._flush()

    def handle_pubrel(self, packet: MQTTPubRelPacket) -> None:
        """Handle a PUBREL packet from the server."""
        if packet.reason_code >= 0x80:
            logger.error(f"Received PUBREL with error code: {packet.reason_code}")
        comp_packet = MQTTPubCompPacket(packet_id=packet.packet_id)
        self._send_packet(comp_packet)

    def handle_pubcomp(self, packet: MQTTPubCompPacket) -> None:
        """Handle a PUBCOMP packet from the server."""
        if packet.reason_code >= 0x80:
            logger.error(f"Received PUBCOMP with error code: {packet.reason_code}")
        with self.protected as protected:
            protected.persistence.ack(packet.packet_id)
            protected.inflight -= 1
            self._flush()

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
