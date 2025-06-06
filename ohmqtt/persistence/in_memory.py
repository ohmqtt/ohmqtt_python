from collections import deque
from dataclasses import dataclass, field
from threading import Condition
from typing import Final, Sequence
import weakref

from .base import Persistence, ReliablePublishHandle, RenderedPacket
from ..logger import get_logger
from ..mqtt_spec import MAX_PACKET_ID
from ..packet import MQTTPublishPacket, MQTTPubRelPacket
from ..property import MQTTPublishProps
from ..topic_alias import AliasPolicy

logger: Final = get_logger("persistence.in_memory")


@dataclass(match_args=True, slots=True)
class RetainedMessage:
    """Represents a qos>0 message in the session."""
    topic: str
    payload: bytes
    packet_id: int
    qos: int
    retain: bool
    properties: MQTTPublishProps
    dup: bool
    received: bool
    handle: weakref.ReferenceType[ReliablePublishHandle]
    alias_policy: AliasPolicy


@dataclass(slots=True)
class InMemoryPersistence(Persistence):
    """Store for retained messages in the session.

    This store is in memory only and is not persistent."""
    _client_id: str = field(default="", init=False)
    _next_packet_id: int = field(default=1, init=False)
    _messages: dict[int, RetainedMessage] = field(init=False, default_factory=dict)
    _pending: deque[int] = field(init=False, default_factory=deque)
    _received: set[int] = field(init=False, default_factory=set)
    _cond: Condition = field(init=False, default_factory=Condition)

    def __len__(self) -> int:
        return len(self._messages)

    def add(
        self,
        topic: str,
        payload: bytes,
        qos: int,
        retain: bool,
        properties: MQTTPublishProps,
        alias_policy: AliasPolicy,
    ) -> ReliablePublishHandle:
        assert alias_policy != AliasPolicy.ALWAYS, "AliasPolicy must not be ALWAYS for retained messages."
        packet_id = self._next_packet_id
        self._next_packet_id += 1
        if self._next_packet_id > MAX_PACKET_ID:
            self._next_packet_id = 1
        if packet_id in self._messages:
            raise ValueError("Out of packet ids")
        if properties is None:
            properties = {}

        handle = ReliablePublishHandle(self._cond)
        message = RetainedMessage(
            topic=topic,
            payload=payload,
            packet_id=packet_id,
            qos=qos,
            retain=retain,
            properties=properties,
            dup=False,
            received=False,
            handle=weakref.ref(handle),
            alias_policy=alias_policy,
        )
        self._messages[packet_id] = message
        self._pending.append(packet_id)
        return handle

    def get(self, count: int) -> Sequence[int]:
        return [self._pending[i] for i in range(min(count, len(self._pending)))]

    def ack(self, packet_id: int) -> None:
        if packet_id not in self._messages:
            raise ValueError(f"Unknown packet_id: {packet_id}")
        message = self._messages[packet_id]
        if message.qos == 1 or message.received:
            handle = message.handle()
            if handle is not None:
                with self._cond:
                    handle.acked = True
                    self._cond.notify_all()
            del self._messages[packet_id]
        else:
            # Prioritize PUBREL over PUBLISH
            self._pending.appendleft(packet_id)
            message.received = True

    def check_rec(self, packet: MQTTPublishPacket) -> bool:
        if packet.qos != 2:
            raise ValueError("Not a QoS 2 PUBLISH packet")
        if packet.packet_id in self._received:
            logger.debug("Received duplicate QoS 2 packet with ID %d", packet.packet_id)
            return False
        return True

    def set_rec(self, packet: MQTTPublishPacket) -> None:
        if packet.qos != 2:
            raise ValueError("Not a QoS 2 PUBLISH packet")
        self._received.add(packet.packet_id)

    def rel(self, packet: MQTTPubRelPacket) -> None:
        self._received.remove(packet.packet_id)

    def render(self, packet_id: int) -> RenderedPacket:
        packet: MQTTPublishPacket | MQTTPubRelPacket
        msg = self._messages[packet_id]
        if msg.received:
            alias_policy = AliasPolicy.NEVER
            packet = MQTTPubRelPacket(packet_id=msg.packet_id)
        else:
            alias_policy = msg.alias_policy
            packet = MQTTPublishPacket(
                topic=msg.topic,
                payload=msg.payload,
                packet_id=msg.packet_id,
                qos=msg.qos,
                retain=msg.retain,
                properties=msg.properties,
                dup=msg.dup,
            )
        if self._pending.popleft() != msg.packet_id:
            raise RuntimeError(f"Packet ID {msg.packet_id} was not first in pending list")
        return RenderedPacket(packet, alias_policy)

    def _reset_inflight(self) -> None:
        """Clear inflight status of all messages."""
        inflight = [i for i in self._messages.keys() if i not in self._pending]
        for packet_id in reversed(inflight):
            self._messages[packet_id].dup = True
            self._pending.appendleft(packet_id)

    def clear(self) -> None:
        with self._cond:
            self._messages.clear()
            self._pending.clear()
            self._next_packet_id = 1

    def open(self, client_id: str, clear: bool = False) -> None:
        if clear or client_id != self._client_id:
            self.clear()
            self._client_id = client_id
        else:
            self._reset_inflight()
