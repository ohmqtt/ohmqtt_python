from collections import deque
from dataclasses import dataclass, field
import threading
from typing import Final, Sequence

from .base import Persistence, ReliablePublishHandle
from ..logger import get_logger
from ..mqtt_spec import MQTTReasonCode, MAX_PACKET_ID
from ..packet import MQTTPublishPacket, MQTTPubRelPacket
from ..property import MQTTPropertyDict

logger: Final = get_logger("persistence.in_memory")


@dataclass(match_args=True, slots=True)
class RetainedMessage:
    """Represents a qos>0 message in the session."""
    topic: str
    payload: bytes
    packet_id: int
    qos: int
    retain: bool
    properties: MQTTPropertyDict
    dup: bool
    received: bool
    handle: ReliablePublishHandle


@dataclass(slots=True)
class InMemoryPersistence(Persistence):
    """Store for retained messages in the session.

    This store is in memory only and is not persistent."""
    _client_id: str = field(default="", init=False)
    _next_packet_id: int = field(default=1, init=False)
    _messages: dict[int, RetainedMessage] = field(init=False, default_factory=dict)
    _pending: deque[int] = field(init=False, default_factory=deque)
    _cond: threading.Condition = field(init=False, default_factory=threading.Condition)

    def __len__(self) -> int:
        """Return the number of messages in the persistence store."""
        return len(self._messages)

    def add(
        self,
        topic: str,
        payload: bytes,
        qos: int,
        retain: bool,
        properties: MQTTPropertyDict | None,
    ) -> ReliablePublishHandle:
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
            handle=handle,
        )
        self._messages[packet_id] = message
        self._pending.append(packet_id)
        return handle

    def get(self, count: int) -> Sequence[int]:
        return [self._pending[i] for i in range(min(count, len(self._pending)))]

    def ack(self, packet_id: int) -> None:
        if packet_id not in self._messages:
            logger.error(f"Packet ID {packet_id} not found in retention store")
            return
        message = self._messages[packet_id]
        if message.qos == 1 or message.received:
            del self._messages[packet_id]
            with self._cond:
                message.handle.acked = True
                self._cond.notify_all()
        else:
            # Prioritize PUBREL over PUBLISH
            self._pending.appendleft(packet_id)
            message.received = True

    def render(self, packet_id: int) -> MQTTPublishPacket | MQTTPubRelPacket:
        packet: MQTTPublishPacket | MQTTPubRelPacket
        msg = self._messages[packet_id]
        if msg.received:
            packet = MQTTPubRelPacket(
                packet_id=msg.packet_id,
                reason_code=MQTTReasonCode.Success,
            )
        else:
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
        return packet

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
            self.next_packet_id = 1

    def open(self, client_id: str, clear: bool = False) -> None:
        if clear or client_id != self._client_id:
            self.clear()
            self._client_id = client_id
        else:
            self._reset_inflight()
