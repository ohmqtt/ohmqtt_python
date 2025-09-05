from collections import deque
from dataclasses import dataclass, field
from threading import Condition
from typing import Final, Sequence
import weakref

from .base import LostMessageError, Persistence, RenderedPacket
from ..error import MQTTError
from ..handles import PublishHandle
from ..logger import get_logger
from ..mqtt_spec import MAX_PACKET_ID, MQTTQoS
from ..packet import (
    MQTTPublishPacket,
    MQTTPubAckPacket,
    MQTTPubRecPacket,
    MQTTPubRelPacket,
    MQTTPubCompPacket,
)
from ..property import MQTTPublishProps
from ..topic_alias import AliasPolicy

logger: Final = get_logger("persistence.in_memory")


@dataclass(match_args=True, slots=True)
class RetainedMessage:
    """Represents a qos>0 message in the session."""
    topic: str
    payload: bytes
    msg_id: int
    qos: MQTTQoS
    retain: bool
    properties: MQTTPublishProps
    dup: bool
    received: bool
    handle: weakref.ReferenceType[PublishHandle]
    alias_policy: AliasPolicy


@dataclass(slots=True)
class InMemoryPersistence(Persistence):
    """Store for retained messages in the session.

    This store is in memory only and is not persistent."""
    _client_id: str = field(default="", init=False)
    _next_msg_id: int = field(default=1, init=False)
    _messages: dict[int, RetainedMessage] = field(init=False, default_factory=dict)
    _packet_id_to_msg_id: dict[int, int] = field(init=False, default_factory=dict)
    _pending: deque[int] = field(init=False, default_factory=deque)
    _received: set[int] = field(init=False, default_factory=set)
    _cond: Condition = field(init=False, default_factory=Condition)

    def __len__(self) -> int:
        return len(self._messages)

    def add(
        self,
        topic: str,
        payload: bytes,
        qos: MQTTQoS,
        retain: bool,
        properties: MQTTPublishProps,
        alias_policy: AliasPolicy,
    ) -> PublishHandle:
        if alias_policy == AliasPolicy.ALWAYS:
            raise ValueError("AliasPolicy must not be ALWAYS for retained messages.")
        msg_id = self._next_msg_id
        self._next_msg_id += 1

        handle = PublishHandle(self._cond)
        message = RetainedMessage(
            topic=topic,
            payload=payload,
            msg_id=msg_id,
            qos=qos,
            retain=retain,
            properties=properties,
            dup=False,
            received=False,
            handle=weakref.ref(handle),
            alias_policy=alias_policy,
        )
        self._messages[msg_id] = message
        self._pending.append(msg_id)
        return handle

    def get(self, count: int) -> Sequence[int]:
        return [self._pending[i] for i in range(min(count, len(self._pending)))]

    def ack(self, packet: MQTTPubAckPacket | MQTTPubRecPacket | MQTTPubCompPacket) -> None:
        packet_id = packet.packet_id
        rc = packet.reason_code
        if packet_id not in self._packet_id_to_msg_id:
            raise ValueError(f"Unknown packet_id: {packet_id}")
        msg_id = self._packet_id_to_msg_id[packet_id]
        message = self._messages[msg_id]
        if message.qos == MQTTQoS.Q1 or message.received or rc.is_error():
            handle = message.handle()
            if handle is not None:
                with self._cond:
                    handle.ack = packet
                    if rc.is_error():
                        handle.exc = MQTTError("Error code in acknowledgement packet", rc)
                    self._cond.notify_all()
            del self._packet_id_to_msg_id[packet_id]
            del self._messages[msg_id]
        else:
            # Prioritize PUBREL over PUBLISH
            self._pending.appendleft(msg_id)
            message.received = True

    def check_rec(self, packet: MQTTPublishPacket) -> bool:
        if packet.qos != MQTTQoS.Q2:
            raise ValueError("Not a QoS 2 PUBLISH packet")
        if packet.packet_id in self._received:
            logger.debug("Received duplicate QoS 2 packet with ID %d", packet.packet_id)
            return False
        return True

    def set_rec(self, packet: MQTTPublishPacket) -> None:
        if packet.qos != MQTTQoS.Q2:
            raise ValueError("Not a QoS 2 PUBLISH packet")
        self._received.add(packet.packet_id)

    def rel(self, packet: MQTTPubRelPacket) -> None:
        self._received.remove(packet.packet_id)

    def render(self, msg_id: int) -> RenderedPacket:
        packet: MQTTPublishPacket | MQTTPubRelPacket
        msg = self._messages[msg_id]
        packet_id = msg.msg_id
        while packet_id > MAX_PACKET_ID:
            packet_id -= MAX_PACKET_ID
        if msg.received:
            alias_policy = AliasPolicy.NEVER
            packet = MQTTPubRelPacket(packet_id=packet_id)
        else:
            alias_policy = msg.alias_policy
            packet = MQTTPublishPacket(
                topic=msg.topic,
                payload=msg.payload,
                packet_id=packet_id,
                qos=msg.qos,
                retain=msg.retain,
                properties=msg.properties,
                dup=msg.dup,
            )
        if self._pending[0] != msg.msg_id:
            raise ValueError(f"Message {packet_id} is not next in queue.")
        self._pending.popleft()
        self._packet_id_to_msg_id[packet_id] = msg.msg_id
        return RenderedPacket(packet, alias_policy)

    def _reset_inflight(self) -> None:
        """Clear inflight status of all messages."""
        inflight = [i for i in self._messages.keys() if i not in self._pending]
        for msg_id in reversed(inflight):
            self._messages[msg_id].dup = True
            self._pending.appendleft(msg_id)

    def clear(self) -> None:
        with self._cond:
            if self._messages:
                for message in self._messages.values():
                    handle = message.handle()
                    if handle is not None:
                        handle.exc = LostMessageError("Message lost from persistence store")
                self._cond.notify_all()
            self._messages.clear()
            self._pending.clear()
            self._packet_id_to_msg_id.clear()
            self._next_msg_id = 1

    def open(self, client_id: str, clear: bool = False) -> None:
        if clear or client_id != self._client_id:
            self.clear()
            self._client_id = client_id
        else:
            self._reset_inflight()
