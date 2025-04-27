from __future__ import annotations

from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
import itertools
import threading
from typing import ClassVar, Final, Sequence

from .logger import get_logger
from .property import MQTTPropertyDict
from .packet import MQTTPacket, MQTTPublishPacket, MQTTPubRelPacket
from .mqtt_spec import MQTTReasonCode, MAX_PACKET_ID

logger: Final = get_logger("retention")


class PublishHandle(metaclass=ABCMeta):
    """Represents a publish operation."""
    __slots__: ClassVar[Sequence[str]] = tuple()

    @abstractmethod
    def is_acked(self) -> bool:
        """Check if the message has been acknowledged.

        For qos=0, this is always False.
        For qos=1, this is True if the message has been acknowledged.
        For qos=2, this is True if the message has been completely acknowledged."""
        ...  # pragma: no cover

    @abstractmethod
    def wait_for_ack(self, timeout: float | None = None) -> bool:
        """Wait for the message to be acknowledged.

        For qos=0, this always returns False immediately.
        For qos=1, this returns True if the message has been acknowledged.
        For qos=2, this returns True if the message has been completely acknowledged.
        If the timeout is exceeded, this returns False."""
        ...  # pragma: no cover


class UnreliablePublishHandle(PublishHandle):
    """Represents a publish operation with qos=0."""
    __slots__ = tuple()

    def is_acked(self) -> bool:
        return False

    def wait_for_ack(self, timeout: float | None = None) -> bool:
        return False


class ReliablePublishHandle(PublishHandle):
    """Represents a publish operation with qos>0."""
    __slots__ = ("acked", "_cond")
    acked: bool

    def __init__(self, cond: threading.Condition) -> None:
        self.acked = False
        self._cond = cond

    def is_acked(self) -> bool:
        return self.acked

    def wait_for_ack(self, timeout: float | None = None) -> bool:
        with self._cond:
            self._cond.wait_for(self.is_acked, timeout)
        return self.acked


@dataclass(slots=True)
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
    inflight: bool
    handle: ReliablePublishHandle

    def render(self) -> MQTTPacket:
        """Render the message as a packet."""
        packet: MQTTPacket
        if self.received:
            packet = MQTTPubRelPacket(
                packet_id=self.packet_id,
                reason_code=MQTTReasonCode["Success"],
            )
        else:
            packet = MQTTPublishPacket(
                topic=self.topic,
                payload=self.payload,
                packet_id=self.packet_id,
                qos=self.qos,
                retain=self.retain,
                properties=self.properties,
                dup=self.dup,
            )
        self.inflight = True
        return packet


@dataclass(match_args=True, slots=True)
class MessageRetention:
    """Container for retained messages in the session."""
    next_packet_id: int = field(default=1, init=False)
    messages: dict[int, RetainedMessage] = field(init=False, default_factory=dict)
    _cond: threading.Condition = field(init=False, default_factory=threading.Condition)

    def add(
        self,
        topic: str,
        payload: bytes,
        qos: int,
        retain: bool,
        properties: MQTTPropertyDict | None,
        *,
        packet_id: int = 0,
        dup: bool = False,
        received: bool = False,
        inflight: bool = False,
    ) -> ReliablePublishHandle:
        """Add a PUBLISH message to the retention store."""
        if packet_id == 0:
            packet_id = self.next_packet_id
            self.next_packet_id += 1
            if self.next_packet_id > MAX_PACKET_ID:
                self.next_packet_id = 1
        if packet_id in self.messages:
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
            dup=dup,
            received=received,
            inflight=inflight,
            handle=handle,
        )
        self.messages[packet_id] = message
        return handle

    def get(self, count: int) -> Sequence[RetainedMessage]:
        """Get some messages from the store."""
        return [x for x in itertools.islice((m for m in self.messages.values() if not m.inflight), count)]

    def ack(self, packet_id: int) -> None:
        """Ack a PUBLISH message in the retention store."""
        if packet_id not in self.messages:
            logger.error(f"Packet ID {packet_id} not found in retention store")
            return
        message = self.messages[packet_id]
        if message.qos == 1 or message.received:
            del self.messages[packet_id]
            if not self.messages:
                self.messages.clear()
            with self._cond:
                message.handle.acked = True
                self._cond.notify_all()
        else:
            message.inflight = False
            message.received = True

    def reset(self) -> None:
        """Reset inflight state for all retained messages."""
        for message in self.messages.values():
            if message.inflight:
                message.dup = True
            message.inflight = False
