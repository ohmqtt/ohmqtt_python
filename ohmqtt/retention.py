from dataclasses import dataclass
import itertools
from typing import Final, Sequence

from .logger import get_logger
from .property import MQTTPropertyDict
from .packet import MQTTPacket, MQTTPublishPacket, MQTTPubRelPacket
from .mqtt_spec import MQTTReasonCode, MAX_PACKET_ID

logger: Final = get_logger("retention")


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


class MessageRetention:
    """Container for retained messages in the session."""
    __slots__ = ("messages", "next_packet_id")
    messages: dict[int, RetainedMessage]
    next_packet_id: int

    def __init__(self) -> None:
        self.messages = {}
        self.next_packet_id = 1

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
    ) -> None:
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
        )
        self.messages[packet_id] = message

    def get(self, count: int) -> Sequence[RetainedMessage]:
        """Get some messages from the store."""
        return tuple(itertools.islice((m for m in self.messages.values() if not m.inflight), count))

    def ack(self, packet_id: int) -> None:
        """Ack a PUBLISH message in the retention store."""
        if packet_id not in self.messages:
            logger.error(f"Packet ID {packet_id} not found in retention store")
            return
        if self.messages[packet_id].qos == 1 or self.messages[packet_id].received:
            del self.messages[packet_id]
        else:
            self.messages[packet_id].inflight = False
            self.messages[packet_id].received = True

    def reset(self) -> None:
        """Reset inflight state for all retained messages."""
        for message in self.messages.values():
            message.inflight = False
