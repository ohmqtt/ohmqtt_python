from abc import ABCMeta, abstractmethod
from typing import ClassVar, NamedTuple, Sequence

from ..handles import PublishHandle
from ..mqtt_spec import MQTTQoS
from ..packet import (
    MQTTPublishPacket,
    MQTTPubAckPacket,
    MQTTPubRecPacket,
    MQTTPubRelPacket,
    MQTTPubCompPacket,
)
from ..property import MQTTPublishProps
from ..topic_alias import AliasPolicy


class RenderedPacket(NamedTuple):
    """Represents a rendered packet."""
    packet: MQTTPublishPacket | MQTTPubRelPacket
    alias_policy: AliasPolicy


class Persistence(metaclass=ABCMeta):
    """Abstract base class for message persistence."""
    __slots__: ClassVar[Sequence[str]] = tuple()

    @abstractmethod
    def __len__(self) -> int:
        """Return the number of outgoing messages in the persistence store."""

    @abstractmethod
    def add(
        self,
        topic: str,
        payload: bytes,
        qos: MQTTQoS,
        retain: bool,
        properties: MQTTPublishProps,
        alias_policy: AliasPolicy,
    ) -> PublishHandle:
        """Add a PUBLISH message to the persistence store."""

    @abstractmethod
    def get(self, count: int) -> Sequence[int]:
        """Get the packet ids of some pending messages from the store."""

    @abstractmethod
    def ack(self, packet: MQTTPubAckPacket | MQTTPubRecPacket | MQTTPubCompPacket) -> None:
        """Ack a PUBLISH or PUBREL message in the persistence store.

        Raises ValueError if the packet_id is not found in the store."""

    @abstractmethod
    def check_rec(self, packet: MQTTPublishPacket) -> bool:
        """Validate that a QoS 2 PUBLISH packet has not already been received.

        Returns True if the packet has not already been received, otherwise False.

        Raises ValueError if the packet is not a QoS 2 PUBLISH packet."""

    @abstractmethod
    def set_rec(self, packet: MQTTPublishPacket) -> None:
        """Indicate that a QoS 2 PUBLISH message has been received.

        Raises ValueError if the packet is not a QoS 2 PUBLISH packet."""

    @abstractmethod
    def rel(self, packet: MQTTPubRelPacket) -> None:
        """Release a QoS 2 PUBLISH message."""

    @abstractmethod
    def render(self, packet_id: int) -> RenderedPacket:
        """Render a PUBLISH message from the persistence store.

        This also indicates to the persistence store that the message is inflight.

        Raises KeyError if the ID is not retained."""

    @abstractmethod
    def clear(self) -> None:
        """Clear the persistence store, discarding all pending messages."""

    @abstractmethod
    def open(self, client_id: str, clear: bool = False) -> None:
        """Indicate to the persistence store that the broker has acknowledged our connection.

        This may clear the persistence store if the client_id is different from the persisted,
        or if clear is True."""
