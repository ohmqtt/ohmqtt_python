from abc import ABCMeta, abstractmethod
from threading import Condition
from typing import ClassVar, NamedTuple, Sequence

from ..packet import MQTTPublishPacket, MQTTPubRelPacket
from ..property import MQTTPublishProps
from ..topic_alias import AliasPolicy


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
    __slots__ = ("__weakref__", "_cond", "acked")

    def __init__(self, cond: Condition) -> None:
        self._cond = cond
        self.acked = False

    def is_acked(self) -> bool:
        return self.acked

    def wait_for_ack(self, timeout: float | None = None) -> bool:
        with self._cond:
            self._cond.wait_for(self.is_acked, timeout)
        return self.acked


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
        ...  # pragma: no cover

    @abstractmethod
    def add(
        self,
        topic: str,
        payload: bytes,
        qos: int,
        retain: bool,
        properties: MQTTPublishProps,
        alias_policy: AliasPolicy,
    ) -> ReliablePublishHandle:
        """Add a PUBLISH message to the persistence store."""
        ...  # pragma: no cover

    @abstractmethod
    def get(self, count: int) -> Sequence[int]:
        """Get the packet ids of some pending messages from the store."""
        ...  # pragma: no cover

    @abstractmethod
    def ack(self, packet_id: int) -> None:
        """Ack a PUBLISH or PUBREL message in the persistence store.

        Raises ValueError if the packet_id is not found in the store."""
        ...  # pragma: no cover

    @abstractmethod
    def check_rec(self, packet: MQTTPublishPacket) -> bool:
        """Validate that a QoS 2 PUBLISH packet has not already been received.

        Returns True if the packet has not already been received, otherwise False.

        Raises ValueError if the packet is not a QoS 2 PUBLISH packet."""
        ...  # pragma: no cover

    @abstractmethod
    def set_rec(self, packet: MQTTPublishPacket) -> None:
        """Indicate that a QoS 2 PUBLISH message has been received.

        Raises ValueError if the packet is not a QoS 2 PUBLISH packet."""
        ...  # pragma: no cover

    @abstractmethod
    def rel(self, packet: MQTTPubRelPacket) -> None:
        """Release a QoS 2 PUBLISH message."""
        ...  # pragma: no cover

    @abstractmethod
    def render(self, packet_id: int) -> RenderedPacket:
        """Render a PUBLISH message from the persistence store.

        This also indicates to the persistence store that the message is inflight."""
        ...  # pragma: no cover

    @abstractmethod
    def clear(self) -> None:
        """Clear the persistence store, destroying all pending messages."""
        ...  # pragma: no cover

    @abstractmethod
    def open(self, client_id: str, clear: bool = False) -> None:
        """Indicate to the persistence store that the broker has acknowledged our connection.

        This may clear the persistence store if the client_id is different from the persisted,
        or if clear is True."""
        ...  # pragma: no cover
