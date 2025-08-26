from __future__ import annotations

from abc import ABCMeta, abstractmethod
from typing import ClassVar, Sequence, TYPE_CHECKING

from ..mqtt_spec import MQTTPacketType

if TYPE_CHECKING:
    from ..property import MQTTProperties


class MQTTPacket(metaclass=ABCMeta):
    """Base class for MQTT packets."""
    packet_type: ClassVar[MQTTPacketType]
    props_type: ClassVar[type[MQTTProperties]]
    __slots__: Sequence[str] = tuple()

    @abstractmethod
    def __str__(self) -> str:
        """Render the packet metadata as a human-readable string."""

    @abstractmethod
    def encode(self) -> bytes:
        """Encode the packet to bytes."""

    @classmethod
    @abstractmethod
    def decode(cls, flags: int, data: memoryview) -> MQTTPacket:
        """Decode a packet from bytes."""
