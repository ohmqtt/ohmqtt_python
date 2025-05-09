from __future__ import annotations

from abc import ABCMeta, abstractmethod
from typing import ClassVar, Sequence, TYPE_CHECKING

from ..mqtt_spec import MQTTPacketType

if TYPE_CHECKING:
    from ..property import MQTTProperties  # pragma: no cover


class MQTTPacket(metaclass=ABCMeta):
    """Base class for MQTT packets."""
    packet_type: ClassVar[MQTTPacketType]
    props_type: ClassVar[type[MQTTProperties]]
    __slots__: Sequence[str] = tuple()

    @abstractmethod
    def __str__(self) -> str:
        ...  # pragma: no cover

    @abstractmethod
    def encode(self) -> bytes:
        ...  # pragma: no cover

    @classmethod
    @abstractmethod
    def decode(cls, flags: int, data: memoryview) -> MQTTPacket:
        ...  # pragma: no cover
