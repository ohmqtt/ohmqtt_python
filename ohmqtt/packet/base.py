from __future__ import annotations

from abc import ABCMeta, abstractmethod
from typing import ClassVar, Sequence, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from ..property import MQTTProperties  # pragma: no cover


class MQTTPacket(metaclass=ABCMeta):
    """Base class for MQTT packets."""
    packet_type: ClassVar[int]
    props_type: ClassVar[Type[MQTTProperties]]
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
