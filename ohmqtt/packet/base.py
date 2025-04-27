from __future__ import annotations

from abc import ABCMeta, abstractmethod
from typing import ClassVar, Sequence


class MQTTPacket(metaclass=ABCMeta):
    """Base class for MQTT packets."""
    packet_type: ClassVar[int]
    __slots__: Sequence[str] = tuple()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return NotImplemented
        return all(getattr(self, attr) == getattr(other, attr) for attr in self.__slots__)

    @abstractmethod
    def __str__(self) -> str:
        ...  # pragma: no cover

    @abstractmethod
    def encode(self) -> bytes:
        ...  # pragma: no cover

    @classmethod
    @abstractmethod
    def decode(cls, flags: int, data: bytes) -> MQTTPacket:
        ...  # pragma: no cover
