"""PINGREQ and PINGRESP packets."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from .base import MQTTPacket
from ..error import MQTTError
from ..mqtt_spec import MQTTPacketType, MQTTReasonCode


@dataclass(match_args=True, slots=True)
class MQTTPingReqPacket(MQTTPacket):
    packet_type = MQTTPacketType.PINGREQ

    def __str__(self) -> str:
        return "PINGREQ[]"

    def encode(self) -> bytes:
        return b"\xc0\x00"  # PINGREQ is a fixed header with no payload.

    @classmethod
    def decode(cls, flags: int, data: memoryview) -> MQTTPingReqPacket:
        if flags != 0:
            raise MQTTError(f"Invalid flags, expected 0 but got {flags}", MQTTReasonCode.MalformedPacket)
        if len(data) != 0:
            raise MQTTError(f"Invalid length, expected 0 but got {len(data)}", MQTTReasonCode.MalformedPacket)
        return MQTTPingReqPacket()


@dataclass(match_args=True, slots=True)
class MQTTPingRespPacket(MQTTPacket):
    packet_type = MQTTPacketType.PINGRESP

    def __str__(self) -> str:
        return "PINGRESP[]"

    def encode(self) -> bytes:
        return b"\xd0\x00"  # PINGRESP is a fixed header with no payload.

    @classmethod
    def decode(cls, flags: int, data: memoryview) -> MQTTPingRespPacket:
        if flags != 0:
            raise MQTTError(f"Invalid flags, expected 0 but got {flags}", MQTTReasonCode.MalformedPacket)
        if len(data) != 0:
            raise MQTTError(f"Invalid length, expected 0 but got {len(data)}", MQTTReasonCode.MalformedPacket)
        return MQTTPingRespPacket()


# Pre-encoded packets for PINGREQ and PINGRESP
PING: Final = MQTTPingReqPacket().encode()
PONG: Final = MQTTPingRespPacket().encode()
