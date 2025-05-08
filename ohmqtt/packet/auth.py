"""AUTH packets."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final

from .base import MQTTPacket
from ..error import MQTTError
from ..mqtt_spec import MQTTPacketType, MQTTReasonCode
from ..property import (
    MQTTAuthProps,
)
from ..serialization import (
    encode_varint,
    decode_uint8,
)

HEAD_AUTH: Final = MQTTPacketType.AUTH << 4


@dataclass(match_args=True, slots=True)
class MQTTAuthPacket(MQTTPacket):
    packet_type = MQTTPacketType.AUTH
    props_type = MQTTAuthProps
    reason_code: MQTTReasonCode = MQTTReasonCode.Success
    properties: MQTTAuthProps = field(default_factory=MQTTAuthProps)

    def __str__(self) -> str:
        attrs = [
            f"reason_code={hex(self.reason_code)}",
            f"properties={self.properties}",
        ]
        return f"AUTH[{', '.join(attrs)}]"

    def encode(self) -> bytes:
        # If the reason code is success and there are no properties, the packet can be empty.
        if self.reason_code == MQTTReasonCode.Success and len(self.properties) == 0:
            return HEAD_AUTH.to_bytes(1, "big") + b"\x00"
        encoded = bytearray()
        encoded.append(HEAD_AUTH)
        props = self.properties.encode()
        encoded.extend(encode_varint(len(props) + 1))
        encoded.append(self.reason_code.value)
        encoded.extend(props)
        return bytes(encoded)

    @classmethod
    def decode(cls, flags: int, data: memoryview) -> MQTTAuthPacket:
        if flags != 0:
            raise MQTTError(f"Invalid flags, expected 0 but got {flags}", MQTTReasonCode.MalformedPacket)
        if len(data) == 0:
            # An empty packet means success with no properties.
            return MQTTAuthPacket()
        reason_code, sz = decode_uint8(data)
        props, props_sz = MQTTAuthProps.decode(data[sz:])
        return MQTTAuthPacket(MQTTReasonCode(reason_code), properties=props)
