"""AUTH packets."""

from __future__ import annotations

from .base import MQTTPacket
from ..error import MQTTError
from ..mqtt_spec import MQTTPacketType, MQTTReasonCode
from ..property import (
    MQTTPropertyDict,
    encode_properties,
    decode_properties,
    hash_properties,
    validate_properties,
)
from ..serialization import (
    encode_varint,
    decode_uint8,
)

HEAD_AUTH = MQTTPacketType["AUTH"] << 4


class MQTTAuthPacket(MQTTPacket):
    packet_type = MQTTPacketType["AUTH"]
    __slots__ = ("properties", "reason_code")

    def __init__(self, reason_code: int = MQTTReasonCode["Success"], *, properties: MQTTPropertyDict | None = None):
        self.reason_code = reason_code
        self.properties = properties if properties is not None else {}

    def __hash__(self) -> int:
        return hash((
            self.packet_type,
            self.reason_code,
            hash_properties(self.properties),
        ))

    def __str__(self) -> str:
        attrs = [
            f"reason_code={self.reason_code}",
            f"properties={self.properties}",
        ]
        return f"AUTH[{', '.join(attrs)}]"

    def encode(self) -> bytes:
        # If the reason code is success and there are no properties, the packet can be empty.
        if self.reason_code == MQTTReasonCode["Success"] and len(self.properties) == 0:
            return HEAD_AUTH.to_bytes(1, "big") + b"\x00"
        encoded = bytearray()
        encoded.append(HEAD_AUTH)
        props = encode_properties(self.properties)
        encoded.extend(encode_varint(len(props) + 1))
        encoded.append(self.reason_code)
        encoded.extend(props)
        return bytes(encoded)
    
    @classmethod
    def decode(cls, flags: int, data: bytes) -> "MQTTAuthPacket":
        if flags != 0:
            raise MQTTError(f"Invalid flags, expected 0 but got {flags}", MQTTReasonCode["MalformedPacket"])
        if len(data) == 0:
            # An empty packet means success with no properties.
            return MQTTAuthPacket()
        reason_code, sz = decode_uint8(data)
        props, props_sz = decode_properties(data[sz:])
        if props:
            validate_properties(props, MQTTPacketType["AUTH"])
        return MQTTAuthPacket(reason_code, properties=props)
