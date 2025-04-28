"""PUBLISH, PUBACK, PUBREC, PUBREL, and PUBCOMP packets."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final, Mapping

from .base import MQTTPacket
from ..error import MQTTError
from ..mqtt_spec import MQTTPacketType, MQTTPacketTypeReverse, MQTTReasonCode
from ..property import (
    MQTTPropertyDict,
    decode_properties,
    encode_properties,
    validate_properties,
)
from ..serialization import (
    encode_string,
    encode_varint,
    decode_string,
    decode_uint8,
    decode_uint16,
)



HEAD_PUBLISH: Final = MQTTPacketType.PUBLISH << 4
HEAD_PUBACKS: Final[Mapping[int, int]] = {
    MQTTPacketType.PUBACK: MQTTPacketType.PUBACK << 4,
    MQTTPacketType.PUBREC: MQTTPacketType.PUBREC << 4,
    MQTTPacketType.PUBREL: (MQTTPacketType.PUBREL << 4) + 0x02,
    MQTTPacketType.PUBCOMP: MQTTPacketType.PUBCOMP << 4,
}
FLAGS_PUBACKS: Final[Mapping[int, int]] = {
    MQTTPacketType.PUBACK: 0,
    MQTTPacketType.PUBREC: 0,
    MQTTPacketType.PUBREL: 2,
    MQTTPacketType.PUBCOMP: 0,
}


@dataclass(match_args=True, slots=True)
class MQTTPublishPacket(MQTTPacket):
    packet_type = MQTTPacketType.PUBLISH
    topic: str = ""
    payload: bytes = b""
    qos: int = 0
    retain: bool = False
    packet_id: int = 0
    properties: MQTTPropertyDict = field(default_factory=lambda: MQTTPropertyDict())
    dup: bool = False

    def __str__(self) -> str:
        attrs = [
            f"topic={self.topic}",
            f"payload={len(self.payload)}B",
            f"qos={self.qos}",
            f"packet_id={self.packet_id}",
            f"retain={self.retain}",
            f"dup={self.dup}",
            f"properties={self.properties}",
        ]
        return f"PUBLISH[{', '.join(attrs)}]"

    def encode(self) -> bytes:
        encoded = bytearray(encode_string(self.topic))
        if self.qos > 0:
            encoded.extend(self.packet_id.to_bytes(2, byteorder="big"))
        if self.properties:
            encoded.extend(encode_properties(self.properties))
        else:
            encoded.append(0)
        encoded.extend(self.payload)
        encoded[0:0] = encode_varint(len(encoded))
        encoded.insert(0, HEAD_PUBLISH + self.retain + (self.qos << 1) + (self.dup << 3))
        return bytes(encoded)

    @classmethod
    def decode(cls, flags: int, data: memoryview) -> MQTTPublishPacket:
        qos = (flags >> 1) & 0x03
        if qos > 2:
            raise MQTTError(f"Invalid QoS level {qos}", MQTTReasonCode["MalformedPacket"])
        retain = (flags % 2) == 1
        dup = (flags & 0x08) == 8

        topic, topic_length = decode_string(data)
        offset = topic_length
        if qos > 0:
            packet_id = int.from_bytes(data[offset:offset + 2], byteorder="big")
            offset += 2
        else:
            packet_id = 0
        props, props_length = decode_properties(data[offset:])
        if props:
            validate_properties(props, MQTTPacketType.PUBLISH)
        offset += props_length
        payload = data[offset:].tobytes("A")
        return MQTTPublishPacket(
            topic,
            payload,
            qos=qos,
            retain=retain,
            dup=dup,
            packet_id=packet_id,
            properties=props,
        )


@dataclass(match_args=True, slots=True)
class MQTTPubAckPacket(MQTTPacket):
    packet_type = MQTTPacketType.PUBACK
    packet_id: int
    reason_code: int = MQTTReasonCode["Success"]
    properties: MQTTPropertyDict = field(default_factory=lambda: MQTTPropertyDict())

    def __str__(self) -> str:
        attrs = [
            f"packet_id={self.packet_id}",
            f"reason_code={hex(self.reason_code)}",
            f"properties={self.properties}",
        ]
        return f"{MQTTPacketTypeReverse[self.packet_type]}[{', '.join(attrs)}]"

    def encode(self) -> bytes:
        head = HEAD_PUBACKS[self.packet_type]
        encoded = bytearray(self.packet_id.to_bytes(2, byteorder="big"))
        if self.reason_code != MQTTReasonCode["Success"]:
            encoded.append(self.reason_code)
        if self.properties:
            encoded.extend(encode_properties(self.properties))
        encoded[0:0] = encode_varint(len(encoded))
        encoded.insert(0, head)
        return bytes(encoded)
    
    @classmethod
    def decode(cls, flags: int, data: memoryview) -> MQTTPubAckPacket:
        if flags != FLAGS_PUBACKS[cls.packet_type]:
            raise MQTTError(f"Invalid flags, expected {FLAGS_PUBACKS[cls.packet_type]} but got {flags}", MQTTReasonCode["MalformedPacket"])

        offset = 0
        packet_id, packet_id_length = decode_uint16(data[offset:])
        offset += packet_id_length
        if offset == len(data):
            # Reason code and properties are optional.
            return cls(packet_id, MQTTReasonCode["Success"], {})
        reason_code, reason_code_length = decode_uint8(data[offset:])
        offset += reason_code_length
        if offset == len(data):
            # Properties alone may be omitted.
            return cls(packet_id, reason_code, {})
        props, _ = decode_properties(data[offset:])
        if props:
            validate_properties(props, cls.packet_type)
        return cls(packet_id, reason_code, props)


@dataclass(match_args=True, slots=True)
class MQTTPubRecPacket(MQTTPubAckPacket):
    packet_type = MQTTPacketType.PUBREC


@dataclass(match_args=True, slots=True)
class MQTTPubRelPacket(MQTTPubAckPacket):
    packet_type = MQTTPacketType.PUBREL


@dataclass(match_args=True, slots=True)
class MQTTPubCompPacket(MQTTPubAckPacket):
    packet_type = MQTTPacketType.PUBCOMP
