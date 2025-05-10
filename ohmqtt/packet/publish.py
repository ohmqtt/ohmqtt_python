"""PUBLISH, PUBACK, PUBREC, PUBREL, and PUBCOMP packets."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar, Final, Mapping, TypeAlias

from .base import MQTTPacket
from ..error import MQTTError
from ..mqtt_spec import MQTTPacketType, MQTTReasonCode
from ..property import (
    MQTTProperties,
    MQTTPublishProps,
    MQTTPubAckProps,
    MQTTPubRecProps,
    MQTTPubRelProps,
    MQTTPubCompProps,
)
from ..serialization import (
    encode_string,
    encode_varint,
    encode_uint16,
    decode_string,
    decode_uint8,
    decode_uint16,
)


HEAD_PUBLISH: Final = MQTTPacketType.PUBLISH << 4
HEAD_PUBACKS: Final[Mapping[MQTTPacketType, int]] = {
    MQTTPacketType.PUBACK: MQTTPacketType.PUBACK << 4,
    MQTTPacketType.PUBREC: MQTTPacketType.PUBREC << 4,
    MQTTPacketType.PUBREL: (MQTTPacketType.PUBREL << 4) + 0x02,
    MQTTPacketType.PUBCOMP: MQTTPacketType.PUBCOMP << 4,
}
FLAGS_PUBACKS: Final[Mapping[MQTTPacketType, int]] = {
    MQTTPacketType.PUBACK: 0,
    MQTTPacketType.PUBREC: 0,
    MQTTPacketType.PUBREL: 2,
    MQTTPacketType.PUBCOMP: 0,
}


@dataclass(match_args=True, slots=True)
class MQTTPublishPacket(MQTTPacket):
    packet_type = MQTTPacketType.PUBLISH
    props_type = MQTTPublishProps
    topic: str = ""
    payload: bytes = b""
    qos: int = 0
    retain: bool = False
    packet_id: int = 0
    properties: MQTTPublishProps = field(default_factory=MQTTPublishProps)
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
            encoded.extend(encode_uint16(self.packet_id))
        if self.properties:
            encoded.extend(self.properties.encode())
        else:
            encoded.append(0)
        encoded.extend(self.payload)
        head = HEAD_PUBLISH | self.retain | (self.qos << 1) | (self.dup << 3)
        encoded[0:0] = head.to_bytes(1, "big") + encode_varint(len(encoded))
        return bytes(encoded)

    @classmethod
    def decode(cls, flags: int, data: memoryview) -> MQTTPublishPacket:
        qos = (flags >> 1) & 0x03
        if qos > 2:
            raise MQTTError(f"Invalid QoS level {qos}", MQTTReasonCode.MalformedPacket)
        retain = (flags & 0x01) == 1
        dup = (flags & 0x08) == 8

        topic, topic_length = decode_string(data)
        offset = topic_length
        if qos > 0:
            packet_id = decode_uint16(data[offset:])[0]
            offset += 2
        else:
            packet_id = 0
        props, props_length = MQTTPublishProps.decode(data[offset:])
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


def _encode_pubacklike(packet: PubAckLikeT) -> bytes:
    """Encode a PUBACK-like packet."""
    head = HEAD_PUBACKS[packet.packet_type]
    encoded = bytearray(encode_uint16(packet.packet_id))
    if packet.reason_code != MQTTReasonCode.Success:
        encoded.append(packet.reason_code.value)
    if packet.properties:
        encoded.extend(packet.properties.encode())
    encoded[0:0] = encode_varint(len(encoded))
    encoded.insert(0, head)
    return bytes(encoded)


def _decode_pubacklike(cls: type[PubAckLikeT], flags: int, data: memoryview) -> tuple[int, int, PubAckLikePropsT]:
    """Decode a PUBACK, PUBREC, PUBREL, or PUBCOMP packet."""
    if flags != FLAGS_PUBACKS[cls.packet_type]:
        raise MQTTError(f"Invalid flags, expected {FLAGS_PUBACKS[cls.packet_type]} but got {flags}", MQTTReasonCode.MalformedPacket)
    # We will be kludging some types to make this work.

    offset = 0
    packet_id, packet_id_length = decode_uint16(data[offset:])
    offset += packet_id_length
    if offset == len(data):
        # Reason code and properties are optional.
        return cls(
            packet_id,
            MQTTReasonCode.Success,
            cls.props_type(),  # type: ignore
        )
    reason_code = MQTTReasonCode(decode_uint8(data[offset:])[0])
    offset += 1
    if offset == len(data):
        # Properties alone may be omitted.
        return cls(
            packet_id,
            reason_code,
            cls.props_type(),  # type: ignore
        )
    props, _ = cls.props_type.decode(data[offset:])
    return cls(
        packet_id,
        reason_code,
        props,  # type: ignore
    )


@dataclass(match_args=True, slots=True)
class MQTTPubAckPacket(MQTTPacket):
    packet_type = MQTTPacketType.PUBACK
    props_type: ClassVar[type[MQTTProperties]] = MQTTPubAckProps
    packet_id: int
    reason_code: MQTTReasonCode = MQTTReasonCode.Success
    properties: MQTTPubAckProps = field(default_factory=MQTTPubAckProps)

    def __str__(self) -> str:
        attrs = [
            f"packet_id={self.packet_id}",
            f"reason_code={hex(self.reason_code)}",
            f"properties={self.properties}",
        ]
        return f"PUBACK[{', '.join(attrs)}]"

    def encode(self) -> bytes:
        return _encode_pubacklike(self)

    @classmethod
    def decode(cls, flags: int, data: memoryview) -> MQTTPubAckPacket:
        return _decode_pubacklike(cls, flags, data)  # type: ignore


@dataclass(match_args=True, slots=True)
class MQTTPubRecPacket(MQTTPacket):
    packet_type = MQTTPacketType.PUBREC
    props_type = MQTTPubRecProps
    packet_id: int
    reason_code: MQTTReasonCode = MQTTReasonCode.Success
    properties: MQTTPubRecProps = field(default_factory=MQTTPubRecProps)

    def __str__(self) -> str:
        attrs = [
            f"packet_id={self.packet_id}",
            f"reason_code={hex(self.reason_code)}",
            f"properties={self.properties}",
        ]
        return f"PUBREC[{', '.join(attrs)}]"

    def encode(self) -> bytes:
        return _encode_pubacklike(self)

    @classmethod
    def decode(cls, flags: int, data: memoryview) -> MQTTPubRecPacket:
        return _decode_pubacklike(cls, flags, data)  # type: ignore


@dataclass(match_args=True, slots=True)
class MQTTPubRelPacket(MQTTPacket):
    packet_type = MQTTPacketType.PUBREL
    props_type = MQTTPubRelProps
    packet_id: int
    reason_code: MQTTReasonCode = MQTTReasonCode.Success
    properties: MQTTPubRelProps = field(default_factory=MQTTPubRelProps)

    def __str__(self) -> str:
        attrs = [
            f"packet_id={self.packet_id}",
            f"reason_code={hex(self.reason_code)}",
            f"properties={self.properties}",
        ]
        return f"PUBREL[{', '.join(attrs)}]"

    def encode(self) -> bytes:
        return _encode_pubacklike(self)

    @classmethod
    def decode(cls, flags: int, data: memoryview) -> MQTTPubRelPacket:
        return _decode_pubacklike(cls, flags, data)  # type: ignore


@dataclass(match_args=True, slots=True)
class MQTTPubCompPacket(MQTTPacket):
    packet_type = MQTTPacketType.PUBCOMP
    props_type = MQTTPubCompProps
    packet_id: int
    reason_code: MQTTReasonCode = MQTTReasonCode.Success
    properties: MQTTPubCompProps = field(default_factory=MQTTPubCompProps)

    def __str__(self) -> str:
        attrs = [
            f"packet_id={self.packet_id}",
            f"reason_code={hex(self.reason_code)}",
            f"properties={self.properties}",
        ]
        return f"PUBCOMP[{', '.join(attrs)}]"

    def encode(self) -> bytes:
        return _encode_pubacklike(self)

    @classmethod
    def decode(cls, flags: int, data: memoryview) -> MQTTPubCompPacket:
        return _decode_pubacklike(cls, flags, data)  # type: ignore


PubAckLikeT: TypeAlias = MQTTPubAckPacket | MQTTPubRecPacket | MQTTPubRelPacket | MQTTPubCompPacket
PubAckLikePropsT: TypeAlias = MQTTPubAckProps | MQTTPubRecProps | MQTTPubRelProps | MQTTPubCompProps
