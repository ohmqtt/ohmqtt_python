"""PUBLISH, PUBACK, PUBREC, PUBREL, and PUBCOMP packets."""

from __future__ import annotations

from typing import Final

from .base import MQTTPacket
from ..error import MQTTError
from ..mqtt_spec import MQTTPacketType, MQTTReasonCode
from ..property import (
    MQTTPropertyDict,
    decode_properties,
    encode_properties,
    hash_properties,
    validate_properties,
)
from ..serialization import (
    encode_string,
    encode_varint,
    decode_string,
    decode_uint8,
    decode_uint16,
)



HEAD_PUBLISH: Final = MQTTPacketType["PUBLISH"] << 4
HEAD_PUBACK: Final = MQTTPacketType["PUBACK"] << 4
HEAD_PUBREC: Final = MQTTPacketType["PUBREC"] << 4
HEAD_PUBREL: Final = (MQTTPacketType["PUBREL"] << 4) + 0x02
HEAD_PUBCOMP: Final = MQTTPacketType["PUBCOMP"] << 4


class MQTTPublishPacket(MQTTPacket):
    packet_type = MQTTPacketType["PUBLISH"]
    __slots__ = ("properties", "packet_id", "topic", "payload", "qos", "retain", "dup")

    def __init__(
        self,
        topic: str,
        payload: bytes,
        *,
        qos: int = 0,
        retain: bool = False,
        dup: bool = False,
        packet_id: int = 0,
        properties: MQTTPropertyDict | None = None,
    ):
        self.topic = topic
        self.payload = payload
        self.qos = qos
        self.retain = retain
        self.dup = dup
        self.packet_id = packet_id
        self.properties = properties if properties is not None else {}

    def __hash__(self) -> int:
        return hash((
            self.packet_type,
            self.topic,
            self.payload,
            self.qos,
            self.retain,
            self.dup,
            self.packet_id,
            hash_properties(self.properties),
        ))

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
    def decode(cls, flags: int, data: bytes) -> MQTTPublishPacket:
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
            validate_properties(props, MQTTPacketType["PUBLISH"])
        offset += props_length
        payload = bytes(data[offset:])
        return MQTTPublishPacket(
            topic,
            payload,
            qos=qos,
            retain=retain,
            dup=dup,
            packet_id=packet_id,
            properties=props,
        )


def _encode_puback_common(
    head: int,
    packet: MQTTPubAckPacket | MQTTPubRecPacket | MQTTPubRelPacket | MQTTPubCompPacket,
) -> bytes:
    """Common encoding logic for PUBACK, PUBREC, PUBREL, and PUBCOMP packets."""
    encoded = bytearray(packet.packet_id.to_bytes(2, byteorder="big"))
    if packet.reason_code != MQTTReasonCode["Success"]:
        encoded.append(packet.reason_code)
    if packet.properties:
        encoded.extend(encode_properties(packet.properties))
    encoded[0:0] = encode_varint(len(encoded))
    encoded.insert(0, head)
    return bytes(encoded)


def _decode_puback_common(packet_type: int, data: bytes) -> tuple[int, int, MQTTPropertyDict]:
    """Common decoding logic for PUBACK, PUBREC, PUBREL, and PUBCOMP packets.

    Validity of flags is checked in the respective classes."""
    offset = 0
    packet_id, packet_id_length = decode_uint16(data[offset:])
    offset += packet_id_length
    if offset == len(data):
        # Reason code and properties are optional.
        return packet_id, MQTTReasonCode["Success"], {}
    reason_code, reason_code_length = decode_uint8(data[offset:])
    offset += reason_code_length
    if offset == len(data):
        # Properties alone may be omitted.
        return packet_id, reason_code, {}
    props, props_length = decode_properties(data[offset:])
    if props:
        validate_properties(props, packet_type)
    return packet_id, reason_code, props


class MQTTPubAckPacket(MQTTPacket):
    packet_type = MQTTPacketType["PUBACK"]
    __slots__ = ("properties", "packet_id", "reason_code",)

    def __init__(
        self,
        packet_id: int,
        reason_code: int = MQTTReasonCode["Success"],
        *,
        properties: MQTTPropertyDict | None = None,
    ):
        self.packet_id = packet_id
        self.reason_code = reason_code
        self.properties = properties if properties is not None else {}

    def __hash__(self) -> int:
        return hash((
            self.packet_type,
            self.packet_id,
            self.reason_code,
            hash_properties(self.properties),
        ))

    def __str__(self) -> str:
        attrs = [
            f"packet_id={self.packet_id}",
            f"reason_code={self.reason_code}",
            f"properties={self.properties}",
        ]
        return f"PUBACK[{', '.join(attrs)}]"

    def encode(self) -> bytes:
        return _encode_puback_common(HEAD_PUBACK, self)
    
    @classmethod
    def decode(cls, flags: int, data: bytes) -> MQTTPubAckPacket:
        if flags != 0:
            raise MQTTError(f"Invalid flags, expected 0 but got {flags}", MQTTReasonCode["MalformedPacket"])
        packet_id, reason_code, props = _decode_puback_common(MQTTPacketType["PUBACK"], data)
        return MQTTPubAckPacket(packet_id, reason_code, properties=props)


class MQTTPubRecPacket(MQTTPacket):
    packet_type = MQTTPacketType["PUBREC"]
    __slots__ = ("properties", "packet_id", "reason_code",)

    def __init__(
        self,
        packet_id: int,
        reason_code: int = MQTTReasonCode["Success"],
        *,
        properties: MQTTPropertyDict | None = None,
    ):
        self.packet_id = packet_id
        self.reason_code = reason_code
        self.properties = properties if properties is not None else {}

    def __hash__(self) -> int:
        return hash((
            self.packet_type,
            self.packet_id,
            self.reason_code,
            hash_properties(self.properties),
        ))

    def __str__(self) -> str:
        attrs = [
            f"packet_id={self.packet_id}",
            f"reason_code={self.reason_code}",
            f"properties={self.properties}",
        ]
        return f"PUBREC[{', '.join(attrs)}]"

    def encode(self) -> bytes:
        return _encode_puback_common(HEAD_PUBREC, self)
    
    @classmethod
    def decode(cls, flags: int, data: bytes) -> MQTTPubRecPacket:
        if flags != 0:
            raise MQTTError(f"Invalid flags, expected 0 but got {flags}", MQTTReasonCode["MalformedPacket"])
        packet_id, reason_code, props = _decode_puback_common(MQTTPacketType["PUBREC"], data)
        return MQTTPubRecPacket(packet_id, reason_code, properties=props)


class MQTTPubRelPacket(MQTTPacket):
    packet_type = MQTTPacketType["PUBREL"]
    __slots__ = ("properties", "packet_id", "reason_code",)

    def __init__(
        self,
        packet_id: int,
        reason_code: int = MQTTReasonCode["Success"],
        *,
        properties: MQTTPropertyDict | None = None,
    ):
        self.packet_id = packet_id
        self.reason_code = reason_code
        self.properties = properties if properties is not None else {}

    def __hash__(self) -> int:
        return hash((
            self.packet_type,
            self.packet_id,
            self.reason_code,
            hash_properties(self.properties),
        ))

    def __str__(self) -> str:
        attrs = [
            f"packet_id={self.packet_id}",
            f"reason_code={self.reason_code}",
            f"properties={self.properties}",
        ]
        return f"PUBREL[{', '.join(attrs)}]"

    def encode(self) -> bytes:
        return _encode_puback_common(HEAD_PUBREL, self)
    
    @classmethod
    def decode(cls, flags: int, data: bytes) -> MQTTPubRelPacket:
        if flags != 2:
            raise MQTTError(f"Invalid flags, expected 0x02 but got {flags}", MQTTReasonCode["MalformedPacket"])
        packet_id, reason_code, props = _decode_puback_common(MQTTPacketType["PUBREL"], data)
        return MQTTPubRelPacket(packet_id, reason_code, properties=props)


class MQTTPubCompPacket(MQTTPacket):
    packet_type = MQTTPacketType["PUBCOMP"]
    __slots__ = ("properties", "packet_id", "reason_code",)

    def __init__(
        self,
        packet_id: int,
        reason_code: int = MQTTReasonCode["Success"],
        *,
        properties: MQTTPropertyDict | None = None,
    ):
        self.packet_id = packet_id
        self.reason_code = reason_code
        self.properties = properties if properties is not None else {}

    def __hash__(self) -> int:
        return hash((
            self.packet_type,
            self.packet_id,
            self.reason_code,
            hash_properties(self.properties),
        ))

    def __str__(self) -> str:
        attrs = [
            f"packet_id={self.packet_id}",
            f"reason_code={self.reason_code}",
            f"properties={self.properties}",
        ]
        return f"PUBCOMP[{', '.join(attrs)}]"

    def encode(self) -> bytes:
        return _encode_puback_common(HEAD_PUBCOMP, self)
    
    @classmethod
    def decode(cls, flags: int, data: bytes) -> MQTTPubCompPacket:
        if flags != 0:
            raise MQTTError(f"Invalid flags, expected 0 but got {flags}", MQTTReasonCode["MalformedPacket"])
        packet_id, reason_code, props = _decode_puback_common(MQTTPacketType["PUBCOMP"], data)
        return MQTTPubCompPacket(packet_id, reason_code, properties=props)