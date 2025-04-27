"""SUBSCRIBE, SUBACK, UNSUBSCRIBE, and UNSUBACK packets."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence

from .base import MQTTPacket
from ..error import MQTTError
from ..mqtt_spec import MQTTPacketType, MQTTReasonCode
from ..property import (
    MQTTPropertyDict,
    encode_properties,
    decode_properties,
    validate_properties,
)
from ..serialization import (
    encode_uint8,
    encode_uint16,
    encode_string,
    encode_varint,
    decode_uint8,
    decode_uint16,
    decode_string,
)

HEAD_SUBSCRIBE = (MQTTPacketType["SUBSCRIBE"] << 4) + 0x02
HEAD_SUBACK = MQTTPacketType["SUBACK"] << 4
HEAD_UNSUBSCRIBE = (MQTTPacketType["UNSUBSCRIBE"] << 4) + 0x02
HEAD_UNSUBACK = MQTTPacketType["UNSUBACK"] << 4


@dataclass(match_args=True, slots=True)
class MQTTSubscribePacket(MQTTPacket):
    packet_type = MQTTPacketType["SUBSCRIBE"]
    topics: Sequence[tuple[str, int]] = field(default_factory=tuple)
    packet_id: int = 0
    properties: MQTTPropertyDict = field(default_factory=lambda: MQTTPropertyDict())

    def __str__(self) -> str:
        attrs = [
            f"packet_id={self.packet_id}",
            f"topics={self.topics}",
            f"properties={self.properties}",
        ]
        return f"SUBSCRIBE[{', '.join(attrs)}]"

    def encode(self) -> bytes:
        encoded = bytearray()
        encoded.append(HEAD_SUBSCRIBE)
        data = encode_uint16(self.packet_id)
        data += encode_properties(self.properties)
        for topic, subscribe_opts in self.topics:
            data += encode_string(topic) + encode_uint8(subscribe_opts)
        encoded.extend(encode_varint(len(data)))
        encoded.extend(data)
        return bytes(encoded)

    @classmethod
    def decode(cls, flags: int, data: bytes) -> MQTTSubscribePacket:
        if flags != 0x02:
            raise MQTTError(f"Invalid flags, expected 0x02 but got {flags}", MQTTReasonCode["MalformedPacket"])
        offset = 0
        packet_id, packet_id_length = decode_uint16(data[offset:])
        offset += packet_id_length
        props, props_length = decode_properties(data[offset:])
        if props:
            validate_properties(props, MQTTPacketType["SUBSCRIBE"])
        offset += props_length
        topics = []
        while offset < len(data):
            topic, topic_length = decode_string(data[offset:])
            offset += topic_length
            subscribe_opts, subscribe_opts_length = decode_uint8(data[offset:])
            offset += subscribe_opts_length
            topics.append((topic, subscribe_opts))
        return MQTTSubscribePacket(topics, packet_id, properties=props)


@dataclass(match_args=True, slots=True)
class MQTTSubAckPacket(MQTTPacket):
    packet_type = MQTTPacketType["SUBACK"]
    packet_id: int
    reason_codes: Sequence[int] = field(default_factory=tuple)
    properties: MQTTPropertyDict = field(default_factory=lambda: MQTTPropertyDict())

    def __str__(self) -> str:
        attrs = [
            f"packet_id={self.packet_id}",
            f"reason_codes={[hex(c) for c in self.reason_codes]}",
            f"properties={self.properties}",
        ]
        return f"SUBACK[{', '.join(attrs)}]"

    def encode(self) -> bytes:
        encoded = bytearray()
        encoded.append(HEAD_SUBACK)
        length = 2 + len(self.reason_codes)
        props = encode_properties(self.properties)
        length += len(props)
        encoded.extend(encode_varint(length))
        encoded.extend(encode_uint16(self.packet_id))
        encoded.extend(props)
        for reason_code in self.reason_codes:
            encoded.append(reason_code)
        return bytes(encoded)

    @classmethod
    def decode(cls, flags: int, data: bytes) -> MQTTSubAckPacket:
        if flags != 0:
            raise MQTTError(f"Invalid flags, expected 0 but got {flags}", MQTTReasonCode["MalformedPacket"])
        offset = 0
        packet_id, packet_id_length = decode_uint16(data[offset:])
        offset += packet_id_length
        props, props_length = decode_properties(data[offset:])
        if props:
            validate_properties(props, MQTTPacketType["SUBACK"])
        offset += props_length
        reason_codes = [b for b in data[offset:]]
        return MQTTSubAckPacket(packet_id, reason_codes, properties=props)


@dataclass(match_args=True, slots=True)
class MQTTUnsubscribePacket(MQTTPacket):
    packet_type = MQTTPacketType["UNSUBSCRIBE"]
    topics: Sequence[str] = field(default_factory=tuple)
    packet_id: int = 0
    properties: MQTTPropertyDict = field(default_factory=lambda: MQTTPropertyDict())

    def __str__(self) -> str:
        attrs = [
            f"packet_id={self.packet_id}",
            f"topics={self.topics}",
            f"properties={self.properties}",
        ]
        return f"UNSUBSCRIBE[{', '.join(attrs)}]"

    def encode(self) -> bytes:
        encoded = bytearray()
        encoded.append(HEAD_UNSUBSCRIBE)
        data = encode_uint16(self.packet_id) + encode_properties(self.properties)
        for topic in self.topics:
            data += encode_string(topic)
        encoded.extend(encode_varint(len(data)))
        encoded.extend(data)
        return bytes(encoded)

    @classmethod
    def decode(cls, flags: int, data: bytes) -> MQTTUnsubscribePacket:
        if flags != 0x02:
            raise MQTTError(f"Invalid flags, expected 0x02 but got {flags}", MQTTReasonCode["MalformedPacket"])
        offset = 0
        packet_id, packet_id_length = decode_uint16(data[offset:])
        offset += packet_id_length
        props, props_length = decode_properties(data[offset:])
        if props:
            validate_properties(props, MQTTPacketType["UNSUBSCRIBE"])
        offset += props_length
        topics = []
        while offset < len(data):
            topic, topic_length = decode_string(data[offset:])
            offset += topic_length
            topics.append(topic)
        return MQTTUnsubscribePacket(topics, packet_id, properties=props)


@dataclass(match_args=True, slots=True)
class MQTTUnsubAckPacket(MQTTPacket):
    packet_type = MQTTPacketType["UNSUBACK"]
    packet_id: int
    reason_codes: Sequence[int] = field(default_factory=tuple)
    properties: MQTTPropertyDict = field(default_factory=lambda: MQTTPropertyDict())

    def __str__(self) -> str:
        attrs = [
            f"packet_id={self.packet_id}",
            f"reason_codes={[hex(c) for c in self.reason_codes]}",
            f"properties={self.properties}",
        ]
        return f"UNSUBACK[{', '.join(attrs)}]"

    def encode(self) -> bytes:
        encoded = bytearray()
        encoded.append(HEAD_UNSUBACK)
        data = encode_uint16(self.packet_id) + encode_properties(self.properties)
        for reason_code in self.reason_codes:
            data += encode_uint8(reason_code)
        encoded.extend(encode_varint(len(data)))
        encoded.extend(data)
        return bytes(encoded)

    @classmethod
    def decode(cls, flags: int, data: bytes) -> MQTTUnsubAckPacket:
        if flags != 0:
            raise MQTTError(f"Invalid flags, expected 0 but got {flags}", MQTTReasonCode["MalformedPacket"])
        offset = 0
        packet_id, packet_id_length = decode_uint16(data[offset:])
        offset += packet_id_length
        props, props_length = decode_properties(data[offset:])
        if props:
            validate_properties(props, MQTTPacketType["UNSUBACK"])
        offset += props_length
        reason_codes = [b for b in data[offset:]]
        return MQTTUnsubAckPacket(packet_id, reason_codes, properties=props)
