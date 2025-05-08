"""SUBSCRIBE, SUBACK, UNSUBSCRIBE, and UNSUBACK packets."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final, Sequence

from .base import MQTTPacket
from ..error import MQTTError
from ..mqtt_spec import MQTTPacketType, MQTTReasonCode
from ..property import (
    MQTTSubscribeProps,
    MQTTSubAckProps,
    MQTTUnsubscribeProps,
    MQTTUnsubAckProps,
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

HEAD_SUBSCRIBE: Final = (MQTTPacketType.SUBSCRIBE << 4) + 0x02
HEAD_SUBACK: Final = MQTTPacketType.SUBACK << 4
HEAD_UNSUBSCRIBE: Final = (MQTTPacketType.UNSUBSCRIBE << 4) + 0x02
HEAD_UNSUBACK: Final = MQTTPacketType.UNSUBACK << 4


@dataclass(match_args=True, slots=True)
class MQTTSubscribePacket(MQTTPacket):
    packet_type = MQTTPacketType.SUBSCRIBE
    props_type = MQTTSubscribeProps
    topics: Sequence[tuple[str, int]] = field(default_factory=tuple)
    packet_id: int = 0
    properties: MQTTSubscribeProps = field(default_factory=MQTTSubscribeProps)

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
        data += self.properties.encode()
        for topic, subscribe_opts in self.topics:
            data += encode_string(topic) + encode_uint8(subscribe_opts)
        encoded.extend(encode_varint(len(data)))
        encoded.extend(data)
        return bytes(encoded)

    @classmethod
    def decode(cls, flags: int, data: memoryview) -> MQTTSubscribePacket:
        if flags != 0x02:
            raise MQTTError(f"Invalid flags, expected 0x02 but got {flags}", MQTTReasonCode.MalformedPacket)
        offset = 0
        packet_id, packet_id_length = decode_uint16(data[offset:])
        offset += packet_id_length
        props, props_length = MQTTSubscribeProps.decode(data[offset:])
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
    packet_type = MQTTPacketType.SUBACK
    props_type = MQTTSubAckProps
    packet_id: int
    reason_codes: Sequence[MQTTReasonCode] = field(default_factory=tuple)
    properties: MQTTSubAckProps = field(default_factory=MQTTSubAckProps)

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
        props = self.properties.encode()
        length += len(props)
        encoded.extend(encode_varint(length))
        encoded.extend(encode_uint16(self.packet_id))
        encoded.extend(props)
        for reason_code in self.reason_codes:
            encoded.append(reason_code.value)
        return bytes(encoded)

    @classmethod
    def decode(cls, flags: int, data: memoryview) -> MQTTSubAckPacket:
        if flags != 0:
            raise MQTTError(f"Invalid flags, expected 0 but got {flags}", MQTTReasonCode.MalformedPacket)
        offset = 0
        packet_id, packet_id_length = decode_uint16(data[offset:])
        offset += packet_id_length
        props, props_length = MQTTSubAckProps.decode(data[offset:])
        offset += props_length
        reason_codes = [MQTTReasonCode(b) for b in data[offset:]]
        return MQTTSubAckPacket(packet_id, reason_codes, properties=props)


@dataclass(match_args=True, slots=True)
class MQTTUnsubscribePacket(MQTTPacket):
    packet_type = MQTTPacketType.UNSUBSCRIBE
    props_type = MQTTUnsubscribeProps
    topics: Sequence[str] = field(default_factory=tuple)
    packet_id: int = 0
    properties: MQTTUnsubscribeProps = field(default_factory=MQTTUnsubscribeProps)

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
        data = encode_uint16(self.packet_id) + self.properties.encode()
        for topic in self.topics:
            data += encode_string(topic)
        encoded.extend(encode_varint(len(data)))
        encoded.extend(data)
        return bytes(encoded)

    @classmethod
    def decode(cls, flags: int, data: memoryview) -> MQTTUnsubscribePacket:
        if flags != 0x02:
            raise MQTTError(f"Invalid flags, expected 0x02 but got {flags}", MQTTReasonCode.MalformedPacket)
        offset = 0
        packet_id, packet_id_length = decode_uint16(data[offset:])
        offset += packet_id_length
        props, props_length = MQTTUnsubscribeProps.decode(data[offset:])
        offset += props_length
        topics = []
        while offset < len(data):
            topic, topic_length = decode_string(data[offset:])
            offset += topic_length
            topics.append(topic)
        return MQTTUnsubscribePacket(topics, packet_id, properties=props)


@dataclass(match_args=True, slots=True)
class MQTTUnsubAckPacket(MQTTPacket):
    packet_type = MQTTPacketType.UNSUBACK
    props_type = MQTTUnsubAckProps
    packet_id: int
    reason_codes: Sequence[MQTTReasonCode] = field(default_factory=tuple)
    properties: MQTTUnsubAckProps = field(default_factory=MQTTUnsubAckProps)

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
        data = encode_uint16(self.packet_id) + self.properties.encode()
        for reason_code in self.reason_codes:
            data += encode_uint8(reason_code.value)
        encoded.extend(encode_varint(len(data)))
        encoded.extend(data)
        return bytes(encoded)

    @classmethod
    def decode(cls, flags: int, data: memoryview) -> MQTTUnsubAckPacket:
        if flags != 0:
            raise MQTTError(f"Invalid flags, expected 0 but got {flags}", MQTTReasonCode.MalformedPacket)
        offset = 0
        packet_id, packet_id_length = decode_uint16(data[offset:])
        offset += packet_id_length
        props, props_length = MQTTUnsubAckProps.decode(data[offset:])
        offset += props_length
        reason_codes = [MQTTReasonCode(b) for b in data[offset:]]
        return MQTTUnsubAckPacket(packet_id, reason_codes, properties=props)
