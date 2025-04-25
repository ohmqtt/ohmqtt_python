from __future__ import annotations

from abc import ABCMeta, abstractmethod
from typing import Final, Mapping, Sequence

from .error import MQTTError
from .mqtt_spec import MQTTPacketType, MQTTReasonCode
from .property import MQTTPropertyDict, encode_properties, decode_properties, hash_properties, validate_properties
from .serialization import (
    encode_bool,
    decode_bool,
    encode_uint8,
    decode_uint8,
    encode_uint16,
    decode_uint16,
    encode_string,
    decode_string,
    encode_binary,
    decode_binary,
    encode_varint,
    decode_varint,
)

HEAD_PUBLISH: Final = MQTTPacketType.PUBLISH << 4
HEAD_PUBACK: Final = MQTTPacketType.PUBACK << 4
HEAD_PUBREC: Final = MQTTPacketType.PUBREC << 4
HEAD_PUBREL: Final = (MQTTPacketType.PUBREL << 4) + 0x02
HEAD_PUBCOMP: Final = MQTTPacketType.PUBCOMP << 4

_MQTTPacketTypeLookup = {t.value: t.name for t in MQTTPacketType}


class MQTTPacket(metaclass=ABCMeta):
    """Base class for MQTT packets."""
    packet_type: MQTTPacketType
    __slots__ = tuple()  # type: ignore

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return NotImplemented
        return all(getattr(self, attr) == getattr(other, attr) for attr in self.__slots__)

    @abstractmethod
    def __hash__(self) -> int:
        ...  # pragma: no cover

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


class MQTTConnectPacket(MQTTPacket):
    packet_type = MQTTPacketType.CONNECT
    __slots__ = (
        "properties",
        "client_id",
        "keep_alive",
        "protocol_version",
        "clean_start",
        "will_props",
        "will_topic",
        "will_payload",
        "will_qos",
        "will_retain",
        "username",
        "password",
    )

    def __init__(
        self,
        client_id: str = "",
        keep_alive: int = 30,
        protocol_version: int = 5,
        *,
        clean_start: bool = False,
        will_props: MQTTPropertyDict | None = None,
        will_topic: str | None = None,
        will_payload: bytes | None = None,
        will_qos: int = 0,
        will_retain: bool = False,
        username: str | None = None,
        password: bytes | None = None,
        properties: MQTTPropertyDict | None = None,
    ):
        self.client_id = client_id
        self.keep_alive = keep_alive
        self.protocol_version = protocol_version
        self.clean_start = clean_start
        self.will_props = will_props if will_props else {}
        self.will_topic = will_topic
        self.will_payload = will_payload
        self.will_qos = will_qos
        self.will_retain = will_retain
        self.username = username
        self.password = password
        self.properties = properties if properties is not None else {}

    def __hash__(self) -> int:
        return hash((
            self.packet_type,
            self.client_id,
            self.keep_alive,
            self.protocol_version,
            self.clean_start,
            self.will_topic,
            self.will_payload,
            self.will_qos,
            self.will_retain,
            self.username,
            self.password,
            hash_properties(self.properties),
            hash_properties(self.will_props),
        ))

    def __str__(self) -> str:
        attrs = [
            f"client_id={self.client_id}",
            f"keep_alive={self.keep_alive}",
            f"protocol_version={self.protocol_version}",
            f"clean_start={self.clean_start}",
            f"username={self.username}",
            f"password={str(len(self.password)) + 'B' if self.password else None}",
            f"will_topic={self.will_topic}",
            f"will_payload={str(len(self.will_payload)) + 'B' if self.will_payload else None}",
            f"will_qos={self.will_qos}",
            f"will_retain={self.will_retain}",
            f"will_props={self.will_props}",
            f"properties={self.properties}",
        ]
        return f"CONNECT[{', '.join(attrs)}]"

    def encode(self) -> bytes:
        connect_flags = (
            (self.clean_start << 1) +
            (self.will_qos << 3) +
            (self.will_retain << 5)
        )

        payload = encode_string(self.client_id)
        if self.will_props is not None and self.will_topic is not None and self.will_payload is not None:
            payload += encode_properties(self.will_props) + encode_string(self.will_topic) + encode_binary(self.will_payload)
            connect_flags += 0x04
        if self.username is not None:
            payload += encode_string(self.username)
            connect_flags += 0x80
        if self.password is not None:
            payload += encode_binary(self.password)
            connect_flags += 0x40

        data = (
            encode_binary(b"MQTT") +
            encode_uint8(self.protocol_version) +
            encode_uint8(connect_flags) +
            encode_uint16(self.keep_alive) +
            encode_properties(self.properties) +
            payload
        )
        return encode_packet(self.packet_type, 0, data)
    
    @classmethod
    def decode(cls, flags: int, data: bytes) -> MQTTConnectPacket:
        if flags != 0:
            raise MQTTError(f"Invalid flags, expected 0 but got {flags}", MQTTReasonCode.MalformedPacket)

        offset = 0
        protocol_name, sz = decode_binary(data)
        offset += sz

        if protocol_name != b"MQTT":
            raise MQTTError("Invalid protocol name", MQTTReasonCode.ProtocolError)
        
        protocol_version, sz = decode_uint8(data[offset:])
        offset += sz

        if protocol_version != 5:
            raise MQTTError(f"Invalid protocol version, expected 5 but got {protocol_version}", MQTTReasonCode.UnsupportedProtocolVersion)

        connect_flags, sz = decode_uint8(data[offset:])
        offset += sz

        clean_start = connect_flags & 0x02 == 2
        will_flag = connect_flags & 0x04 == 4
        will_qos = connect_flags >> 3 & 0x03
        will_retain = connect_flags & 0x20 == 32
        password_flag = connect_flags & 0x40 == 64
        username_flag = connect_flags & 0x80 == 128

        keep_alive, sz = decode_uint16(data[offset:])
        offset += sz

        props, sz = decode_properties(data[offset:])
        offset += sz
        if props:
            validate_properties(props, MQTTPacketType.CONNECT)

        client_id, sz = decode_string(data[offset:])
        offset += sz

        if will_flag:
            will_props, sz = decode_properties(data[offset:])
            offset += sz
            if will_props:
                validate_properties(will_props, is_will=True)
            will_topic, sz = decode_string(data[offset:])
            offset += sz
            will_payload, sz = decode_binary(data[offset:])
            offset += sz
        else:
            will_props = None
            will_topic = None
            will_payload = None
        
        if username_flag:
            username, sz = decode_string(data[offset:])
            offset += sz
        else:
            username = None

        if password_flag:
            password, sz = decode_binary(data[offset:])
            offset += sz
        else:
            password = None

        return MQTTConnectPacket(
            client_id,
            keep_alive,
            protocol_version,
            clean_start=clean_start,
            will_props=will_props,
            will_topic=will_topic,
            will_payload=will_payload,
            will_qos=will_qos,
            will_retain=will_retain,
            password=password,
            username=username,
            properties=props,
        )


class MQTTConnAckPacket(MQTTPacket):
    packet_type = MQTTPacketType.CONNACK
    __slots__ = ("properties", "reason_code", "session_present")

    def __init__(
        self,
        reason_code: MQTTReasonCode = MQTTReasonCode.Success,
        session_present: bool = False,
        *,
        properties: MQTTPropertyDict | None = None,
    ):
        self.reason_code = reason_code
        self.session_present = session_present
        self.properties = properties if properties is not None else {}

    def __hash__(self) -> int:
        return hash((
            self.packet_type,
            self.session_present,
            self.reason_code,
            hash_properties(self.properties),
        ))

    def __str__(self) -> str:
        attrs = [
            f"reason_code={self.reason_code}",
            f"session_present={self.session_present}",
            f"properties={self.properties}",
        ]
        return f"CONNACK[{', '.join(attrs)}]"
    
    def encode(self) -> bytes:
        data = encode_bool(self.session_present) + encode_uint8(self.reason_code.value) + encode_properties(self.properties)
        return encode_packet(self.packet_type, 0, data)

    @classmethod
    def decode(cls, flags: int, data: bytes) -> MQTTConnAckPacket:
        if flags != 0:
            raise MQTTError(f"Invalid flags, expected 0 but got {flags}", MQTTReasonCode.MalformedPacket)

        session_present, _ = decode_bool(data[0:])
        reason_code, _ = decode_uint8(data[1:])
        props, props_sz = decode_properties(data[2:])
        return MQTTConnAckPacket(MQTTReasonCode(reason_code), session_present, properties=props)


class MQTTPublishPacket(MQTTPacket):
    packet_type = MQTTPacketType.PUBLISH
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
        encoded = bytearray()
        head = HEAD_PUBLISH + self.retain + (self.qos << 1) + (self.dup << 3)
        length = len(self.topic) + len(self.payload) + 2
        if self.qos > 0:
            length += 2
        if self.properties:
            props = encode_properties(self.properties)
        else:
            props = b"\x00"
        length += len(props)
        encoded.append(head)
        encoded.extend(encode_varint(length))
        encoded.extend(encode_string(self.topic))
        if self.qos > 0:
            encoded.extend(self.packet_id.to_bytes(2, byteorder="big"))
        encoded.extend(props)
        encoded.extend(self.payload)
        return bytes(encoded)

    @classmethod
    def decode(cls, flags: int, data: bytes) -> MQTTPublishPacket:
        qos = (flags >> 1) & 0x03
        if qos > 2:
            raise MQTTError(f"Invalid QoS level {qos}", MQTTReasonCode.MalformedPacket)
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
    encoded = bytearray()
    length = 2
    has_reason_code = packet.reason_code != MQTTReasonCode.Success
    has_properties = bool(packet.properties)
    if has_reason_code:
        length += 1
    if has_properties:
        props = encode_properties(packet.properties)
        length += len(props)
    encoded.append(head)
    encoded.extend(encode_varint(length))
    encoded.extend(packet.packet_id.to_bytes(2, byteorder="big"))
    if has_reason_code:
        encoded.extend(packet.reason_code.value.to_bytes(1, byteorder="big"))
    if has_properties:
        encoded.extend(props)
    return bytes(encoded)


def _decode_puback_common(packet_type: MQTTPacketType, data: bytes) -> tuple[int, MQTTReasonCode, MQTTPropertyDict]:
    """Common decoding logic for PUBACK, PUBREC, PUBREL, and PUBCOMP packets.

    Validity of flags is checked in the respective classes."""
    offset = 0
    packet_id, packet_id_length = decode_uint16(data[offset:])
    offset += packet_id_length
    if offset == len(data):
        # Reason code and properties are optional.
        return packet_id, MQTTReasonCode.Success, {}
    reason_code, reason_code_length = decode_uint8(data[offset:])
    offset += reason_code_length
    if offset == len(data):
        # Properties alone may be omitted.
        return packet_id, MQTTReasonCode(reason_code), {}
    props, props_length = decode_properties(data[offset:])
    if props:
        validate_properties(props, packet_type)
    return packet_id, MQTTReasonCode(reason_code), props


class MQTTPubAckPacket(MQTTPacket):
    packet_type = MQTTPacketType.PUBACK
    __slots__ = ("properties", "packet_id", "reason_code",)

    def __init__(
        self,
        packet_id: int,
        reason_code: MQTTReasonCode = MQTTReasonCode.Success,
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
            raise MQTTError(f"Invalid flags, expected 0 but got {flags}", MQTTReasonCode.MalformedPacket)
        packet_id, reason_code, props = _decode_puback_common(MQTTPacketType.PUBACK, data)
        return MQTTPubAckPacket(packet_id, MQTTReasonCode(reason_code), properties=props)


class MQTTPubRecPacket(MQTTPacket):
    packet_type = MQTTPacketType.PUBREC
    __slots__ = ("properties", "packet_id", "reason_code",)

    def __init__(
        self,
        packet_id: int,
        reason_code: MQTTReasonCode = MQTTReasonCode.Success,
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
            raise MQTTError(f"Invalid flags, expected 0 but got {flags}", MQTTReasonCode.MalformedPacket)
        packet_id, reason_code, props = _decode_puback_common(MQTTPacketType.PUBREC, data)
        return MQTTPubRecPacket(packet_id, MQTTReasonCode(reason_code), properties=props)


class MQTTPubRelPacket(MQTTPacket):
    packet_type = MQTTPacketType.PUBREL
    __slots__ = ("properties", "packet_id", "reason_code",)

    def __init__(
        self,
        packet_id: int,
        reason_code: MQTTReasonCode = MQTTReasonCode.Success,
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
            raise MQTTError(f"Invalid flags, expected 0x02 but got {flags}", MQTTReasonCode.MalformedPacket)
        packet_id, reason_code, props = _decode_puback_common(MQTTPacketType.PUBREL, data)
        return MQTTPubRelPacket(packet_id, MQTTReasonCode(reason_code), properties=props)


class MQTTPubCompPacket(MQTTPacket):
    packet_type = MQTTPacketType.PUBCOMP
    __slots__ = ("properties", "packet_id", "reason_code",)

    def __init__(
        self,
        packet_id: int,
        reason_code: MQTTReasonCode = MQTTReasonCode.Success,
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
            raise MQTTError(f"Invalid flags, expected 0 but got {flags}", MQTTReasonCode.MalformedPacket)
        packet_id, reason_code, props = _decode_puback_common(MQTTPacketType.PUBCOMP, data)
        return MQTTPubCompPacket(packet_id, MQTTReasonCode(reason_code), properties=props)


class MQTTSubscribePacket(MQTTPacket):
    packet_type = MQTTPacketType.SUBSCRIBE
    __slots__ = ("properties", "packet_id", "topics",)

    def __init__(
        self,
        topics: Sequence[tuple[str, int]],
        packet_id: int,
        *,
        properties: MQTTPropertyDict | None = None,
    ):
        self.topics = tuple(topics)
        self.packet_id = packet_id
        self.properties = properties if properties is not None else {}

    def __hash__(self) -> int:
        return hash((
            self.packet_type,
            self.packet_id,
            self.topics,
            hash_properties(self.properties),
        ))

    def __str__(self) -> str:
        attrs = [
            f"packet_id={self.packet_id}",
            f"topics={self.topics}",
            f"properties={self.properties}",
        ]
        return f"SUBSCRIBE[{', '.join(attrs)}]"

    def encode(self) -> bytes:
        data = encode_uint16(self.packet_id)
        data += encode_properties(self.properties)
        for topic, subscribe_opts in self.topics:
            data += encode_string(topic) + encode_uint8(subscribe_opts)
        return encode_packet(self.packet_type, 0x02, data)

    @classmethod
    def decode(cls, flags: int, data: bytes) -> MQTTSubscribePacket:
        if flags != 0x02:
            raise MQTTError(f"Invalid flags, expected 0x02 but got {flags}", MQTTReasonCode.MalformedPacket)
        offset = 0
        packet_id, packet_id_length = decode_uint16(data[offset:])
        offset += packet_id_length
        props, props_length = decode_properties(data[offset:])
        if props:
            validate_properties(props, MQTTPacketType.SUBSCRIBE)
        offset += props_length
        topics = []
        while offset < len(data):
            topic, topic_length = decode_string(data[offset:])
            offset += topic_length
            subscribe_opts, subscribe_opts_length = decode_uint8(data[offset:])
            offset += subscribe_opts_length
            topics.append((topic, subscribe_opts))
        return MQTTSubscribePacket(topics, packet_id, properties=props)


class MQTTSubAckPacket(MQTTPacket):
    packet_type = MQTTPacketType.SUBACK
    __slots__ = ("properties", "packet_id", "reason_codes",)

    def __init__(
        self,
        packet_id: int,
        reason_codes: Sequence[MQTTReasonCode],
        *,
        properties: MQTTPropertyDict | None = None,
    ):
        self.packet_id = packet_id
        self.reason_codes = tuple(reason_codes)
        self.properties = properties if properties is not None else {}

    def __hash__(self) -> int:
        return hash((
            self.packet_type,
            self.packet_id,
            self.reason_codes,
            hash_properties(self.properties),
        ))

    def __str__(self) -> str:
        attrs = [
            f"packet_id={self.packet_id}",
            f"reason_codes={self.reason_codes}",
            f"properties={self.properties}",
        ]
        return f"SUBACK[{', '.join(attrs)}]"

    def encode(self) -> bytes:
        data = encode_uint16(self.packet_id)
        data += encode_properties(self.properties)
        for reason_code in self.reason_codes:
            data += encode_uint8(reason_code)
        return encode_packet(self.packet_type, 0, data)

    @classmethod
    def decode(cls, flags: int, data: bytes) -> MQTTSubAckPacket:
        if flags != 0:
            raise MQTTError(f"Invalid flags, expected 0 but got {flags}", MQTTReasonCode.MalformedPacket)
        offset = 0
        packet_id, packet_id_length = decode_uint16(data[offset:])
        offset += packet_id_length
        props, props_length = decode_properties(data[offset:])
        if props:
            validate_properties(props, MQTTPacketType.SUBACK)
        offset += props_length
        reason_codes = [MQTTReasonCode(b) for b in data[offset:]]
        return MQTTSubAckPacket(packet_id, reason_codes, properties=props)


class MQTTUnsubscribePacket(MQTTPacket):
    packet_type = MQTTPacketType.UNSUBSCRIBE
    __slots__ = ("packet_id", "topics", "properties")

    def __init__(self, topics: Sequence[str], packet_id: int, *, properties: MQTTPropertyDict | None = None):
        self.topics = tuple(topics)
        self.packet_id = packet_id
        self.properties = properties if properties is not None else {}

    def __hash__(self) -> int:
        return hash((
            self.packet_type,
            self.packet_id,
            self.topics,
            hash_properties(self.properties),
        ))

    def __str__(self) -> str:
        attrs = [
            f"packet_id={self.packet_id}",
            f"topics={self.topics}",
            f"properties={self.properties}",
        ]
        return f"UNSUBSCRIBE[{', '.join(attrs)}]"

    def encode(self) -> bytes:
        data = encode_uint16(self.packet_id) + encode_properties(self.properties)
        for topic in self.topics:
            data += encode_string(topic)
        return encode_packet(self.packet_type, 0x02, data)

    @classmethod
    def decode(cls, flags: int, data: bytes) -> MQTTUnsubscribePacket:
        if flags != 0x02:
            raise MQTTError(f"Invalid flags, expected 0x02 but got {flags}", MQTTReasonCode.MalformedPacket)
        offset = 0
        packet_id, packet_id_length = decode_uint16(data[offset:])
        offset += packet_id_length
        props, props_length = decode_properties(data[offset:])
        if props:
            validate_properties(props, MQTTPacketType.UNSUBSCRIBE)
        offset += props_length
        topics = []
        while offset < len(data):
            topic, topic_length = decode_string(data[offset:])
            offset += topic_length
            topics.append(topic)
        return MQTTUnsubscribePacket(topics, packet_id, properties=props)


class MQTTUnsubAckPacket(MQTTPacket):
    packet_type = MQTTPacketType.UNSUBACK
    __slots__ = ("packet_id", "reason_codes", "properties")

    def __init__(self, packet_id: int, reason_codes: Sequence[MQTTReasonCode], *, properties: MQTTPropertyDict | None = None):
        self.packet_id = packet_id
        self.reason_codes = tuple(reason_codes)
        self.properties = properties if properties is not None else {}

    def __hash__(self) -> int:
        return hash((
            self.packet_type,
            self.packet_id,
            self.reason_codes,
            hash_properties(self.properties),
        ))

    def __str__(self) -> str:
        attrs = [
            f"packet_id={self.packet_id}",
            f"reason_codes={self.reason_codes}",
            f"properties={self.properties}",
        ]
        return f"UNSUBACK[{', '.join(attrs)}]"

    def encode(self) -> bytes:
        data = encode_uint16(self.packet_id) + encode_properties(self.properties)
        for reason_code in self.reason_codes:
            data += encode_uint8(reason_code)
        return encode_packet(self.packet_type, 0, data)

    @classmethod
    def decode(cls, flags: int, data: bytes) -> MQTTUnsubAckPacket:
        if flags != 0:
            raise MQTTError(f"Invalid flags, expected 0 but got {flags}", MQTTReasonCode.MalformedPacket)
        offset = 0
        packet_id, packet_id_length = decode_uint16(data[offset:])
        offset += packet_id_length
        props, props_length = decode_properties(data[offset:])
        if props:
            validate_properties(props, MQTTPacketType.UNSUBACK)
        offset += props_length
        reason_codes = [MQTTReasonCode(b) for b in data[offset:]]
        return MQTTUnsubAckPacket(packet_id, reason_codes, properties=props)


class MQTTPingReqPacket(MQTTPacket):
    packet_type = MQTTPacketType.PINGREQ
    __slots__ = tuple()

    def __hash__(self) -> int:
        return hash((
            self.packet_type,
        ))

    def __str__(self) -> str:
        return "PINGREQ[]"

    def encode(self) -> bytes:
        return encode_packet(self.packet_type, 0, b"")
    
    @classmethod
    def decode(cls, flags: int, data: bytes) -> MQTTPingReqPacket:
        if flags != 0:
            raise MQTTError(f"Invalid flags, expected 0 but got {flags}", MQTTReasonCode.MalformedPacket)
        if len(data) != 0:
            raise MQTTError(f"Invalid length, expected 0 but got {len(data)}", MQTTReasonCode.MalformedPacket)
        return MQTTPingReqPacket()
    

class MQTTPingRespPacket(MQTTPacket):
    packet_type = MQTTPacketType.PINGRESP
    __slots__ = tuple()

    def __hash__(self) -> int:
        return hash((
            self.packet_type,
        ))

    def __str__(self) -> str:
        return "PINGRESP[]"

    def encode(self) -> bytes:
        return encode_packet(self.packet_type, 0, b"")
    
    @classmethod
    def decode(cls, flags: int, data: bytes) -> MQTTPingRespPacket:
        if flags != 0:
            raise MQTTError(f"Invalid flags, expected 0 but got {flags}", MQTTReasonCode.MalformedPacket)
        if len(data) != 0:
            raise MQTTError(f"Invalid length, expected 0 but got {len(data)}", MQTTReasonCode.MalformedPacket)
        return MQTTPingRespPacket()


class MQTTDisconnectPacket(MQTTPacket):
    packet_type = MQTTPacketType.DISCONNECT
    __slots__ = ("properties", "reason_code",)

    def __init__(self, reason_code: MQTTReasonCode = MQTTReasonCode.Success, *, properties: MQTTPropertyDict | None = None):
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
        return f"DISCONNECT[{', '.join(attrs)}]"

    def encode(self) -> bytes:
        # If the reason code is success and there are no properties, the packet can be empty.
        if self.reason_code == MQTTReasonCode.Success and len(self.properties) == 0:
            return encode_packet(self.packet_type, 0, b"")
        var_header = encode_uint8(self.reason_code.value)
        var_header += encode_properties(self.properties)
        return encode_packet(self.packet_type, 0, var_header)
    
    @classmethod
    def decode(cls, flags: int, data: bytes) -> MQTTDisconnectPacket:
        if flags != 0:
            raise MQTTError(f"Invalid flags, expected 0 but got {flags}", MQTTReasonCode.MalformedPacket)
        if len(data) == 0:
            # An empty packet means success with no properties.
            return MQTTDisconnectPacket()
        reason_code, sz = decode_uint8(data)
        props, props_sz = decode_properties(data[sz:])
        return MQTTDisconnectPacket(MQTTReasonCode(reason_code), properties=props)


class MQTTAuthPacket(MQTTPacket):
    packet_type = MQTTPacketType.AUTH
    __slots__ = ("properties", "reason_code")

    def __init__(self, reason_code: MQTTReasonCode = MQTTReasonCode.Success, *, properties: MQTTPropertyDict | None = None):
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
        if self.reason_code == MQTTReasonCode.Success and len(self.properties) == 0:
            return encode_packet(self.packet_type, 0, b"")
        data = encode_uint8(self.reason_code.value) + encode_properties(self.properties)
        return encode_packet(self.packet_type, 0, data)
    
    @classmethod
    def decode(cls, flags: int, data: bytes) -> "MQTTAuthPacket":
        if flags != 0:
            raise MQTTError(f"Invalid flags, expected 0 but got {flags}", MQTTReasonCode.MalformedPacket)
        if len(data) == 0:
            # An empty packet means success with no properties.
            return MQTTAuthPacket()
        reason_code, sz = decode_uint8(data)
        props, props_sz = decode_properties(data[sz:])
        if props:
            validate_properties(props, MQTTPacketType.AUTH)
        return MQTTAuthPacket(MQTTReasonCode(reason_code), properties=props)


# Map of packet types to their respective classes.
_ControlPacketClasses: Mapping[int, type[MQTTPacket]] = {
    MQTTPacketType.CONNECT.value: MQTTConnectPacket,
    MQTTPacketType.CONNACK.value: MQTTConnAckPacket,
    MQTTPacketType.PUBLISH.value: MQTTPublishPacket,
    MQTTPacketType.PUBACK.value: MQTTPubAckPacket,
    MQTTPacketType.PUBREC.value: MQTTPubRecPacket,
    MQTTPacketType.PUBREL.value: MQTTPubRelPacket,
    MQTTPacketType.PUBCOMP.value: MQTTPubCompPacket,
    MQTTPacketType.SUBSCRIBE.value: MQTTSubscribePacket,
    MQTTPacketType.SUBACK.value: MQTTSubAckPacket,
    MQTTPacketType.UNSUBSCRIBE.value: MQTTUnsubscribePacket,
    MQTTPacketType.UNSUBACK.value: MQTTUnsubAckPacket,
    MQTTPacketType.PINGREQ.value: MQTTPingReqPacket,
    MQTTPacketType.PINGRESP.value: MQTTPingRespPacket,
    MQTTPacketType.DISCONNECT.value: MQTTDisconnectPacket,
    MQTTPacketType.AUTH.value: MQTTAuthPacket,
}


def decode_packet(data: bytes) -> MQTTPacket:
    """Decode a packet from binary data.

    The packet must be complete and correctly framed."""
    try:
        decoder = _ControlPacketClasses[data[0] // 0x10]
    except KeyError:
        raise MQTTError(f"Invalid packet type {data[0] // 16}", MQTTReasonCode.MalformedPacket)
    flags = data[0] % 0x10

    length, sz = decode_varint(data[1:])
    offset = sz + 1
    remainder = data[offset:]
    if len(remainder) != length:
        raise MQTTError(f"Invalid length, expected {length} bytes but got {len(remainder)}", MQTTReasonCode.MalformedPacket)
    return decoder.decode(flags, remainder)


def decode_packet_from_parts(head: int, data: bytes) -> MQTTPacket:
    """Finish decoding a packet which has already been split into parts by an incremental reader."""
    try:
        decoder = _ControlPacketClasses[head // 0x10]
    except KeyError:
        raise MQTTError(f"Invalid packet type {head // 16}", MQTTReasonCode.MalformedPacket)
    flags = head % 0x10

    return decoder.decode(flags, data)


def encode_packet(packet_type: MQTTPacketType, flags: int, data: bytes) -> bytes:
    """Helper for finalizing encoded packets"""
    head = (packet_type.value * 16) + flags
    length = encode_varint(len(data))
    return head.to_bytes(1, byteorder="big") + length + data


# Ping packets can be singletons, pre-encode them.
PING: Final = MQTTPingReqPacket().encode()
PONG: Final = MQTTPingRespPacket().encode()
