"""CONNECT, CONNACK, and DISCONNECT packets."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final

from .base import MQTTPacket
from ..error import MQTTError
from ..mqtt_spec import MQTTReasonCode, MQTTPacketType
from ..property import (
    MQTTConnectProps,
    MQTTConnAckProps,
    MQTTDisconnectProps,
    MQTTWillProps,
)
from ..serialization import (
    encode_string,
    encode_binary,
    encode_uint8,
    encode_uint16,
    encode_bool,
    encode_varint,
    decode_string,
    decode_binary,
    decode_uint8,
    decode_uint16,
    decode_bool,
)


HEAD_CONNECT: Final = MQTTPacketType.CONNECT << 4
HEAD_CONNACK: Final = MQTTPacketType.CONNACK << 4
HEAD_DISCONNECT: Final = MQTTPacketType.DISCONNECT << 4


@dataclass(match_args=True, slots=True)
class MQTTConnectPacket(MQTTPacket):
    packet_type = MQTTPacketType.CONNECT
    props_type = MQTTConnectProps
    client_id: str = ""
    keep_alive: int = 0
    protocol_version: int = 5
    clean_start: bool = False
    will_props: MQTTWillProps = field(default_factory=MQTTWillProps)
    will_topic: str = ""
    will_payload: bytes = b""
    will_qos: int = 0
    will_retain: bool = False
    username: str | None = None
    password: bytes | None = None
    properties: MQTTConnectProps = field(default_factory=MQTTConnectProps)

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
            (self.clean_start << 1) |
            (self.will_qos << 3) |
            (self.will_retain << 5)
        )

        payload = bytearray()
        payload.extend(encode_string(self.client_id))
        if self.will_topic:
            payload.extend(self.will_props.encode() + encode_string(self.will_topic) + encode_binary(self.will_payload))
            connect_flags |= 0x04
        if self.username is not None:
            payload.extend(encode_string(self.username))
            connect_flags |= 0x80
        if self.password is not None:
            payload.extend(encode_binary(self.password))
            connect_flags |= 0x40

        data = b"".join((
            encode_binary(b"MQTT"),
            encode_uint8(self.protocol_version),
            encode_uint8(connect_flags),
            encode_uint16(self.keep_alive),
            self.properties.encode(),
            payload,
        ))
        head = HEAD_CONNECT
        length = encode_varint(len(data))
        encoded = bytearray()
        encoded.append(head)
        encoded.extend(length)
        encoded.extend(data)
        return bytes(encoded)

    @classmethod
    def decode(cls, flags: int, data: memoryview) -> MQTTConnectPacket:
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

        props, sz = MQTTConnectProps.decode(data[offset:])
        offset += sz

        client_id, sz = decode_string(data[offset:])
        offset += sz

        if will_flag:
            will_props, sz = MQTTWillProps.decode(data[offset:])
            offset += sz
            will_topic, sz = decode_string(data[offset:])
            offset += sz
            will_payload, sz = decode_binary(data[offset:])
            offset += sz
        else:
            will_props = MQTTWillProps()
            will_topic = ""
            will_payload = b""

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


@dataclass(match_args=True, slots=True)
class MQTTConnAckPacket(MQTTPacket):
    packet_type = MQTTPacketType.CONNACK
    props_type = MQTTConnAckProps
    reason_code: MQTTReasonCode = MQTTReasonCode.Success
    session_present: bool = False
    properties: MQTTConnAckProps = field(default_factory=MQTTConnAckProps)

    def __str__(self) -> str:
        attrs = [
            f"reason_code={hex(self.reason_code)}",
            f"session_present={self.session_present}",
            f"properties={self.properties}",
        ]
        return f"CONNACK[{', '.join(attrs)}]"

    def encode(self) -> bytes:
        head = HEAD_CONNACK
        data = encode_bool(self.session_present) + encode_uint8(self.reason_code.value) + self.properties.encode()
        length = encode_varint(len(data))
        return b"".join((bytes([head]), length, data))

    @classmethod
    def decode(cls, flags: int, data: memoryview) -> MQTTConnAckPacket:
        if flags != 0:
            raise MQTTError(f"Invalid flags, expected 0 but got {flags}", MQTTReasonCode.MalformedPacket)

        session_present, _ = decode_bool(data[0:])
        reason_code = MQTTReasonCode(decode_uint8(data[1:])[0])
        props, props_sz = MQTTConnAckProps.decode(data[2:])
        return MQTTConnAckPacket(reason_code, session_present, properties=props)


@dataclass(match_args=True, slots=True)
class MQTTDisconnectPacket(MQTTPacket):
    packet_type = MQTTPacketType.DISCONNECT
    props_type = MQTTDisconnectProps
    reason_code: MQTTReasonCode = MQTTReasonCode.Success
    properties: MQTTDisconnectProps = field(default_factory=MQTTDisconnectProps)

    def __str__(self) -> str:
        attrs = [
            f"reason_code={hex(self.reason_code)}",
            f"properties={self.properties}",
        ]
        return f"DISCONNECT[{', '.join(attrs)}]"

    def encode(self) -> bytes:
        # If the reason code is success and there are no properties, the packet can be empty.
        if self.reason_code == MQTTReasonCode.Success and len(self.properties) == 0:
            return HEAD_DISCONNECT.to_bytes(1, "big") + b"\x00"
        encoded = bytearray()
        encoded.append(HEAD_DISCONNECT)
        props = self.properties.encode()
        length = 1 + len(props)
        encoded.extend(encode_varint(length))
        encoded.append(self.reason_code.value)
        encoded.extend(props)
        return bytes(encoded)

    @classmethod
    def decode(cls, flags: int, data: memoryview) -> MQTTDisconnectPacket:
        if flags != 0:
            raise MQTTError(f"Invalid flags, expected 0 but got {flags}", MQTTReasonCode.MalformedPacket)
        if len(data) == 0:
            # An empty packet means success with no properties.
            return MQTTDisconnectPacket()
        reason_code = MQTTReasonCode(decode_uint8(data)[0])
        props, props_sz = MQTTDisconnectProps.decode(data[1:])
        return MQTTDisconnectPacket(reason_code, properties=props)
