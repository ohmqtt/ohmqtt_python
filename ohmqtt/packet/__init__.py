from __future__ import annotations

from typing import Final, Mapping

from ..error import MQTTError
from ..mqtt_spec import MQTTPacketType, MQTTReasonCode
from ..serialization import decode_varint

from .base import MQTTPacket as MQTTPacket
from .connect import MQTTConnectPacket as MQTTConnectPacket
from .connect import MQTTConnAckPacket as MQTTConnAckPacket
from .connect import MQTTDisconnectPacket as MQTTDisconnectPacket
from .publish import MQTTPublishPacket as MQTTPublishPacket
from .publish import MQTTPubAckPacket as MQTTPubAckPacket
from .publish import MQTTPubRecPacket as MQTTPubRecPacket
from .publish import MQTTPubRelPacket as MQTTPubRelPacket
from .publish import MQTTPubCompPacket as MQTTPubCompPacket
from .subscribe import MQTTSubscribePacket as MQTTSubscribePacket
from .subscribe import MQTTSubAckPacket as MQTTSubAckPacket
from .subscribe import MQTTUnsubscribePacket as MQTTUnsubscribePacket
from .subscribe import MQTTUnsubAckPacket as MQTTUnsubAckPacket
from .ping import MQTTPingReqPacket as MQTTPingReqPacket
from .ping import MQTTPingRespPacket as MQTTPingRespPacket
from .ping import PING as PING
from .ping import PONG as PONG
from .auth import MQTTAuthPacket as MQTTAuthPacket


# Map of packet types to their respective classes.
_ControlPacketClasses: Final[Mapping[int, type[MQTTPacket]]] = {
    MQTTPacketType.CONNECT: MQTTConnectPacket,
    MQTTPacketType.CONNACK: MQTTConnAckPacket,
    MQTTPacketType.PUBLISH: MQTTPublishPacket,
    MQTTPacketType.PUBACK: MQTTPubAckPacket,
    MQTTPacketType.PUBREC: MQTTPubRecPacket,
    MQTTPacketType.PUBREL: MQTTPubRelPacket,
    MQTTPacketType.PUBCOMP: MQTTPubCompPacket,
    MQTTPacketType.SUBSCRIBE: MQTTSubscribePacket,
    MQTTPacketType.SUBACK: MQTTSubAckPacket,
    MQTTPacketType.UNSUBSCRIBE: MQTTUnsubscribePacket,
    MQTTPacketType.UNSUBACK: MQTTUnsubAckPacket,
    MQTTPacketType.PINGREQ: MQTTPingReqPacket,
    MQTTPacketType.PINGRESP: MQTTPingRespPacket,
    MQTTPacketType.DISCONNECT: MQTTDisconnectPacket,
    MQTTPacketType.AUTH: MQTTAuthPacket,
}


def decode_packet(data: bytes) -> MQTTPacket:
    """Decode a packet from binary data.

    The packet must be complete and correctly framed."""
    try:
        cls_id = data[0] // 0x10
        decoder = _ControlPacketClasses[cls_id]
    except KeyError:
        raise MQTTError(f"Invalid packet type {cls_id}", MQTTReasonCode.MalformedPacket)
    flags = data[0] % 0x10

    view = memoryview(data)
    length, sz = decode_varint(view[1:])
    offset = sz + 1
    remainder = view[offset:]
    if len(remainder) != length:
        raise MQTTError(f"Invalid length, expected {length} bytes but got {len(remainder)}", MQTTReasonCode.MalformedPacket)
    return decoder.decode(flags, remainder)


def decode_packet_from_parts(head: int, data: memoryview) -> MQTTPacket:
    """Finish decoding a packet which has already been split into parts by an incremental reader."""
    try:
        cls_id = head // 0x10
        decoder = _ControlPacketClasses[cls_id]
    except KeyError:
        raise MQTTError(f"Invalid packet type {cls_id}", MQTTReasonCode.MalformedPacket)
    flags = head % 0x10

    return decoder.decode(flags, data)
