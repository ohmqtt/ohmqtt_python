from __future__ import annotations

from dataclasses import dataclass, field
import socket
import ssl
from typing import Callable, TypeAlias

from .address import Address
from .decoder import IncrementalDecoder
from .keepalive import KeepAlive
from .timeout import Timeout
from ..mqtt_spec import MQTTReasonCode
from ..packet import (
    MQTTConnAckPacket,
    MQTTDisconnectPacket,
    MQTTAuthPacket,
    MQTTPublishPacket,
    MQTTPubAckPacket,
    MQTTPubRecPacket,
    MQTTPubRelPacket,
    MQTTPubCompPacket,
    MQTTSubscribePacket,
    MQTTUnsubscribePacket,
    MQTTSubAckPacket,
    MQTTUnsubAckPacket,
)
from ..property import MQTTConnectProps, MQTTWillProps


ReceivablePacketT: TypeAlias = (
    MQTTConnAckPacket |
    MQTTPublishPacket |
    MQTTPubAckPacket |
    MQTTPubRecPacket |
    MQTTPubRelPacket |
    MQTTPubCompPacket |
    MQTTSubAckPacket |
    MQTTUnsubAckPacket |
    MQTTAuthPacket |
    MQTTDisconnectPacket
)
SendablePacketT: TypeAlias = (
    MQTTPublishPacket |
    MQTTPubAckPacket |
    MQTTPubRecPacket |
    MQTTPubRelPacket |
    MQTTPubCompPacket |
    MQTTSubscribePacket |
    MQTTUnsubscribePacket |
    MQTTAuthPacket
)
ConnectionReadCallback: TypeAlias = Callable[[ReceivablePacketT], None]


@dataclass(slots=True, match_args=True, frozen=True)
class ConnectParams:
    """Parameters for the MQTT connection."""
    address: Address = field(default_factory=Address)
    client_id: str = ""
    connect_timeout: float | None = None
    reconnect_delay: int = 0
    keepalive_interval: int = 0
    tcp_nodelay: bool = True
    tls_context: ssl.SSLContext | None = None
    tls_hostname: str = ""
    protocol_version: int = 5
    clean_start: bool = False
    will_topic: str = ""
    will_payload: bytes = b""
    will_qos: int = 0
    will_retain: bool = False
    will_properties: MQTTWillProps = field(default_factory=MQTTWillProps)
    connect_properties: MQTTConnectProps = field(default_factory=MQTTConnectProps)


@dataclass(kw_only=True, slots=True)
class StateData:
    """State data for the connection.

    This should contain any attributes needed by multiple states.

    The data in this class should never be accessed from outside the state methods."""
    sock: socket.socket | ssl.SSLSocket = field(init=False, default_factory=socket.socket)
    disconnect_rc: MQTTReasonCode | None = field(init=False, default=None)
    keepalive: KeepAlive = field(init=False, default_factory=KeepAlive)
    timeout: Timeout = field(init=False, default_factory=Timeout)
    decoder: IncrementalDecoder = field(init=False, default_factory=IncrementalDecoder)
    connack: MQTTConnAckPacket | None = field(init=False, default=None)
    open_called: bool = field(init=False, default=False)


@dataclass(slots=True, kw_only=True)
class StateEnvironment:
    """State environment for the connection.

    Data in this class is shared with the outside world."""
    packet_callback: ConnectionReadCallback
    write_buffer: bytearray = field(init=False, default_factory=bytearray)
