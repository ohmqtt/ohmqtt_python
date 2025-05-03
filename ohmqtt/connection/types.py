from __future__ import annotations

from dataclasses import dataclass, field
import socket
import ssl
from typing import Callable

from .address import Address
from .decoder import IncrementalDecoder
from .keepalive import KeepAlive
from .timeout import Timeout
from ..mqtt_spec import MQTTReasonCode
from ..packet import MQTTPacket, MQTTConnAckPacket
from ..property import MQTTConnectProps, MQTTWillProps


ConnectionCloseCallback = Callable[[], None]
ConnectionOpenCallback = Callable[[MQTTConnAckPacket], None]
ConnectionReadCallback = Callable[[MQTTPacket], None]


class ConnectionCloseCondition(Exception):
    """Any exception which should cause the socket to close."""
    pass


@dataclass(slots=True, match_args=True, frozen=True)
class ConnectParams:
    """Parameters for the MQTT connection."""
    address: Address = field(default_factory=Address)
    client_id: str = ""
    connect_timeout: float | None = None
    reconnect_delay: int = 0
    keepalive_interval: int = 0
    tcp_nodelay: bool = True
    tls_context: ssl.SSLContext = field(default_factory=ssl.create_default_context)
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
    disconnect_rc: int = field(init=False, default=MQTTReasonCode.NormalDisconnection)
    keepalive: KeepAlive = field(init=False, default_factory=KeepAlive)
    timeout: Timeout = field(init=False, default_factory=Timeout)
    decoder: IncrementalDecoder = field(init=False, default_factory=IncrementalDecoder)


@dataclass(slots=True, kw_only=True)
class StateEnvironment:
    """State environment for the connection.
    
    Data in this class is shared with the outside world."""
    close_callback: ConnectionCloseCallback
    open_callback: ConnectionOpenCallback
    read_callback: ConnectionReadCallback
    write_buffer: bytearray = field(init=False, default_factory=bytearray)
