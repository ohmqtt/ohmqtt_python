from __future__ import annotations

from dataclasses import dataclass, field
import socket
import ssl
import threading
from typing import Callable

from .keepalive import KeepAlive
from .selector import InterruptibleSelector
from ..decoder import IncrementalDecoder
from ..mqtt_spec import MQTTReasonCode
from ..packet import MQTTPacket, MQTTConnAckPacket
from ..property import MQTTPropertyDict


ConnectionCloseCallback = Callable[[], None]
ConnectionOpenCallback = Callable[[MQTTConnAckPacket], None]
ConnectionReadCallback = Callable[[MQTTPacket], None]


class ConnectionCloseCondition(Exception):
    """Any exception which should cause the socket to close."""
    pass


@dataclass(slots=True, match_args=True, frozen=True)
class ConnectParams:
    """Parameters for the MQTT connection."""
    host: str = ""
    port: int = 0
    client_id: str = ""
    reconnect_delay: int = 0
    keepalive_interval: int = 0
    tcp_nodelay: bool = True
    use_tls: bool = False
    tls_context: ssl.SSLContext = field(default_factory=ssl.create_default_context)
    tls_hostname: str = ""
    protocol_version: int = 5
    clean_start: bool = False
    will_topic: str = ""
    will_payload: bytes = b""
    will_qos: int = 0
    will_retain: bool = False
    will_properties: MQTTPropertyDict = field(default_factory=lambda: MQTTPropertyDict())
    connect_properties: MQTTPropertyDict = field(default_factory=lambda: MQTTPropertyDict())


@dataclass(kw_only=True, slots=True)
class StateData:
    """State data for the connection.

    This should contain any attributes needed by multiple states.

    The data in this class should never be accessed from outside the state methods."""
    sock: socket.socket | ssl.SSLSocket = field(init=False, default_factory=socket.socket)
    disconnect_rc: int = field(init=False, default=MQTTReasonCode.NormalDisconnection)
    keepalive: KeepAlive = field(init=False, default_factory=KeepAlive)
    decoder: IncrementalDecoder = field(init=False, default_factory=IncrementalDecoder)


class InterruptGuard:
    """Prevent unnecessary interrupts in the connection loop by wrapping calls to select with this guard."""
    __slots__ = ("should_interrupt",)

    def __init__(self) -> None:
        self.should_interrupt = False

    def __enter__(self) -> None:
        self.should_interrupt = True

    def __exit__(self, *args: object) -> None:
        self.should_interrupt = False


@dataclass(slots=True, kw_only=True)
class StateEnvironment:
    """State environment for the connection.
    
    Data in this class is shared with the outside world."""
    close_callback: ConnectionCloseCallback
    open_callback: ConnectionOpenCallback
    read_callback: ConnectionReadCallback
    write_buffer_lock: threading.Lock = field(init=False, default_factory=threading.Lock)
    write_buffer: bytearray = field(init=False, default_factory=bytearray)
    selector: InterruptibleSelector = field(init=False, default_factory=InterruptibleSelector)
