import socket
import ssl
from typing import Callable, cast, Final

from .decoder import IncrementalDecoder
from .logger import get_logger
from .mqtt_spec import MQTTPacketType
from .packet import MQTTPacket, MQTTConnAckPacket, PING, PONG
from .socket_wrapper import SocketWrapper

logger: Final = get_logger("connection")

ConnectionCloseCallback = Callable[[], None]
ConnectionOpenCallback = Callable[[], None]
ConnectionReadCallback = Callable[[MQTTPacket], None]


class Connection:
    """Manage the lifecycle of a connection to the MQTT broker."""
    __slots__ = (
        "_close_callback",
        "_open_callback",
        "_read_callback",
        "_decoder",
        "sock",
    )

    def __init__(self,
        host: str,
        port: int,
        close_callback: ConnectionCloseCallback,
        open_callback: ConnectionOpenCallback,
        read_callback: ConnectionReadCallback,
        *,
        keepalive_interval: int = 0,
        use_tls: bool = False,
        tls_context: ssl.SSLContext | None = None,
        tls_hostname: str = "",
    ):
        self._close_callback = close_callback
        self._open_callback = open_callback
        self._read_callback = read_callback
        self._decoder = IncrementalDecoder()
        self.sock = SocketWrapper(
            host,
            port,
            close_callback=self._close_callback,
            keepalive_callback=self._keepalive_callback,
            open_callback=self._open_callback,
            read_callback=self._read_packet,
            keepalive_interval=keepalive_interval,
            use_tls=use_tls,
            tls_context=tls_context,
            tls_hostname=tls_hostname,
        )
        self.sock.start()

    def close(self) -> None:
        """Signal the connection to close."""
        self.sock.close()

    def send(self, data: bytes) -> None:
        """Send data to the broker."""
        self.sock.send(data)

    def _keepalive_callback(self, sock: SocketWrapper) -> None:
        """Called by the socket wrapper when a keepalive is due."""
        sock.send(PING)
        sock.ping_sent()
        logger.debug("---> ping")

    def _read_packet(self, sock: socket.socket | ssl.SSLSocket) -> None:
        """Called by the underlying SocketWrapper when the socket is ready to read.
        
        Incrementally reads and decodes a packet from the socket.
        Complete packets are passed up to the read callback."""
        packet = self._decoder.decode(sock)
        if packet is None:
            # No complete packet available yet.
            return

        # Ping requests and responses are handled at this layer.
        if packet.packet_type == MQTTPacketType["PINGRESP"]:
            self.sock.pong_received()
            logger.debug("<--- pong")
        elif packet.packet_type == MQTTPacketType["PINGREQ"]:
            self.sock.send(PONG)
            logger.debug("<--- ping pong --->")
        else:
            # All non-ping packets are passed to the read callback.
            self._read_callback(packet)
        # Handle connection-level server parameters here.
        if packet.packet_type == MQTTPacketType["CONNACK"]:
            packet = cast(MQTTConnAckPacket, packet)
            if "ServerKeepAlive" in packet.properties:
                # Override the keepalive interval with the server's value.
                keepalive = packet.properties["ServerKeepAlive"]
                self.sock.set_keepalive_interval(keepalive)
                logger.debug(f"Keepalive interval set by server to {keepalive} seconds")
