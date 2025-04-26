import socket
import ssl
from typing import Callable, cast, Final, NamedTuple

from .error import MQTTError
from .mqtt_spec import MQTTReasonCode
from .logger import get_logger
from .mqtt_spec import MQTTPacketType
from .packet import decode_packet_from_parts, MQTTPacket, MQTTConnAckPacket, PING, PONG
from .serialization import MAX_VARINT
from .socket_wrapper import SocketWrapper, SocketWrapperCloseCondition

logger: Final = get_logger("connection")

ConnectionCloseCallback = Callable[[], None]
ConnectionOpenCallback = Callable[[], None]
ConnectionReadCallback = Callable[[MQTTPacket], None]


class VarintDecodeResult(NamedTuple):
    """Result of decoding a variable length integer."""
    value: int
    multiplier: int
    complete: bool


InitVarintDecodeState: Final = VarintDecodeResult(0, 1, False)


def decode_varint_from_socket(sock: socket.socket | ssl.SSLSocket, partial: int, mult: int) -> VarintDecodeResult:
    """Incrementally decode a variable length integer from a socket.

    Returns -1 if there is not enough data to decode the varint."""
    result = partial
    sz = 0
    while True:
        try:
            data = sock.recv(1)
        except (BlockingIOError, ssl.SSLWantReadError):
            return VarintDecodeResult(result, mult, False)
        if not data:
            raise SocketWrapperCloseCondition("Empty read")
        byte = data[0]
        sz += 1
        result += byte % 0x80 * mult
        if result > MAX_VARINT:
            raise MQTTError("Varint overflow", MQTTReasonCode["MalformedPacket"])
        if byte < 0x80:
            return VarintDecodeResult(result, mult, True)
        mult *= 0x80


class Connection:
    """Manage the lifecycle of a connection to the MQTT broker."""
    __slots__ = (
        "_close_callback",
        "_open_callback",
        "_read_callback",
        "_partial_head",
        "_partial_length",
        "_partial_length_mult",
        "_partial_length_complete",
        "_partial_data",
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

        # Buffer for partial packets on the receiving end.
        self._partial_head = -1
        self._partial_length: VarintDecodeResult = InitVarintDecodeState
        self._partial_data = bytearray()

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
        try:
            if self._partial_head == -1:
                partial_head = sock.recv(1)
                if not partial_head:
                    raise SocketWrapperCloseCondition("Empty read")
                self._partial_head = partial_head[0]
            if not self._partial_length.complete:
                self._partial_length = decode_varint_from_socket(sock, self._partial_length.value, self._partial_length.multiplier)
                if not self._partial_length.complete:
                    # If the socket didn't have enough data for us, we need to wait for more.
                    return

            while len(self._partial_data) < self._partial_length.value:
                # Read the rest of the packet.
                data = sock.recv(self._partial_length.value - len(self._partial_data))
                if not data:
                    raise SocketWrapperCloseCondition("Empty read")
                self._partial_data.extend(data)
        except (BlockingIOError, ssl.SSLWantReadError):
            # If the socket doesn't have enough data for us, we need to wait for more.
            return

        # We have a complete packet, decode it and clear the read buffer.
        # Reset the partial state first, in case of decoding errors.
        packet_data = bytes(self._partial_data)
        packet_head = self._partial_head
        self._partial_head = -1
        self._partial_length = InitVarintDecodeState
        self._partial_data.clear()
        packet = decode_packet_from_parts(packet_head, packet_data)

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
