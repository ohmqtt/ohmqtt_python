import logging
import socket
import ssl
from typing import Callable, cast, Final

from .mqtt_spec import MQTTPacketType
from .packet import decode_packet_from_parts, MQTTPacket, MQTTConnAckPacket, PING, PONG
from .serialization import decode_varint_from_socket
from .socket_wrapper import SocketWrapper

logger: Final = logging.getLogger(__name__)

ConnectionCloseCallback = Callable[[], None]
ConnectionOpenCallback = Callable[[], None]
ConnectionReadCallback = Callable[[MQTTPacket], None]


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
    sock: SocketWrapper

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
        self._partial_length = 0
        self._partial_length_mult = 1
        self._partial_length_complete = False
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

    def _read_packet(self, sock: socket.socket | ssl.SSLSocket) -> None:
        """Called by the client when a new packet is received.
        
        Here we deal with picking out MQTT packets from the stream."""
        try:
            if self._partial_head == -1:
                partial_head = sock.recv(1)
                if not partial_head:
                    # Socket closed.
                    return
                self._partial_head = partial_head[0]
            if not self._partial_length_complete:
                decoded_length = decode_varint_from_socket(sock, self._partial_length, self._partial_length_mult)
                self._partial_length, self._partial_length_mult, self._partial_length_complete = decoded_length
            if not self._partial_length_complete:
                # We don't have the full packet yet.
                return

            while len(self._partial_data) < self._partial_length:
                # Read the rest of the packet.
                data = sock.recv(self._partial_length - len(self._partial_data))
                if not data:
                    # Socket closed.
                    return
                self._partial_data.extend(data)
        except (BlockingIOError, ssl.SSLWantReadError):
            # Packet is incomplete.
            return

        packet = decode_packet_from_parts(self._partial_head, bytes(self._partial_data))
        self._partial_head = -1
        self._partial_length = 0
        self._partial_length_mult = 1
        self._partial_length_complete = False
        self._partial_data.clear()

        if packet.packet_type == MQTTPacketType.PINGRESP:
            self.sock.pong_received()
        elif packet.packet_type == MQTTPacketType.PINGREQ:
            self.sock.send(PONG)
        else:
            # All non-ping packets are passed to the read callback.
            self._read_callback(packet)
        if packet.packet_type == MQTTPacketType.CONNACK:
            packet = cast(MQTTConnAckPacket, packet)
            if "ServerKeepAlive" in packet.properties:
                # Override the keepalive interval with the server's value.
                self.sock.set_keepalive_interval(packet.properties["ServerKeepAlive"])
