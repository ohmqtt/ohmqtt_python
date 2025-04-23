import logging
import ssl
from typing import Callable, cast, Final

from .error import MQTTError
from .mqtt_spec import MQTTPacketType
from .packet import decode_packet, MQTTPacket, MQTTConnAckPacket
from .serialization import decode_varint
from .socket_wrapper import SocketWrapper

logger: Final = logging.getLogger(__name__)

ConnectionCloseCallback = Callable[[], None]
ConnectionOpenCallback = Callable[[], None]
ConnectionReadCallback = Callable[[MQTTPacket], None]


class Connection:
    """Manage the lifecycle of a connection to the MQTT broker."""
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
            open_callback=self._open_callback,
            read_callback=self._read_packet,
            keepalive_interval=keepalive_interval,
            use_tls=use_tls,
            tls_context=tls_context,
            tls_hostname=tls_hostname,
        )
        self.sock.start()

        # Buffer for partial packets on the receiving end.
        self._partial = b""

    def close(self) -> None:
        """Signal the connection to close."""
        self.sock.close()

    def send(self, data: bytes) -> None:
        """Send data to the broker."""
        self.sock.send(data)

    def _read_packet(self, data: bytes) -> None:
        """Called by the client when a new packet is received.
        
        Here we deal with picking out MQTT packets from the stream."""
        if self._partial:
            data = self._partial + data
            self._partial = b""
        while data:
            if len(data) < 2:
                # Not enough data to parse the fixed header.
                # Save the partial data and return.
                self._partial = data
                return
            # Check the length of the packet against the buffer size.
            # We assume that the packet is well-formed and the length is correct.
            # If it is not, we will catch it later in decode_packet.
            offset = 1  # Skip the first byte of the fixed header.
            try:
                remaining_len, consumed = decode_varint(data[offset:])
            except MQTTError:
                # In this case, the partial data boundary may be in the middle of the varint.
                # If so, save the partial data and return.
                if len(data) < 5:
                    self._partial = data
                    return
                raise
            offset += consumed
            if len(data[offset:]) < remaining_len:
                # If the packet is incomplete, save the partial data and return.
                self._partial = data
                return
            packet = decode_packet(data[:offset + remaining_len])
            if packet.packet_type == MQTTPacketType.PINGRESP:
                self.sock.pong_received()
            elif packet.packet_type == MQTTPacketType.PINGREQ:
                self.sock.send(b"\xd0\x00")
            else:
                # All non-ping packets are passed to the read callback.
                self._read_callback(packet)
            if packet.packet_type == MQTTPacketType.CONNACK:
                packet = cast(MQTTConnAckPacket, packet)
                if "ServerKeepAlive" in packet.properties:
                    # Override the keepalive interval with the server's value.
                    self.sock.set_keepalive_interval(packet.properties["ServerKeepAlive"])
            data = data[offset + remaining_len:]
