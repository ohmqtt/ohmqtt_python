from dataclasses import dataclass, field
import socket
import ssl
from typing import NamedTuple, Final

from ..error import MQTTError
from ..logger import get_logger
from ..mqtt_spec import MQTTReasonCode
from ..packet import decode_packet_from_parts, MQTTPacket

logger: Final = get_logger("connection.decoder")


class VarintDecodeResult(NamedTuple):
    """Result of decoding a variable length integer, in part or whole.

    This state can be used to resume decoding if the socket doesn't have enough data."""
    value: int
    multiplier: int
    complete: bool


InitVarintDecodeState: Final = VarintDecodeResult(0, 1, False)


class ClosedSocketError(Exception):
    """Exception raised when the socket is closed."""


class WantReadError(Exception):
    """Indicates that the socket is not ready for reading."""


@dataclass(slots=True)
class IncrementalDecoder:
    """Incremental decoder for MQTT messages coming in from a socket."""
    head: int = field(default=-1, init=False)  #: The first byte of the packet.
    length: VarintDecodeResult = field(default=InitVarintDecodeState, init=False)  #: Variable integer decoding state of the packet length.
    data: bytearray = field(init=False, default_factory=bytearray)  #: The remaining data of the packet.

    def reset(self) -> None:
        """Reset the decoder state."""
        self.head = -1
        self.length = InitVarintDecodeState
        self.data.clear()

    def _recv_one_byte(self, sock: socket.socket | ssl.SSLSocket) -> int:
        """Receive one byte from the socket.

        :raises WantReadError: The socket is not ready for reading.
        :raises ClosedSocketError: The socket is closed."""
        try:
            data = sock.recv(1)
        except (BlockingIOError, ssl.SSLWantReadError, ssl.SSLWantWriteError):
            raise WantReadError("Socket not ready for reading")
        if not data:
            raise ClosedSocketError("Socket closed")
        return data[0]

    def _extract_head(self, sock: socket.socket | ssl.SSLSocket) -> None:
        """Extract the head byte of the next packet from the socket, if needed."""
        if self.head != -1:
            return
        self.head = self._recv_one_byte(sock)

    def _extract_length(self, sock: socket.socket | ssl.SSLSocket) -> None:
        """Incrementally decode a variable length integer from a socket, if needed.

        :raises WantReadError: The socket is not ready for reading."""
        # See ohmqtt.serialization.decode_varint for a cleaner implementation.
        assert self.head != -1  # We shouldn't be here unless we have a head byte.
        if self.length.complete:
            return
        result = self.length.value
        mult = self.length.multiplier
        try:
            while mult < 0x200000:  # This magic is the mult value after pulling 4 bytes.
                byte = self._recv_one_byte(sock)
                result += byte % 0x80 * mult
                if byte < 0x80:
                    # We have the complete varint.
                    self.length = VarintDecodeResult(result, mult, True)
                    return
                mult *= 0x80
            raise MQTTError("Varint overflow", MQTTReasonCode.MalformedPacket)
        except WantReadError:
            # Not done yet, the socket is neither closed nor ready for reading.
            # Save the partial state and re-raise.
            self.length = VarintDecodeResult(result, mult, False)
            raise

    def _extract_data(self, sock: socket.socket | ssl.SSLSocket) -> None:
        """Extract all data after the packet length from the socket, if needed.

        :raises ClosedSocketError: The socket is closed."""
        assert self.length.complete  # We shouldn't be here unless we have a complete length.
        while len(self.data) < self.length.value:
            data = sock.recv(self.length.value - len(self.data))
            if not data:
                raise ClosedSocketError("Socket closed")
            self.data.extend(data)

    def decode(self, sock: socket.socket | ssl.SSLSocket) -> MQTTPacket | None:
        """Decode a single packet straight from the socket.

        :return: None if the socket doesn't have enough data for us to decode a packet.
        :raises ClosedSocketError: The socket is closed."""
        try:
            self._extract_head(sock)
            self._extract_length(sock)
            self._extract_data(sock)
        except (BlockingIOError, ssl.SSLWantReadError, ssl.SSLWantWriteError, WantReadError):
            # If the socket is open but doesn't have enough data for us, we need to wait for more.
            return None
        except OSError as exc:
            raise ClosedSocketError("Socket closed") from exc

        # We have a complete packet, decode it and clear the read buffer.
        packet_head = self.head
        packet_data = memoryview(self.data)
        packet_data.toreadonly()
        try:
            return decode_packet_from_parts(packet_head, packet_data)
        finally:
            packet_data.release()
            self.reset()
