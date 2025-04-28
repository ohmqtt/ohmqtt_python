from dataclasses import dataclass, field
import socket
import ssl
from typing import NamedTuple, Final

from .error import MQTTError
from .mqtt_spec import MQTTReasonCode
from .packet import decode_packet_from_parts, MQTTPacket
from .serialization import MAX_VARINT


class VarintDecodeResult(NamedTuple):
    """Result of decoding a variable length integer, in part or whole.

    This state can be used to resume decoding if the socket doesn't have enough data."""
    value: int
    multiplier: int
    complete: bool


InitVarintDecodeState: Final = VarintDecodeResult(0, 1, False)


class ClosedSocketError(Exception):
    """Exception raised when the socket is closed."""
    pass


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
            return VarintDecodeResult(result, mult, False)
        byte = data[0]
        sz += 1
        result += byte % 0x80 * mult
        if result > MAX_VARINT:
            raise MQTTError("Varint overflow", MQTTReasonCode["MalformedPacket"])
        if byte < 0x80:
            return VarintDecodeResult(result, mult, True)
        mult *= 0x80


@dataclass(slots=True)
class IncrementalDecoder:
    """Incremental decoder for MQTT messages coming in from a socket."""
    _partial_head: int = field(default=-1, init=False)
    _partial_length: VarintDecodeResult = field(default=InitVarintDecodeState, init=False)
    _partial_data: bytearray = field(init=False, default_factory=bytearray)

    def reset(self) -> None:
        """Reset the decoder state."""
        self._partial_head = -1
        self._partial_length = InitVarintDecodeState
        self._partial_data.clear()

    def decode(self, sock: socket.socket | ssl.SSLSocket) -> MQTTPacket | None:
        """Decode a packet from the socket."""
        try:
            if self._partial_head == -1:
                partial_head = sock.recv(1)
                if not partial_head:
                    return None
                self._partial_head = partial_head[0]
            if not self._partial_length.complete:
                self._partial_length = decode_varint_from_socket(sock, self._partial_length.value, self._partial_length.multiplier)
                if not self._partial_length.complete:
                    # If the socket didn't have enough data for us, we need to wait for more.
                    return None

            while len(self._partial_data) < self._partial_length.value:
                # Read the rest of the packet.
                data = sock.recv(self._partial_length.value - len(self._partial_data))
                if not data:
                    return None
                self._partial_data.extend(data)
        except (BlockingIOError, ssl.SSLWantReadError, OSError):
            # If the socket doesn't have enough data for us, we need to wait for more.
            return None

        # We have a complete packet, decode it and clear the read buffer.
        packet_data = memoryview(self._partial_data)
        packet_data.toreadonly()
        packet_head = self._partial_head
        try:
            return decode_packet_from_parts(packet_head, packet_data)
        finally:
            packet_data.release()
            self.reset()
