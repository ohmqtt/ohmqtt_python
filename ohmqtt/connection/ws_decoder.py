from dataclasses import dataclass, field
import socket
import ssl
from typing import NamedTuple, Final

from .decoder import ClosedSocketError, WantReadError
from .wslib import OpCode, WebsocketError
from ..logger import get_logger
from ..serialization import MAX_VARINT

logger: Final = get_logger("connection.ws_decoder")


class LengthDecodeResult(NamedTuple):
    """Result of decoding frame length, in part or whole.

    This state can be used to resume decoding if the socket doesn't have enough data."""
    value: int
    remaining: int


InitLengthDecodeState: Final = LengthDecodeResult(0, 9)
CONTROL_OPCODES: Final = frozenset((OpCode.CLOSE, OpCode.PING, OpCode.PONG))
MAX_PAYLOAD_LENGTH: Final = 1 + 4 + MAX_VARINT  # The largest possible MQTT packet.


@dataclass(slots=True)
class WebsocketDecoder:
    """Incremental decoder for Websocket frames coming in from a socket."""
    opcode: OpCode | None = field(default=None, init=False)  #: The opcode of the frame.
    length: LengthDecodeResult = field(default=InitLengthDecodeState, init=False)  #: Variable integer decoding state of the payload length.
    data: bytearray = field(init=False, default_factory=bytearray)  #: The payload of the frame.
    pending: bytearray = field(init=False, default_factory=bytearray)  #: Data read from the socket ahead of the decoder.

    def reset(self) -> None:
        """Reset the decoder state."""
        self._reset_frame()
        self.pending.clear()

    def _reset_frame(self) -> None:
        self.opcode = None
        self.length = InitLengthDecodeState
        self.data.clear()

    def feed(self, data: bytes | bytearray) -> None:
        """Queue data already read from the socket, to be decoded before reading the socket again."""
        self.pending.extend(data)

    def _recv(self, sock: socket.socket | ssl.SSLSocket, bufsize: int) -> bytes:
        if self.pending:
            data = bytes(self.pending[:bufsize])
            del self.pending[:bufsize]
            return data
        return sock.recv(bufsize)

    def _recv_one_byte(self, sock: socket.socket | ssl.SSLSocket) -> int:
        """Receive one byte from the socket.

        :raises WantReadError: The socket is not ready for reading.
        :raises ClosedSocketError: The socket is closed."""
        try:
            data = self._recv(sock, 1)
        except (BlockingIOError, ssl.SSLWantReadError, ssl.SSLWantWriteError):
            raise WantReadError("Socket not ready for reading")
        if not data:
            raise ClosedSocketError("Socket closed")
        return data[0]

    def _extract_opcode(self, sock: socket.socket | ssl.SSLSocket) -> None:
        """Extract the opcode from a WebSocket frame, if needed.

        :raises WantReadError: The socket is not ready for reading.
        :raises WebsocketError: The frame is invalid."""
        if self.opcode is not None:
            return
        byte = self._recv_one_byte(sock)
        if byte & 0x80 == 0:
            raise WebsocketError("Fragmented WebSocket frames are not supported")
        if byte & 0x70 != 0:
            raise WebsocketError("Reserved WebSocket bits are set")
        opcode_value = byte & 0x0F
        try:
            self.opcode = OpCode(opcode_value)
        except ValueError as exc:
            raise WebsocketError("Invalid WebSocket opcode") from exc

    def _extract_length(self, sock: socket.socket | ssl.SSLSocket) -> None:
        """Incrementally decode frame length integer from a socket, if needed.

        :raises WantReadError: The socket is not ready for reading.
        :raises WebsocketError: The frame is invalid."""
        result = self.length.value
        remaining = self.length.remaining
        if remaining == 0:
            return

        try:
            if remaining > 8:
                # First byte of length.
                byte = self._recv_one_byte(sock)
                if byte & 0x80:
                    raise WebsocketError("Masked frames from server are not allowed")
                if byte <= 125:
                    result = byte
                    remaining = 0
                elif self.opcode in CONTROL_OPCODES:
                    raise WebsocketError("WebSocket control frame payload is longer than 125 bytes")
                elif byte == 126:
                    remaining = 2
                else:
                    remaining = 8

            while remaining > 0:
                byte = self._recv_one_byte(sock)
                result = (result << 8) + byte
                remaining -= 1
            if result > MAX_PAYLOAD_LENGTH:
                raise WebsocketError("WebSocket frame payload is longer than the largest MQTT packet")

            # We have the complete length.
            self.length = LengthDecodeResult(result, 0)
        except WantReadError:
            # Not done yet, the socket is neither closed nor ready for reading.
            # Save the partial state and re-raise.
            self.length = LengthDecodeResult(result, remaining)
            raise

    def _extract_data(self, sock: socket.socket | ssl.SSLSocket) -> None:
        """Extract all data after the packet length from the socket, if needed.

        :raises ClosedSocketError: The socket is closed."""
        assert self.length.remaining == 0, "Can't extract data without complete length"
        while len(self.data) < self.length.value:
            data = self._recv(sock, self.length.value - len(self.data))
            if not data:
                raise ClosedSocketError("Socket closed")
            self.data.extend(data)

    def decode(self, sock: socket.socket | ssl.SSLSocket) -> tuple[OpCode, bytes] | None:
        """Decode a single frame straight from the socket.

        :return: None if the socket doesn't have enough data for us to decode a packet.
        :raises ClosedSocketError: The socket is closed.
        :raises WebsocketError: A protocol error was detected."""
        try:
            self._extract_opcode(sock)
            self._extract_length(sock)
            self._extract_data(sock)
        except (BlockingIOError, ssl.SSLWantReadError, ssl.SSLWantWriteError, WantReadError):
            # If the socket is open but doesn't have enough data for us, we need to wait for more.
            return None
        except OSError as exc:
            raise ClosedSocketError("Socket closed") from exc

        assert self.opcode is not None
        opcode = self.opcode
        data = bytes(self.data)
        self._reset_frame()
        return opcode, data
