import logging
import selectors
import socket
import ssl
import threading
import time
from typing import Callable, cast, Final

from .error import MQTTError
from .mqtt_spec import MQTTPacketType, MQTTReasonCode
from .packet import decode_packet, MQTTPacket, MQTTConnAckPacket, MQTTDisconnectPacket
from .serialization import decode_varint
from .socket_writer import SocketWriter
from .tls_socket import TLSSocket

logger: Final = logging.getLogger(__name__)

CloseCallback = Callable[["Connection", Exception | None], None]
ConnectCallback = Callable[["Connection"], None]
ReadCallback = Callable[["Connection", MQTTPacket], None]


class Connection(threading.Thread):
    """A thread managing a socket connection to an MQTT broker.
    
    This thread will read from the socket, decode buffers from the stream,
    send and monitor keepalive pings, and call the appropriate callbacks."""
    sock: socket.socket | TLSSocket | None = None
    _writer: SocketWriter | None = None
    _close_exc: Exception | None = None

    def __init__(self,
        host: str,
        port: int,
        keepalive_interval: int,
        close_cb: CloseCallback,
        connect_cb: ConnectCallback,
        read_cb: ReadCallback,
        *,
        recv_buffer_sz: int = 65535,
        tls: bool = False,
        tls_context: ssl.SSLContext | None = None,
        tls_hostname: str | None = None,
    ):
        super().__init__(daemon=True)

        # The keepalive interval is set by the client, but can be overridden by the server in CONNACK.
        self.keepalive_interval = keepalive_interval
        self.host = host
        self.port = port
        self.close_cb = close_cb
        self.connect_cb = connect_cb
        self.read_cb = read_cb
        self.tls = tls
        self.tls_context = tls_context
        self.tls_hostname = tls_hostname

        self._recv_buffer = bytearray(recv_buffer_sz)
        # Use a pipe to signal the thread to close.
        self._close_pipe_r, self._close_pipe_w = socket.socketpair()
        # Buffer for partial packets on the receiving end.
        self._partial = b""
        # The monotonic time of the last packet sent.
        self._last_send = 0.0
        # The monotonic time of the last packet received.
        self._last_recv = 0.0
        # The time by which we must have received a PINGRESP packet.
        self._pong_deadline = 0.0
        # If using TLS, keep trying to handshake until it is done.
        self._handshake_done = False
        # If closed externally with an exception (from SocketWriter), retain it.
        self._close_exc = None

    def __enter__(self) -> "Connection":
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        # Block until the thread is closed, to ensure that the thread shuts down cleanly.
        # Otherwise we may race dereferencing the callbacks.
        self.close()

    def _do_close(self, exc: Exception | None = None) -> None:
        """Close the socket and run the close callback."""
        # This method must only be called from the thread itself.
        try:
            if self.sock is not None:
                reason_code = MQTTReasonCode.NormalDisconnection
                if isinstance(exc, MQTTError):
                    reason_code = exc.reason_code
                disconnect_packet = MQTTDisconnectPacket(reason_code=reason_code)
                self._send(disconnect_packet.encode())
                logger.debug(f"---> {disconnect_packet}")
                # TODO: Wait for writer to finish sending.
        except Exception:
            logger.exception("Failed to send DISCONNECT packet")
        finally:
            if self.sock is not None:
                self.sock.close()
            if self._close_exc is not None:
                exc = self._close_exc
            self.close_cb(self, exc)

    def close(self, *, block: bool = True, exc: Exception | None = None) -> None:
        """Signal the thread to shutdown."""
        # This is the only way to close the thread from the public interface.
        # It must never be called from the thread itself.
        if exc is not None:
            self._close_exc = exc
        self._close_pipe_w.send(b"\0")
        if block and threading.current_thread() is not self:
            self.join()

    def send(self, data: bytes) -> None:
        """Send data to the broker.
        
        Any exceptions will close the socket and raise.
        The close callback will be called, but without the exception."""
        try:
            self._send(data)
        except:
            self.close(block=False)
            raise

    def _send(self, data: bytes) -> None:
        """Send data to the broker.
        
        This method may be called from any thread."""
        if self._writer is None:
            raise RuntimeError("Socket is not connected yet")
        self._writer.send(data)
        self._last_send = time.monotonic()

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
                logger.debug("<--- pong")
                self._pong_deadline = 0.0
            elif packet.packet_type == MQTTPacketType.PINGREQ:
                logger.debug("<--- ping")
                logger.debug("---> pong")
                self._send(b"\xd0\x00")
                self._last_send = time.monotonic()
            else:
                # All non-ping packets are passed to the read callback.
                self.read_cb(self, packet)
            if packet.packet_type == MQTTPacketType.CONNACK:
                packet = cast(MQTTConnAckPacket, packet)
                if "ServerKeepAlive" in packet.properties:
                    # Override the keepalive interval with the server's value.
                    self.keepalive_interval = packet.properties["ServerKeepAlive"]
            self._last_recv = time.monotonic()
            data = data[offset + remaining_len:]

    def handle_write_error(self, exc: Exception) -> None:
        """Handle a write error.
        
        This method is called from the SocketWriter thread when an error occurs."""
        self.close(block=False, exc=exc)

    def run(self):
        try:
            self.sock = socket.create_connection((self.host, self.port), all_errors=True)
            if self.tls:
                self.sock = TLSSocket(self.sock, self.tls_hostname, self.tls_context)
                self.sock.setblocking(False)
            else:
                self.sock.setblocking(False)
                self._writer = SocketWriter(self.sock, self.handle_write_error)
                self.connect_cb(self)
            t0 = time.monotonic()
            self._last_send = t0
            self._last_recv = t0

            sel = selectors.DefaultSelector()
            sel.register(self._close_pipe_r, selectors.EVENT_READ)
            sel.register(self.sock, selectors.EVENT_READ)
            if self.tls:
                sel.modify(self.sock, selectors.EVENT_READ | selectors.EVENT_WRITE)
                while not self._handshake_done:
                    events = sel.select()
                    for key, _ in events:
                        if key.fileobj == self._close_pipe_r:
                            # The close pipe has been written to.
                            self._do_close()
                            return
                        else:
                            try:
                                self.sock.do_handshake()
                                self._handshake_done = True
                                self.connect_cb(self)
                            except (ssl.SSLWantReadError, ssl.SSLWantWriteError):
                                continue
                self._writer = SocketWriter(self.sock, self.handle_write_error)
                sel.modify(self.sock, selectors.EVENT_READ)
            while True:
                do_keepalive = self.keepalive_interval > 0
                if do_keepalive:
                    t1 = time.monotonic()
                    send_timeout = self._last_send + self.keepalive_interval
                    recv_timeout = self._last_recv + self.keepalive_interval * 1.5
                    pong_timeout = self._pong_deadline if self._pong_deadline > 0.0 else recv_timeout + 1.0
                    next_timeout = min(
                        send_timeout - t1,
                        recv_timeout - t1,
                        pong_timeout - t1,
                    )
                else:
                    # No keepalive, wait indefinitely.
                    next_timeout = None

                events = sel.select(timeout=next_timeout)
                for key, _ in events:
                    if key.fileobj == self._close_pipe_r:
                        # The close pipe has been written to.
                        self._do_close()
                        return
                    else:
                        try:
                            num_read = self.sock.recv_into(self._recv_buffer)
                        except (BlockingIOError, ssl.SSLWantReadError):
                            logging.exception("Failed to read from socket")
                            continue
                        if num_read == 0:
                            self._do_close()
                            return
                        try:
                            self._read_packet(self._recv_buffer[:num_read])
                        except:
                            logging.error(self._recv_buffer[:num_read].hex())
                            raise

                if do_keepalive:
                    t2 = time.monotonic()
                    if t2 > recv_timeout:
                        raise MQTTError("Keepalive timeout", MQTTReasonCode.KeepAliveTimeout)
                    if t2 > pong_timeout:
                        raise MQTTError("Did not receive a PINGRESP in time", MQTTReasonCode.KeepAliveTimeout)
                    if t2 > send_timeout:
                        # Send a PINGREQ packet.
                        logger.debug("---> ping")
                        self._send(b"\xc0\x00")
                        self._pong_deadline = t2 + self.keepalive_interval
        except MQTTError as exc:
            logger.exception("MQTTError in Connection thread")
            self._do_close(exc)
        except Exception as exc:
            logger.exception("Unhandled exception in Connection thread")
            self._do_close(exc)
