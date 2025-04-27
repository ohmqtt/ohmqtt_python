from __future__ import annotations

from dataclasses import dataclass, field
import select
import socket
import ssl
import threading
import time
from typing import Callable, cast, Final
from types import TracebackType

from .decoder import IncrementalDecoder
from .error import MQTTError
from .logger import get_logger
from .mqtt_spec import MQTTPacketType, MQTTReasonCode
from .packet import (
    MQTTPacket,
    MQTTConnAckPacket,
    MQTTDisconnectPacket,
    PING,
    PONG,
)

logger: Final = get_logger("connection")

ConnectionCloseCallback = Callable[[], None]
ConnectionOpenCallback = Callable[[], None]
ConnectionReadCallback = Callable[[MQTTPacket], None]


STATE_SHUTDOWN: Final = 0
STATE_CLOSING: Final = 1
STATE_CLOSED: Final = 2
STATE_CONNECT: Final = 3


_InitAddress: Final[tuple[str, int]] = ("", -1)


class ConnectionCloseCondition(Exception):
    """Any exception which should cause the socket to close."""
    pass


def _get_socket() -> socket.socket:
    """Helper function to get a socket.

    This mostly helps us mock sockets in tests."""
    return socket.socket(socket.AF_INET, socket.SOCK_STREAM)


@dataclass(slots=True, match_args=True, frozen=True)
class ConnectionConnectParams:
    host: str
    port: int
    reconnect_delay: float = 0.0
    keepalive_interval: int = 0
    tcp_nodelay: bool = True
    use_tls: bool = False
    tls_context: ssl.SSLContext = field(default_factory=ssl.create_default_context)
    tls_hostname: str = ""


class Connection(threading.Thread):
    """Non-blocking socket wrapper with TLS and application keepalive support."""
    __slots__ = (
        "_address",
        "_close_callback",
        "_open_callback",
        "_read_callback",
        "_tls_context",
        "_tls_hostname",
        "_use_tls",
        "_write_buffer",
        "_write_buffer_lock",
        "_interrupt_r",
        "_interrupt_w",
        "_keepalive_interval",
        "_last_send",
        "_last_recv",
        "_pong_deadline",
        "_in_read",
        "_tcp_nodelay",
        "_reconnect_delay",
        "_state",
        "_decoder",
    )

    def __init__(
        self,
        close_callback: ConnectionCloseCallback,
        open_callback: ConnectionOpenCallback,
        read_callback: ConnectionReadCallback,
    ) -> None:
        super().__init__(daemon=True)
        self._address = _InitAddress
        self._close_callback = close_callback
        self._open_callback = open_callback
        self._read_callback = read_callback
        self._use_tls = False
        self._tls_context = ssl.create_default_context()
        self._tls_hostname = ""
        self._tcp_nodelay = True
        self._reconnect_delay = 1.0
        self._keepalive_interval = 0

        self._state = STATE_CLOSED
        self._write_buffer = bytearray()
        self._write_buffer_lock = threading.Lock()
        self._interrupt_r, self._interrupt_w = socket.socketpair()
        self._last_send = 0.0
        self._last_recv = 0.0
        self._pong_deadline = 0.0
        self._in_read = False
        self._connect_cond = threading.Condition()
        self._decoder = IncrementalDecoder()

    def __enter__(self) -> Connection:
        self.start()
        return self

    def __exit__(self, exc_type: type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        self.shutdown()

    def _goto_state(self, state: int) -> None:
        """Set the state of the connection.

        This will wake up any sleeping thread states."""
        with self._connect_cond:
            self._state = state
            self._connect_cond.notify_all()
            self._interrupt()

    def connect(self, params: ConnectionConnectParams) -> None:
        """Connect to the broker.

        This method is non-blocking and will return immediately. The connection will be established in the background."""
        self._keepalive_interval = params.keepalive_interval
        self._reconnect_delay = params.reconnect_delay
        self._tcp_nodelay = params.tcp_nodelay
        self._use_tls = params.use_tls
        self._tls_hostname = params.tls_hostname
        self._tls_context = params.tls_context
        self._address = params.host, params.port
        self._goto_state(STATE_CONNECT)

    def disconnect(self) -> None:
        """Close the connection.

        This method does not guarantee pending reads or writes will be completed."""
        self._address = _InitAddress
        self._goto_state(STATE_CLOSING)

    def wait_for_disconnect(self, timeout: float | None = None) -> None:
        """Wait for the connection to be closed.

        This method will block until the connection is closed or the timeout is reached."""
        with self._connect_cond:
            self._connect_cond.wait_for(lambda: self._state <= STATE_CLOSED, timeout=timeout)
            if self._state > STATE_CLOSED:
                raise TimeoutError("Socket did not close in time")

    def shutdown(self) -> None:
        """Shutdown the connection, finalizing the instance.

        This method does not guarantee pending reads or writes will be completed."""
        self._goto_state(STATE_SHUTDOWN)

    def send(self, data: bytes) -> None:
        """Write data to the broker."""
        if self._state < STATE_CONNECT:
            return
        with self._write_buffer_lock:
            do_interrupt = not self._in_read and not self._write_buffer
            self._write_buffer.extend(data)
        if do_interrupt:
            self._interrupt()

    def _try_to_send_disconnect(self, sock: socket.socket | ssl.SSLSocket, reason_code: int) -> None:
        """Try to send a DISCONNECT packet to the broker and close the socket.

        If the socket is not ready to write, we will not wait for it."""
        disconnect_packet = MQTTDisconnectPacket(reason_code=reason_code)
        try:
            sock.send(disconnect_packet.encode())
            logger.debug(f"---> {str(disconnect_packet)}")
        except (BlockingIOError, ssl.SSLWantWriteError, OSError):
            pass

    def _interrupt(self) -> None:
        """Interrupt select to wake up the thread."""
        self._interrupt_w.send(b"\x00")

    def _handshake_loop(self, sock: socket.socket) -> ssl.SSLSocket:
        """Run the TLS handshake in a loop until it is complete."""
        assert self._tls_context is not None
        sock = self._tls_context.wrap_socket(sock, server_hostname=self._tls_hostname, do_handshake_on_connect=False)
        while self._state == STATE_CONNECT:
            try:
                sock.do_handshake()
                return sock
            except ssl.SSLWantReadError:
                select.select([sock, self._interrupt_r], [], [])
            except ssl.SSLWantWriteError:
                select.select([self._interrupt_r], [sock], [])
        raise ConnectionCloseCondition("Handshake loop did not complete")

    def _try_write(self, sock: socket.socket | ssl.SSLSocket) -> None:
        """Try to flush the write buffer to the socket."""
        try:
            with self._write_buffer_lock:
                sent = sock.send(self._write_buffer)
                self._last_send = time.monotonic()
                del self._write_buffer[:sent]
        except ssl.SSLWantWriteError:
            pass

    def _try_read(self, sock: socket.socket | ssl.SSLSocket) -> None:
        """Try to read data from the socket."""
        self._in_read = True
        try:
            self._read_packet(sock)
            self._last_recv = time.monotonic()
        except ssl.SSLWantReadError:
            pass
        finally:
            self._in_read = False

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
            self._pong_deadline = 0.0
            logger.debug("<--- PONG")
        elif packet.packet_type == MQTTPacketType["PINGREQ"]:
            sock.send(PONG)
            logger.debug("<--- PING PONG --->")
        else:
            # All non-ping packets are passed to the read callback.
            self._read_callback(packet)
        # Handle connection-level server parameters here.
        if packet.packet_type == MQTTPacketType["CONNACK"]:
            packet = cast(MQTTConnAckPacket, packet)
            if "ServerKeepAlive" in packet.properties:
                # Override the keepalive interval with the server's value.
                keepalive_interval = packet.properties["ServerKeepAlive"]
                self._keepalive_interval = keepalive_interval
                self._pong_deadline = 0.0  # Cancel any pending ping expectations.
                logger.debug(f"Keepalive interval set by server to {keepalive_interval} seconds")

    def _get_next_timeout(self) -> float | None:
        """Get the next timeout for the connection.

        This is used to implement the keepalive interval."""
        if self._keepalive_interval > 0:
            now = time.monotonic()
            send_timeout = self._last_send + self._keepalive_interval
            recv_timeout = self._last_recv + self._keepalive_interval * 2
            pong_timeout = self._pong_deadline if self._pong_deadline > 0.0 else recv_timeout + 1.0
            next_timeout = min(send_timeout, recv_timeout, pong_timeout) - now
            return max(0.001, next_timeout)
        return None

    def _check_keepalive(self) -> None:
        """Check if the keepalive interval has been reached."""
        now = time.monotonic()
        if now - self._last_send > self._keepalive_interval and self._pong_deadline == 0.0:
            self.send(PING)
            self._pong_deadline = time.monotonic() + self._keepalive_interval
            logger.debug("---> PING")
        elif now - self._last_recv > self._keepalive_interval * 2:
            raise ConnectionCloseCondition("No data received in time")
        elif self._pong_deadline > 0.0 and now > self._pong_deadline:
            raise ConnectionCloseCondition("No pong received in time")

    def _check_connect(self) -> bool:
        """Check if we should connect to the server.

        Raises ConnectionCloseCondition if the thread should be shutdown instead."""
        if self._state == STATE_SHUTDOWN:
            raise ConnectionCloseCondition("Shutting down")
        return bool(self._state == STATE_CONNECT and self._address != _InitAddress)

    def run(self) -> None:
        while self._state > STATE_SHUTDOWN:
            was_open = False
            sent_disconnect = False
            sock: socket.socket | ssl.SSLSocket = _get_socket()
            try:
                with self._connect_cond:
                    self._connect_cond.wait_for(self._check_connect)
                    address = self._address

                if self._tcp_nodelay:
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.connect(address)
                sock.setblocking(False)

                if self._use_tls:
                    sock = self._handshake_loop(sock)
                if self._state == STATE_CONNECT:
                    self._open_callback()
                    was_open = True

                self._last_send = time.monotonic()
                self._last_recv = self._last_send
                self._pong_deadline = 0.0

                while self._state == STATE_CONNECT:
                    next_timeout = self._get_next_timeout()
                    write_check = (sock,) if self._write_buffer else tuple()
                    readable, writable, _ = select.select([sock, self._interrupt_r], write_check, [], next_timeout)

                    if sock in writable:
                        self._try_write(sock)

                    if sock in readable:
                        self._try_read(sock)

                    if self._keepalive_interval > 0 and self._state == STATE_CONNECT:
                        self._check_keepalive()

                    if self._interrupt_r in readable:
                        self._interrupt_r.recv(1024)

            except MQTTError as exc:
                logger.error(f"There was a problem with data from broker, closing connection: {exc}")
                self._try_to_send_disconnect(sock, exc.reason_code)
                sent_disconnect = True
            except ConnectionCloseCondition as exc:
                logger.debug(f"Closing socket: {exc}")
            except Exception:
                logger.exception("Unhandled error in socket read thread, shutting down")
                self._state = STATE_SHUTDOWN
            finally:
                self._state = min(self._state, STATE_CLOSED)
                with self._connect_cond:
                    # Wake up wait_for_disconnect.
                    self._connect_cond.notify_all()
                if was_open:
                    try:
                        self._close_callback()
                    except Exception:
                        logger.exception("Error while calling close callback")
                    if not sent_disconnect:
                        self._try_to_send_disconnect(sock, MQTTReasonCode["NormalDisconnection"])
                sock.close()
                self._decoder.reset()
                self._write_buffer.clear()
                if self._state > STATE_SHUTDOWN and self._reconnect_delay > 0:
                    with self._connect_cond:
                        self._connect_cond.wait(timeout=self._reconnect_delay)
                        if self._address == _InitAddress:
                            logger.debug("Reconnect cancelled")
                            break
        logger.debug("Shutdown complete")
