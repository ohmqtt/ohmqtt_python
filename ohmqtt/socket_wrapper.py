import logging
import select
import socket
import ssl
import threading
import time
from typing import Callable

logger = logging.getLogger(__name__)

SocketCloseCallback = Callable[[], None]
SocketOpenCallback = Callable[[], None]
SocketReadCallback = Callable[[socket.socket | ssl.SSLSocket], None]
SocketKeepaliveCallback = Callable[["SocketWrapper"], None]


class SocketWrapperCloseCondition(Exception):
    """Any exception which should cause the socket to close."""
    pass


class SocketWrapper(threading.Thread):
    """Non-blocking socket wrapper with TLS and application keepalive support."""
    __slots__ = (
        "_close_callback",
        "_keepalive_callback",
        "_open_callback",
        "_read_callback",
        "_tls_context",
        "_tls_hostname",
        "_use_tls",
        "_write_buffer",
        "_write_buffer_lock",
        "_interrupt_r",
        "_interrupt_w",
        "_closing",
        "_keepalive_interval",
        "_last_send",
        "_last_recv",
        "_pong_deadline",
        "_in_read",
    )
    host: str
    port: int
    sock: socket.socket | ssl.SSLSocket

    def __init__(
        self,
        host: str,
        port: int,
        close_callback: SocketCloseCallback,
        keepalive_callback: SocketKeepaliveCallback,
        open_callback: SocketOpenCallback,
        read_callback: SocketReadCallback,
        *,
        keepalive_interval: int = 0,
        tcp_nodelay: bool = True,
        use_tls: bool = False,
        tls_context: ssl.SSLContext | None = None,
        tls_hostname: str = "",
    ) -> None:
        super().__init__(daemon=True)
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if tcp_nodelay:
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._close_callback = close_callback
        self._keepalive_callback = keepalive_callback
        self._open_callback = open_callback
        self._read_callback = read_callback
        self._use_tls = use_tls
        self._tls_context = ssl.create_default_context() if use_tls and tls_context is None else tls_context
        self._tls_hostname = tls_hostname

        self._write_buffer = bytearray()
        self._write_buffer_lock = threading.Lock()
        self._interrupt_r, self._interrupt_w = socket.socketpair()
        self._closing = False
        self._keepalive_interval = keepalive_interval
        self._last_send = 0.0
        self._last_recv = 0.0
        self._pong_deadline = 0.0
        self._in_read = False

    def close(self) -> None:
        """Close the socket.

        This method does not guarantee pending reads or writes will be completed."""
        self._closing = True
        self._interrupt()

    def send(self, data: bytes) -> None:
        """Write data to the socket."""
        if self._closing:
            return
        with self._write_buffer_lock:
            do_interrupt = not self._in_read and not self._write_buffer
            self._write_buffer.extend(data)
        if do_interrupt:
            self._interrupt()

    def ping_sent(self) -> None:
        """Indicate that a ping was sent to the server."""
        self._pong_deadline = time.monotonic() + self._keepalive_interval * 1.5

    def pong_received(self) -> None:
        """Indicate that a pong was received from the server."""
        self._pong_deadline = 0.0

    def set_keepalive_interval(self, interval: int) -> None:
        """Set the keepalive interval for the socket.

        This is the interval in seconds between pings to the server. A value of 0 disables keepalive."""
        self._keepalive_interval = interval
        if interval > 0:
            self._pong_deadline = 0.0
            self._interrupt()

    def _interrupt(self) -> None:
        """Interrupt select to wake up the thread."""
        self._interrupt_w.send(b"\x00")

    def _handshake_loop(self) -> None:
        """Run the TLS handshake in a loop until it is complete."""
        assert self._tls_context is not None
        self.sock = self._tls_context.wrap_socket(self.sock, server_hostname=self._tls_hostname, do_handshake_on_connect=False)
        self.sock.setblocking(False)
        while not self._closing:
            try:
                self.sock.do_handshake()
                break
            except ssl.SSLWantReadError:
                select.select([self.sock, self._interrupt_r], [], [])
            except ssl.SSLWantWriteError:
                select.select([self._interrupt_r], [self.sock], [])

    def _try_write(self) -> None:
        """Try to flush the write buffer to the socket."""
        try:
            with self._write_buffer_lock:
                sent = self.sock.send(self._write_buffer)
                self._last_send = time.monotonic()
                del self._write_buffer[:sent]
        except ssl.SSLWantWriteError:
            pass

    def _try_read(self) -> None:
        """Try to read data from the socket."""
        self._in_read = True
        try:
            self._last_recv = time.monotonic()
            self._read_callback(self.sock)
        except ssl.SSLWantReadError:
            pass
        finally:
            self._in_read = False

    def _get_next_timeout(self) -> float | None:
        """Get the next timeout for the socket.

        This is used to implement the keepalive interval."""
        if self._keepalive_interval > 0:
            now = time.monotonic()
            send_timeout = self._last_send + self._keepalive_interval
            recv_timeout = self._last_recv + self._keepalive_interval * 1.5
            pong_timeout = self._pong_deadline if self._pong_deadline > 0.0 else recv_timeout + 1.0
            next_timeout = min(send_timeout, recv_timeout, pong_timeout) - now
            return max(0.001, next_timeout)
        return None

    def _check_keepalive(self) -> None:
        """Check if the keepalive interval has been reached."""
        now = time.monotonic()
        if now - self._last_send > self._keepalive_interval and self._pong_deadline == 0.0:
            self._keepalive_callback(self)
        elif now - self._last_recv > self._keepalive_interval * 1.5:
            raise SocketWrapperCloseCondition("No data received in time")
        elif self._pong_deadline > 0.0 and now > self._pong_deadline:
            raise SocketWrapperCloseCondition("No pong received in time")

    def run(self) -> None:
        try:
            self.sock.connect((self.host, self.port))

            if self._use_tls:
                self._handshake_loop()
            else:
                self.sock.setblocking(False)
            if not self._closing:
                self._open_callback()
            self._last_recv = time.monotonic()
            self._last_send = self._last_recv

            while not self._closing:
                next_timeout = self._get_next_timeout()
                write_check = (self.sock,) if self._write_buffer else tuple()
                readable, writable, _ = select.select([self.sock, self._interrupt_r], write_check, [], next_timeout)

                if self.sock in writable:
                    self._try_write()

                if self.sock in readable:
                    self._try_read()

                if self._keepalive_interval > 0 and not self._closing:
                    self._check_keepalive()

                if self._interrupt_r in readable:
                    self._interrupt_r.recv(1024)

        except SocketWrapperCloseCondition as exc:
            logger.info(f"Closing socket: {exc}")
        except Exception:
            logger.exception("Unhandled error in socket read thread")
        finally:
            self._closing = True
            try:
                self._close_callback()
            except Exception:
                logger.exception("Error while calling close callback")
            self._write_buffer.clear()
            self.sock.close()
