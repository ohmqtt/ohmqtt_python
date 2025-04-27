from __future__ import annotations

import select
import socket
import ssl
import threading
import time
from typing import Callable, Final
from types import TracebackType

from .logger import get_logger

logger: Final = get_logger("socket_wrapper")

SocketCloseCallback = Callable[[], None]
SocketOpenCallback = Callable[[], None]
SocketReadCallback = Callable[[socket.socket | ssl.SSLSocket], None]
SocketKeepaliveCallback = Callable[["SocketWrapper"], None]


STATE_SHUTDOWN: Final = 0
STATE_CLOSED: Final = 1
STATE_CONNECT: Final = 2


InitAddress: Final[tuple[str, int]] = ("", -1)


class SocketWrapperCloseCondition(Exception):
    """Any exception which should cause the socket to close."""
    pass


def _get_socket() -> socket.socket:
    return socket.socket(socket.AF_INET, socket.SOCK_STREAM)


class SocketWrapper(threading.Thread):
    """Non-blocking socket wrapper with TLS and application keepalive support."""
    __slots__ = (
        "_address",
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
        "_keepalive_interval",
        "_last_send",
        "_last_recv",
        "_pong_deadline",
        "_in_read",
        "_connect_lock",
        "_tcp_nodelay",
        "_reconnect_delay",
        "_state",
    )
    sock: socket.socket | ssl.SSLSocket

    def __init__(
        self,
        close_callback: SocketCloseCallback,
        keepalive_callback: SocketKeepaliveCallback,
        open_callback: SocketOpenCallback,
        read_callback: SocketReadCallback,
    ) -> None:
        super().__init__(daemon=True)
        self._address = InitAddress
        self._close_callback = close_callback
        self._keepalive_callback = keepalive_callback
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

    def __enter__(self) -> SocketWrapper:
        self.start()
        return self

    def __exit__(self, exc_type: type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        self.shutdown()

    def connect(
        self,
        host: str,
        port: int,
        *,
        reconnect_delay: float = 0.0,
        keepalive_interval: int = 0,
        tcp_nodelay: bool = True,
        use_tls: bool = False,
        tls_context: ssl.SSLContext | None = None,
        tls_hostname: str = "",
    ) -> None:
        """Connect to the given host and port.

        This method is non-blocking and will return immediately. The connection will be established in the background."""
        self._keepalive_interval = keepalive_interval
        self._reconnect_delay = reconnect_delay
        self._tcp_nodelay = tcp_nodelay
        self._use_tls = use_tls
        self._tls_hostname = tls_hostname
        self._tls_context = tls_context if tls_context is not None else ssl.create_default_context()
        with self._connect_cond:
            self._address = host, port
            self._state = STATE_CONNECT
            self._connect_cond.notify_all()

    def disconnect(self) -> None:
        """Close the socket.

        This method does not guarantee pending reads or writes will be completed."""
        self._state = STATE_CLOSED
        self._address = InitAddress
        self._interrupt()

    def wait_for_disconnect(self, timeout: float | None = None) -> None:
        """Wait for the socket to be closed.

        This method will block until the socket is closed or the timeout is reached."""
        with self._connect_cond:
            self._connect_cond.wait_for(lambda: self._state <= STATE_CLOSED, timeout=timeout)
            if self._state > STATE_CLOSED:
                raise TimeoutError("Socket did not close in time")

    def shutdown(self) -> None:
        """Shutdown the socket wrapper.

        This method will block until the socket is closed and the thread is terminated."""
        with self._connect_cond:
            self._state = STATE_SHUTDOWN
            self._address = InitAddress
            self._connect_cond.notify_all()
            self._interrupt()

    def send(self, data: bytes) -> None:
        """Write data to the socket."""
        if self._state < STATE_CONNECT:
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
        raise SocketWrapperCloseCondition("Handshake loop did not complete")

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
            self._last_recv = time.monotonic()
            self._read_callback(sock)
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

    def _check_connect(self) -> bool:
        """Check if we should connect to the server.

        Raises SocketWrapperCloseCondition if the thread should be shutdown instead."""
        if self._state == STATE_SHUTDOWN:
            raise SocketWrapperCloseCondition("Shutting down")
        return bool(self._state == STATE_CONNECT and self._address != InitAddress)

    def run(self) -> None:
        while self._state > STATE_SHUTDOWN:
            was_open = False
            sock: socket.socket | ssl.SSLSocket = _get_socket()
            try:
                with self._connect_cond:
                    self._connect_cond.wait_for(self._check_connect)
                    was_open = True
                    address = self._address

                if self._tcp_nodelay:
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.connect(address)
                sock.setblocking(False)

                if self._use_tls:
                    sock = self._handshake_loop(sock)
                if self._state == STATE_CONNECT:
                    self._open_callback()

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

            except SocketWrapperCloseCondition as exc:
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
                self._write_buffer.clear()
                sock.close()
                if self._state > STATE_SHUTDOWN and self._reconnect_delay > 0:
                    with self._connect_cond:
                        self._connect_cond.wait(timeout=self._reconnect_delay)
                        if self._address == InitAddress:
                            logger.debug("Reconnect cancelled")
                            break
        logger.debug("Shutdown complete")
