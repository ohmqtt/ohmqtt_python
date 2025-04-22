import logging
import select
import socket
import ssl
import threading
from typing import Callable
import weakref

logger = logging.getLogger(__name__)

CloseCallback = Callable[["SocketWrapper"], None]
OpenCallback = Callable[["SocketWrapper"], None]
ReadCallback = Callable[["SocketWrapper", bytes], None]


class SocketWrapperCloseCondition(Exception):
    """Any exception which should cause the socket to close."""
    pass


class SocketWrapper(threading.Thread):
    """Non-blocking socket wrapper with TLS and application keepalive support."""
    _sock: socket.socket | ssl.SSLSocket

    def __init__(
        self,
        host: str,
        port: int,
        close_callback: CloseCallback,
        open_callback: OpenCallback,
        read_callback: ReadCallback,
        *,
        tcp_nodelay: bool = False,
        use_tls: bool = False,
        tls_context: ssl.SSLContext | None = None,
        tls_hostname: str = "",
    ) -> None:
        super().__init__(daemon=True)
        self._host = host
        self._port = port
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if tcp_nodelay:
            self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._close_callback_ref = weakref.ref(close_callback)
        self._open_callback_ref = weakref.ref(open_callback)
        self._read_callback_ref = weakref.ref(read_callback)
        self._use_tls = use_tls
        self._tls_context = tls_context
        self._tls_hostname = tls_hostname
        self._use_tls = use_tls
        self._tls_context = ssl.create_default_context() if use_tls and tls_context is None else tls_context
        self._tls_hostname = tls_hostname if tls_hostname else host

        self._write_buffer = bytearray()
        self._interrupt_r, self._interrupt_w = socket.socketpair()
        self._closing = False

    def close(self) -> None:
        """Close the socket.
        
        This method does not guarantee pending reads or writes will be completed."""
        self._closing = True
        self._interrupt_w.send(b"\x00")

    def send(self, data: bytes) -> None:
        """Write data to the socket.
        
        This method is non-blocking and will raise an exception if the socket is not connected.
        Any data not written immediately will be buffered and sent when the socket is ready."""
        try:
            sent = self._sock.send(data)
            if sent < len(data):
                # If some but not all data was sent, buffer the remaining data.
                self._write_buffer.extend(data[sent:])
                self._interrupt_w.send(b"\x00")
        except (ssl.SSLWantWriteError, BlockingIOError):
            # If the socket was not ready for writing, buffer the data.
            self._write_buffer.extend(data)
            self._interrupt_w.send(b"\x00")

    def _call_open_callback(self) -> None:
        """Call the open callback if it is still available.

        Otherwise raise a SocketWrapperCloseCondition exception."""
        open_callback = self._open_callback_ref()
        if open_callback is not None:
            try:
                open_callback(self)
            except Exception:
                logger.exception("Error while calling open callback")
        else:
            raise SocketWrapperCloseCondition("Open callback is no longer available")

    def _call_read_callback(self, data: bytes) -> None:
        """Call the read callback if it is still available.

        Otherwise raise a SocketWrapperCloseCondition exception."""
        read_callback = self._read_callback_ref()
        if read_callback is not None:
            try:
                read_callback(self, data)
            except Exception:
                logger.exception("Error while calling read callback")
        else:
            raise SocketWrapperCloseCondition("Read callback is no longer available")

    def _handshake_loop(self) -> None:
        """Run the TLS handshake in a loop until it is complete."""
        assert self._tls_context is not None
        self._sock = self._tls_context.wrap_socket(self._sock, server_hostname=self._tls_hostname, do_handshake_on_connect=False)
        self._sock.setblocking(False)
        while not self._closing:
            try:
                self._sock.do_handshake()
                break
            except ssl.SSLWantReadError:
                select.select([self._sock, self._interrupt_r], [], [])
            except ssl.SSLWantWriteError:
                select.select([self._interrupt_r], [self._sock], [])

    def _try_write(self) -> None:
        """Try to flush the write buffer to the socket."""
        try:
            sent = self._sock.send(self._write_buffer)
        except (ssl.SSLWantWriteError, BlockingIOError):
            pass
        else:
            if sent < len(self._write_buffer):
                self._write_buffer = self._write_buffer[sent:]
            else:
                self._write_buffer.clear()

    def _try_read(self) -> None:
        """Try to read data from the socket."""
        try:
            data = self._sock.recv(65535)
        except (ssl.SSLWantReadError, BlockingIOError):
            pass
        else:
            if not data:
                raise SocketWrapperCloseCondition("no data during read")
            self._call_read_callback(data)

    def run(self) -> None:
        try:
            self._sock.connect((self._host, self._port))

            if self._use_tls:
                self._handshake_loop()
            else:
                self._sock.setblocking(False)
            self._call_open_callback()

            while not self._closing:
                write_check = (self._sock,) if self._write_buffer else tuple()
                readable, writable, err = select.select([self._sock, self._interrupt_r], write_check, [self._sock])

                if self._sock in err:
                    raise SocketWrapperCloseCondition("Socket in error")

                if self._write_buffer and self._sock in writable:
                    self._try_write()

                if self._sock in readable:
                    self._try_read()

                if self._interrupt_r in readable:
                    self._interrupt_r.recv(128)

        except SocketWrapperCloseCondition as exc:
            logger.debug(f"SocketWrapperCloseCondition: {exc}")
        except Exception:
            logger.exception("Unhandled error in socket read thread")
        finally:
            try:
                close_callback = self._close_callback_ref()
                if close_callback is not None:
                    close_callback(self)
                else:
                    logger.debug("Close callback is no longer available")
                del close_callback  # Future-proofing: remove the reference to the callback.
            except Exception:
                logger.exception("Error while calling close callback")
            self._write_buffer.clear()
            self._sock.close()
