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


class SocketWrapper(threading.Thread):
    """Non-blocking socket wrapper with TLS support."""
    sock: socket.socket | ssl.SSLSocket

    def __init__(
        self,
        host: str,
        port: int,
        close_callback: CloseCallback,
        open_callback: OpenCallback,
        read_callback: ReadCallback,
        *,
        use_tls: bool = False,
        tls_context: ssl.SSLContext | None = None,
        tls_hostname: str = "",
    ) -> None:
        super().__init__(daemon=True)
        self._host = host
        self._port = port
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._close_callback_ref = weakref.ref(close_callback)
        self._open_callback_ref = weakref.ref(open_callback)
        self._read_callback_ref = weakref.ref(read_callback)
        self._use_tls = use_tls
        self._tls_context = tls_context
        self._tls_hostname = tls_hostname
        self._write_buffer = bytearray()
        self._use_tls = use_tls
        self._tls_context = ssl.create_default_context() if tls_context is None else tls_context
        self._tls_hostname = tls_hostname if tls_hostname else host

    def close(self) -> None:
        """Close the socket."""
        self._sock.shutdown(socket.SHUT_RDWR)
        self._sock.close()
        self._write_buffer.clear()

    def send(self, data: bytes) -> None:
        """Write data to the socket.
        
        This method is non-blocking and will raise an exception if the socket is not connected.
        Any data not written immediately will be buffered and sent when the socket is ready."""
        try:
            sent = self._sock.send(data)
            if sent < len(data):
                # If some but not all data was sent, buffer the remaining data.
                self._write_buffer.extend(data[sent:])
        except (ssl.SSLWantWriteError, BlockingIOError):
            # If the socket was not ready for writing, buffer the data.
            self._write_buffer.extend(data)

    def run(self) -> None:
        try:
            self._sock.connect((self._host, self._port))

            if self._use_tls:
                handshake_done = False
                self._sock = self._tls_context.wrap_socket(self._sock, server_hostname=self._tls_hostname, do_handshake_on_connect=False)
                self._sock.setblocking(False)
                while True:
                    try:
                        self._sock.do_handshake()
                        break
                    except ssl.SSLWantReadError:
                        select.select([self._sock], [], [])
                    except ssl.SSLWantWriteError:
                        select.select([], [self._sock], [])
            else:
                self._sock.setblocking(False)
            self._open_callback_ref()(self)

            while True:
                try:
                    readable, writable, err = select.select([self._sock], [self._sock], [self._sock])
                except ValueError:
                    logger.debug("Socket closed (select ValueError), stopping socket thread")
                    return

                if self._sock in err:
                    logger.error("Socket in error, stopping socket thread")
                    return

                if self._write_buffer and self._sock in writable:
                    try:
                        sent = self._sock.send(self._write_buffer)
                    except (ssl.SSLWantWriteError, BlockingIOError):
                        pass
                    else:
                        if sent < len(self._write_buffer):
                            self._write_buffer = self._write_buffer[sent:]
                        else:
                            self._write_buffer.clear()

                if self._sock in readable:
                    try:
                        data = self._sock.recv(65535)
                    except (ssl.SSLWantReadError, BlockingIOError):
                        pass
                    except OSError:
                        logger.debug("Socket closed (OSError), stopping socket thread")
                        return
                    else:
                        if not data:
                            logger.debug("Socket closed (no data), stopping socket thread")
                            return
                        read_callback = self._read_callback_ref()
                        if read_callback is None:
                            logger.debug("Read callback is no longer available, stopping socket thread")
                            return
                        read_callback(self, data)

        except Exception:
            logger.exception("Error in socket read thread")
        finally:
            try:
                self._close_callback_ref()(self)
            except Exception:
                logger.exception("Error while calling close callback")
