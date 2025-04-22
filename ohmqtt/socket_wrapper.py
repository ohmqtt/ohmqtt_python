import logging
import selectors
import socket
import ssl
import threading
from typing import Callable
import weakref

logger = logging.getLogger(__name__)

OpenCallback = Callable[["SocketWrapper"], None]
ReadCallback = Callable[["SocketWrapper", bytes], None]


class SocketWrapper(threading.Thread):
    """Non-blocking socket wrapper with TLS support."""
    sock: socket.socket | ssl.SSLSocket

    def __init__(
        self,
        host: str,
        port: int,
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
        self._open_callback_ref = weakref.ref(open_callback)
        self._read_callback_ref = weakref.ref(read_callback)
        self._use_tls = use_tls
        self._tls_context = tls_context
        self._tls_hostname = tls_hostname
        self._write_buffer = bytearray()

    def close(self) -> None:
        """Close the socket."""
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
        except BlockingIOError:
            # If the socket was not ready for writing, buffer the data.
            self._write_buffer.extend(data)

    def run(self) -> None:
        try:
            self._sock.connect((self._host, self._port))
            self._sock.setblocking(False)
            self._open_callback_ref()(self)
            sel = selectors.DefaultSelector()
            sel.register(self._sock, selectors.EVENT_READ | selectors.EVENT_WRITE)
            while True:
                events = sel.select()
                for key, _ in events:
                    if self._write_buffer and key.events & selectors.EVENT_WRITE:
                        try:
                            sent = self._sock.send(self._write_buffer)
                        except BlockingIOError:
                            pass
                        else:
                            if sent < len(self._write_buffer):
                                self._write_buffer = self._write_buffer[sent:]
                            else:
                                self._write_buffer.clear()
                    if key.events & selectors.EVENT_READ:
                        try:
                            data = self._sock.recv(65535)
                        except BlockingIOError:
                            pass
                        else:
                            if not data:
                                logger.debug("Socket closed, stopping socket thread")
                                return
                            read_callback = self._read_callback_ref()
                            if read_callback is None:
                                logger.debug("Read callback is no longer available, stopping socket thread")
                                return
                            read_callback(self, data)
        except Exception:
            logger.exception("Error in socket read thread")
