import logging
import selectors
import socket
import threading
from typing import Callable


logger = logging.getLogger(__name__)

SocketWriterErrorCallback = Callable[[Exception], None]


class SocketWriter(threading.Thread):
    """Non-blocking send interface for a socket."""
    def __init__(self, sock: socket.socket, error_cb: SocketWriterErrorCallback | None = None) -> None:
        super().__init__(daemon=True)
        assert not sock.getblocking(), "Socket must be non-blocking"
        self._sock = sock
        self._error_cb = error_cb
        self._closed = False
        # The buffer is used to store data that has been deferred for sending.
        self._buffer = b""
        # This condition protects two variables:
        # 1. _buffer: the data that has been deferred for sending.
        # 2. _closed: whether the writer is closed.
        # Calls to send() and close() must notify the condition after changing these values.
        # No party may block on I/O while holding the condition lock.
        self._cond = threading.Condition()
        # This socket pair is used to wake up the thread when the writer is closed.
        self._close_sock_r, self._close_sock_w = socket.socketpair()

    def __enter__(self) -> "SocketWriter":
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def send(self, data: bytes) -> bool:
        """Send data to the socket.
        
        Returns True if the data was completely sent, False if it was deferred."""
        try:
            num_sent = self._sock.send(data)
            if num_sent == len(data):
                return True
            else:
                self._defer_send(data[num_sent:])
                return False
        except BlockingIOError:
            # Socket is non-blocking and the send would block.
            self._defer_send(data)
            return False

    def _defer_send(self, data: bytes) -> None:
        """Defer sending data to the socket.
        
        This method is called from the public interface when the socket is not writable."""
        with self._cond:
            self._buffer += data
            self._cond.notify()

    def close(self) -> None:
        """Close the writer immediately, without flushing the buffer."""
        self._close_sock_w.send(b"\x00")
        with self._cond:
            self._closed = True
            self._cond.notify()

    def _send_buffered(self) -> None:
        """Send buffered data to the socket."""
        with self._cond:
            num_sent = self._sock.send(self._buffer)
            if num_sent == len(self._buffer):
                # Ensure we release the old buffer.
                self._buffer = b""
            else:
                self._buffer = self._buffer[num_sent:]

    def run(self):
        try:
            sel = selectors.DefaultSelector()
            sel.register(self._sock, selectors.EVENT_WRITE)
            sel.register(self._close_sock_r, selectors.EVENT_READ)
            while True:
                with self._cond:
                    while not self._buffer and not self._closed:
                        self._cond.wait()
                    if self._closed:
                        break
                events = sel.select()
                for key, _ in events:
                    if key.fileobj == self._close_sock_r:
                        break
                    elif key.fileobj == self._sock:
                        self._send_buffered()
        except Exception as exc:
            if not self._closed:
                if self._error_cb is not None:
                    self._error_cb(exc)
                else:
                    logger.exception("Unhandled exception in SocketWriter thread")
