from __future__ import annotations

import selectors
import socket
import ssl
import threading
from typing import Final, TypeAlias

from ..logger import get_logger
from ..protected import Protected, protect, LockLike

logger: Final = get_logger("connection.selector")

SocketLike: TypeAlias = socket.socket | ssl.SSLSocket


class InterruptibleSelector(Protected):
    """A select() method which can be interrupted by a call to interrupt().

    This can be used to interrupt a blocking select() call from another thread."""
    __slots__ = (
        "_closed",
        "_in_select",
        "_interrupt_r",
        "_interrupt_w",
        "_interrupted",
        "_selector",
        "_sock",
    )

    def __init__(self, lock: LockLike | None = None) -> None:
        super().__init__(lock if lock is not None else threading.RLock())
        self._selector = selectors.DefaultSelector()
        self._sock: SocketLike | None = None
        self._closed = False
        self._in_select = False
        self._interrupted = False
        self._interrupt_r, self._interrupt_w = socket.socketpair()
        self._interrupt_r.setblocking(False)
        self._interrupt_w.setblocking(False)
        self._selector.register(self._interrupt_r, selectors.EVENT_READ)

    def _drain(self) -> None:
        """Drain the interrupt socket."""
        data = self._interrupt_r.recv(2)
        if len(data) > 1:
            raise RuntimeError(f"Expected 1 interrupt per select, got {len(data)}")

    @protect
    def close(self) -> None:
        """Finalize this instance."""
        self._closed = True
        self._interrupt_r.close()
        self._interrupt_w.close()
        self._selector.close()

    @protect
    def interrupt(self) -> None:
        """Interrupt the select call, if we are in select."""
        if self._closed:
            raise RuntimeError("Selector is closed")
        if self._in_select and not self._interrupted:
            # We are in select, send an interrupt.
            self._interrupted = True
            self._interrupt_w.send(b"\x00")

    def change_sock(self, sock: SocketLike) -> None:
        """Change the socket for the selector."""
        if self._closed:
            raise RuntimeError("Selector is closed")
        if sock == self._sock:
            return
        if self._sock is not None:
            self._selector.unregister(self._sock)
        self._selector.register(sock, selectors.EVENT_READ)
        self._sock = sock

    @protect
    def select(
        self,
        *,
        read: bool = False,
        write: bool = False,
        timeout: float | None = None,
    ) -> tuple[bool, bool]:
        """Select an optional socket with a timeout, allowing for interruption.

        This method must be called with the lock already held."""
        if self._closed:
            raise RuntimeError("InterruptibleSelector is closed")
        if self._sock is None:
            raise RuntimeError("No socket set for InterruptibleSelector")
        wanted_events = (read * selectors.EVENT_READ) | (write * selectors.EVENT_WRITE)
        if wanted_events == 0:
            raise ValueError("Must select either read or write or both")
        self._selector.modify(self._sock, wanted_events)
        readable, writable = False, False
        self._in_select = True
        self._interrupted = False
        self.release()
        try:
            events = self._selector.select(timeout)
            for key, event in events:
                if key.fileobj is self._sock:
                    readable = bool(event & selectors.EVENT_READ)
                    writable = bool(event & selectors.EVENT_WRITE)
        finally:
            self.acquire()
            if self._interrupted:
                self._drain()
            self._in_select = False
            self._interrupted = False
        return readable, writable
