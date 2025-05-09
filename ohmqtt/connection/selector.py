from __future__ import annotations

import select
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
    __slots__ = ("_closed", "_in_select", "_interrupt_r", "_interrupt_w", "_interrupted")

    def __init__(self, lock: LockLike | None = None) -> None:
        super().__init__(lock if lock is not None else threading.RLock())
        self._closed = False
        self._in_select = False
        self._interrupted = False
        self._interrupt_r, self._interrupt_w = socket.socketpair()
        self._interrupt_r.setblocking(False)
        self._interrupt_w.setblocking(False)

    def _drain(self) -> None:
        """Drain the interrupt socket."""
        data = self._interrupt_r.recv(16)
        assert len(data) == 1, "Expected 1 interrupt per select"

    @protect
    def close(self) -> None:
        """Finalize this instance."""
        self._closed = True
        self._interrupt_r.close()
        self._interrupt_w.close()

    @protect
    def interrupt(self) -> None:
        """Interrupt the select call, if we are in select."""
        if self._closed:
            raise RuntimeError("Selector is closed")
        if self._in_select and not self._interrupted:
            # We are in select, send an interrupt.
            self._interrupt_w.send(b"\x00")
            self._interrupted = True

    @protect
    def select(
        self,
        rlist: list[SocketLike],
        wlist: list[SocketLike],
        xlist: list[SocketLike],
        timeout: float | None = None,
    ) -> tuple[list[SocketLike], list[SocketLike], list[SocketLike]]:
        """Select sockets with a timeout, allowing for interruption.

        This method must be called with the lock already held."""
        if self._closed:
            raise RuntimeError("Selector is closed")
        self._in_select = True
        _rlist = [self._interrupt_r, *rlist]
        self.release()
        try:
            readable, writable, exc = select.select(_rlist, wlist, xlist, timeout)
        finally:
            self.acquire()
            self._in_select = False
        if self._interrupt_r in readable:
            readable.remove(self._interrupt_r)
        if self._interrupted:
            self._interrupted = False
            self._drain()
        return readable, writable, exc
