from __future__ import annotations

from dataclasses import dataclass, field
import threading
from types import TracebackType
from typing import Type

import select
import socket
import ssl
from typing import Final

from ..logger import get_logger

logger: Final = get_logger("connection.selector")

SocketLike = socket.socket | ssl.SSLSocket


@dataclass(slots=True)
class InterruptibleSelector:
    """A select() method which can be interrupted by a call to interrupt().

    This can be used to interrupt a blocking select() call from another thread."""
    interrupt_r: socket.socket = field(init=False)
    interrupt_w: socket.socket = field(init=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False)
    _in_select: bool = field(default=False, init=False)
    _interrupted: bool = field(default=False, init=False)
    _owner: threading.Thread | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        self.interrupt_r, self.interrupt_w = socket.socketpair()
        self.interrupt_r.setblocking(False)
        self.interrupt_w.setblocking(False)

    def __enter__(self) -> InterruptibleSelector:
        """Enter the context manager and acquire the lock."""
        self.acquire()
        return self

    def __exit__(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        """Exit the context manager and release the lock."""
        self.release()

    def _is_owner(self) -> bool:
        return self._owner == threading.current_thread()

    def _drain(self) -> None:
        """Drain the interrupt socket."""
        while True:
            try:
                data = self.interrupt_r.recv(16)
                assert len(data) == 1, "Expected 1 interrupt per select"
            except BlockingIOError:
                break

    def acquire(self) -> None:
        """Acquire the lock and set the owner."""
        self._lock.acquire()
        self._owner = threading.current_thread()

    def release(self) -> None:
        """Release the lock and clear the owner."""
        self._owner = None
        self._lock.release()

    def interrupt(self) -> None:
        """Interrupt the select call, if we are in select."""
        with self:
            if self._in_select and not self._interrupted:
                # We are in select, send an interrupt.
                self.interrupt_w.send(b"\x00")
                self._interrupted = True

    def select(
        self,
        rlist: list[SocketLike],
        wlist: list[SocketLike],
        xlist: list[SocketLike],
        timeout: float | None = None,
    ) -> tuple[list[SocketLike], list[SocketLike], list[SocketLike]]:
        """Select sockets with a timeout, allowing for interruption.

        This method must be called with the lock already held."""
        if not self._is_owner():
            raise RuntimeError("Cannot call select without the lock")
        self._in_select = True
        _rlist = [self.interrupt_r] + rlist
        self._lock.release()
        try:
            readable, writable, exc = select.select(_rlist, wlist, xlist, timeout)
        finally:
            self._lock.acquire()
            self._in_select = False
        if self.interrupt_r in readable:
            readable.remove(self.interrupt_r)
        if self._interrupted:
            self._interrupted = False
            self._drain()
        return readable, writable, exc
