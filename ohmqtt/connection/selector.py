from dataclasses import dataclass, field

import select
import socket
import ssl
from typing import Final

from ..logger import get_logger

logger: Final = get_logger("connection.selector")

SocketLike = socket.socket | ssl.SSLSocket


@dataclass(slots=True)
class InterruptibleSelector:
    interrupt_r: socket.socket = field(init=False)
    interrupt_w: socket.socket = field(init=False)
    _in_select: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        self.interrupt_r, self.interrupt_w = socket.socketpair()
        self.interrupt_r.setblocking(False)
        self.interrupt_w.setblocking(False)

    def drain(self) -> None:
        """Drain the interrupt socket."""
        while True:
            try:
                self.interrupt_r.recv(1024)
            except BlockingIOError:
                break

    def interrupt(self) -> None:
        """Interrupt the select call, if we are in select."""
        try:
            if self._in_select:
                # We are in select, send an interrupt.
                self.interrupt_w.send(b"\x00")
        except BlockingIOError:
            # The socket is full, ignore the error.
            pass

    def select(
        self,
        rlist: list[SocketLike],
        wlist: list[SocketLike],
        xlist: list[SocketLike],
        timeout: float | None = None,
    ) -> tuple[list[SocketLike], list[SocketLike], list[SocketLike], bool]:
        """Select sockets with a timeout, allowing for interruption.

        The fourth return value is True if the call was interrupted."""
        self._in_select = True
        was_interrupted = False
        _rlist = [self.interrupt_r] + rlist
        readable, writable, exc = select.select(_rlist, wlist, xlist, timeout)
        self._in_select = False
        if self.interrupt_r in readable:
            was_interrupted = True
            self.drain()
            readable.remove(self.interrupt_r)
        return readable, writable, exc, was_interrupted
