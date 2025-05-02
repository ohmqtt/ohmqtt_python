from __future__ import annotations

import threading
import time
from typing import Callable, TypeAlias, TypeVar, Union

from .protected import Protected, protect

_time = time.monotonic

LockLike: TypeAlias = Union[threading.Lock, threading.RLock]
WaitForT = TypeVar("WaitForT")


class ConditionLite(Protected):
    """A lightweight condition variable implementation."""
    # This implementation is a memory-reduced version of threading.Condition.
    __slots__ = ("_waiters",)
    _waiters: list[threading.Lock]

    def __init__(self, lock: LockLike | None = None) -> None:
        super().__init__(lock if lock is not None else threading.RLock())
        self._waiters = []

    @protect
    def wait(self, timeout: float | None = None) -> bool:
        """Wait until notified or until a timeout occurs.

        Must be called with the lock held.

        Returns True if the condition was notified, False if the timeout is reached."""
        if not self._is_owned():
            raise RuntimeError("cannot wait on un-acquired lock")
        waiter = threading.Lock()
        waiter.acquire()
        self._waiters.append(waiter)
        self.release()
        success = False
        try:
            if timeout is None:
                waiter.acquire()
                success = True
            else:
                if timeout > 0:
                    success = waiter.acquire(True, timeout)
                else:
                    success = waiter.acquire(False)
            return success
        finally:
            self.acquire()
            if not success:
                try:
                    self._waiters.remove(waiter)
                except ValueError:
                    pass

    def wait_for(self, predicate: Callable[[], WaitForT], timeout: float | None = None) -> WaitForT:
        """Wait until a condition evaluates to True."""
        endtime = None
        waittime = timeout
        result = predicate()
        while not result:
            if waittime is not None:
                if endtime is None:
                    endtime = _time() + waittime
                else:
                    waittime = endtime - _time()
                    if waittime <= 0:
                        break
            self.wait(waittime)
            result = predicate()
        return result

    @protect
    def notify(self, n: int = 1) -> None:
        """Wake up one or more threads waiting on this condition, if any."""
        if not self._is_owned():
            raise RuntimeError("cannot notify on un-acquired lock")
        waiters = self._waiters
        while waiters and n > 0:
            waiter = waiters[0]
            try:
                waiter.release()
            except RuntimeError:
                pass
            else:
                n -= 1
            try:
                waiters.remove(waiter)
            except ValueError:
                pass

    @protect
    def notify_all(self) -> None:
        """Wake up all threads waiting on this condition."""
        self.notify(len(self._waiters))
