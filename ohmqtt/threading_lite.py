from __future__ import annotations

import threading
import time
from typing import Any, Callable, Type, TypeVar
from types import TracebackType


_time = time.monotonic


WaitForT = TypeVar("WaitForT")


class ConditionLite:
    """A lightweight condition variable implementation."""
    # This implementation is a memory-reduced version of threading.Condition.
    __slots__ = (
        "acquire",
        "release",
        "_is_owned",
        "_acquire_restore",
        "_release_save",
        "_lock",
        "_waiters",
        "_owner",
        "__weakref__",
    )
    _lock: threading.Lock | threading.RLock
    _waiters: list[threading.Lock]
    _owner: threading.Thread | None

    def __init__(self, lock: threading.Lock | threading.RLock | None = None) -> None:
        self._lock = lock if lock is not None else threading.RLock()
        self.acquire = self._lock.acquire
        self.release = self._lock.release
        self._waiters = []
        self._owner = None

        if hasattr(self._lock, "_is_owned"):
            self._is_owned = self._lock._is_owned
        else:
            def _is_owned(self: ConditionLite) -> bool:
                """Check if the current thread owns the lock."""
                return self._owner == threading.current_thread()
            self._is_owned = _is_owned

        if hasattr(self._lock, "_acquire_restore"):
            self._acquire_restore = self._lock._acquire_restore
        else:
            def _acquire_restore(self: ConditionLite, _: Any) -> None:
                """Restore the lock state after acquiring."""
                self._lock.acquire()
            self._acquire_restore = _acquire_restore

        if hasattr(self._lock, "_release_save"):
            self._release_save = self._lock._release_save
        else:
            def _release_save(self: ConditionLite) -> None:
                """Save the lock state before releasing."""
                self._lock.release()
            self._release_save = _release_save


    def __enter__(self) -> ConditionLite:
        self.acquire()
        return self
    
    def __exit__(self, exc_type: Type[BaseException], exc_value: BaseException, exc_tb: TracebackType) -> None:
        self.release()
    
    def wait(self, timeout: float | None = None) -> bool:
        """Wait until notified or until a timeout occurs.

        Must be called with the lock held.

        Returns True if the condition was notified, False if the timeout is reached."""
        if not self._is_owned():
            raise RuntimeError("cannot wait on un-acquired lock")
        waiter = threading.Lock()
        waiter.acquire()
        self._waiters.append(waiter)
        saved_state = self._release_save()
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
            self._acquire_restore(saved_state)
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

    def _notify(self, n: int) -> None:
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

    def notify(self, n: int = 1) -> None:
        """Wake up one or more threads waiting on this condition, if any."""
        self._notify(n)

    def notify_all(self) -> None:
        """Wake up all threads waiting on this condition."""
        self._notify(len(self._waiters))
