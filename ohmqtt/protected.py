from __future__ import annotations

from functools import wraps
import sys
import threading
from typing import Any, Callable, Concatenate, ParamSpec, TypeAlias, TypeVar, Union

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


LockLike: TypeAlias = Union[threading.RLock, threading.Lock]
ProtectedT = TypeVar("ProtectedT", bound="Protected")
ProtectP = ParamSpec("ProtectP")
ProtectR = TypeVar("ProtectR")


def protect(
    func: Callable[Concatenate[ProtectedT, ProtectP], ProtectR],
) -> Callable[Concatenate[ProtectedT, ProtectP], ProtectR]:
    """Decorator to protect a method of a Protected instance."""
    @wraps(func)
    def wrapper(self: ProtectedT, /, *args: Any, **kwargs: Any) -> ProtectR:
        if not self._is_owned():
            raise RuntimeError(f"{self.__class__.__name__} instance lock is not owned by this thread")
        return func(self, *args, **kwargs)
    return wrapper


class Protected:
    """A wrapper to protect a resource or resources.

    Combine with the `@protect` decorator to protect methods of this class."""
    __slots__ = ("__weakref__", "_is_owned", "_lock", "acquire", "release")

    def __init__(self, lock: LockLike | None = None) -> None:
        self._lock = threading.RLock() if lock is None else lock
        self.acquire = self._lock.acquire
        self.release = self._lock.release

        if hasattr(self._lock, "_is_owned"):
            self._is_owned = self._lock._is_owned  # noqa: SLF001
        else:
            raise RuntimeError(f"{self.__class__.__name__} lock does not support _is_owned() method")

    def __enter__(self) -> Self:
        self.acquire()
        return self

    def __exit__(self, *args: object) -> None:
        self.release()
