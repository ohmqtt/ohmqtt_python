import threading
from typing import ClassVar, Generic, TypeAlias, TypeVar
import weakref

from .packet import (
    MQTTPacket,
    MQTTPubAckPacket,
    MQTTPubRecPacket,
    MQTTPubCompPacket,
    MQTTSubAckPacket,
    MQTTUnsubAckPacket,
)

_T = TypeVar("_T", bound=MQTTPacket)

class Handle(Generic[_T]):
    __slots__: ClassVar[tuple[str, ...]] = ("__weakref__", "_cond", "ack", "exc")
    ack: _T | None
    exc: Exception | None
    _cond: weakref.ref[threading.Condition]

    def __init__(self, cond: threading.Condition) -> None:
        self.ack = None
        self.exc = None
        self._cond = weakref.ref(cond)

    def wait_for_ack(self, timeout: float | None = None) -> _T:
        """Wait for the ack packet or raise an exception if the operation failed."""
        cond = self._cond()
        if cond is None:
            raise RuntimeError("Condition variable is no longer available")
        with cond:
            if self.ack is None and self.exc is None:
                cond.wait_for(lambda: self.ack is not None or self.exc is not None, timeout=timeout)
            if self.exc is not None:
                raise self.exc
            if self.ack is None:
                raise TimeoutError("Timeout waiting for ack")
            return self.ack


PublishHandle: TypeAlias = Handle[MQTTPubAckPacket | MQTTPubRecPacket | MQTTPubCompPacket]
SubscribeHandle: TypeAlias = Handle[MQTTSubAckPacket]
UnsubscribeHandle: TypeAlias = Handle[MQTTUnsubAckPacket]
