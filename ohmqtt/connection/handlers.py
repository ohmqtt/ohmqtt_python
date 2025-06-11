from __future__ import annotations

from typing import Any, cast, Callable, TypeVar

from .types import ReceivablePacketT
from ..packet import (
    MQTTPacket,
    MQTTPublishPacket,
    MQTTPubAckPacket,
    MQTTPubRecPacket,
    MQTTPubRelPacket,
    MQTTPubCompPacket,
    MQTTSubAckPacket,
    MQTTUnsubAckPacket,
    MQTTConnAckPacket,
    MQTTAuthPacket,
    MQTTDisconnectPacket,
)
PacketT = TypeVar("PacketT", bound=ReceivablePacketT)


class MessageHandlers:
    """Container for intentional registration of message handlers for MQTT packets.

    Handlers may only be registered within the context manager.

    The context manager may only be entered once."""
    __slots__ = ("_handlers", "_registered", "_registering")

    def __init__(self) -> None:
        # Typing is kludged internally for brevity and speed.  But mostly speed.
        self._handlers: dict[type[MQTTPacket], list[Callable[[Any], None]]] = {
            MQTTPublishPacket: [],
            MQTTPubAckPacket: [],
            MQTTPubRecPacket: [],
            MQTTPubRelPacket: [],
            MQTTPubCompPacket: [],
            MQTTSubAckPacket: [],
            MQTTUnsubAckPacket: [],
            MQTTConnAckPacket: [],
            MQTTAuthPacket: [],
            MQTTDisconnectPacket: [],
        }
        self._registering = False
        self._registered = False

    def __enter__(self) -> MessageHandlers:
        """Start registering the message handlers.

        :raises RuntimeError: Message handlers are already registered (the context manager may only be entered once)."""
        if self._registered:
            raise RuntimeError("Message handlers already registered")
        self._registering = True
        return self

    def __exit__(self, *args: object) -> None:
        """Stop registering the message handlers."""
        self._registering = False
        self._registered = True

    def get_handlers(self, packet_type: type[PacketT]) -> list[Callable[[PacketT], None]]:
        """Get the handlers for a given packet type.

        :raises RuntimeError: Message handlers registration has not been completed."""
        if not self._registered:
            raise RuntimeError("Message handlers not registered")
        return cast(list[Callable[[PacketT], None]], self._handlers[packet_type])

    def register(self, packet_type: type[PacketT], handler: Callable[[PacketT], None]) -> None:
        """Register a handler for a given packet type.

        :raises RuntimeError: Not in the context manager."""
        if not self._registering:
            raise RuntimeError("Message handlers not in registration block")
        self._handlers[packet_type].append(handler)

    def handle(self, packet: ReceivablePacketT) -> list[Exception]:
        """Handle a packet by calling the appropriate handlers.

        Guarantees that all handlers are called in the order they were registered.

        Guarantees that all handlers are run regardless of Exceptions.

        :return: A list of Exceptions raised by handlers."""
        exceptions = []
        handlers = self.get_handlers(type(packet))
        for handler in handlers:
            try:
                handler(packet)
            except Exception as exc:  # noqa: PERF203
                exceptions.append(exc)
        return exceptions
