from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from .logger import get_logger
from .message import MQTTMessage
from .packet import MQTTPublishPacket
from .property import MQTTPropertyDict
from .retention import PublishHandle
from .session import Session
from .subscriptions import Subscriptions, SubscribeCallback

logger: Final = get_logger("client")


@dataclass(match_args=True, slots=True, frozen=True)
class SubscriptionHandle:
    """Represents a subscription to a topic filter with a callback."""
    topic_filter: str
    callback: SubscribeCallback
    _client: Client

    def unsubscribe(self) -> None:
        """Unsubscribe from the topic filter."""
        self._client.unsubscribe(self.topic_filter, self.callback)


class Client:
    """High level interface for the MQTT client."""
    __slots__ = ("client_id", "keepalive_interval", "session", "subscriptions")
    client_id: str
    keepalive_interval: int
    session: Session
    subscriptions: Subscriptions

    def __init__(
        self,
        client_id: str = "",
        *,
        keepalive_interval: int = 30,
    ) -> None:
        self.client_id = client_id
        self.keepalive_interval = keepalive_interval
        self.subscriptions = Subscriptions()
        self.session = Session(
            client_id,
            message_callback=self.on_message,
        )

    def connect(self, host: str, port: int) -> None:
        self.session.connect(host, port, keepalive_interval=self.keepalive_interval)

    def publish(
        self,
        topic: str,
        payload: bytes,
        *,
        qos: int = 0,
        retain: bool = False,
        properties: MQTTPropertyDict | None = None,
    ) -> PublishHandle:
        return self.session.publish(topic, payload, qos=qos, retain=retain, properties=properties)
    
    def subscribe(
        self,
        topic_filter: str,
        callback: SubscribeCallback,
        qos: int = 2,
        properties: MQTTPropertyDict | None = None,
    ) -> SubscriptionHandle:
        """Subscribe to a topic filter with a callback."""
        self.subscriptions.add(topic_filter, callback)
        self.session.subscribe(topic_filter, qos=qos, properties=properties)
        return SubscriptionHandle(
            topic_filter=topic_filter,
            callback=callback,
            _client=self,
        )

    def unsubscribe(self, topic_filter: str, callback: SubscribeCallback | None = None) -> None:
        """Unsubscribe from a topic filter.

        If a callback is provided it will be removed, otherwise all callbacks for the topic filter will be removed."""
        remaining = 0
        if callback is None:
            self.subscriptions.remove_all(topic_filter)
        else:
            remaining = self.subscriptions.remove(topic_filter, callback)
        if remaining == 0:
            self.session.unsubscribe(topic_filter)

    def on_message(self, packet: MQTTPublishPacket) -> None:
        """Callback for when a message is received."""
        callbacks = self.subscriptions.get_callbacks(packet.topic)
        if not callbacks:
            logger.debug(f"No callbacks for topic: {packet.topic}")
            return
        message = MQTTMessage(
            topic=packet.topic,
            payload=packet.payload,
            qos=packet.qos,
            packet_id=packet.packet_id,
            retain=packet.retain,
            dup=packet.dup,
            properties=packet.properties,
        )
        for callback in callbacks:
            try:
                callback(message)
            except Exception:
                logger.exception(f"Unhandled error in subscribe callback: {callback} for topic: {message.topic}")
