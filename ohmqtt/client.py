import logging
from typing import Final

from .property import MQTTPropertyDict
from .session import Session
from .subscriptions import Subscriptions, SubscribeCallback

logger: Final = logging.getLogger(__name__)



class Client:
    """High level interface for the MQTT client."""
    client_id: str
    session: Session
    subscriptions: Subscriptions

    def __init__(
        self,
        client_id: str,
    ) -> None:
        self.client_id = client_id
        self.subscriptions = Subscriptions()
        self.session = Session(client_id, message_callback=self.on_message)

    def connect(self, host: str, port: int) -> None:
        self.session.connect(host, port)

    def publish(
        self,
        topic: str,
        payload: bytes,
        *,
        qos: int = 0,
        retain: bool = False,
        properties: MQTTPropertyDict | None = None,
    ) -> None:
        self.session.publish(topic, payload, qos=qos, retain=retain, properties=properties)
    
    def subscribe(
        self,
        topic_filter: str,
        callback: SubscribeCallback,
        qos: int = 2,
        properties: MQTTPropertyDict | None = None,
    ) -> None:
        """Subscribe to a topic filter with a callback."""
        self.subscriptions.add(topic_filter, callback)
        self.session.subscribe(topic_filter, qos=qos, properties=properties)

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

    def on_message(self, session: Session, topic: str, payload: bytes, properties: MQTTPropertyDict) -> None:
        """Callback for when a message is received."""
        logger.debug(f"Received message on topic {topic}: {payload.hex()}")
        callbacks = self.subscriptions.get_callbacks(topic)
        for callback in callbacks:
            try:
                callback(topic, payload, properties)
            except Exception:
                logger.exception(f"Unhandled error in subscribe callback for topic: {topic}")
