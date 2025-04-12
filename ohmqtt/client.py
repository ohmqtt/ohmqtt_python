import logging
from typing import Callable, Final

from .property import MQTTPropertyDict
from .session import Session
from .topic_filter import MQTTTopicFilter

logger: Final = logging.getLogger(__name__)

ClientSubscribeCallback = Callable[["Client", str, bytes, MQTTPropertyDict], None]


class Client:
    """High level interface for the MQTT client."""
    client_id: str
    session: Session
    subscriptions: dict[MQTTTopicFilter, list[ClientSubscribeCallback]]

    def __init__(
        self,
        client_id: str,
    ) -> None:
        self.client_id = client_id
        self.subscriptions = {}
        self.session = Session(client_id, message_cb=self.on_message)

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
        callback: ClientSubscribeCallback,
        qos: int = 2,
        properties: MQTTPropertyDict | None = None,
    ) -> None:
        """Subscribe to a topic filter with a callback."""
        self.subscriptions[MQTTTopicFilter(topic_filter)] = callback
        self.session.subscribe(topic_filter, qos=qos, properties=properties)

    def unsubscribe(self, topic_filter: str) -> None:
        """Unsubscribe from a topic filter."""
        try:
            del self.subscriptions[topic_filter]
        except KeyError:
            logger.exception(f"Unsubscribe called for an untracked topic filter: {topic_filter}")
        # Unsubscribe from the server even if the topic filter was not tracked.
        self.session.unsubscribe(topic_filter)

    def on_message(self, session: Session, topic: str, payload: bytes, properties: MQTTPropertyDict) -> None:
        """Callback for when a message is received."""
        logger.debug(f"Received message on topic {topic}: {payload.hex()}")
        callbacks = [callbacks for filter, callbacks in self.subscriptions.items() if filter.match(topic)]
        for callback in callbacks:
            try:
                callback(self, topic, payload, properties)
            except Exception:
                logger.exception(f"Unhandled error in subscribe callback for topic: {topic}")
