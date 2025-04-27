from __future__ import annotations

from dataclasses import dataclass
import ssl
import threading
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
    __slots__ = (
        "client_id",
        "session",
        "subscriptions",
        "_is_connected",
    )
    client_id: str
    keepalive_interval: int
    session: Session
    subscriptions: Subscriptions

    def __init__(self, client_id: str = "") -> None:
        self.client_id = client_id
        self.subscriptions = Subscriptions()
        self.session = Session(
            client_id,
            close_callback=self._handle_close,
            message_callback=self._handle_message,
            open_callback=self._handle_open,
        )
        self._is_connected = threading.Event()

    @property
    def is_connected(self) -> bool:
        """Check if the client is connected to the broker."""
        return self._is_connected.is_set()

    def connect(
        self,
        host: str,
        port: int,
        *,
        reconnect_delay: float = 0.0,
        keepalive_interval: int = 0,
        tcp_nodelay: bool = True,
        use_tls: bool = False,
        tls_context: ssl.SSLContext | None = None,
        tls_hostname: str = "",
        connect_properties: MQTTPropertyDict | None = None,
    ) -> None:
        """Connect to the broker."""
        self.session.connect(
            host,
            port,
            reconnect_delay=reconnect_delay,
            keepalive_interval=keepalive_interval,
            tcp_nodelay=tcp_nodelay,
            use_tls=use_tls,
            tls_context=tls_context,
            tls_hostname=tls_hostname,
            connect_properties=connect_properties,
        )

    def disconnect(self) -> None:
        """Disconnect from the broker."""
        self.session.disconnect()

    def shutdown(self) -> None:
        """Shutdown the client and close the connection."""
        self.session.shutdown()

    def wait_for_connect(self, timeout: float | None = None) -> None:
        """Wait for the client to connect to the broker.

        Raises TimeoutError if the timeout is exceeded."""
        if not self._is_connected.wait(timeout):
            raise TimeoutError("Connection timed out")

    def wait_for_disconnect(self, timeout: float | None = None) -> None:
        """Wait for the client to disconnect from the broker.

        Raises TimeoutError if the timeout is exceeded."""
        self.session.wait_for_disconnect(timeout)

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

    def _handle_message(self, packet: MQTTPublishPacket) -> None:
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

    def _handle_open(self) -> None:
        """Callback for when the connection is opened."""
        self._is_connected.set()

    def _handle_close(self) -> None:
        """Callback for when the connection is closed."""
        self._is_connected.clear()
        self.subscriptions.clear()
