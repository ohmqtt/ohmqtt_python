from __future__ import annotations

import ssl
from types import TracebackType
from typing import Final, Sequence

from .connection import ConnectParams
from .logger import get_logger
from .message import MQTTMessage
from .mqtt_spec import MQTTReasonCode
from .packet import MQTTPublishPacket
from .property import MQTTPropertyDict
from .persistence.base import PublishHandle
from .session import Session
from .subscriptions import Subscriptions, SubscribeCallback, SubscriptionHandle

logger: Final = get_logger("client")


class Client:
    """High level interface for the MQTT client."""
    __slots__ = ("session", "subscriptions")

    def __init__(self, db_path: str = "") -> None:
        self.subscriptions = Subscriptions()
        self.session = Session(
            auth_callback=self._handle_auth,
            close_callback=self._handle_close,
            message_callback=self._handle_message,
            open_callback=self._handle_open,
            db_path=db_path,
        )

    def __enter__(self) -> Client:
        self.start_loop()
        return self

    def __exit__(self, exc_type: type[BaseException], exc_val: BaseException, tb: TracebackType) -> None:
        self.shutdown()

    def start_loop(self) -> None:
        """Start the MQTT client loop.

        This will block until the client is stopped or shutdown.
        """
        self.session.connection.start_loop()

    def loop_forever(self) -> None:
        """Run the MQTT client loop.

        This will run until the client is stopped or shutdown.
        """
        self.session.connection.loop_forever()

    @property
    def is_connected(self) -> bool:
        """Check if the client is connected to the broker."""
        return self.session.connection.is_connected()

    def connect(
        self,
        host: str,
        port: int,
        *,
        client_id: str = "",
        reconnect_delay: float = 0.0,
        keepalive_interval: int = 0,
        tcp_nodelay: bool = True,
        use_tls: bool = False,
        tls_context: ssl.SSLContext | None = None,
        tls_hostname: str = "",
        connect_properties: MQTTPropertyDict | None = None,
    ) -> None:
        """Connect to the broker."""
        self.session.connect(ConnectParams(
            host,
            port,
            client_id=client_id,
            reconnect_delay=reconnect_delay,
            keepalive_interval=keepalive_interval,
            tcp_nodelay=tcp_nodelay,
            use_tls=use_tls,
            tls_context=tls_context if tls_context is not None else ssl.create_default_context(),
            tls_hostname=tls_hostname,
            connect_properties=connect_properties if connect_properties is not None else MQTTPropertyDict(),
        ))

    def disconnect(self) -> None:
        """Disconnect from the broker."""
        self.session.disconnect()

    def shutdown(self) -> None:
        """Shutdown the client and close the connection."""
        self.session.shutdown()

    def wait_for_connect(self, timeout: float | None = None) -> None:
        """Wait for the client to connect to the broker.

        Raises TimeoutError if the timeout is exceeded."""
        self.session.connection.wait_for_connect(timeout)

    def wait_for_disconnect(self, timeout: float | None = None) -> None:
        """Wait for the client to disconnect from the broker.

        Raises TimeoutError if the timeout is exceeded."""
        self.session.connection.wait_for_disconnect(timeout)

    def publish(
        self,
        topic: str,
        payload: bytes,
        *,
        qos: int = 0,
        retain: bool = False,
        properties: MQTTPropertyDict | None = None,
    ) -> PublishHandle:
        """Publish a message to a topic."""
        return self.session.publish(topic, payload, qos=qos, retain=retain, properties=properties)
    
    def subscribe(
        self,
        topic_filter: str,
        callback: SubscribeCallback,
        qos: int = 2,
        properties: MQTTPropertyDict | None = None,
    ) -> SubscriptionHandle:
        """Subscribe to a topic filter with a callback."""
        self.subscriptions.add(topic_filter, qos, callback)
        if self.is_connected:
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

    def auth(
        self,
        *,
        authentication_method: str | None = None,
        authentication_data: bytes | None = None,
        reason_string: str | None = None,
        user_properties: Sequence[tuple[str, str]] | None = None,
        reason_code: int = MQTTReasonCode.Success,
    ) -> None:
        """Send an AUTH packet to the broker."""
        self.session.auth(
            reason_code=reason_code,
            authentication_method=authentication_method,
            authentication_data=authentication_data,
            reason_string=reason_string,
            user_properties=user_properties,
        )

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
                callback(self, message)
            except Exception:
                logger.exception(f"Unhandled error in subscribe callback: {callback} for topic: {message.topic}")

    def _handle_open(self) -> None:
        """Callback for when the connection is opened."""
        for topic_filter, qos in self.subscriptions.get_topics().items():
            self.session.subscribe(topic_filter, qos=qos)

    def _handle_close(self) -> None:
        """Callback for when the connection is closed."""
        self.subscriptions.clear()

    def _handle_auth(
        self,
        reason_code: int,
        authentication_method: str | None,
        authentication_data: bytes | None,
        reason_string: str | None,
        user_properties: Sequence[tuple[str, str]] | None,
    ) -> None:
        """Callback for an AUTH packet from the broker."""
        logger.debug(
            f"Received AUTH packet: {reason_code=}, {authentication_method=}, {authentication_data=}, {reason_string=}, {user_properties=}")
