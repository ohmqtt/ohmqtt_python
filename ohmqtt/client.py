from __future__ import annotations

import ssl
from typing import Final, Sequence
import weakref

from .topic_alias import AliasPolicy
from .connection import Address, ConnectParams
from .logger import get_logger
from .mqtt_spec import MQTTReasonCode
from .packet import MQTTConnAckPacket, MQTTPublishPacket
from .property import MQTTConnectProps, MQTTPublishProps, MQTTSubscribeProps, MQTTWillProps
from .persistence.base import PublishHandle
from .session import Session
from .subscriptions import Subscriptions, SubscribeCallback, SubscriptionHandle

logger: Final = get_logger("client")


class Client:
    """High level interface for the MQTT client."""
    __slots__ = ("session", "subscriptions", "__weakref__")

    def __init__(self, db_path: str = "", *, db_fast: bool = False) -> None:
        self.subscriptions = Subscriptions()
        self.session = Session(
            auth_callback=self._handle_auth,
            close_callback=self._handle_close,
            message_callback=self._handle_message,
            open_callback=self._handle_open,
            db_path=db_path,
            db_fast=db_fast,
        )

    def __enter__(self) -> Client:
        self.start_loop()
        return self

    def __exit__(self, *args: object) -> None:
        self.shutdown()

    def start_loop(self) -> None:
        """Start the MQTT client loop.

        This will block until the client is stopped or shutdown.
        """
        self.session.connection.start_loop()

    def loop_once(self) -> None:
        """Run a single iteration of the MQTT client loop, without blocking."""
        self.session.connection.loop_once()

    def loop_forever(self) -> None:
        """Run the MQTT client loop.

        This will run until the client is stopped or shutdown.
        """
        self.session.connection.loop_forever()

    def loop_until_connected(self) -> None:
        """Run the MQTT client loop until the client is connected to the broker."""
        self.session.connection.loop_until_connected()

    def is_connected(self) -> bool:
        """Check if the client is connected to the broker."""
        return self.session.connection.is_connected()

    def connect(
        self,
        address: str,
        *,
        client_id: str = "",
        connect_timeout: float | None = None,
        reconnect_delay: int = 0,
        keepalive_interval: int = 0,
        tcp_nodelay: bool = True,
        tls_context: ssl.SSLContext | None = None,
        tls_hostname: str = "",
        will_topic: str = "",
        will_payload: bytes = b"",
        will_qos: int = 0,
        will_retain: bool = False,
        will_properties: MQTTWillProps | None = None,
        connect_properties: MQTTConnectProps | None = None,
    ) -> None:
        """Connect to the broker."""
        _address = Address(address)
        self.session.connect(ConnectParams(
            address=_address,
            client_id=client_id,
            connect_timeout=connect_timeout,
            reconnect_delay=reconnect_delay,
            keepalive_interval=keepalive_interval,
            tcp_nodelay=tcp_nodelay,
            tls_context=tls_context if tls_context is not None else ssl.create_default_context(),
            tls_hostname=tls_hostname,
            will_topic=will_topic,
            will_payload=will_payload,
            will_qos=will_qos,
            will_retain=will_retain,
            will_properties=will_properties if will_properties is not None else MQTTWillProps(),
            connect_properties=connect_properties if connect_properties is not None else MQTTConnectProps(),
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
        if not self.session.connection.wait_for_connect(timeout):
            raise TimeoutError("Waiting for connection timed out")

    def wait_for_disconnect(self, timeout: float | None = None) -> None:
        """Wait for the client to disconnect from the broker.

        Raises TimeoutError if the timeout is exceeded."""
        if not self.session.connection.wait_for_disconnect(timeout):
            raise TimeoutError("Waiting for disconnection timed out")

    def publish(
        self,
        topic: str,
        payload: bytes,
        *,
        qos: int = 0,
        retain: bool = False,
        properties: MQTTPublishProps | None = None,
        alias_policy: AliasPolicy = AliasPolicy.NEVER,
    ) -> PublishHandle:
        """Publish a message to a topic."""
        properties = properties if properties is not None else None
        return self.session.publish(
            topic,
            payload,
            qos=qos,
            retain=retain,
            properties=properties,
            alias_policy=alias_policy,
        )
    
    def subscribe(
        self,
        topic_filter: str,
        callback: SubscribeCallback,
        qos: int = 2,
        properties: MQTTSubscribeProps | None = None,
        share_name: str | None = None,
    ) -> SubscriptionHandle:
        """Subscribe to a topic filter with a callback."""
        with self.subscriptions as subscriptions:
            subscriptions.add(topic_filter, share_name, qos, callback)
        self.session.subscribe(topic_filter, share_name, qos, properties)
        return SubscriptionHandle(
            topic_filter=topic_filter,
            share_name=share_name,
            callback=callback,
            _client=weakref.ref(self),
        )

    def unsubscribe(
        self,
        topic_filter: str,
        callback: SubscribeCallback | None = None,
        share_name: str | None = None,
    ) -> None:
        """Unsubscribe from a topic filter.

        If a callback is provided it will be removed, otherwise all callbacks for the topic filter will be removed."""
        remaining = 0
        with self.subscriptions as subscriptions:
            if callback is None:
                subscriptions.remove_all(topic_filter, share_name)
            else:
                remaining = subscriptions.remove(topic_filter, share_name, callback)
        if remaining == 0:
            self.session.unsubscribe(topic_filter, share_name)

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
        with self.subscriptions as subscriptions:
            callbacks = subscriptions.get_callbacks(packet.topic)
        if not callbacks:
            logger.error(f"No callbacks for topic: {packet.topic}")
            return
        for callback in callbacks:
            try:
                callback(self, packet)
            except Exception:
                logger.exception(f"Unhandled error in subscribe callback: {callback} for topic: {packet.topic}")

    def _handle_open(self, connack: MQTTConnAckPacket) -> None:
        """Callback for when the connection is opened."""
        with self.subscriptions as subscriptions:
            for sub_id, qos in subscriptions.get_topics().items():
                self.session.subscribe(sub_id.topic_filter, sub_id.share_name, qos=qos)

    def _handle_close(self) -> None:
        """Callback for when the connection is closed."""
        with self.subscriptions as subscriptions:
            subscriptions.clear()

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
