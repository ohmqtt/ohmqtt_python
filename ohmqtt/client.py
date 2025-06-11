from __future__ import annotations

import ssl
import threading
from typing import Final, Iterable, Sequence
import weakref

from .connection import Address, ConnectParams, Connection, MessageHandlers
from .logger import get_logger
from .mqtt_spec import MQTTReasonCode
from .packet import MQTTAuthPacket
from .property import MQTTAuthProps, MQTTConnectProps, MQTTPublishProps, MQTTWillProps
from .persistence.base import PublishHandle
from .session import Session
from .subscriptions import Subscriptions, SubscribeCallback, SubscribeHandle, UnsubscribeHandle, RetainPolicy
from .topic_alias import AliasPolicy

logger: Final = get_logger("client")


class Client:
    """High level interface for the MQTT client.

    :param db_path: Path to the database file for persistence.
    :param db_fast: If True, use a faster database implementation (e.g. SQLite WAL).
    """
    __slots__ = (
        "__weakref__",
        "_thread",
        "connection",
        "session",
    )

    def __init__(self, db_path: str = "", *, db_fast: bool = False) -> None:
        self._thread: threading.Thread | None = None
        message_handlers = MessageHandlers()
        with message_handlers as handlers:
            self.connection = Connection(handlers)
            subscriptions = Subscriptions(handlers, self.connection, weakref.ref(self))
            self.session = Session(handlers, subscriptions, self.connection, db_path=db_path, db_fast=db_fast)
            handlers.register(MQTTAuthPacket, self.handle_auth)

    def __enter__(self) -> Client:
        self.start_loop()
        return self

    def __exit__(self, *args: object) -> None:
        self.shutdown()

    def connect(
        self,
        address: str,
        *,
        client_id: str = "",
        clean_start: bool = False,
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
        """Connect to the broker.

        :param address: The address of the broker to connect to.
        :param client_id: The client ID to use for the connection, or empty string to request one from the broker.
        :param clean_start: If True, an existing session will not be resumed.
        :param connect_timeout: Timeout for the connection attempt in seconds, or None for no timeout.
        :param reconnect_delay: Delay in seconds before attempting to reconnect after a disconnection.
        :param keepalive_interval: The keep alive interval in seconds, or 0 to disable keep alive.
        :param tcp_nodelay: If True, enable TCP_NODELAY to disable Nagle's algorithm.
        :param tls_context: An SSLContext for TLS connections, or None to use the default.
        :param tls_hostname: The hostname to use for TLS connections, or empty string to determine from the address.
        :param will_topic: The topic for the Will message, or empty string to disable.
        :param will_payload: The payload for the Will message.
        :param will_qos: The QoS level for the Will message (0, 1, or 2).
        :param will_retain: If True, the Will message will be retained.
        :param will_properties: Properties for the Will message.
        :param connect_properties: Properties for the CONNECT packet.
        """
        _address = Address(address)
        params = ConnectParams(
            address=_address,
            client_id=client_id,
            clean_start=clean_start,
            connect_timeout=connect_timeout,
            reconnect_delay=reconnect_delay,
            keepalive_interval=keepalive_interval,
            tcp_nodelay=tcp_nodelay,
            tls_context=tls_context,
            tls_hostname=tls_hostname,
            will_topic=will_topic,
            will_payload=will_payload,
            will_qos=will_qos,
            will_retain=will_retain,
            will_properties=will_properties if will_properties is not None else MQTTWillProps(),
            connect_properties=connect_properties if connect_properties is not None else MQTTConnectProps(),
        )
        self.session.set_params(params)
        self.connection.connect(params)

    def disconnect(self) -> None:
        """Disconnect from the broker."""
        self.connection.disconnect()

    def shutdown(self) -> None:
        """Shutdown the client and close the connection."""
        self.connection.shutdown()

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
        """Publish a message to a topic.

        :param topic: The topic to publish to.
        :param payload: The payload of the message.
        :param qos: The QoS level for the message (0, 1, or 2).
        :param retain: If True, the message will be retained by the broker.
        :param properties: Properties for the PUBLISH packet.
        :param alias_policy: The policy for using automatic topic aliases.
        """
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
        max_qos: int = 2,
        *,
        share_name: str | None = None,
        no_local: bool = False,
        retain_as_published: bool = False,
        retain_policy: RetainPolicy = RetainPolicy.ALWAYS,
        sub_id: int | None = None,
        user_properties: Sequence[tuple[str, str]] | None = None,
    ) -> SubscribeHandle | None:
        """Subscribe to a topic filter with a callback.

        If the client is connected, returns a handle which can be used to unsubscribe from the topic filter
        or wait for the subscription to be acknowledged.

        If the client is not connected, returns None.

        :param topic_filter: The topic filter to subscribe to.
        :param callback: The callback to call when a message is received on the subscribed topic.
        :param max_qos: The maximum QoS level for the subscription (0, 1, or 2).
        :param share_name: The name of a shared subscription to use.
        :param no_local: If True, do not receive messages published by this client.
        :param retain_as_published: If True, the retain flag of messages will match the original message.
        :param retain_policy: The policy for retained messages.
        :param sub_id: An optional subscription ID for the subscription.
        :param user_properties: Optional user properties to include in the subscription request.
        """
        return self.session.subscriptions.subscribe(
            topic_filter,
            callback,
            max_qos=max_qos,
            share_name=share_name,
            no_local=no_local,
            retain_as_published=retain_as_published,
            retain_policy=retain_policy,
            sub_id=sub_id,
            user_properties=user_properties,
        )

    def unsubscribe(
        # This method must have the same signature as the subscribe method.
        # This lets us match the unsubscribe to the subscribe with the same args.
        self,
        topic_filter: str,
        callback: SubscribeCallback,
        max_qos: int = 2,
        *,
        share_name: str | None = None,
        no_local: bool = False,
        retain_as_published: bool = False,
        retain_policy: RetainPolicy = RetainPolicy.ALWAYS,
        sub_id: int | None = None,
        user_properties: Iterable[tuple[str, str]] | None = None,
        unsub_user_properties: Iterable[tuple[str, str]] | None = None,
    ) -> UnsubscribeHandle | None:
        """Unsubscribe from a topic filter.

        If the client is connected, returns a handle which can be used to wait for the unsubscription to be acknowledged.

        If the client is not connected, returns None.

        :param topic_filter: The topic filter to unsubscribe from.
        :param callback: The callback that was used to subscribe to the topic filter.
        :param max_qos: The maximum QoS level for the unsubscription (0, 1, or 2).
        :param share_name: The name of a shared subscription to use.
        :param no_local: If True, do not receive messages published by this client.
        :param retain_as_published: If True, the retain flag of messages will match the original message.
        :param retain_policy: The policy for retained messages.
        :param sub_id: An optional subscription ID for the unsubscription.
        :param user_properties: Optional user properties which were included in the subscription request.
        :param unsub_user_properties: Optional user properties to include in the unsubscription request.
        """
        return self.session.subscriptions.unsubscribe(
            topic_filter,
            callback,
            max_qos=max_qos,
            share_name=share_name,
            no_local=no_local,
            retain_as_published=retain_as_published,
            retain_policy=retain_policy,
            sub_id=sub_id,
            user_properties=user_properties,
            unsub_user_properties=unsub_user_properties,
        )

    def auth(
        self,
        *,
        authentication_method: str | None = None,
        authentication_data: bytes | None = None,
        reason_string: str | None = None,
        user_properties: Sequence[tuple[str, str]] | None = None,
        reason_code: MQTTReasonCode = MQTTReasonCode.Success,
    ) -> None:
        """Send an AUTH packet to the broker.

        :param authentication_method: The authentication method to use.
        :param authentication_data: Authentication data to send.
        :param reason_string: A reason string to include in the AUTH packet.
        :param user_properties: Optional user properties to include in the AUTH packet.
        :param reason_code: The reason code for the AUTH packet.
        """
        properties = MQTTAuthProps()
        if authentication_method is not None:
            properties.AuthenticationMethod = authentication_method
        if authentication_data is not None:
            properties.AuthenticationData = authentication_data
        if reason_string is not None:
            properties.ReasonString = reason_string
        if user_properties is not None:
            properties.UserProperty = user_properties
        packet = MQTTAuthPacket(
            reason_code=reason_code,
            properties=properties,
        )
        self.connection.send(packet)

    def wait_for_connect(self, timeout: float | None = None) -> None:
        """Wait for the client to connect to the broker.

        :raises TimeoutError: The timeout was exceeded."""
        if not self.connection.wait_for_connect(timeout):
            raise TimeoutError("Waiting for connection timed out")

    def wait_for_disconnect(self, timeout: float | None = None) -> None:
        """Wait for the client to disconnect from the broker.

        :raises TimeoutError: The timeout was exceeded."""
        if not self.connection.wait_for_disconnect(timeout):
            raise TimeoutError("Waiting for disconnection timed out")

    def wait_for_shutdown(self, timeout: float | None = None) -> None:
        """Wait for the client to disconnect and finalize.

        :raises TimeoutError: The timeout was exceeded."""
        if not self.connection.wait_for_shutdown(timeout):
            raise TimeoutError("Waiting for disconnection timed out")

    def start_loop(self) -> None:
        """Start the client state machine in a separate thread."""
        if self._thread is not None:
            raise RuntimeError("Connection loop already started")
        self._thread = threading.Thread(target=self.loop_forever, daemon=True)
        self._thread.start()

    def loop_once(self, max_wait: float | None = 0.0) -> None:
        """Run a single iteration of the MQTT client loop.

        If max_wait is 0.0 (the default), this call will not block.

        If max_wait is None, this call will block until the next event.

        Any other numeric max_wait value may block for maximum that amount of time in seconds."""
        self.connection.loop_once(max_wait)

    def loop_forever(self) -> None:
        """Run the MQTT client loop.

        This will run until the client is stopped or shutdown.
        """
        self.connection.loop_forever()

    def loop_until_connected(self, timeout: float | None = None) -> bool:
        """Run the MQTT client loop until the client is connected to the broker.

        If a timeout is provided, the loop will give up after that amount of time.

        Returns True if the client is connected, False if the timeout was reached."""
        return self.connection.loop_until_connected(timeout)

    def is_connected(self) -> bool:
        """Check if the client is connected to the broker."""
        return self.connection.is_connected()

    def handle_auth(self, packet: MQTTAuthPacket) -> None:
        """Callback for an AUTH packet from the broker."""
        logger.debug("Got an AUTH packet")
