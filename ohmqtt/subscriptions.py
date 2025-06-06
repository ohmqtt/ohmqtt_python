from __future__ import annotations

from collections import deque
from enum import IntEnum
from threading import Condition
from typing import Callable, Final, Iterable, NamedTuple, Sequence, TYPE_CHECKING
import weakref

from .connection import Connection, InvalidStateError, MessageHandlers
from .logger import get_logger
from .mqtt_spec import MAX_PACKET_ID
from .packet import (
    MQTTPublishPacket,
    MQTTSubscribePacket,
    MQTTSubAckPacket,
    MQTTUnsubscribePacket,
    MQTTUnsubAckPacket,
    MQTTConnAckPacket,
)
from .property import MQTTSubscribeProps, MQTTUnsubscribeProps
from .topic_alias import InboundTopicAlias
from .topic_filter import validate_topic_filter, validate_share_name, join_share, match_topic_filter

if TYPE_CHECKING:
    from .client import Client  # pragma: no cover

logger: Final = get_logger("subscriptions")

SubscribeCallback = Callable[["Client", MQTTPublishPacket], None]


class NoMatchingSubscriptionError(Exception):
    """Exception raised when no matching subscription is found when unsubscribing."""


class RetainPolicy(IntEnum):
    """Policy for broker sending retained messages upon a subscription.

    ALWAYS: Always send retained messages on subscription.

    ONCE: Only send retained messages on the first subscription to a topic.

    NEVER: Never send retained messages on subscription."""
    ALWAYS = 0
    ONCE = 1
    NEVER = 2


class SubscribeHandle:
    """Represents a subscription to a topic filter with a callback."""
    __slots__ = ("_data", "_subscriptions", "ack", "failed")

    ack: MQTTSubAckPacket | None
    failed: bool
    _data: Subscription
    _subscriptions: weakref.ReferenceType[Subscriptions]

    def __init__(self, data: Subscription, subscriptions: weakref.ReferenceType[Subscriptions]) -> None:
        self._data = data
        self._subscriptions = subscriptions
        self.ack = None
        self.failed = False

    def unsubscribe(self) -> UnsubscribeHandle | None:
        """Unsubscribe from the topic filter.

        Returns an UnsubscribeHandle if the operation was successful, otherwise None."""
        subscriptions = self._subscriptions()
        if subscriptions is not None:
            return subscriptions.unsubscribe(**(self._data._asdict()))
        return None

    def wait_for_ack(self, timeout: float | None = None) -> MQTTSubAckPacket | None:
        """Wait for the subscription acknowledgement.

        Returns the acknowledgement packet if the subscription was sent to the broker and acknowledged.

        Returns None if the subscription was not sent to the broker or timeout was reached."""
        subscriptions = self._subscriptions()
        if subscriptions is not None:
            return subscriptions.wait_for_suback(self, timeout=timeout)
        return None


class UnsubscribeHandle:
    """Represents an unsubscribe request which was sent to the broker."""
    __slots__ = ("_subscriptions", "ack", "failed")

    ack: MQTTUnsubAckPacket | None
    failed: bool
    _subscriptions: weakref.ReferenceType[Subscriptions]

    def __init__(self, subscriptions: weakref.ReferenceType[Subscriptions]) -> None:
        self.ack = None
        self.failed = False
        self._subscriptions = subscriptions

    def wait_for_ack(self, timeout: float | None = None) -> MQTTUnsubAckPacket | None:
        """Wait for the unsubscribe acknowledgement.

        Returns None if the unsubscribe was not sent to the broker or timeout was reached."""
        subscriptions = self._subscriptions()
        if subscriptions is not None:
            return subscriptions.wait_for_unsuback(self, timeout=timeout)
        return None


class Subscription(NamedTuple):
    """All the data about a request for subscription."""
    topic_filter: str
    max_qos: int
    share_name: str | None
    no_local: bool
    retain_as_published: bool
    retain_policy: RetainPolicy
    sub_id: int | None
    user_properties: tuple[tuple[str, str], ...] | None
    callback: SubscribeCallback

    def render(self) -> MQTTSubscribePacket:
        """Render the subscription into a SUBSCRIBE packet."""
        opts = self.max_qos | (self.retain_policy << 4) | (self.retain_as_published << 3) | (self.no_local << 2)
        props = MQTTSubscribeProps()
        if self.sub_id is not None:
            props.SubscriptionIdentifier = {self.sub_id}
        if self.user_properties is not None:
            props.UserProperty = self.user_properties
        topic_filter = join_share(self.topic_filter, self.share_name)
        return MQTTSubscribePacket(
            topics=[(topic_filter, opts)],
            properties=props,
        )


class Subscriptions:
    """Container for MQTT subscriptions and their callbacks."""
    __slots__ = (
        "__weakref__",
        "_client",
        "_cond",
        "_connection",
        "_next_sub_packet_id",
        "_next_unsub_packet_id",
        "_out_of_session",
        "_sub_handles",
        "_subscriptions",
        "_topic_alias",
        "_unsub_handles",
    )

    def __init__(
        self,
        handlers: MessageHandlers,
        connection: Connection,
        client: weakref.ReferenceType[Client],
    ) -> None:
        self._connection = connection
        self._client = client
        self._cond = Condition()
        self._topic_alias = InboundTopicAlias()
        self._subscriptions: dict[str, list[Subscription]] = {}
        self._sub_handles: dict[int, SubscribeHandle] = {}
        self._unsub_handles: dict[int, UnsubscribeHandle] = {}
        self._next_sub_packet_id = 1
        self._next_unsub_packet_id = 1
        self._out_of_session: deque[MQTTSubscribePacket | MQTTUnsubscribePacket] = deque()

        handlers.register(MQTTSubAckPacket, self.handle_suback)
        handlers.register(MQTTUnsubAckPacket, self.handle_unsuback)
        handlers.register(MQTTConnAckPacket, self.handle_connack)

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
        """Add a subscription with a callback.

        If successful, returns a SubscribeHandle which can be used to unsubscribe the callback.

        Returns None if the subscription was not sent to the broker."""
        validate_topic_filter(topic_filter)
        if share_name is not None:
            validate_share_name(share_name)
        sub = Subscription(
            topic_filter=topic_filter,
            max_qos=max_qos,
            share_name=share_name,
            no_local=no_local,
            retain_as_published=retain_as_published,
            retain_policy=retain_policy,
            sub_id=sub_id,
            user_properties=tuple(user_properties) if user_properties is not None else None,
            callback=callback,
        )
        with self._cond:
            if topic_filter not in self._subscriptions:
                self._subscriptions[topic_filter] = []
            self._subscriptions[topic_filter].append(sub)
            packet = sub.render()
            packet.packet_id = self._get_next_sub_packet_id()
            try:
                self._connection.send(packet)
            except InvalidStateError:
                logger.debug("Connection not ready, SUBSCRIBE not sent")
                self._out_of_session.append(packet)
                return None
            else:
                handle = SubscribeHandle(sub, weakref.ref(self))
                self._sub_handles[packet.packet_id] = handle
                return handle

    def _get_next_sub_packet_id(self) -> int:
        """Get the next SUBSCRIBE packet identifier."""
        sub_id = self._next_sub_packet_id
        self._next_sub_packet_id += 1
        if self._next_sub_packet_id > MAX_PACKET_ID:
            self._next_sub_packet_id = 1
        return sub_id

    def wait_for_suback(
        self,
        handle: SubscribeHandle,
        timeout: float | None = None,
    ) -> MQTTSubAckPacket | None:
        """Wait for a SUBACK for a SubscribeHandle.

        Returns the SUBACK packet if the subscription was sent to the broker and acknowledged.

        Returns None if the subscription was not sent to the broker or timeout was reached."""
        with self._cond:
            if self._cond.wait_for(lambda: handle.ack is not None or handle.failed, timeout=timeout):
                return handle.ack
            return None

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

        Returns an UnsubscribeHandle if the operation was successful, otherwise None."""
        with self._cond:
            if topic_filter not in self._subscriptions:
                # Nothing to unsubscribe.
                return None
            user_props = tuple(user_properties) if user_properties is not None else None
            matches = [
                sub for sub in self._subscriptions[topic_filter]
                if (
                    sub.share_name == share_name
                    and sub.callback == callback
                    and sub.max_qos == max_qos
                    and sub.no_local is no_local
                    and sub.retain_as_published is retain_as_published
                    and sub.retain_policy == retain_policy
                    and sub.sub_id == sub_id
                    and sub.user_properties == user_props
                )
            ]
            for match in matches:
                self._subscriptions[topic_filter].remove(match)
            remaining = len([sub for sub in self._subscriptions[topic_filter] if sub.share_name == share_name])
            if remaining > 0:
                # There are still subscriptions left for this topic filter.
                return None
            # Nothing left, send UNSUBSCRIBE.
            del self._subscriptions[topic_filter]
            packet = MQTTUnsubscribePacket(
                topics=[join_share(topic_filter, share_name)],
                packet_id=self._get_next_unsub_packet_id(),
                properties=MQTTUnsubscribeProps(),
            )
            if unsub_user_properties is not None:
                packet.properties.UserProperty = tuple(unsub_user_properties)
            try:
                self._connection.send(packet)
            except InvalidStateError:
                logger.debug("Connection closed, UNSUBSCRIBE not sent")
                self._out_of_session.append(packet)
                return None
            else:
                handle = UnsubscribeHandle(weakref.ref(self))
                self._unsub_handles[packet.packet_id] = handle
                return handle

    def _get_next_unsub_packet_id(self) -> int:
        """Get the next UNSUBSCRIBE packet identifier."""
        unsub_id = self._next_unsub_packet_id
        self._next_unsub_packet_id += 1
        if self._next_unsub_packet_id > MAX_PACKET_ID:
            self._next_unsub_packet_id = 1
        return unsub_id

    def wait_for_unsuback(
        self,
        handle: UnsubscribeHandle,
        timeout: float | None = None,
    ) -> MQTTUnsubAckPacket | None:
        """Wait for a UNSUBACK for an UnsubscribeHandle.

        Returns the UNSUBACK packet if the subscription was sent to the broker and acknowledged.

        Returns None if the subscription was not sent to the broker or timeout was reached."""
        with self._cond:
            if self._cond.wait_for(lambda: handle.ack is not None or handle.failed, timeout=timeout):
                return handle.ack
            return None

    def handle_publish(self, packet: MQTTPublishPacket) -> None:
        """Handle incoming PUBLISH packets."""
        with self._cond:
            client = self._client()
            if client is None:
                raise RuntimeError("Client went out of scope")
            self._topic_alias.handle(packet)
            sub_lists = [sub for tf, sub in self._subscriptions.items() if match_topic_filter(tf, packet.topic)]
            subs = [sub for sub_list in sub_lists for sub in sub_list]
            if packet.properties.SubscriptionIdentifier is not None:
                subs = [sub for sub in subs if sub.sub_id in packet.properties.SubscriptionIdentifier]
            for sub in subs:
                try:
                    sub.callback(client, packet)
                except Exception:  # noqa: PERF203
                    logger.exception("Unhandled exception in subscription callback")

    def handle_suback(self, packet: MQTTSubAckPacket) -> None:
        """Handle incoming SUBACK packets."""
        if any(True for code in packet.reason_codes if code.is_error()):
            errs = [hex(code) for code in packet.reason_codes if code.is_error()]
            logger.error("Errors found in SUBACK return: %s", errs)
        with self._cond:
            handle = self._sub_handles.pop(packet.packet_id, None)
            if handle is not None:
                handle.ack = packet
                self._cond.notify_all()

    def handle_unsuback(self, packet: MQTTUnsubAckPacket) -> None:
        """Handle incoming UNSUBACK packets."""
        if any(True for code in packet.reason_codes if code.is_error()):
            errs = [hex(code) for code in packet.reason_codes if code.is_error()]
            logger.error("Errors found in UNSUBACK return: %s", errs)
        with self._cond:
            handle = self._unsub_handles.pop(packet.packet_id, None)
            if handle is not None:
                handle.ack = packet
                self._cond.notify_all()

    def handle_connack(self, packet: MQTTConnAckPacket) -> None:
        """Handle incoming CONNACK packets."""
        with self._cond:
            self._reset_connection_states()
            if not packet.session_present:
                # Replay all known subscriptions.
                self._replay_session_not_present()
            else:
                # Replay only packets which were not sent.
                self._replay_session_present()

    def _reset_connection_states(self) -> None:
        """Reset all connection-level states."""
        with self._cond:
            self._next_sub_packet_id = 1
            self._next_unsub_packet_id = 1
            self._topic_alias.reset()
            for sub_handle in self._sub_handles.values():
                if sub_handle.ack is None:
                    sub_handle.failed = True
            self._sub_handles.clear()
            for unsub_handle in self._unsub_handles.values():
                if unsub_handle.ack is None:
                    unsub_handle.failed = True
            self._unsub_handles.clear()
            self._cond.notify_all()  # Should clear all waiting for handles.

    def _replay_session_not_present(self) -> None:
        """Replay all subscriptions."""
        with self._cond:
            self._out_of_session.clear()
            for topic_filter in self._subscriptions:
                for sub in self._subscriptions[topic_filter]:
                    sub_packet = sub.render()
                    sub_packet.packet_id = self._get_next_sub_packet_id()
                    self._connection.send(sub_packet)

    def _replay_session_present(self) -> None:
        """Replay all subscriptions which were not sent to the broker."""
        with self._cond:
            while self._out_of_session:
                packet = self._out_of_session.popleft()
                if isinstance(packet, MQTTSubscribePacket):
                    packet.packet_id = self._get_next_sub_packet_id()
                else:
                    packet.packet_id = self._get_next_unsub_packet_id()
                try:
                    self._connection.send(packet)
                except InvalidStateError:
                    logger.debug("Connection not ready, packet not sent")
                    self._out_of_session.appendleft(packet)
                    break
