from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, NamedTuple, TYPE_CHECKING
import weakref

from .packet import MQTTPublishPacket
from .topic_filter import match_topic_filter, validate_topic_filter, validate_share_name, join_share

if TYPE_CHECKING:
    from .client import Client  # pragma: no cover


SubscribeCallback = Callable[["Client", MQTTPublishPacket], None]


@dataclass(match_args=True, slots=True, frozen=True)
class SubscriptionHandle:
    """Represents a subscription to a topic filter with a callback."""
    topic_filter: str
    share_name: str | None
    callback: SubscribeCallback
    _client: weakref.ReferenceType[Client]

    def unsubscribe(self) -> None:
        """Unsubscribe from the topic filter."""
        client = self._client()
        if client is not None:
            client.unsubscribe(self.topic_filter, self.callback, self.share_name)


class SubscriptionId(NamedTuple):
    """Represents a subscription ID for a topic filter."""
    topic_filter: str
    share_name: str | None

    def __str__(self) -> str:
        return join_share(self.topic_filter, self.share_name)


@dataclass(match_args=True, slots=True)
class SubscriptionData:
    """Represents subscriptions to a topic filter with a callback."""
    max_qos: int = field(default=0, init=False)
    callbacks: set[SubscribeCallback] = field(init=False, default_factory=set)


class Subscriptions:
    """Container for MQTT subscriptions and their callbacks."""
    __slots__ = ("_subscriptions",)
    _subscriptions: dict[SubscriptionId, SubscriptionData]

    def __init__(self) -> None:
        self._subscriptions = {}

    def add(self, topic_filter: str, share_name: str | None, qos: int, callback: SubscribeCallback) -> None:
        """Add a subscription with a callback."""
        validate_topic_filter(topic_filter)
        if share_name is not None:
            validate_share_name(share_name)
        sub_id = SubscriptionId(topic_filter, share_name)
        if sub_id not in self._subscriptions:
            self._subscriptions[sub_id] = SubscriptionData()
        self._subscriptions[sub_id].callbacks.add(callback)
        self._subscriptions[sub_id].max_qos = max(
            self._subscriptions[sub_id].max_qos, qos
        )

    def remove(self, topic_filter: str, share_name: str | None, callback: SubscribeCallback) -> int:
        """Remove a callback from a subscription.
        
        Returns the number of remaining callbacks for the topic filter."""
        sub_id = SubscriptionId(topic_filter, share_name)
        if sub_id in self._subscriptions:
            self._subscriptions[sub_id].callbacks.discard(callback)
            remaining = len(self._subscriptions[sub_id].callbacks)
            if remaining == 0:
                del self._subscriptions[sub_id]
            return remaining
        else:
            return 0

    def remove_all(self, topic_filter: str, share_name: str | None) -> None:
        """Remove all callbacks from a subscription."""
        sub_id = SubscriptionId(topic_filter, share_name)
        if sub_id in self._subscriptions:
            del self._subscriptions[sub_id]

    def get_callbacks(self, topic: str) -> frozenset[SubscribeCallback]:
        """Get all callbacks for a given topic."""
        callbacks = set()
        for sub_id, subscription in self._subscriptions.items():
            if match_topic_filter(sub_id.topic_filter, topic):
                callbacks.update(subscription.callbacks)
        return frozenset(callbacks)

    def clear(self) -> None:
        """Clear all subscriptions."""
        self._subscriptions.clear()

    def get_topics(self) -> dict[SubscriptionId, int]:
        """Get a dictionary of all subscriptions and their max QoS.

        The keys are topic filters (including share names) and the values are the max QoS."""
        return {sub_id: subscription.max_qos for sub_id, subscription in self._subscriptions.items()}
