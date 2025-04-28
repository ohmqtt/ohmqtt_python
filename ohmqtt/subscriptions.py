from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, TYPE_CHECKING

from .message import MQTTMessage
from .topic_filter import MQTTTopicFilter

if TYPE_CHECKING:
    from .client import Client


SubscribeCallback = Callable[["Client", MQTTMessage], None]


@dataclass(match_args=True, slots=True)
class Subscription:
    """Represents subscriptions to a topic filter with a callback."""
    max_qos: int = field(default=0, init=False)
    callbacks: set[SubscribeCallback] = field(init=False, default_factory=set)


class Subscriptions:
    """Container for MQTT subscriptions and their callbacks."""
    __slots__ = ("_subscriptions",)
    _subscriptions: dict[MQTTTopicFilter, Subscription]

    def __init__(self) -> None:
        self._subscriptions = {}

    def add(self, topic_filter: str, qos: int, callback: SubscribeCallback) -> None:
        """Add a subscription with a callback."""
        filter_obj = MQTTTopicFilter(topic_filter)
        if filter_obj not in self._subscriptions:
            self._subscriptions[filter_obj] = Subscription()
        self._subscriptions[filter_obj].callbacks.add(callback)
        self._subscriptions[filter_obj].max_qos = max(
            self._subscriptions[filter_obj].max_qos, qos
        )

    def remove(self, topic_filter: str, callback: SubscribeCallback) -> int:
        """Remove a callback from a subscription.
        
        Returns the number of remaining callbacks for the topic filter."""
        filter_obj = MQTTTopicFilter(topic_filter)
        if filter_obj in self._subscriptions:
            self._subscriptions[filter_obj].callbacks.discard(callback)
            remaining = len(self._subscriptions[filter_obj].callbacks)
            if remaining == 0:
                del self._subscriptions[filter_obj]
            return remaining
        else:
            return 0

    def remove_all(self, topic_filter: str) -> None:
        """Remove all callbacks from a subscription."""
        filter_obj = MQTTTopicFilter(topic_filter)
        if filter_obj in self._subscriptions:
            del self._subscriptions[filter_obj]

    def get_callbacks(self, topic: str) -> frozenset[SubscribeCallback]:
        """Get all callbacks for a given topic."""
        callbacks = set()
        for filter_obj, subscription in self._subscriptions.items():
            if filter_obj.match(topic):
                callbacks.update(subscription.callbacks)
        return frozenset(callbacks)

    def clear(self) -> None:
        """Clear all subscriptions."""
        self._subscriptions.clear()

    def get_topics(self) -> dict[str, int]:
        """Get a dictionary of all subscriptions and their max QoS."""
        return {filter_obj.topic_filter: subscription.max_qos for filter_obj, subscription in self._subscriptions.items()}
