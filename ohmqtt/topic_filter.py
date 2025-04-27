from dataclasses import dataclass


@dataclass(match_args=True, slots=True, frozen=True)
class MQTTTopicFilter:
    """A topic filter for MQTT."""
    topic_filter: str

    def __post_init__(self) -> None:
        if len(self.topic_filter) == 0:
            raise ValueError("Topic filter cannot be empty")
        if "\u0000" in self.topic_filter:
            raise ValueError("Topic filter cannot contain null characters")
        if len(self.topic_filter.encode("utf-8")) > 65535:
            raise ValueError("Topic filter is too long (> 65535 bytes encoded as UTF-8)")
        multi_level_wildcard_index = self.topic_filter.find("#")
        if multi_level_wildcard_index != -1:
            if multi_level_wildcard_index != len(self.topic_filter) - 1:
                raise ValueError("Multi-level wildcard '#' must be the last character in the topic filter")
            if len(self.topic_filter) > 1 and self.topic_filter[-2] != "/":
                raise ValueError("Multi-level wildcard '#' must be preceded by a '/' unless it is the only character in the topic filter")

    def match(self, topic: str) -> bool:
        """Check if the topic matches the filter."""
        if len(topic) == 0:
            raise ValueError("Topic name cannot be empty")
        if "\u0000" in topic:
            raise ValueError("Topic name cannot contain null characters")
        if "#" in topic or "+" in topic:
            raise ValueError("Topic name cannot contain wildcards '#' or '+'")
        if len(topic.encode("utf-8")) > 65535:
            raise ValueError("Topic name is too long (> 65535 bytes encoded as UTF-8)")
        if self.topic_filter == topic:
            return True  # Exact match.
        hidden_topic = topic.startswith("$")
        if self.topic_filter == "#" and not hidden_topic:
            return True  # Matches everything that doesn't start with '$'.
        if "#" in self.topic_filter:
            base = self.topic_filter[:-2]
            if base and topic.startswith(base):
                return True  # Matches everything under the base topic.
            return False
        if "+" in self.topic_filter:
            if self.topic_filter.startswith("+") and hidden_topic:
                return False
            filter_levels = self.topic_filter.split("/")
            topic_levels = topic.split("/")
            if len(filter_levels) != len(topic_levels):
                return False
            for filter_level, topic_level in zip(filter_levels, topic_levels):
                if filter_level != "+" and filter_level != topic_level:
                    return False
            return True
        return False
