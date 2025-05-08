import re


def validate_topic(topic: str) -> None:
    """Validate an MQTT topic name.

    Raises ValueError if the topic name is invalid."""
    if len(topic) == 0:
        raise ValueError("Topic name cannot be empty")
    if "\u0000" in topic:
        raise ValueError("Topic name cannot contain null characters")
    if "#" in topic or "+" in topic:
        raise ValueError("Topic name cannot contain wildcards '#' or '+'")
    if len(topic.encode("utf-8")) > 65535:
        raise ValueError("Topic name is too long (> 65535 bytes encoded as UTF-8)")


def validate_topic_filter(topic_filter: str) -> None:
    """Validate an MQTT topic filter.

    Raises ValueError if the topic filter is invalid."""
    if len(topic_filter) == 0:
        raise ValueError("Topic filter cannot be empty")
    if "\u0000" in topic_filter:
        raise ValueError("Topic filter cannot contain null characters")
    if len(topic_filter.encode("utf-8")) > 65535:
        raise ValueError("Topic filter is too long (> 65535 bytes encoded as UTF-8)")
    multi_level_wildcard_index = topic_filter.find("#")
    if multi_level_wildcard_index != -1:
        if multi_level_wildcard_index != len(topic_filter) - 1:
            raise ValueError("Multi-level wildcard '#' must be the last character in the topic filter")
        if len(topic_filter) > 1 and topic_filter[-2] != "/":
            raise ValueError("Multi-level wildcard '#' must be preceded by a '/' unless it is the only character in the topic filter")


def validate_share_name(share_name: str) -> None:
    """Validate an MQTT shared subscription name.

    Raises ValueError if the share name is invalid."""
    if len(share_name) == 0:
        raise ValueError("Share name cannot be empty")
    if "\u0000" in share_name:
        raise ValueError("Share name cannot contain null characters")
    if re.search(r"[+#/]", share_name):
        raise ValueError("Share name cannot contain characters '#' or '+' or '/'")
    if len(share_name.encode("utf-8")) > 65535:
        raise ValueError("Share name is too long (> 65535 bytes encoded as UTF-8)")


def _check_multi_level_wildcard(topic_filter: str, topic: str) -> bool:
    """Check if the topic matches the filter with a multi-level wildcard."""
    if len(topic_filter) == 1 and not topic.startswith("$"):
        return True  # Matches everything that doesn't start with '$'.
    # Otherwise the # must come at the end of the filter.
    # Match the path up to the last '/' in the filter.
    base = topic_filter[:-2]
    if base and topic.startswith(base):
        return True
    return False


def _check_single_level_wildcard(topic_filter: str, topic: str) -> bool:
    if topic_filter.startswith("+") and topic.startswith("$"):
        return False
    filter_levels = topic_filter.split("/")
    topic_levels = topic.split("/")
    if len(filter_levels) != len(topic_levels):
        return False
    for filter_level, topic_level in zip(filter_levels, topic_levels):
        if filter_level != "+" and filter_level != topic_level:
            return False
    return True


def match_topic_filter(topic_filter: str, topic: str) -> bool:
    """Check if the topic matches the filter.

    This method will validate the topic, but assumes that the filter is already validated."""
    validate_topic(topic)
    if topic_filter == topic:
        return True
    if "#" in topic_filter and _check_multi_level_wildcard(topic_filter, topic):
        return True
    if "+" in topic_filter and _check_single_level_wildcard(topic_filter, topic):
        return True
    return False


def join_share(topic_filter: str, share_name: str | None) -> str:
    """Join a topic filter with a share name."""
    return f"$share/{share_name}/{topic_filter}" if share_name is not None else topic_filter
