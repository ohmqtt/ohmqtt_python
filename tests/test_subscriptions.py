from ohmqtt.subscriptions import Subscriptions, SubscriptionId


def test_subscriptions():
    callback1 = lambda: None
    callback2 = lambda: None
    callback3 = lambda: None

    subscriptions = Subscriptions()
    subscriptions.add("test/topic", None, 0, callback1)
    subscriptions.add("test/topic", None, 1, callback2)
    subscriptions.add("test/topic2", None, 2, callback3)
    subscriptions.add("#", None, 0, callback1)
    subscriptions.add("very", "cherry", 0, callback1)

    topics = subscriptions.get_topics()
    assert topics == {
        SubscriptionId("test/topic", None): 1,
        SubscriptionId("test/topic2", None): 2,
        SubscriptionId("#", None): 0,
        SubscriptionId("very", "cherry"): 0,
    }
    topic_strs = [str(topic) for topic in topics.keys()]
    assert topic_strs[0] == "test/topic"
    assert topic_strs[3] == "$share/cherry/very"

    callbacks = subscriptions.get_callbacks("test/topic")
    assert len(callbacks) == 2
    assert callback1 in callbacks
    assert callback2 in callbacks

    callbacks = subscriptions.get_callbacks("test/topic2")
    assert len(callbacks) == 2
    assert callback1 in callbacks
    assert callback3 in callbacks

    callbacks = subscriptions.get_callbacks("test/topic3")
    assert len(callbacks) == 1
    assert callback1 in callbacks

    assert subscriptions.remove("#", None, callback3) == 1
    assert subscriptions.remove("#", None, callback2) == 1
    assert subscriptions.remove("#", None, callback1) == 0
    assert subscriptions.get_callbacks("test/topic3") == frozenset()
    assert subscriptions.remove("#", None, callback1) == 0

    subscriptions.remove_all("test/topic", None)
    assert subscriptions.get_callbacks("test/topic") == frozenset()

    subscriptions.add("test/topic", None, 0, callback1)
    subscriptions.clear()
    assert subscriptions.get_topics() == {}

    assert not hasattr(subscriptions, "__dict__")
    assert all(hasattr(subscriptions, attr) for attr in subscriptions.__slots__)
