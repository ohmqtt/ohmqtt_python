from ohmqtt.subscriptions import Subscriptions


def test_subscriptions():
    callback1 = lambda: None
    callback2 = lambda: None
    callback3 = lambda: None

    subscriptions = Subscriptions()
    subscriptions.add("test/topic", callback1)
    subscriptions.add("test/topic", callback2)
    subscriptions.add("test/topic2", callback3)
    subscriptions.add("#", callback1)

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

    assert subscriptions.remove("#", callback3) == 1
    assert subscriptions.remove("#", callback2) == 1
    assert subscriptions.remove("#", callback1) == 0
    assert subscriptions.get_callbacks("test/topic3") == frozenset()
    assert subscriptions.remove("#", callback1) == 0

    subscriptions.remove_all("test/topic")
    assert subscriptions.get_callbacks("test/topic") == frozenset()

