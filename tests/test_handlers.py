import pytest

from ohmqtt.connection.handlers import MessageHandlers
from ohmqtt.packet import MQTTPublishPacket


def test_handlers() -> None:
    handlers = MessageHandlers()
    calls = []
    def callback(packet: MQTTPublishPacket) -> None:
        calls.append(packet)
    class TestError(Exception):
        pass
    def broken_callback(packet: MQTTPublishPacket) -> None:
        raise TestError("This handler is broken")

    with pytest.raises(RuntimeError):
        handlers.get_handlers(MQTTPublishPacket)
    with pytest.raises(RuntimeError):
        handlers.register(MQTTPublishPacket, callback)
    with pytest.raises(RuntimeError):
        handlers.handle(MQTTPublishPacket())

    with handlers:
        handlers.register(MQTTPublishPacket, broken_callback)
        handlers.register(MQTTPublishPacket, callback)
    assert handlers.get_handlers(MQTTPublishPacket) == [broken_callback, callback]
    packet = MQTTPublishPacket(topic="test/topic", payload=b"test payload")
    exc_list = handlers.handle(packet)
    assert len(exc_list) == 1
    assert isinstance(exc_list[0], TestError)
    assert calls == [packet]

    with pytest.raises(RuntimeError):
        with handlers:
            pass
