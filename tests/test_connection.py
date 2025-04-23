import pytest
import time
import weakref

from ohmqtt.connection import (
    Connection,
    ConnectionCloseCallback,
    ConnectionOpenCallback,
    ConnectionReadCallback,
)
from ohmqtt.error import MQTTError
from ohmqtt.mqtt_spec import MQTTReasonCode
from ohmqtt.packet import (
    MQTTConnAckPacket,
    MQTTPublishPacket,
    MQTTPingReqPacket,
    MQTTPingRespPacket,
)
from ohmqtt.socket_wrapper import SocketWrapper


class Callbacks:
    """Container for Connection callbacks."""
    def __init__(self, mocker):
        self.close_callback = mocker.Mock(spec=ConnectionCloseCallback)
        self.open_callback = mocker.Mock(spec=ConnectionOpenCallback)
        self.read_callback = mocker.Mock(spec=ConnectionReadCallback)


@pytest.fixture
def callbacks(mocker):
    return Callbacks(mocker)


def wait_for(callback, timeout=1.0):
    """Wait for a condition to be true, or raise a TimeoutError."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        if callback():
            return
        time.sleep(0.01)
    raise TimeoutError()


def test_connection_happy_path(callbacks, mocker):
    mock_socket_wrapper = mocker.Mock(spec=SocketWrapper)
    MockSocketWrapper = mocker.patch("ohmqtt.connection.SocketWrapper", return_value=mock_socket_wrapper)
    connection = Connection(
        "localhost",
        1883,
        close_callback=callbacks.close_callback,
        open_callback=callbacks.open_callback,
        read_callback=callbacks.read_callback,
        keepalive_interval=3,
        use_tls=True,
        tls_context=None,
        tls_hostname="localhost",
    )

    MockSocketWrapper.assert_called_once_with(
        "localhost",
        1883,
        close_callback=callbacks.close_callback,
        open_callback=callbacks.open_callback,
        read_callback=connection._read_packet,
        keepalive_interval=3,
        use_tls=True,
        tls_context=None,
        tls_hostname="localhost",
    )
    mock_socket_wrapper.start.assert_called_once()

    connection._open_callback()
    callbacks.open_callback.assert_called_once_with()
    callbacks.open_callback.reset_mock()

    connection.send(b"hello")
    mock_socket_wrapper.send.assert_called_once_with(b"hello")
    mock_socket_wrapper.send.reset_mock()

    # Receiving a CONNACK
    packet = MQTTConnAckPacket(
        session_present=False,
        reason_code=MQTTReasonCode.Success,
        properties={"ServerKeepAlive": 60},
    )
    connection._read_packet(packet.encode())
    callbacks.read_callback.assert_called_once_with(packet)
    callbacks.read_callback.reset_mock()
    mock_socket_wrapper.set_keepalive_interval.assert_called_once_with(60)
    mock_socket_wrapper.set_keepalive_interval.reset_mock()

    # Receiving a PINGREQ
    connection._read_packet(MQTTPingReqPacket().encode())
    mock_socket_wrapper.send.assert_called_once_with(MQTTPingRespPacket().encode())
    mock_socket_wrapper.send.reset_mock()

    # Receiving a PINGRESP
    connection._read_packet(MQTTPingRespPacket().encode())
    mock_socket_wrapper.pong_received.assert_called_once_with()
    mock_socket_wrapper.pong_received.reset_mock()

    # Receiving a PUBLISH
    packet = MQTTPublishPacket(
        topic="test/topic",
        payload=b"test_payload",
        qos=0,
    )
    connection._read_packet(packet.encode())
    callbacks.read_callback.assert_called_once_with(packet)
    callbacks.read_callback.reset_mock()

    connection.close()
    mock_socket_wrapper.close.assert_called_once_with()
    mock_socket_wrapper.close.reset_mock()
    connection._close_callback()
    callbacks.close_callback.assert_called_once_with()
    callbacks.close_callback.reset_mock()


def test_connection_partial_read(callbacks, mocker):
    mock_socket_wrapper = mocker.Mock(spec=SocketWrapper)
    mocker.patch("ohmqtt.connection.SocketWrapper", return_value=mock_socket_wrapper)
    connection = Connection(
        "localhost",
        1883,
        close_callback=callbacks.close_callback,
        open_callback=callbacks.open_callback,
        read_callback=callbacks.read_callback,
    )

    packet = MQTTPublishPacket(
        topic="test/topic",
        payload=b"x" * 255,  # Length of packet must be long enough that the length varint is split.
        qos=0,
    )
    encoded = packet.encode()

    for n in range(1, len(encoded)):
        connection._read_packet(encoded[:n])
        assert not callbacks.read_callback.called
        connection._read_packet(encoded[n:])
        callbacks.read_callback.assert_called_once_with(packet)
        callbacks.read_callback.reset_mock()


def test_connection_garbage_read(callbacks, mocker):
    mock_socket_wrapper = mocker.Mock(spec=SocketWrapper)
    mocker.patch("ohmqtt.connection.SocketWrapper", return_value=mock_socket_wrapper)
    connection = Connection(
        "localhost",
        1883,
        close_callback=callbacks.close_callback,
        open_callback=callbacks.open_callback,
        read_callback=callbacks.read_callback,
    )
    encoded = b"\xff\xff\xff\xff\xffThis is not a valid MQTT packet."
    with pytest.raises(MQTTError):
        connection._read_packet(encoded)


def test_connection_refs(mocker):
    """Test that the Connection class does not have internal circular references."""
    mock_socket_wrapper = mocker.Mock(spec=SocketWrapper)
    MockSocketWrapper = mocker.patch("ohmqtt.connection.SocketWrapper", return_value=mock_socket_wrapper)
    close_callback = lambda: None
    open_callback = lambda: None
    read_callback = lambda _: None
    connection = Connection(
        "localhost",
        1883,
        close_callback,
        open_callback,
        read_callback,
    )

    # Clear mock retained references to callbacks.
    MockSocketWrapper.reset_mock()
    mock_socket_wrapper.reset_mock()

    # Also check for internal circular references.
    ref = weakref.ref(connection)
    del connection
    assert ref() is None
