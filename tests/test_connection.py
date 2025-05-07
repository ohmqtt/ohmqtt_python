import pytest

from ohmqtt.connection import (
    Address,
    Connection,
    ConnectParams,
    MessageHandlers,
    InvalidStateError,
)
from ohmqtt.connection.fsm import FSM
from ohmqtt.connection.states import (
    ConnectingState,
    ConnectedState,
    ClosingState,
    ClosedState,
    ReconnectWaitState,
    ShutdownState,
)
from ohmqtt.error import MQTTError
from ohmqtt.mqtt_spec import MQTTReasonCode
from ohmqtt.packet import (
    MQTTPublishPacket,
)


@pytest.fixture
def mock_handlers(mocker):
    return mocker.Mock(spec=MessageHandlers)


@pytest.fixture
def mock_fsm(mocker):
    mock_fsm = mocker.Mock(spec=FSM)
    mock_fsm.lock = mocker.MagicMock()
    mock_fsm.lock.__enter__.return_value = mock_fsm.lock
    mocker.patch("ohmqtt.connection.FSM", return_value=mock_fsm)
    yield mock_fsm


def test_connection_handle_packet(mocker):
    handlers = MessageHandlers()
    with handlers:
        handlers.register(MQTTPublishPacket, mocker.Mock())
        handlers.register(MQTTPublishPacket, mocker.Mock())
    connection = Connection(handlers)
    packet = MQTTPublishPacket()

    connection.handle_packet(packet)
    for handler in handlers.get_handlers(MQTTPublishPacket):
        handler.assert_called_once_with(packet)
        handler.reset_mock()

    # At least one exception should be raised if any handler raises an exception
    handlers.get_handlers(MQTTPublishPacket)[0].side_effect = Exception("TEST")
    with pytest.raises(Exception):
        connection.handle_packet(packet)
    # All handlers are still called
    for handler in handlers.get_handlers(MQTTPublishPacket):
        handler.assert_called_once_with(packet)
        handler.reset_mock()

    # MQTTError has priority over other exceptions
    handlers.get_handlers(MQTTPublishPacket)[1].side_effect = MQTTError("TEST", MQTTReasonCode.UnspecifiedError)
    with pytest.raises(MQTTError):
        connection.handle_packet(packet)
    # All handlers are still called
    for handler in handlers.get_handlers(MQTTPublishPacket):
        handler.assert_called_once_with(packet)
        handler.reset_mock()


def test_connection_can_send(mock_fsm, mock_handlers):
    connection = Connection(mock_handlers)
    mock_fsm.get_state.return_value = ClosedState
    assert not connection.can_send()
    mock_fsm.get_state.return_value = ConnectedState
    assert connection.can_send()


def test_connection_send(mocker, mock_fsm, mock_handlers):
    connection = Connection(mock_handlers)
    mock_fsm.get_state.return_value = ConnectedState
    mock_fsm.env.write_buffer = bytearray()
    packet = MQTTPublishPacket(topic="test/topic", payload=b"test")
    connection.send(packet)
    assert mock_fsm.env.write_buffer == packet.encode()
    mock_fsm.get_state.return_value = ClosedState
    with pytest.raises(InvalidStateError):
        connection.send(packet)


def test_connection_connect(mocker, mock_fsm, mock_handlers):
    connection = Connection(mock_handlers)
    params = ConnectParams(address=Address("test_address"))
    connection.connect(params)
    mock_fsm.set_params.assert_called_once_with(params)
    mock_fsm.request_state.assert_called_once_with(ConnectingState)
    mock_fsm.selector.interrupt.assert_called_once_with()


def test_connection_disconnect(mocker, mock_fsm, mock_handlers):
    connection = Connection(mock_handlers)
    connection.disconnect()
    mock_fsm.request_state.assert_called_once_with(ClosingState)
    mock_fsm.selector.interrupt.assert_called_once_with()


def test_connection_shutdown(mocker, mock_fsm, mock_handlers):
    connection = Connection(mock_handlers)
    connection.shutdown()
    mock_fsm.request_state.assert_called_once_with(ShutdownState)
    mock_fsm.selector.interrupt.assert_called_once_with()


def test_connection_is_connected(mocker, mock_fsm, mock_handlers):
    connection = Connection(mock_handlers)
    mock_fsm.get_state.return_value = ConnectedState
    assert connection.is_connected() is True
    mock_fsm.get_state.return_value = ClosedState
    assert connection.is_connected() is False


def test_connection_wait_for_connect(mocker, mock_fsm, mock_handlers):
    connection = Connection(mock_handlers)
    mock_fsm.wait_for_state.return_value = True
    assert connection.wait_for_connect(0.1) is True
    mock_fsm.wait_for_state.assert_called_once_with((ConnectedState,), 0.1)
    mock_fsm.wait_for_state.return_value = False
    assert connection.wait_for_connect(0.1) is False


def test_connection_wait_for_disconnect(mocker, mock_fsm, mock_handlers):
    connection = Connection(mock_handlers)
    mock_fsm.wait_for_state.return_value = True
    assert connection.wait_for_disconnect(0.1) is True
    mock_fsm.wait_for_state.assert_called_once_with((ClosedState, ShutdownState, ReconnectWaitState), 0.1)
    mock_fsm.wait_for_state.return_value = False
    assert connection.wait_for_disconnect(0.1) is False


def test_connection_wait_for_shutdown(mocker, mock_fsm, mock_handlers):
    connection = Connection(mock_handlers)
    mock_fsm.wait_for_state.return_value = True
    assert connection.wait_for_shutdown(0.1) is True
    mock_fsm.wait_for_state.assert_called_once_with((ShutdownState,), 0.1)
    mock_fsm.wait_for_state.return_value = False
    assert connection.wait_for_shutdown(0.1) is False


def test_connection_loop_once(mocker, mock_fsm, mock_handlers):
    connection = Connection(mock_handlers)
    connection.loop_once(0.1)
    mock_fsm.loop_once.assert_called_once_with(0.1)
    connection.loop_once()
    # Default should be non-blocking.
    mock_fsm.loop_once.assert_called_with(0.0)


def test_connection_loop_forever(mocker, mock_fsm, mock_handlers):
    connection = Connection(mock_handlers)
    connection.loop_forever()
    mock_fsm.loop_until_state.assert_called_once_with((ShutdownState,))


def test_connection_loop_until_connected(mocker, mock_fsm, mock_handlers):
    connection = Connection(mock_handlers)
    mock_fsm.loop_until_state.return_value = True
    assert connection.loop_until_connected(0.1) is True
    mock_fsm.loop_until_state.assert_called_once_with((ConnectedState,), 0.1)
    mock_fsm.loop_until_state.return_value = False
    assert connection.loop_until_connected(0.1) is False
