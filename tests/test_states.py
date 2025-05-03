import socket

import pytest

from ohmqtt.connection import Address
from ohmqtt.connection.decoder import IncrementalDecoder
from ohmqtt.connection.fsm import FSM
from ohmqtt.connection.selector import InterruptibleSelector
from ohmqtt.connection.states import (
    ConnectingState,
    TLSHandshakeState,
    MQTTHandshakeConnectState,
    #MQTTHandshakeConnAckState,
    #ConnectedState,
    #ClosingState,
    #ClosedState,
    #ShutdownState,
)
from ohmqtt.connection.types import ConnectParams, StateData, StateEnvironment


@pytest.fixture
def callbacks(mocker):
    class EnvironmentCallbacks:
        """Container for StateEnvironment callbacks."""
        def __init__(self, mocker):
            self.close = mocker.Mock()
            self.open = mocker.Mock()
            self.read = mocker.Mock()

        def reset(self):
            self.close.reset_mock()
            self.open.reset_mock()
            self.read.reset_mock()

        def assert_not_called(self):
            self.close.assert_not_called()
            self.open.assert_not_called()
            self.read.assert_not_called()

    return EnvironmentCallbacks(mocker)


@pytest.fixture
def state_env(callbacks):
    return StateEnvironment(
        close_callback=callbacks.close,
        open_callback=callbacks.open,
        read_callback=callbacks.read,
    )


@pytest.fixture
def mock_socket(mocker):
    mock_socket = mocker.Mock(spec=socket.socket)
    mocker.patch("ohmqtt.connection.states._get_socket", return_value=mock_socket)
    yield mock_socket


@pytest.fixture
def decoder(mocker):
    return mocker.Mock(spec=IncrementalDecoder)


@pytest.fixture
def state_data(decoder, mock_socket):
    data = StateData()
    data.decoder = decoder
    data.sock = mock_socket
    return data


@pytest.fixture
def params():
    return ConnectParams(address=Address("mqtt://localhost"))


@pytest.fixture
def mock_select(mocker):
    mock_select = mocker.Mock(spec=InterruptibleSelector.select)
    mock_select.return_value = ([], [], [])
    mocker.patch("ohmqtt.connection.selector.InterruptibleSelector.select", mock_select)
    yield mock_select


@pytest.mark.parametrize("address", ["mqtt://localhost", "mqtts://localhost", "unix:///dev/null"])
def test_states_connecting_happy_path(address, callbacks, state_data, state_env, decoder, mock_select, mock_socket):
    """Test the ConnectingState."""
    if address.startswith("unix:") and not hasattr(socket, "AF_UNIX"):
        pytest.skip("Unix socket not supported on this platform")
    params = ConnectParams(address=Address(address))
    fsm = FSM(env=state_env, init_state=ConnectingState)

    ConnectingState.enter(fsm, state_data, state_env, params)
    if params.address.scheme != "unix":
        mock_socket.setsockopt.assert_called_once_with(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    mock_socket.reset_mock()
    decoder.reset.assert_called_once()
    decoder.reset_mock()

    mock_select.return_value = ([], [], [])
    mock_socket.connect.side_effect = BlockingIOError
    ret = ConnectingState.handle(fsm, state_data, state_env, params, False)
    assert ret is False
    mock_select.assert_called_once_with([], [mock_socket], [], 0)
    mock_select.reset_mock()
    if params.address.scheme == "unix":
        mock_socket.connect.assert_called_once_with(params.address.host)
    else:
        mock_socket.connect.assert_called_once_with((params.address.host, params.address.port))
    mock_socket.reset_mock()

    mock_select.return_value = ([], [mock_socket], [])
    ret = ConnectingState.handle(fsm, state_data, state_env, params, False)
    assert ret is True
    mock_select.assert_called_once_with([], [mock_socket], [], 0)
    mock_select.reset_mock()
    mock_socket.setblocking.assert_called_once_with(False)
    mock_socket.reset_mock()
    if params.address.use_tls:
        assert fsm.state is TLSHandshakeState
    else:
        assert fsm.state is MQTTHandshakeConnectState

    callbacks.assert_not_called()
