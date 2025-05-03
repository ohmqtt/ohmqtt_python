import socket
import ssl

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
    ClosedState,
    #ShutdownState,
)
from ohmqtt.connection.timeout import Timeout
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
    mock_socket = mocker.Mock(spec=ssl.SSLSocket)
    mocker.patch("ohmqtt.connection.states._get_socket", return_value=mock_socket)
    yield mock_socket


@pytest.fixture
def decoder(mocker):
    return mocker.Mock(spec=IncrementalDecoder)


@pytest.fixture
def mock_timeout(mocker):
    mock_timeout = mocker.Mock(spec=Timeout)
    mock_timeout.get_timeout.return_value = 1
    mock_timeout.exceeded.return_value = False
    return mock_timeout


@pytest.fixture
def state_data(decoder, mock_socket, mock_timeout):
    data = StateData()
    data.decoder = decoder
    data.sock = mock_socket
    data.timeout = mock_timeout
    return data


@pytest.fixture
def params():
    return ConnectParams(address=Address("mqtt://testhost"))


@pytest.fixture
def mock_select(mocker):
    mock_select = mocker.Mock(spec=InterruptibleSelector.select)
    mock_select.return_value = ([], [], [])
    mocker.patch("ohmqtt.connection.selector.InterruptibleSelector.select", mock_select)
    yield mock_select


@pytest.mark.parametrize("address", ["mqtt://testhost", "mqtts://testhost", "unix:///testpath"])
@pytest.mark.parametrize("block", [True, False])
def test_states_connecting_happy_path(address, block, callbacks, state_data, state_env, decoder, mock_select, mock_socket, mock_timeout):
    if address.startswith("unix:") and not hasattr(socket, "AF_UNIX"):
        pytest.skip("Unix socket not supported on this platform")
    params = ConnectParams(address=Address(address))
    fsm = FSM(env=state_env, init_state=ConnectingState)

    # Enter state.
    ConnectingState.enter(fsm, state_data, state_env, params)
    if params.address.scheme != "unix":
        mock_socket.setsockopt.assert_called_once_with(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    mock_socket.reset_mock()
    decoder.reset.assert_called_once()
    decoder.reset_mock()
    assert mock_timeout.interval == params.connect_timeout
    mock_timeout.mark.assert_called_once()
    assert fsm.state is ConnectingState

    # First handle, begin non-blocking connect.
    mock_select.return_value = ([], [], [])
    mock_socket.connect.side_effect = BlockingIOError
    ret = ConnectingState.handle(fsm, state_data, state_env, params, block)
    assert ret is False
    mock_select.assert_called_once_with([], [mock_socket], [], 1 if block else 0)
    mock_select.reset_mock()
    if params.address.scheme == "unix":
        mock_socket.connect.assert_called_once_with(params.address.host)
    else:
        mock_socket.connect.assert_called_once_with((params.address.host, params.address.port))
    mock_socket.reset_mock()
    assert fsm.state is ConnectingState

    # Second handle, finish non-blocking connect.
    mock_select.return_value = ([], [mock_socket], [])
    ret = ConnectingState.handle(fsm, state_data, state_env, params, block)
    assert ret is True
    mock_select.assert_called_once_with([], [mock_socket], [], 1 if block else 0)
    mock_select.reset_mock()
    mock_socket.setblocking.assert_called_once_with(False)
    mock_socket.reset_mock()
    if params.address.use_tls:
        assert fsm.state is TLSHandshakeState
    else:
        assert fsm.state is MQTTHandshakeConnectState

    callbacks.assert_not_called()


@pytest.mark.parametrize("address", ["mqtt://testhost", "mqtts://testhost", "unix:///testpath"])
@pytest.mark.parametrize("block", [True, False])
def test_states_connecting_timeout(address, block, callbacks, state_data, state_env, mock_timeout):
    if address.startswith("unix:") and not hasattr(socket, "AF_UNIX"):
        pytest.skip("Unix socket not supported on this platform")
    params = ConnectParams(address=Address(address))
    fsm = FSM(env=state_env, init_state=ConnectingState)

    mock_timeout.exceeded.return_value = True
    ConnectingState.enter(fsm, state_data, state_env, params)
    mock_timeout.mark.assert_called_once()
    ret = ConnectingState.handle(fsm, state_data, state_env, params, block)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("address", ["mqtt://testhost", "mqtts://testhost", "unix:///testpath"])
@pytest.mark.parametrize("block", [True, False])
def test_states_connecting_error(address, block, callbacks, state_data, state_env, mock_socket):
    if address.startswith("unix:") and not hasattr(socket, "AF_UNIX"):
        pytest.skip("Unix socket not supported on this platform")
    params = ConnectParams(address=Address(address))
    fsm = FSM(env=state_env, init_state=ConnectingState)

    mock_socket.connect.side_effect = ConnectionError("TEST")
    ConnectingState.enter(fsm, state_data, state_env, params)
    ret = ConnectingState.handle(fsm, state_data, state_env, params, block)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("block", [True, False])
def test_states_tls_handshake_happy_path(block, callbacks, state_data, state_env, mock_select, mock_socket, mocker):
    params = ConnectParams(address=Address("mqtts://testhost"), tls_context=mocker.Mock())
    fsm = FSM(env=state_env, init_state=TLSHandshakeState)

    # Enter state.
    params.tls_context.wrap_socket.return_value = mock_socket
    TLSHandshakeState.enter(fsm, state_data, state_env, params)
    params.tls_context.wrap_socket.assert_called_once_with(
        mock_socket,
        server_hostname=params.address.host,
        do_handshake_on_connect=False,
    )

    # First handle, want write.
    mock_socket.do_handshake.side_effect = ssl.SSLWantWriteError
    mock_select.return_value = ([], [mock_socket], [])
    ret = TLSHandshakeState.handle(fsm, state_data, state_env, params, block)
    assert ret is False
    mock_select.assert_called_once_with([], [mock_socket], [], 1 if block else 0)
    mock_select.reset_mock()
    mock_socket.do_handshake.assert_called_once()
    mock_socket.reset_mock()
    assert fsm.state is TLSHandshakeState

    # Second handle, want read.
    mock_socket.do_handshake.side_effect = ssl.SSLWantReadError
    mock_select.return_value = ([mock_socket], [], [])
    ret = TLSHandshakeState.handle(fsm, state_data, state_env, params, block)
    assert ret is False
    mock_select.assert_called_once_with([mock_socket], [], [], 1 if block else 0)
    mock_select.reset_mock()
    mock_socket.do_handshake.assert_called_once()
    mock_socket.reset_mock()
    assert fsm.state is TLSHandshakeState

    # Third handle, complete.
    mock_socket.do_handshake.side_effect = None
    ret = TLSHandshakeState.handle(fsm, state_data, state_env, params, block)
    assert ret is True
    mock_socket.do_handshake.assert_called_once()
    mock_socket.reset_mock()
    assert fsm.state is MQTTHandshakeConnectState

    callbacks.assert_not_called()


@pytest.mark.parametrize("block", [True, False])
def test_states_tls_handshake_timeout(block, callbacks, state_data, state_env, mocker, mock_timeout):
    params = ConnectParams(address=Address("mqtts://testhost"), tls_context=mocker.Mock())
    fsm = FSM(env=state_env, init_state=TLSHandshakeState)

    mock_timeout.exceeded.return_value = True
    TLSHandshakeState.enter(fsm, state_data, state_env, params)
    mock_timeout.mark.assert_not_called()
    ret = TLSHandshakeState.handle(fsm, state_data, state_env, params, block)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()
