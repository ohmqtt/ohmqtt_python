import socket
import ssl

import pytest

from ohmqtt.connection import Address
from ohmqtt.connection.decoder import IncrementalDecoder, ClosedSocketError
from ohmqtt.connection.fsm import FSM
from ohmqtt.connection.selector import InterruptibleSelector
from ohmqtt.connection.states import (
    ConnectingState,
    TLSHandshakeState,
    MQTTHandshakeConnectState,
    MQTTHandshakeConnAckState,
    ConnectedState,
    #ClosingState,
    ClosedState,
    #ShutdownState,
)
from ohmqtt.connection.timeout import Timeout
from ohmqtt.connection.types import ConnectParams, StateData, StateEnvironment
from ohmqtt.packet import decode_packet, MQTTConnectPacket, MQTTConnAckPacket, MQTTPublishPacket
from ohmqtt.property import MQTTConnectProps, MQTTConnAckProps, MQTTWillProps


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
def env(callbacks):
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
def test_states_connecting_happy_path(address, block, callbacks, state_data, env, decoder, mock_select, mock_socket, mock_timeout):
    if address.startswith("unix:") and not hasattr(socket, "AF_UNIX"):
        pytest.skip("Unix socket not supported on this platform")
    params = ConnectParams(address=Address(address))
    fsm = FSM(env=env, init_state=ConnectingState)

    # Enter state.
    ConnectingState.enter(fsm, state_data, env, params)
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
    ret = ConnectingState.handle(fsm, state_data, env, params, block)
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
    ret = ConnectingState.handle(fsm, state_data, env, params, block)
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
def test_states_connecting_timeout(address, block, callbacks, state_data, env, mock_timeout):
    if address.startswith("unix:") and not hasattr(socket, "AF_UNIX"):
        pytest.skip("Unix socket not supported on this platform")
    params = ConnectParams(address=Address(address))
    fsm = FSM(env=env, init_state=ConnectingState)

    mock_timeout.exceeded.return_value = True
    ConnectingState.enter(fsm, state_data, env, params)
    mock_timeout.mark.assert_called_once()
    ret = ConnectingState.handle(fsm, state_data, env, params, block)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("address", ["mqtt://testhost", "mqtts://testhost", "unix:///testpath"])
@pytest.mark.parametrize("block", [True, False])
def test_states_connecting_error(address, block, callbacks, state_data, env, mock_socket):
    if address.startswith("unix:") and not hasattr(socket, "AF_UNIX"):
        pytest.skip("Unix socket not supported on this platform")
    params = ConnectParams(address=Address(address))
    fsm = FSM(env=env, init_state=ConnectingState)

    mock_socket.connect.side_effect = ConnectionError("TEST")
    ConnectingState.enter(fsm, state_data, env, params)
    ret = ConnectingState.handle(fsm, state_data, env, params, block)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("block", [True, False])
def test_states_tls_handshake_happy_path(block, callbacks, state_data, env, mock_select, mock_socket, mocker):
    params = ConnectParams(address=Address("mqtts://testhost"), tls_context=mocker.Mock())
    fsm = FSM(env=env, init_state=TLSHandshakeState)

    # Enter state.
    params.tls_context.wrap_socket.return_value = mock_socket
    TLSHandshakeState.enter(fsm, state_data, env, params)
    params.tls_context.wrap_socket.assert_called_once_with(
        mock_socket,
        server_hostname=params.address.host,
        do_handshake_on_connect=False,
    )

    # First handle, want write.
    mock_socket.do_handshake.side_effect = ssl.SSLWantWriteError
    mock_select.return_value = ([], [mock_socket], [])
    ret = TLSHandshakeState.handle(fsm, state_data, env, params, block)
    assert ret is False
    mock_select.assert_called_once_with([], [mock_socket], [], 1 if block else 0)
    mock_select.reset_mock()
    mock_socket.do_handshake.assert_called_once()
    mock_socket.reset_mock()
    assert fsm.state is TLSHandshakeState

    # Second handle, want read.
    mock_socket.do_handshake.side_effect = ssl.SSLWantReadError
    mock_select.return_value = ([mock_socket], [], [])
    ret = TLSHandshakeState.handle(fsm, state_data, env, params, block)
    assert ret is False
    mock_select.assert_called_once_with([mock_socket], [], [], 1 if block else 0)
    mock_select.reset_mock()
    mock_socket.do_handshake.assert_called_once()
    mock_socket.reset_mock()
    assert fsm.state is TLSHandshakeState

    # Third handle, complete.
    mock_socket.do_handshake.side_effect = None
    ret = TLSHandshakeState.handle(fsm, state_data, env, params, block)
    assert ret is True
    mock_socket.do_handshake.assert_called_once()
    mock_socket.reset_mock()
    assert fsm.state is MQTTHandshakeConnectState

    callbacks.assert_not_called()


@pytest.mark.parametrize("block", [True, False])
def test_states_tls_handshake_timeout(block, callbacks, state_data, env, mocker, mock_timeout):
    params = ConnectParams(address=Address("mqtts://testhost"), tls_context=mocker.Mock())
    fsm = FSM(env=env, init_state=TLSHandshakeState)

    mock_timeout.exceeded.return_value = True
    TLSHandshakeState.enter(fsm, state_data, env, params)
    mock_timeout.mark.assert_not_called()
    ret = TLSHandshakeState.handle(fsm, state_data, env, params, block)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize(
    "address,user,pwd", [
        ("mqtt://testhost", None, None),
        ("mqtt://test_user:test_pass@testhost", "test_user", "test_pass"),
    ],
)
@pytest.mark.parametrize("block", [True, False])
def test_states_mqtt_connect_happy_path(address, user, pwd, block, callbacks, state_data, env, mock_select, mock_socket, mocker):
    env.write_buffer = mocker.Mock()
    params = ConnectParams(
        address=Address(address),
        client_id="test_client",
        keepalive_interval=60,
        clean_start=True,
        will_topic="test_topic",
        will_payload=b"test_payload",
        will_qos=1,
        will_retain=True,
        will_properties=MQTTWillProps(),
        connect_properties=MQTTConnectProps(),
    )
    fsm = FSM(env=env, init_state=MQTTHandshakeConnectState)

    # Enter state.
    MQTTHandshakeConnectState.enter(fsm, state_data, env, params)
    env.write_buffer.clear.assert_called_once()
    env.write_buffer.extend.assert_called_once()
    data = env.write_buffer.extend.call_args[0][0]
    env.write_buffer.reset_mock()
    env.write_buffer = bytearray(data)
    packet = decode_packet(data)
    assert isinstance(packet, MQTTConnectPacket)
    assert packet.client_id == params.client_id
    assert packet.keep_alive == params.keepalive_interval
    assert packet.clean_start is params.clean_start
    assert packet.will_topic == params.will_topic
    assert packet.will_payload == params.will_payload
    assert packet.will_qos == params.will_qos
    assert packet.will_retain is params.will_retain
    assert packet.will_props == params.will_properties
    assert packet.properties == params.connect_properties
    assert packet.username == user
    assert packet.password == (pwd.encode() if pwd else None)
    assert fsm.state is MQTTHandshakeConnectState

    # First handle, blocked write.
    if params.address.use_tls:
        mock_socket.send.side_effect = ssl.SSLWantWriteError
    else:
        mock_socket.send.side_effect = BlockingIOError
    ret = MQTTHandshakeConnectState.handle(fsm, state_data, env, params, block)
    assert ret is False
    mock_socket.send.assert_called_once_with(env.write_buffer)
    mock_socket.reset_mock()
    assert fsm.state is MQTTHandshakeConnectState
    if block:
        mock_select.assert_called_once_with([], [mock_socket], [], 1)

    # Second handle, complete.
    mock_socket.send.side_effect = None
    mock_socket.send.return_value = len(data)
    ret = MQTTHandshakeConnectState.handle(fsm, state_data, env, params, block)
    assert ret is True
    mock_socket.send.assert_called_once_with(env.write_buffer)
    mock_socket.reset_mock()
    assert fsm.state is MQTTHandshakeConnAckState

    callbacks.assert_not_called()


@pytest.mark.parametrize("block", [True, False])
def test_states_mqtt_connect_timeout(block, callbacks, state_data, env, mock_timeout):
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=MQTTHandshakeConnectState)

    MQTTHandshakeConnectState.enter(fsm, state_data, env, params)
    mock_timeout.mark.assert_not_called()
    mock_timeout.exceeded.return_value = True
    ret = MQTTHandshakeConnectState.handle(fsm, state_data, env, params, block)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("block", [True, False])
def test_states_mqtt_connect_partial(block, callbacks, state_data, env, mock_socket, mocker):
    env.write_buffer = mocker.Mock()
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=MQTTHandshakeConnectState)

    # One byte on the first write.
    MQTTHandshakeConnectState.enter(fsm, state_data, env, params)
    data = env.write_buffer.extend.call_args[0][0]
    env.write_buffer.reset_mock()
    env.write_buffer = bytearray(data)
    mock_socket.send.side_effect = None
    mock_socket.send.return_value = 1
    ret = MQTTHandshakeConnectState.handle(fsm, state_data, env, params, block)
    assert ret is False
    mock_socket.send.assert_called_once_with(env.write_buffer)
    mock_socket.reset_mock()
    assert env.write_buffer == bytearray(data[1:])
    assert fsm.state is MQTTHandshakeConnectState

    # The rest on the second write.
    mock_socket.send.return_value = len(data) - 1
    ret = MQTTHandshakeConnectState.handle(fsm, state_data, env, params, block)
    assert ret is True
    mock_socket.send.assert_called_once_with(env.write_buffer)
    mock_socket.reset_mock()
    assert env.write_buffer == bytearray()
    assert fsm.state is MQTTHandshakeConnAckState

    callbacks.assert_not_called()


@pytest.mark.parametrize("block", [True, False])
def test_states_mqtt_connect_error(block, callbacks, state_data, env, mock_socket):
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=MQTTHandshakeConnectState)

    MQTTHandshakeConnectState.enter(fsm, state_data, env, params)
    mock_socket.send.return_value = 0
    ret = MQTTHandshakeConnectState.handle(fsm, state_data, env, params, block)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("block", [True, False])
def test_states_mqtt_connack_happy_path(block, callbacks, state_data, env, decoder, mock_select, mock_socket):
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=MQTTHandshakeConnAckState)

    # Enter state.
    MQTTHandshakeConnAckState.enter(fsm, state_data, env, params)
    assert fsm.state is MQTTHandshakeConnAckState

    # First handle, blocked read.
    decoder.decode.return_value = None
    mock_select.return_value = ([mock_socket], [], [])
    ret = MQTTHandshakeConnAckState.handle(fsm, state_data, env, params, block)
    assert ret is False
    mock_select.assert_called_once_with([mock_socket], [], [], 1 if block else 0)
    mock_select.reset_mock()
    assert fsm.state is MQTTHandshakeConnAckState

    # Second handle, complete.
    connack = MQTTConnAckPacket(properties=MQTTConnAckProps(ServerKeepAlive=23))
    decoder.decode.return_value = connack
    mock_select.return_value = ([], [], [])
    ret = MQTTHandshakeConnAckState.handle(fsm, state_data, env, params, block)
    assert ret is True
    assert state_data.connack == connack
    assert state_data.keepalive.keepalive_interval == connack.properties.ServerKeepAlive
    assert fsm.state is ConnectedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("block", [True, False])
def test_states_mqtt_connack_timeout(block, callbacks, state_data, env, mock_timeout):
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=MQTTHandshakeConnAckState)

    MQTTHandshakeConnAckState.enter(fsm, state_data, env, params)
    mock_timeout.mark.assert_not_called()
    mock_timeout.exceeded.return_value = True
    ret = MQTTHandshakeConnAckState.handle(fsm, state_data, env, params, block)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("block", [True, False])
def test_states_mqtt_connack_closed_socket(block, callbacks, decoder, state_data, env, mock_socket):
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=MQTTHandshakeConnAckState)

    MQTTHandshakeConnAckState.enter(fsm, state_data, env, params)
    decoder.decode.side_effect = ClosedSocketError("TEST")
    ret = MQTTHandshakeConnAckState.handle(fsm, state_data, env, params, block)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("block", [True, False])
def test_states_mqtt_connack_unexpected(block, callbacks, decoder, state_data, env):
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=MQTTHandshakeConnAckState)

    MQTTHandshakeConnAckState.enter(fsm, state_data, env, params)
    decoder.decode.return_value = MQTTPublishPacket(topic="test/topic", payload=b"test_payload")
    ret = MQTTHandshakeConnAckState.handle(fsm, state_data, env, params, block)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()
