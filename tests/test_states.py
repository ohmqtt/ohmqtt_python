import socket
import ssl

import pytest

from ohmqtt.connection import Address
from ohmqtt.connection.decoder import IncrementalDecoder, ClosedSocketError
from ohmqtt.connection.fsm import FSM
from ohmqtt.connection.keepalive import KeepAlive
from ohmqtt.connection.selector import InterruptibleSelector
from ohmqtt.connection.states import (
    ConnectingState,
    TLSHandshakeState,
    MQTTHandshakeConnectState,
    MQTTHandshakeConnAckState,
    ConnectedState,
    ReconnectWaitState,
    ClosingState,
    ClosedState,
    ShutdownState,
)
from ohmqtt.connection.timeout import Timeout
from ohmqtt.connection.types import ConnectParams, StateData, StateEnvironment
from ohmqtt.error import MQTTError
from ohmqtt.mqtt_spec import MQTTReasonCode
from ohmqtt.packet import (
    decode_packet,
    MQTTConnectPacket,
    MQTTConnAckPacket,
    MQTTDisconnectPacket,
    MQTTPublishPacket,
    MQTTPingReqPacket,
    MQTTPingRespPacket,
    PING,
    PONG,
)
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
def mock_keepalive(mocker):
    mock_keepalive = mocker.Mock(spec=KeepAlive)
    mock_keepalive.get_next_timeout.return_value = 1
    mock_keepalive.should_close.return_value = False
    mock_keepalive.should_send_ping.return_value = False
    return mock_keepalive

@pytest.fixture
def mock_timeout(mocker):
    mock_timeout = mocker.Mock(spec=Timeout)
    mock_timeout.interval = None
    mock_timeout.get_timeout.return_value = 1
    mock_timeout.exceeded.return_value = False
    return mock_timeout

@pytest.fixture
def state_data(decoder, mock_socket, mock_keepalive, mock_timeout):
    data = StateData()
    data.decoder = decoder
    data.keepalive = mock_keepalive
    data.sock = mock_socket
    data.timeout = mock_timeout
    data.connack = MQTTConnAckPacket()
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

@pytest.fixture
def mock_read(mocker):
    mock_read = mocker.Mock(spec=ConnectedState.read_packet)
    mocker.patch("ohmqtt.connection.states.ConnectedState.read_packet", mock_read)
    yield mock_read


@pytest.mark.parametrize("address", ["mqtt://testhost", "mqtts://testhost", "unix:///testpath"])
@pytest.mark.parametrize("block", [True, False])
def test_states_connecting_happy_path(address, block, callbacks, state_data, env, decoder, mock_select, mock_socket, mock_timeout):
    if address.startswith("unix:") and not hasattr(socket, "AF_UNIX"):
        pytest.skip("Unix socket not supported on this platform")
    params = ConnectParams(address=Address(address))
    fsm = FSM(env=env, init_state=ConnectingState, error_state=ShutdownState)

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
    fsm = FSM(env=env, init_state=ConnectingState, error_state=ShutdownState)

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
    fsm = FSM(env=env, init_state=ConnectingState, error_state=ShutdownState)

    mock_socket.connect.side_effect = ConnectionError("TEST")
    ConnectingState.enter(fsm, state_data, env, params)
    ret = ConnectingState.handle(fsm, state_data, env, params, block)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("block", [True, False])
def test_states_tls_handshake_happy_path(block, callbacks, state_data, env, mock_select, mock_socket, mocker):
    params = ConnectParams(address=Address("mqtts://testhost"), tls_context=mocker.Mock())
    fsm = FSM(env=env, init_state=TLSHandshakeState, error_state=ShutdownState)

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
    fsm = FSM(env=env, init_state=TLSHandshakeState, error_state=ShutdownState)

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
    fsm = FSM(env=env, init_state=MQTTHandshakeConnectState, error_state=ShutdownState)

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
    fsm = FSM(env=env, init_state=MQTTHandshakeConnectState, error_state=ShutdownState)

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
    fsm = FSM(env=env, init_state=MQTTHandshakeConnectState, error_state=ShutdownState)

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
    fsm = FSM(env=env, init_state=MQTTHandshakeConnectState, error_state=ShutdownState)

    MQTTHandshakeConnectState.enter(fsm, state_data, env, params)
    mock_socket.send.return_value = 0
    ret = MQTTHandshakeConnectState.handle(fsm, state_data, env, params, block)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("block", [True, False])
def test_states_mqtt_connack_happy_path(block, callbacks, state_data, env, decoder, mock_select, mock_socket):
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=MQTTHandshakeConnAckState, error_state=ShutdownState)

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
    fsm = FSM(env=env, init_state=MQTTHandshakeConnAckState, error_state=ShutdownState)

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
    fsm = FSM(env=env, init_state=MQTTHandshakeConnAckState, error_state=ShutdownState)

    MQTTHandshakeConnAckState.enter(fsm, state_data, env, params)
    decoder.decode.side_effect = ClosedSocketError("TEST")
    ret = MQTTHandshakeConnAckState.handle(fsm, state_data, env, params, block)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("block", [True, False])
def test_states_mqtt_connack_unexpected(block, callbacks, decoder, state_data, env):
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=MQTTHandshakeConnAckState, error_state=ShutdownState)

    MQTTHandshakeConnAckState.enter(fsm, state_data, env, params)
    decoder.decode.return_value = MQTTPublishPacket(topic="test/topic", payload=b"test_payload")
    ret = MQTTHandshakeConnAckState.handle(fsm, state_data, env, params, block)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("block", [True, False])
def test_states_connected_happy_path(block, callbacks, state_data, env, mock_select, mock_socket, mock_read):
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=ConnectedState, error_state=ShutdownState)

    # Enter state.
    ConnectedState.enter(fsm, state_data, env, params)
    state_data.keepalive.mark_init.assert_called_once()
    callbacks.open.assert_called_once_with(state_data.connack)
    callbacks.reset()
    assert fsm.state is ConnectedState

    # First handle, nothing to read.
    mock_select.return_value = ([], [], [])
    ret = ConnectedState.handle(fsm, state_data, env, params, block)
    assert ret is False
    mock_select.assert_called_once_with([mock_socket], [], [], 1 if block else 0)
    mock_select.reset_mock()
    assert fsm.state is ConnectedState

    # Second handle, send data in write buffer.
    env.write_buffer.extend(b"test_data")
    mock_select.return_value = ([], [mock_socket], [])
    mock_socket.send.return_value = len(env.write_buffer)
    ret = ConnectedState.handle(fsm, state_data, env, params, block)
    assert ret is False
    mock_select.assert_called_once_with([mock_socket], [mock_socket], [], 1 if block else 0)
    mock_select.reset_mock()
    mock_socket.send.assert_called_once_with(env.write_buffer)
    mock_socket.reset_mock()
    assert len(env.write_buffer) == 0
    state_data.keepalive.mark_send.assert_called_once()
    assert fsm.state is ConnectedState

    # Third handle, read data.
    mock_select.return_value = ([mock_socket], [], [])
    mock_read.side_effect = [True, False]
    ret = ConnectedState.handle(fsm, state_data, env, params, block)
    assert ret is False
    mock_select.assert_called_once_with([mock_socket], [], [], 1 if block else 0)
    mock_select.reset_mock()
    assert mock_read.call_count == 2

    callbacks.assert_not_called()


@pytest.mark.parametrize("block", [True, False])
def test_states_connected_keepalive(block, callbacks, state_data, env, mock_keepalive, mock_select, mock_socket):
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=ConnectedState, error_state=ShutdownState)

    ConnectedState.enter(fsm, state_data, env, params)
    callbacks.reset()

    # First handle, send a PINGREQ.
    mock_keepalive.should_send_ping.return_value = True
    mock_select.return_value = ([], [mock_socket], [])
    mock_socket.send.return_value = len(PING)
    ret = ConnectedState.handle(fsm, state_data, env, params, block)
    assert ret is False
    mock_keepalive.should_send_ping.assert_called_once()
    mock_keepalive.mark_ping.assert_called_once()
    mock_keepalive.reset_mock()
    mock_socket.send.assert_called_once_with(env.write_buffer)
    mock_socket.reset_mock()
    assert len(env.write_buffer) == 0

    # Second handle, keepalive timeout.
    mock_keepalive.should_send_ping.return_value = False
    mock_keepalive.should_close.return_value = True
    ret = ConnectedState.handle(fsm, state_data, env, params, block)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("exc", [ssl.SSLWantWriteError, BlockingIOError])
@pytest.mark.parametrize("block", [True, False])
def test_states_connected_send_errors(block, exc, callbacks, state_data, env, mock_read, mock_select, mock_socket):
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=ConnectedState, error_state=ShutdownState)

    ConnectedState.enter(fsm, state_data, env, params)
    callbacks.reset()

    env.write_buffer.extend(b"test_data")
    mock_read.return_value = False
    mock_select.return_value = ([mock_socket], [mock_socket], [])

    mock_socket.send.side_effect = exc("TEST")
    ret = ConnectedState.handle(fsm, state_data, env, params, block)
    assert ret is False
    mock_read.assert_called_once()
    mock_read.reset_mock()
    assert fsm.state is ConnectedState

    mock_select.return_value = ([mock_socket], [mock_socket], [])
    mock_socket.send.side_effect = BrokenPipeError("TEST")
    ret = ConnectedState.handle(fsm, state_data, env, params, block)
    assert ret is True
    mock_read.assert_not_called()
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("block", [True, False])
def test_states_connected_read_closed(block, callbacks, state_data, env, mock_read, mock_select, mock_socket):
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=ConnectedState, error_state=ShutdownState)

    ConnectedState.enter(fsm, state_data, env, params)
    callbacks.reset()

    mock_select.return_value = ([mock_socket], [], [])
    mock_read.side_effect = ClosedSocketError("TEST")
    ret = ConnectedState.handle(fsm, state_data, env, params, block)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("block", [True, False])
def test_states_connected_read_mqtt_error(block, callbacks, state_data, env, mock_read, mock_select, mock_socket):
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=ConnectedState, error_state=ShutdownState)

    ConnectedState.enter(fsm, state_data, env, params)
    callbacks.reset()

    mock_select.return_value = ([mock_socket], [], [])
    mock_read.side_effect = MQTTError("TEST", reason_code=MQTTReasonCode.ProtocolError)
    ret = ConnectedState.handle(fsm, state_data, env, params, block)
    assert ret is True
    assert state_data.disconnect_rc == MQTTReasonCode.ProtocolError
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


def test_states_connected_read_packet(callbacks, state_data, env, decoder, mock_keepalive):
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=ConnectedState, error_state=ShutdownState)

    # Handle incomplete packet.
    decoder.decode.return_value = None
    ret = ConnectedState.read_packet(fsm, state_data, env, params)
    assert ret is False
    decoder.decode.assert_called_once()
    decoder.reset_mock()
    assert fsm.state is ConnectedState

    # Handle complete PUBLISH packet.
    packet = MQTTPublishPacket(topic="test/topic", payload=b"test_payload")
    decoder.decode.return_value = packet
    ret = ConnectedState.read_packet(fsm, state_data, env, params)
    assert ret is True
    decoder.decode.assert_called_once()
    decoder.reset_mock()
    callbacks.read.assert_called_once_with(packet)
    callbacks.reset()
    assert fsm.state is ConnectedState

    # Handle complete PINGRESP packet.
    packet = MQTTPingRespPacket()
    decoder.decode.return_value = packet
    ret = ConnectedState.read_packet(fsm, state_data, env, params)
    assert ret is True
    decoder.decode.assert_called_once()
    decoder.reset_mock()
    mock_keepalive.mark_pong.assert_called_once()
    callbacks.assert_not_called()
    assert fsm.state is ConnectedState

    # Handle complete PINGREQ packet.
    packet = MQTTPingReqPacket()
    decoder.decode.return_value = packet
    ret = ConnectedState.read_packet(fsm, state_data, env, params)
    assert ret is True
    decoder.decode.assert_called_once()
    decoder.reset_mock()
    assert env.write_buffer == PONG
    env.write_buffer.clear()
    callbacks.assert_not_called()
    assert fsm.state is ConnectedState

    # Handle complete DISCONNECT packet.
    packet = MQTTDisconnectPacket()
    decoder.decode.return_value = packet
    ret = ConnectedState.read_packet(fsm, state_data, env, params)
    assert ret is True
    decoder.decode.assert_called_once()
    decoder.reset_mock()
    callbacks.assert_not_called()
    assert fsm.state is ClosingState


@pytest.mark.parametrize("block", [True, False])
def test_states_reconnect_wait_happy_path(block, callbacks, state_data, env, mocker, mock_timeout):
    mock_wait = mocker.patch("ohmqtt.connection.fsm.FSM.wait")
    params = ConnectParams(address=Address("mqtt://testhost"), reconnect_delay=5)
    fsm = FSM(env=env, init_state=ReconnectWaitState, error_state=ShutdownState)

    ReconnectWaitState.enter(fsm, state_data, env, params)
    assert mock_timeout.interval == params.reconnect_delay
    state_data.timeout.mark.assert_called_once()
    state_data.timeout.reset_mock()

    # First handle, waiting.
    mock_wait.return_value = True
    ret = ReconnectWaitState.handle(fsm, state_data, env, params, block)
    assert ret is False
    state_data.timeout.exceeded.assert_called_once()
    state_data.timeout.reset_mock()
    if block:
        mock_wait.assert_called_once_with(state_data.timeout.get_timeout.return_value)
        mock_wait.reset_mock()
    else:
        mock_wait.assert_not_called()
    assert fsm.state is ReconnectWaitState

    # Second handle, transition.
    state_data.timeout.exceeded.return_value = True
    ret = ReconnectWaitState.handle(fsm, state_data, env, params, block)
    assert ret is True
    state_data.timeout.exceeded.assert_called_once()
    mock_wait.assert_not_called()
    assert fsm.state is ConnectingState

    callbacks.assert_not_called()


@pytest.mark.parametrize("address", ["mqtt://testhost", "mqtts://testhost"])
@pytest.mark.parametrize("block", [True, False])
def test_states_closing_happy_path(address, block, callbacks, state_data, env, mock_select, mock_socket, mock_timeout):
    params = ConnectParams(address=Address(address), connect_timeout=5)
    fsm = FSM(env=env, init_state=ClosingState, error_state=ShutdownState)

    # Start from ConnectedState to test transition through ClosingState to ClosedState.
    fsm.previous_state = ConnectedState
    ClosingState.enter(fsm, state_data, env, params)
    assert mock_timeout.interval == params.connect_timeout
    mock_timeout.mark.assert_called_once()
    mock_timeout.reset_mock()
    assert state_data.disconnect_rc == MQTTReasonCode.NormalDisconnection
    if not params.address.use_tls:
        mock_socket.shutdown.assert_called_once_with(socket.SHUT_RD)
    else:
        mock_socket.shutdown.assert_not_called()
    mock_socket.reset_mock()
    assert fsm.state is ClosingState

    # First handle, write all.
    mock_select.return_value = ([], [mock_socket], [])
    env.write_buffer.extend(b"test_data")
    mock_socket.send.return_value = len(env.write_buffer)
    ret = ClosingState.handle(fsm, state_data, env, params, block)
    assert ret is False
    mock_select.assert_called_once_with([], [mock_socket], [], 1 if block else 0)
    mock_select.reset_mock()
    mock_socket.send.assert_called_once_with(env.write_buffer)
    mock_socket.reset_mock()
    mock_timeout.exceeded.assert_called_once()
    mock_timeout.reset_mock()
    assert fsm.state is ClosingState

    # Second handle, done writing, transition to ClosedState.
    ret = ClosingState.handle(fsm, state_data, env, params, block)
    assert ret is True
    mock_timeout.exceeded.assert_called_once()
    mock_timeout.reset_mock()
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


# Try all states which can transition to ClosingState, except ConnectedState.
@pytest.mark.parametrize("prev_state", [
    ConnectingState,
    TLSHandshakeState,
    MQTTHandshakeConnectState,
    MQTTHandshakeConnAckState,
    ReconnectWaitState,
])
def test_states_closing_skip(prev_state, callbacks, state_data, env, mock_socket):
    params = ConnectParams(address=Address("mqtt://testhost"), connect_timeout=5)
    fsm = FSM(env=env, init_state=ClosingState, error_state=ShutdownState)

    fsm.previous_state = prev_state
    ClosingState.enter(fsm, state_data, env, params)
    mock_socket.shutdown.assert_not_called()
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("block", [True, False])
def test_states_closing_timeout(block, callbacks, state_data, env, mock_timeout):
    params = ConnectParams(address=Address("mqtt://testhost"), connect_timeout=5)
    fsm = FSM(env=env, init_state=ClosingState, error_state=ShutdownState)

    fsm.previous_state = ConnectedState
    ClosingState.enter(fsm, state_data, env, params)
    mock_timeout.exceeded.return_value = True
    ret = ClosingState.handle(fsm, state_data, env, params, block)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


def test_states_closing_shutdown_error(callbacks, state_data, env, mock_socket):
    params = ConnectParams(address=Address("mqtt://testhost"), connect_timeout=5)
    fsm = FSM(env=env, init_state=ClosingState, error_state=ShutdownState)

    fsm.previous_state = ConnectedState
    mock_socket.shutdown.side_effect = OSError("TEST")
    ClosingState.enter(fsm, state_data, env, params)
    mock_socket.shutdown.assert_called_once()
    assert fsm.state is ClosingState

    callbacks.assert_not_called()


@pytest.mark.parametrize("exc", [ssl.SSLWantWriteError, BlockingIOError])
@pytest.mark.parametrize("block", [True, False])
def test_states_closing_send_error(block, exc, callbacks, state_data, env, mock_select, mock_socket):
    params = ConnectParams(address=Address("mqtt://testhost"), connect_timeout=5)
    fsm = FSM(env=env, init_state=ClosingState, error_state=ShutdownState)

    fsm.previous_state = ConnectedState
    ClosingState.enter(fsm, state_data, env, params)

    env.write_buffer.extend(b"test_data")
    mock_select.return_value = ([], [mock_socket], [])
    mock_socket.send.side_effect = exc("TEST")
    ret = ClosingState.handle(fsm, state_data, env, params, block)
    assert ret is False
    assert fsm.state is ClosingState

    mock_socket.send.side_effect = BrokenPipeError("TEST")
    ret = ClosingState.handle(fsm, state_data, env, params, block)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()
