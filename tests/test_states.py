from __future__ import annotations

import socket
import ssl
from threading import Condition
from typing import Any
from unittest.mock import Mock

import pytest
from pytest_mock import MockerFixture

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
from ohmqtt.platform import HAS_AF_UNIX
from ohmqtt.property import MQTTConnectProps, MQTTConnAckProps, MQTTWillProps


class EnvironmentCallbacks:
    """Container for StateEnvironment callbacks."""
    def __init__(self, mocker: MockerFixture) -> None:
        self.packet = mocker.Mock()

    def reset(self) -> None:
        self.packet.reset_mock()

    def assert_not_called(self) -> None:
        self.packet.assert_not_called()


@pytest.fixture
def callbacks(mocker: MockerFixture) -> EnvironmentCallbacks:
    return EnvironmentCallbacks(mocker)

@pytest.fixture
def env(callbacks: EnvironmentCallbacks) -> StateEnvironment:
    return StateEnvironment(
        packet_callback=callbacks.packet,
    )

@pytest.fixture
def mock_socket(mocker: MockerFixture) -> Mock:
    mock_socket = mocker.Mock(spec=ssl.SSLSocket)
    mocker.patch("ohmqtt.connection.states._get_socket", return_value=mock_socket)
    return mock_socket  # type: ignore[no-any-return]

@pytest.fixture
def decoder(mocker: MockerFixture) -> Mock:
    return mocker.Mock(spec=IncrementalDecoder)  # type: ignore[no-any-return]

@pytest.fixture
def mock_keepalive(mocker: MockerFixture) -> Mock:
    mock_keepalive = mocker.Mock(spec=KeepAlive)
    mock_keepalive.get_next_timeout.return_value = 1
    mock_keepalive.should_close.return_value = False
    mock_keepalive.should_send_ping.return_value = False
    return mock_keepalive  # type: ignore[no-any-return]

@pytest.fixture
def mock_timeout(mocker: MockerFixture) -> Mock:
    mock_timeout = mocker.Mock(spec=Timeout)
    mock_timeout.interval = None
    mock_timeout.get_timeout.return_value = 1
    mock_timeout.exceeded.return_value = False
    return mock_timeout  # type: ignore[no-any-return]

@pytest.fixture
def state_data(decoder: Mock, mock_socket: Mock, mock_keepalive: Mock, mock_timeout: Mock) -> StateData:
    data = StateData()
    data.decoder = decoder
    data.keepalive = mock_keepalive
    data.sock = mock_socket
    data.timeout = mock_timeout
    data.connack = MQTTConnAckPacket()
    return data

@pytest.fixture
def params() -> ConnectParams:
    return ConnectParams(address=Address("mqtt://testhost"))

@pytest.fixture
def mock_select(mocker: MockerFixture) -> Mock:
    mock_select = mocker.MagicMock(spec=InterruptibleSelector)
    mock_select.__enter__.return_value = mock_select
    mock_select.select.return_value = (False, False)
    mocker.patch("ohmqtt.connection.fsm.InterruptibleSelector", return_value=mock_select)
    return mock_select  # type: ignore[no-any-return]

@pytest.fixture
def mock_read(mocker: MockerFixture) -> Mock:
    mock_read = mocker.patch.object(ConnectedState, "read_packet", autospec=True)
    mock_read.return_value = False
    return mock_read


@pytest.mark.parametrize("address", ["mqtt://testhost", "mqtts://testhost", "unix:///testpath"])
@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_connecting_happy_path(
    address: str,
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    decoder: Mock,
    mock_select: Mock,
    mock_socket: Mock,
    mock_timeout: Mock
) -> None:
    if address.startswith("unix:") and not HAS_AF_UNIX:
        pytest.skip("Unix socket not supported on this platform")
    params = ConnectParams(address=Address(address))
    fsm = FSM(env=env, init_state=ConnectingState, error_state=ShutdownState)
    mock_timeout.get_timeout.return_value = 1 if max_wait is None else max_wait

    # Enter state.
    ConnectingState.enter(fsm, state_data, env, params)
    if params.address.scheme != "unix":
        mock_socket.setsockopt.assert_called_once_with(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    mock_socket.reset_mock()
    decoder.reset.assert_called_once()
    decoder.reset_mock()
    assert mock_timeout.interval == params.connect_timeout
    mock_timeout.mark.assert_called_once()
    mock_timeout.reset_mock()
    assert state_data.disconnect_rc is None
    mock_select.change_sock.assert_called_once_with(mock_socket)
    assert fsm.state is ConnectingState

    # First handle, begin non-blocking connect.
    mock_select.select.return_value = (False, False)
    mock_socket.connect.side_effect = BlockingIOError
    ret = ConnectingState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    mock_select.select.assert_called_once_with(write=True, timeout=1 if max_wait is None else max_wait)
    mock_select.reset_mock()
    if params.address.scheme == "unix":
        mock_socket.connect.assert_called_once_with(params.address.host)
    else:
        mock_socket.connect.assert_called_once_with((params.address.host, params.address.port))
    mock_socket.reset_mock()
    assert fsm.state is ConnectingState

    # Second handle, finish non-blocking connect.
    mock_select.select.return_value = (False, True)
    ret = ConnectingState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    mock_select.select.assert_called_once_with(write=True, timeout=1 if max_wait is None else max_wait)
    mock_select.reset_mock()
    mock_socket.setblocking.assert_called_once_with(False)
    mock_socket.reset_mock()
    if params.address.use_tls:
        assert fsm.state is TLSHandshakeState
    else:
        assert fsm.state is MQTTHandshakeConnectState

    callbacks.assert_not_called()


@pytest.mark.parametrize("address", ["mqtt://testhost", "mqtts://testhost", "unix:///testpath"])
@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_connecting_timeout(
    address: str,
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_select: Mock,
    mock_timeout: Mock
) -> None:
    if address.startswith("unix:") and not HAS_AF_UNIX:
        pytest.skip("Unix socket not supported on this platform")
    params = ConnectParams(address=Address(address))
    fsm = FSM(env=env, init_state=ConnectingState, error_state=ShutdownState)
    mock_timeout.get_timeout.return_value = 1 if max_wait is None else max_wait

    mock_timeout.exceeded.return_value = True
    ConnectingState.enter(fsm, state_data, env, params)
    mock_timeout.mark.assert_called_once()
    ret = ConnectingState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("address", ["mqtt://testhost", "mqtts://testhost", "unix:///testpath"])
@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_connecting_error(
    address: str,
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_select: Mock,
    mock_socket: Mock
) -> None:
    if address.startswith("unix:") and not HAS_AF_UNIX:
        pytest.skip("Unix socket not supported on this platform")
    params = ConnectParams(address=Address(address))
    fsm = FSM(env=env, init_state=ConnectingState, error_state=ShutdownState)

    mock_socket.connect.side_effect = ConnectionError("TEST")
    ConnectingState.enter(fsm, state_data, env, params)
    ret = ConnectingState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_tls_handshake_happy_path(
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_select: Mock,
    mock_socket: Mock,
    mock_timeout: Mock,
    mocker: MockerFixture
) -> None:
    params = ConnectParams(address=Address("mqtts://testhost"), tls_context=mocker.Mock())
    fsm = FSM(env=env, init_state=TLSHandshakeState, error_state=ShutdownState)
    mock_timeout.get_timeout.return_value = 1 if max_wait is None else max_wait

    # Enter state.
    params.tls_context.wrap_socket.return_value = mock_socket  # type: ignore[union-attr]
    TLSHandshakeState.enter(fsm, state_data, env, params)
    params.tls_context.wrap_socket.assert_called_once_with(  # type: ignore[union-attr]
        mock_socket,
        server_hostname=params.address.host,
        do_handshake_on_connect=False,
    )
    mock_select.change_sock.assert_called_once_with(mock_socket)

    # First handle, want write.
    mock_socket.do_handshake.side_effect = ssl.SSLWantWriteError
    mock_select.select.return_value = (False, True)
    ret = TLSHandshakeState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    mock_select.select.assert_called_once_with(write=True, timeout=1 if max_wait is None else max_wait)
    mock_select.reset_mock()
    mock_socket.do_handshake.assert_called_once()
    mock_socket.reset_mock()
    assert fsm.state is TLSHandshakeState

    # Second handle, want read.
    mock_socket.do_handshake.side_effect = ssl.SSLWantReadError
    mock_select.select.return_value = (True, False)
    ret = TLSHandshakeState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    mock_select.select.assert_called_once_with(read=True, timeout=1 if max_wait is None else max_wait)
    mock_select.reset_mock()
    mock_socket.do_handshake.assert_called_once()
    mock_socket.reset_mock()
    assert fsm.state is TLSHandshakeState

    # Third handle, complete.
    mock_socket.do_handshake.side_effect = None
    ret = TLSHandshakeState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    mock_socket.do_handshake.assert_called_once()
    mock_socket.reset_mock()
    assert fsm.state is MQTTHandshakeConnectState

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_tls_handshake_timeout(
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mocker: MockerFixture,
    mock_select: Mock,
    mock_timeout: Mock
) -> None:
    params = ConnectParams(address=Address("mqtts://testhost"), tls_context=mocker.Mock())
    fsm = FSM(env=env, init_state=TLSHandshakeState, error_state=ShutdownState)

    mock_timeout.exceeded.return_value = True
    TLSHandshakeState.enter(fsm, state_data, env, params)
    mock_timeout.mark.assert_not_called()
    ret = TLSHandshakeState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize(
    ("address", "user", "pwd"), [
        ("mqtt://testhost", None, None),
        ("mqtt://test_user:test_pass@testhost", "test_user", "test_pass"),
    ],
)
@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_mqtt_connect_happy_path(
    address: str,
    user: str | None,
    pwd: str | None,
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: Mock,
    mock_select: Mock,
    mock_socket: Mock,
    mock_timeout: Mock,
    mocker: MockerFixture
) -> None:
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
    mock_timeout.get_timeout.return_value = 1 if max_wait is None else max_wait

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
    ret = MQTTHandshakeConnectState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    mock_socket.send.assert_called_once_with(env.write_buffer)
    mock_socket.reset_mock()
    assert fsm.state is MQTTHandshakeConnectState
    if max_wait:
        mock_select.select.assert_called_once_with(write=True, timeout=1)

    # Second handle, complete.
    mock_socket.send.side_effect = None
    mock_socket.send.return_value = len(data)
    ret = MQTTHandshakeConnectState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    mock_socket.send.assert_called_once_with(env.write_buffer)
    mock_socket.reset_mock()
    assert fsm.state is MQTTHandshakeConnAckState

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_mqtt_connect_timeout(
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_timeout: Mock
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=MQTTHandshakeConnectState, error_state=ShutdownState)

    MQTTHandshakeConnectState.enter(fsm, state_data, env, params)
    mock_timeout.mark.assert_not_called()
    mock_timeout.exceeded.return_value = True
    ret = MQTTHandshakeConnectState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_mqtt_connect_partial(
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: Mock,
    mock_socket: Mock,
    mock_timeout: Mock,
    mocker: MockerFixture
) -> None:
    env.write_buffer = mocker.Mock()
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=MQTTHandshakeConnectState, error_state=ShutdownState)
    mock_timeout.get_timeout.return_value = 1 if max_wait is None else max_wait

    # One byte on the first write.
    MQTTHandshakeConnectState.enter(fsm, state_data, env, params)
    data = env.write_buffer.extend.call_args[0][0]
    env.write_buffer.reset_mock()
    env.write_buffer = bytearray(data)
    mock_socket.send.side_effect = None
    mock_socket.send.return_value = 1
    ret = MQTTHandshakeConnectState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    mock_socket.send.assert_called_once_with(env.write_buffer)
    mock_socket.reset_mock()
    assert env.write_buffer == bytearray(data[1:])
    assert fsm.state is MQTTHandshakeConnectState

    # The rest on the second write.
    mock_socket.send.return_value = len(data) - 1
    ret = MQTTHandshakeConnectState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    mock_socket.send.assert_called_once_with(env.write_buffer)
    mock_socket.reset_mock()
    assert env.write_buffer == bytearray()
    assert fsm.state is MQTTHandshakeConnAckState

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_mqtt_connect_error(
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_socket: Mock
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=MQTTHandshakeConnectState, error_state=ShutdownState)

    MQTTHandshakeConnectState.enter(fsm, state_data, env, params)
    mock_socket.send.return_value = 0
    ret = MQTTHandshakeConnectState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_mqtt_connack_happy_path(
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    decoder: Mock,
    mock_select: Mock,
    mock_socket: Mock,
    mock_timeout: Mock
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=MQTTHandshakeConnAckState, error_state=ShutdownState)
    mock_timeout.get_timeout.return_value = 1 if max_wait is None else max_wait

    # Enter state.
    MQTTHandshakeConnAckState.enter(fsm, state_data, env, params)
    assert fsm.state is MQTTHandshakeConnAckState

    # First handle, blocked read.
    decoder.decode.return_value = None
    mock_select.select.return_value = (True, False)
    ret = MQTTHandshakeConnAckState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    mock_select.select.assert_called_once_with(read=True, timeout=1 if max_wait is None else max_wait)
    mock_select.reset_mock()
    assert fsm.state is MQTTHandshakeConnAckState

    # Second handle, complete.
    connack = MQTTConnAckPacket(properties=MQTTConnAckProps(ServerKeepAlive=23))
    decoder.decode.return_value = connack
    mock_select.select.return_value = (False, False)
    ret = MQTTHandshakeConnAckState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert state_data.connack == connack
    assert state_data.keepalive.keepalive_interval == connack.properties.ServerKeepAlive
    assert fsm.state is ConnectedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_mqtt_connack_timeout(
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_timeout: Mock
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=MQTTHandshakeConnAckState, error_state=ShutdownState)

    MQTTHandshakeConnAckState.enter(fsm, state_data, env, params)
    mock_timeout.mark.assert_not_called()
    mock_timeout.exceeded.return_value = True
    ret = MQTTHandshakeConnAckState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_mqtt_connack_closed_socket(
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    decoder: Mock,
    state_data: StateData,
    env: StateEnvironment,
    mock_socket: Mock
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=MQTTHandshakeConnAckState, error_state=ShutdownState)

    MQTTHandshakeConnAckState.enter(fsm, state_data, env, params)
    decoder.decode.side_effect = ClosedSocketError("TEST")
    ret = MQTTHandshakeConnAckState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_mqtt_connack_unexpected(
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    decoder: Mock,
    state_data: StateData,
    env: StateEnvironment
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=MQTTHandshakeConnAckState, error_state=ShutdownState)

    MQTTHandshakeConnAckState.enter(fsm, state_data, env, params)
    decoder.decode.return_value = MQTTPublishPacket(topic="test/topic", payload=b"test_payload")
    ret = MQTTHandshakeConnAckState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_connected_happy_path(
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: Mock,
    mock_keepalive: Mock,
    mock_select: Mock,
    mock_socket: Mock,
    mock_read: Mock,
    mocker: MockerFixture
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=ConnectedState, error_state=ShutdownState)
    mock_keepalive.get_next_timeout.return_value = 1 if max_wait is None else max_wait

    # Enter state.
    ConnectedState.enter(fsm, state_data, env, params)
    mock_keepalive.mark_init.assert_called_once()
    callbacks.packet.assert_called_once_with(state_data.connack)
    callbacks.reset()
    assert state_data.open_called is True
    assert fsm.state is ConnectedState

    # First handle, nothing to read.
    mock_select.select.return_value = (False, False)
    ret = ConnectedState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    mock_select.select.assert_called_once_with(read=True, write=False, timeout=1 if max_wait is None else max_wait)
    mock_select.reset_mock()
    assert fsm.state is ConnectedState

    # Second handle, send data in write buffer.
    env.write_buffer.extend(b"test_data")
    mock_select.select.return_value = (False, True)
    mock_socket.send.return_value = len(env.write_buffer)
    ret = ConnectedState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    mock_select.select.assert_called_once_with(read=True, write=True, timeout=1 if max_wait is None else max_wait)
    mock_select.reset_mock()
    mock_socket.send.assert_called_once_with(env.write_buffer)
    mock_socket.reset_mock()
    assert len(env.write_buffer) == 0
    mock_keepalive.mark_send.assert_called_once()
    assert fsm.state is ConnectedState

    # Third handle, read data.
    mock_select.select.return_value = (True, False)
    mock_read.side_effect = [True, False]
    ret = ConnectedState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    mock_select.select.assert_called_once_with(read=True, write=False, timeout=1 if max_wait is None else max_wait)
    mock_select.reset_mock()
    assert mock_read.call_count == 2

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_connected_keepalive(
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_keepalive: Mock,
    mock_select: Mock,
    mock_socket: Mock
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=ConnectedState, error_state=ShutdownState)

    ConnectedState.enter(fsm, state_data, env, params)
    callbacks.reset()

    # First handle, send a PINGREQ.
    mock_keepalive.should_send_ping.return_value = True
    mock_select.select.return_value = (False, True)
    mock_socket.send.return_value = len(PING)
    ret = ConnectedState.handle(fsm, state_data, env, params, max_wait)
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
    ret = ConnectedState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("block_exc", [ssl.SSLWantReadError, ssl.SSLWantWriteError, BlockingIOError])
@pytest.mark.parametrize("fatal_exc", [BrokenPipeError, ConnectionResetError])
@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_connected_send_errors(
    max_wait: float | None,
    block_exc: type[Exception],
    fatal_exc: type[Exception],
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_read: Mock,
    mock_select: Mock,
    mock_socket: Mock
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=ConnectedState, error_state=ShutdownState)

    ConnectedState.enter(fsm, state_data, env, params)
    callbacks.reset()

    env.write_buffer.extend(b"test_data")
    mock_read.return_value = False
    mock_select.select.return_value = (True, True)

    mock_socket.send.side_effect = block_exc("TEST")
    ret = ConnectedState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    mock_read.assert_called_once()
    mock_read.reset_mock()
    assert fsm.state is ConnectedState

    mock_select.select.return_value = (True, True)
    mock_socket.send.side_effect = fatal_exc("TEST")
    ret = ConnectedState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    mock_read.assert_not_called()
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_connected_read_closed(
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_read: Mock,
    mock_select: Mock,
    mock_socket: Mock
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=ConnectedState, error_state=ShutdownState)

    ConnectedState.enter(fsm, state_data, env, params)
    callbacks.reset()

    mock_select.select.return_value = (True, False)
    mock_read.side_effect = ClosedSocketError("TEST")
    ret = ConnectedState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_connected_read_mqtt_error(
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_read: Mock,
    mock_select: Mock,
    mock_socket: Mock
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=ConnectedState, error_state=ShutdownState)

    ConnectedState.enter(fsm, state_data, env, params)
    callbacks.reset()

    mock_select.select.return_value = (True, False)
    mock_read.side_effect = MQTTError("TEST", reason_code=MQTTReasonCode.ProtocolError)
    ret = ConnectedState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert state_data.disconnect_rc == MQTTReasonCode.ProtocolError
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


def test_states_connected_read_packet(
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    decoder: Mock,
    mock_keepalive: Mock
) -> None:
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
    pub_packet = MQTTPublishPacket(topic="test/topic", payload=b"test_payload")
    decoder.decode.return_value = pub_packet
    ret = ConnectedState.read_packet(fsm, state_data, env, params)
    assert ret is True
    decoder.decode.assert_called_once()
    decoder.reset_mock()
    callbacks.packet.assert_called_once_with(pub_packet)
    callbacks.reset()
    assert fsm.state is ConnectedState

    # Handle complete PINGRESP packet.
    pong_packet = MQTTPingRespPacket()
    decoder.decode.return_value = pong_packet
    ret = ConnectedState.read_packet(fsm, state_data, env, params)
    assert ret is True
    decoder.decode.assert_called_once()
    decoder.reset_mock()
    mock_keepalive.mark_pong.assert_called_once()
    callbacks.assert_not_called()
    assert fsm.state is ConnectedState

    # Handle complete PINGREQ packet.
    ping_packet = MQTTPingReqPacket()
    decoder.decode.return_value = ping_packet
    ret = ConnectedState.read_packet(fsm, state_data, env, params)
    assert ret is True
    decoder.decode.assert_called_once()
    decoder.reset_mock()
    assert env.write_buffer == PONG
    env.write_buffer.clear()
    callbacks.assert_not_called()
    assert fsm.state is ConnectedState

    # Handle complete DISCONNECT packet.
    dc_packet = MQTTDisconnectPacket()
    decoder.decode.return_value = dc_packet
    ret = ConnectedState.read_packet(fsm, state_data, env, params)
    assert ret is True
    decoder.decode.assert_called_once()
    decoder.reset_mock()
    callbacks.assert_not_called()
    assert fsm.state is ClosingState


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_reconnect_wait_happy_path(
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mocker: MockerFixture,
    mock_timeout: Mock
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"), reconnect_delay=5)
    fsm = FSM(env=env, init_state=ReconnectWaitState, error_state=ShutdownState)
    mock_timeout.get_timeout.return_value = 1 if max_wait is None else max_wait
    mock_cond = mocker.MagicMock(spec=Condition)
    mock_cond.wait.return_value = True
    mock_cond.__enter__.return_value = mock_cond
    fsm.cond = mock_cond

    ReconnectWaitState.enter(fsm, state_data, env, params)
    assert mock_timeout.interval == params.reconnect_delay
    mock_timeout.mark.assert_called_once()
    mock_timeout.reset_mock()

    # First handle, waiting.
    ret = ReconnectWaitState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    mock_timeout.exceeded.assert_called_once()
    mock_timeout.reset_mock()
    if max_wait is None:
        mock_cond.wait.assert_called_once_with(mock_timeout.get_timeout.return_value)
        mock_cond.wait.reset_mock()
    else:
        mock_cond.wait.assert_not_called()
    assert fsm.state is ReconnectWaitState

    # Second handle, transition.
    mock_timeout.exceeded.return_value = True
    ret = ReconnectWaitState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    mock_timeout.exceeded.assert_called_once()
    mock_cond.wait.assert_not_called()
    assert fsm.state is ConnectingState

    callbacks.assert_not_called()


def test_states_reconnect_wait_race(
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mocker: MockerFixture
) -> None:
    """Check for a race condition between state changes and waiting for state changes."""
    params = ConnectParams(address=Address("mqtt://testhost"), reconnect_delay=5)
    fsm = FSM(env=env, init_state=ReconnectWaitState, error_state=ShutdownState)
    mock_cond = mocker.MagicMock(spec=Condition)
    mock_cond.wait.return_value = True
    mock_cond.__enter__.return_value = mock_cond
    fsm.cond = mock_cond

    ReconnectWaitState.enter(fsm, state_data, env, params)
    fsm.change_state(ConnectingState)
    ret = ReconnectWaitState.handle(fsm, state_data, env, params, True)
    assert ret is True

    callbacks.assert_not_called()


@pytest.mark.parametrize("address", ["mqtt://testhost", "mqtts://testhost"])
@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_closing_happy_path(
    address: str,
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_select: Mock,
    mock_socket: Mock,
    mock_timeout: Mock
) -> None:
    params = ConnectParams(address=Address(address), connect_timeout=5)
    fsm = FSM(env=env, init_state=ClosingState, error_state=ShutdownState)
    mock_timeout.get_timeout.return_value = 1 if max_wait is None else max_wait

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
    mock_select.select.return_value = (False, True)
    env.write_buffer.extend(b"test_data")
    mock_socket.send.return_value = len(env.write_buffer)
    ret = ClosingState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    mock_select.select.assert_called_once_with(write=True, timeout=1 if max_wait is None else max_wait)
    mock_select.reset_mock()
    mock_socket.send.assert_called_once_with(env.write_buffer)
    mock_socket.reset_mock()
    mock_timeout.exceeded.assert_called_once()
    mock_timeout.reset_mock()
    assert fsm.state is ClosingState

    # Second handle, done writing, transition to ClosedState.
    ret = ClosingState.handle(fsm, state_data, env, params, max_wait)
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
def test_states_closing_skip(
    prev_state: Any,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_socket: Mock
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"), connect_timeout=5)
    fsm = FSM(env=env, init_state=ClosingState, error_state=ShutdownState)

    fsm.previous_state = prev_state
    ClosingState.enter(fsm, state_data, env, params)
    mock_socket.shutdown.assert_not_called()
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_closing_timeout(
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_timeout: Mock
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"), connect_timeout=5)
    fsm = FSM(env=env, init_state=ClosingState, error_state=ShutdownState)

    fsm.previous_state = ConnectedState
    ClosingState.enter(fsm, state_data, env, params)
    mock_timeout.exceeded.return_value = True
    ret = ClosingState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


def test_states_closing_shutdown_error(
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_socket: Mock
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"), connect_timeout=5)
    fsm = FSM(env=env, init_state=ClosingState, error_state=ShutdownState)

    fsm.previous_state = ConnectedState
    mock_socket.shutdown.side_effect = OSError("TEST")
    ClosingState.enter(fsm, state_data, env, params)
    mock_socket.shutdown.assert_called_once()
    assert fsm.state is ClosingState

    callbacks.assert_not_called()


@pytest.mark.parametrize("exc", [ssl.SSLWantReadError, ssl.SSLWantWriteError, BlockingIOError])
@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_closing_send_error(
    max_wait: float | None,
    exc: type[Exception],
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_select: Mock,
    mock_socket: Mock
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"), connect_timeout=5)
    fsm = FSM(env=env, init_state=ClosingState, error_state=ShutdownState)

    fsm.previous_state = ConnectedState
    ClosingState.enter(fsm, state_data, env, params)

    env.write_buffer.extend(b"test_data")
    mock_select.select.return_value = (False, True)
    mock_socket.send.side_effect = exc("TEST")
    ret = ClosingState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    assert fsm.state is ClosingState

    mock_socket.send.side_effect = BrokenPipeError("TEST")
    ret = ClosingState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("open_called", [True, False])
@pytest.mark.parametrize("rc", [None, MQTTReasonCode.NormalDisconnection])
@pytest.mark.parametrize("reconn_delay", [0, 1])
def test_states_closed_happy_path(
    open_called: bool,
    rc: MQTTReasonCode | None,
    reconn_delay: int,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    decoder: Mock,
    mock_socket: Mock
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"), reconnect_delay=reconn_delay)
    fsm = FSM(env=env, init_state=ClosedState, error_state=ShutdownState)

    fsm.requested_state = ConnectingState
    state_data.disconnect_rc = rc
    state_data.open_called = open_called
    ClosedState.enter(fsm, state_data, env, params)
    if open_called:
        if rc is not None:
            mock_socket.send.assert_called_once_with(MQTTDisconnectPacket().encode())
    mock_socket.shutdown.assert_called_once_with(socket.SHUT_RDWR)
    mock_socket.close.assert_called_once()
    decoder.reset.assert_called_once()
    # We will assert that the write buffer is empty in another test.
    if reconn_delay > 0 and open_called:
        assert fsm.state is ReconnectWaitState
    else:
        assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("exc", [ssl.SSLWantReadError, ssl.SSLWantWriteError, BlockingIOError, OSError])
def test_states_closed_errors(
    exc: type[Exception],
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_socket: Mock
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=ClosedState, error_state=ShutdownState)

    fsm.previous_state = ClosingState
    state_data.open_called = True
    state_data.disconnect_rc = MQTTReasonCode.NormalDisconnection
    mock_socket.send.side_effect = exc("TEST")
    mock_socket.shutdown.side_effect = OSError("TEST")
    mock_socket.close.side_effect = OSError("TEST")
    ClosedState.enter(fsm, state_data, env, params)
    mock_socket.send.assert_called_once_with(MQTTDisconnectPacket().encode())
    mock_socket.shutdown.assert_called_once_with(socket.SHUT_RDWR)
    mock_socket.close.assert_called_once()
    assert state_data.open_called is False
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


def test_states_closed_write_buffer_not_empty(
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_socket: Mock
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=ClosedState, error_state=ShutdownState)

    fsm.previous_state = ConnectingState
    env.write_buffer.extend(b"test_data")
    ClosedState.enter(fsm, state_data, env, params)
    mock_socket.send.assert_not_called()
    assert len(env.write_buffer) == 0
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("close_exc", [OSError, None])
@pytest.mark.parametrize("open_called", [True, False])
def test_states_shutdown_enter(
    open_called: bool,
    close_exc: type[Exception] | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: Mock,
    decoder: Mock,
    mocker: MockerFixture,
    mock_socket: Mock
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=ShutdownState, error_state=ShutdownState)

    env.write_buffer = mocker.Mock()
    state_data.open_called = open_called
    mock_socket.close.side_effect = close_exc("TEST") if close_exc else None
    ShutdownState.enter(fsm, state_data, env, params)
    if open_called:
        pass
    mock_socket.close.assert_called_once()
    decoder.reset.assert_called_once()
    env.write_buffer.clear.assert_called_once()
    assert fsm.state is ShutdownState

    callbacks.assert_not_called()
