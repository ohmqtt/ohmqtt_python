from __future__ import annotations

from collections import deque
import errno
import socket
import ssl
from threading import Condition
from typing import Any, cast
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
    WebsocketHandshakeRequestState,
    WebsocketHandshakeResponseState,
)
from ohmqtt.connection.states.base import FSMState
from ohmqtt.connection.states.connecting import _resolve
from ohmqtt.connection.timeout import Timeout
from ohmqtt.connection.types import ConnectParams, StateData, StateEnvironment
from ohmqtt.connection.ws_decoder import WebsocketDecoder
from ohmqtt.connection.wslib import (
    OpCode,
    apply_mask,
    frame_ws_data,
    generate_nonce,
    generate_handshake_key,
    WebsocketError,
)
from ohmqtt.error import MQTTError
from ohmqtt.mqtt_spec import MQTTReasonCode
from ohmqtt.packet import (
    decode_packet,
    MQTTPacket,
    MQTTConnectPacket,
    MQTTConnAckPacket,
    MQTTDisconnectPacket,
    MQTTPublishPacket,
    MQTTPingReqPacket,
    MQTTPingRespPacket,
    MQTTSubscribePacket,
    MQTTUnsubscribePacket,
    PING,
)
from ohmqtt.platform import AF_UNIX, HAS_AF_UNIX
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
    mock_socket.pending.return_value = 0
    mock_socket.getsockopt.return_value = 0
    mocker.patch("ohmqtt.connection.states.connecting._get_socket", return_value=mock_socket)
    return mock_socket  # type: ignore[no-any-return]

@pytest.fixture
def mock_resolve(mocker: MockerFixture) -> Mock:
    def resolve(address: Address) -> list[tuple[socket.AddressFamily, Any]]:
        if address.scheme == "unix":
            return [(AF_UNIX, address.host)]
        return [(socket.AF_INET, (address.host, address.port))]
    return mocker.patch("ohmqtt.connection.states.connecting._resolve", side_effect=resolve)

@pytest.fixture
def decoder(mocker: MockerFixture) -> Mock:
    return mocker.Mock(spec=IncrementalDecoder)  # type: ignore[no-any-return]

@pytest.fixture
def ws_decoder(mocker: MockerFixture) -> Mock:
    return mocker.Mock(spec=WebsocketDecoder)  # type: ignore[no-any-return]

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
def state_data(decoder: Mock, mock_socket: Mock, mock_keepalive: Mock, mock_timeout: Mock, ws_decoder: Mock) -> StateData:
    data = StateData()
    data.decoder = decoder
    data.keepalive = mock_keepalive
    data.sock = mock_socket
    data.timeout = mock_timeout
    data.ws_decoder = ws_decoder
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


@pytest.mark.parametrize("address", [
    "mqtt://testhost", "mqtts://testhost",
    "ws://testhost", "wss://testhost",
    "unix:///testpath",
])
@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_connecting_happy_path(
    address: str,
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    decoder: Mock,
    mock_resolve: Mock,
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
    env.connect_error = ConnectionRefusedError("TEST")
    ConnectingState.enter(fsm, state_data, env, params)
    assert env.connect_error is None
    decoder.reset.assert_called_once()
    decoder.reset_mock()
    assert mock_timeout.interval == params.connect_timeout
    mock_timeout.mark.assert_called_once()
    mock_timeout.reset_mock()
    assert state_data.disconnect_rc is None
    assert fsm.state is ConnectingState

    # First handle, begin non-blocking connect.
    mock_select.select.return_value = (False, False)
    mock_socket.connect.side_effect = BlockingIOError
    ret = ConnectingState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    mock_resolve.assert_called_once_with(params.address)
    mock_socket.setblocking.assert_called_once_with(False)
    if params.address.scheme != "unix":
        mock_socket.setsockopt.assert_called_once_with(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    mock_select.change_sock.assert_called_once_with(mock_socket)
    mock_select.select.assert_called_once_with(write=True, timeout=1 if max_wait is None else max_wait)
    mock_select.reset_mock()
    if params.address.scheme == "unix":
        mock_socket.connect.assert_called_once_with(params.address.host)
    else:
        mock_socket.connect.assert_called_once_with((params.address.host, params.address.port))
    mock_socket.reset_mock()
    assert fsm.state is ConnectingState

    # Second handle, still connecting.  connect() is only called once.
    ret = ConnectingState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    mock_socket.connect.assert_not_called()
    mock_select.reset_mock()
    assert fsm.state is ConnectingState

    # Third handle, finish non-blocking connect.
    mock_select.select.return_value = (False, True)
    ret = ConnectingState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    mock_select.select.assert_called_once_with(write=True, timeout=1 if max_wait is None else max_wait)
    mock_select.reset_mock()
    mock_socket.connect.assert_not_called()
    mock_socket.getsockopt.assert_called_once_with(socket.SOL_SOCKET, socket.SO_ERROR)
    mock_socket.reset_mock()
    if params.address.use_tls:
        assert fsm.state is TLSHandshakeState
    elif params.address.is_websocket():
        assert fsm.state is WebsocketHandshakeRequestState
    else:
        assert fsm.state is MQTTHandshakeConnectState

    callbacks.assert_not_called()


@pytest.mark.parametrize(("previous_state", "was_connected"), [(ClosedState, False), (ReconnectWaitState, True)])
def test_states_connecting_was_connected(
    previous_state: type[FSMState],
    was_connected: bool,
    state_data: StateData,
    env: StateEnvironment,
    mock_select: Mock,
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=ConnectingState, error_state=ShutdownState)
    fsm.previous_state = previous_state
    state_data.was_connected = True
    ConnectingState.enter(fsm, state_data, env, params)
    assert state_data.was_connected is was_connected


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
    assert isinstance(env.connect_error, TimeoutError)

    callbacks.assert_not_called()


@pytest.mark.parametrize("exc", [
    ConnectionRefusedError("TEST"),
    socket.gaierror(socket.EAI_NONAME, "Name or service not known"),
    OSError("TEST"),
])
@pytest.mark.parametrize("address", ["mqtt://testhost", "mqtts://testhost", "unix:///testpath"])
@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_connecting_error(
    address: str,
    max_wait: float | None,
    exc: Exception,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_resolve: Mock,
    mock_select: Mock,
    mock_socket: Mock
) -> None:
    if address.startswith("unix:") and not HAS_AF_UNIX:
        pytest.skip("Unix socket not supported on this platform")
    params = ConnectParams(address=Address(address))
    fsm = FSM(env=env, init_state=ConnectingState, error_state=ShutdownState)

    mock_socket.connect.side_effect = exc
    ConnectingState.enter(fsm, state_data, env, params)
    ret = ConnectingState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState
    assert env.connect_error is exc
    mock_select.select.assert_not_called()

    callbacks.assert_not_called()


@pytest.mark.parametrize("address", ["mqtt://testhost", "mqtts://testhost", "unix:///testpath"])
@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_connecting_failed_in_progress(
    address: str,
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_resolve: Mock,
    mock_select: Mock,
    mock_socket: Mock
) -> None:
    if address.startswith("unix:") and not HAS_AF_UNIX:
        pytest.skip("Unix socket not supported on this platform")
    params = ConnectParams(address=Address(address))
    fsm = FSM(env=env, init_state=ConnectingState, error_state=ShutdownState)

    mock_socket.connect.side_effect = BlockingIOError
    ConnectingState.enter(fsm, state_data, env, params)
    mock_select.select.return_value = (False, True)
    mock_socket.getsockopt.return_value = errno.ECONNREFUSED
    ret = ConnectingState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState
    assert isinstance(env.connect_error, ConnectionRefusedError)

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_connecting_resolve_error(
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_resolve: Mock,
    mock_select: Mock,
    mock_socket: Mock
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=ConnectingState, error_state=ShutdownState)

    exc = socket.gaierror(socket.EAI_NONAME, "Name or service not known")
    mock_resolve.side_effect = exc
    ConnectingState.enter(fsm, state_data, env, params)
    ret = ConnectingState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState
    assert env.connect_error is exc
    mock_socket.connect.assert_not_called()
    mock_select.select.assert_not_called()

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_connecting_next_address(
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_resolve: Mock,
    mock_select: Mock,
    mocker: MockerFixture,
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=ConnectingState, error_state=ShutdownState)
    addresses = [
        (socket.AF_INET6, ("2001:db8::1", 1883, 0, 0)),
        (socket.AF_INET6, ("2001:db8::2", 1883, 0, 0)),
        (socket.AF_INET, ("192.0.2.1", 1883)),
    ]
    mock_resolve.side_effect = None
    mock_resolve.return_value = list(addresses)
    socks = [mocker.Mock(spec=socket.socket) for _ in addresses]
    get_socket = mocker.patch("ohmqtt.connection.states.connecting._get_socket", side_effect=socks)
    # The first address fails immediately, the second fails in progress, the third connects.
    socks[0].connect.side_effect = OSError(errno.ENETUNREACH, "Network is unreachable")
    socks[1].connect.side_effect = BlockingIOError
    socks[1].getsockopt.return_value = errno.ECONNREFUSED
    socks[2].connect.side_effect = BlockingIOError
    socks[2].getsockopt.return_value = 0
    mock_select.select.return_value = (False, True)

    ConnectingState.enter(fsm, state_data, env, params)
    ret = ConnectingState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    assert fsm.state is ConnectingState
    socks[0].close.assert_called_once()
    socks[1].close.assert_called_once()
    socks[2].close.assert_not_called()

    ret = ConnectingState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is MQTTHandshakeConnectState
    assert state_data.sock is socks[2]
    assert get_socket.call_args_list == [mocker.call(family) for family, _ in addresses]
    for sock, (_, sockaddr) in zip(socks, addresses):
        sock.connect.assert_called_once_with(sockaddr)
    assert mock_select.change_sock.call_args_list == [mocker.call(sock) for sock in socks]

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_connecting_all_addresses_fail(
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_resolve: Mock,
    mock_select: Mock,
    mocker: MockerFixture,
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=ConnectingState, error_state=ShutdownState)
    mock_resolve.side_effect = None
    mock_resolve.return_value = [
        (socket.AF_INET6, ("2001:db8::1", 1883, 0, 0)),
        (socket.AF_INET, ("192.0.2.1", 1883)),
    ]
    socks = [mocker.Mock(spec=socket.socket) for _ in range(2)]
    mocker.patch("ohmqtt.connection.states.connecting._get_socket", side_effect=socks)
    socks[0].connect.side_effect = BlockingIOError
    socks[0].getsockopt.return_value = errno.ECONNREFUSED
    last_exc = OSError(errno.ENETUNREACH, "Network is unreachable")
    socks[1].connect.side_effect = last_exc
    mock_select.select.return_value = (False, True)

    ConnectingState.enter(fsm, state_data, env, params)
    ret = ConnectingState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState
    assert env.connect_error is last_exc

    callbacks.assert_not_called()


@pytest.mark.parametrize(("address", "expected"), [
    ("mqtt://127.0.0.1:1234", [(socket.AF_INET, ("127.0.0.1", 1234))]),
    ("mqtt://[::1]:1234", [(socket.AF_INET6, ("::1", 1234, 0, 0))]),
])
def test_states_connecting_resolve(address: str, expected: list[tuple[socket.AddressFamily, Any]]) -> None:
    assert _resolve(Address(address)) == expected


def test_states_connecting_resolve_unix() -> None:
    if not HAS_AF_UNIX:
        pytest.skip("Unix socket not supported on this platform")
    assert _resolve(Address("unix:///testpath")) == [(AF_UNIX, "/testpath")]


@pytest.mark.parametrize("address", ["mqtts://testhost", "wss://testhost"])
@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_tls_handshake_happy_path(
    address: str,
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_select: Mock,
    mock_socket: Mock,
    mock_timeout: Mock,
    mocker: MockerFixture
) -> None:
    params = ConnectParams(address=Address(address), tls_context=mocker.Mock())
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
    if params.address.is_websocket():
        assert fsm.state is WebsocketHandshakeRequestState
    else:
        assert fsm.state is MQTTHandshakeConnectState

    callbacks.assert_not_called()


@pytest.mark.parametrize("exc", [
    ssl.SSLCertVerificationError("certificate verify failed"),
    ssl.SSLError("wrong version number"),
    ConnectionResetError("TEST"),
])
@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_tls_handshake_error(
    max_wait: float | None,
    exc: Exception,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_select: Mock,
    mock_socket: Mock,
    mocker: MockerFixture
) -> None:
    params = ConnectParams(address=Address("mqtts://testhost"), tls_context=mocker.Mock())
    fsm = FSM(env=env, init_state=TLSHandshakeState, error_state=ShutdownState)

    params.tls_context.wrap_socket.return_value = mock_socket  # type: ignore[union-attr]
    TLSHandshakeState.enter(fsm, state_data, env, params)
    mock_socket.do_handshake.side_effect = exc
    ret = TLSHandshakeState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState
    assert env.connect_error is exc
    mock_select.select.assert_not_called()

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
    assert isinstance(env.connect_error, TimeoutError)

    callbacks.assert_not_called()


def test_states_tls_handshake_default_context_options(
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mocker: MockerFixture
) -> None:
    params = ConnectParams(address=Address("mqtts://testhost"), tls_context=None)
    fsm = FSM(env=env, init_state=TLSHandshakeState, error_state=ShutdownState)
    state_data.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    TLSHandshakeState.enter(fsm, state_data, env, params)
    tls_context = cast(ssl.SSLSocket, state_data.sock).context
    assert tls_context.minimum_version == ssl.TLSVersion.TLSv1_3

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_mqtt_connect_happy_path(
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: Mock,
    mock_select: Mock,
    mock_socket: Mock,
    mock_timeout: Mock,
    mocker: MockerFixture
) -> None:
    address = "mqtt://testhost"
    mock_write_buffer = mocker.Mock()
    state_data.write_buffer = mock_write_buffer
    params = ConnectParams(
        address=Address(address),
        client_id="test_client",
        keepalive_interval=60,
        clean_start=True,
        username="johndoe",
        password=b"secret",
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
    mock_write_buffer.clear.assert_called_once()
    mock_write_buffer.extend.assert_called_once()
    data = mock_write_buffer.extend.call_args[0][0]
    mock_write_buffer.reset_mock()
    state_data.write_buffer = bytearray(data)
    if params.address.is_websocket():
        opcode = data[0] & 0x0F
        assert opcode == OpCode.BINARY
        length = data[1] & 0x7F
        payload_offset = 2
        if length == 126:
            payload_offset = 4
        elif length == 127:
            payload_offset = 10
        mask = data[payload_offset:payload_offset + 4]
        masked_payload = data[payload_offset + 4:]
        unmasked_payload = bytearray(b ^ mask[i % 4] for i, b in enumerate(masked_payload))
        packet = decode_packet(bytes(unmasked_payload))
    else:
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
    assert packet.username == params.username
    assert packet.password == params.password
    assert fsm.state is MQTTHandshakeConnectState

    # First handle, blocked write.
    if params.address.use_tls:
        mock_socket.send.side_effect = ssl.SSLWantWriteError
    else:
        mock_socket.send.side_effect = BlockingIOError
    ret = MQTTHandshakeConnectState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    mock_socket.send.assert_called_once_with(state_data.write_buffer)
    mock_socket.reset_mock()
    assert fsm.state is MQTTHandshakeConnectState
    if max_wait:
        mock_select.select.assert_called_once_with(write=True, timeout=1)

    # Second handle, complete.
    mock_socket.send.side_effect = None
    mock_socket.send.return_value = len(data)
    ret = MQTTHandshakeConnectState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    mock_socket.send.assert_called_once_with(state_data.write_buffer)
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
    assert isinstance(env.connect_error, TimeoutError)

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
    mock_write_buffer = mocker.Mock()
    state_data.write_buffer = mock_write_buffer
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=MQTTHandshakeConnectState, error_state=ShutdownState)
    mock_timeout.get_timeout.return_value = 1 if max_wait is None else max_wait

    # One byte on the first write.
    MQTTHandshakeConnectState.enter(fsm, state_data, env, params)
    data = mock_write_buffer.extend.call_args[0][0]
    mock_write_buffer.reset_mock()
    state_data.write_buffer = bytearray(data)
    mock_socket.send.side_effect = None
    mock_socket.send.return_value = 1
    ret = MQTTHandshakeConnectState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    mock_socket.send.assert_called_once_with(state_data.write_buffer)
    mock_socket.reset_mock()
    assert state_data.write_buffer == bytearray(data[1:])
    assert fsm.state is MQTTHandshakeConnectState

    # The rest on the second write.
    mock_socket.send.return_value = len(data) - 1
    ret = MQTTHandshakeConnectState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    mock_socket.send.assert_called_once_with(state_data.write_buffer)
    mock_socket.reset_mock()
    assert state_data.write_buffer == bytearray()
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
    assert isinstance(env.connect_error, ConnectionError)

    callbacks.assert_not_called()


@pytest.mark.parametrize("exc", [BrokenPipeError, ConnectionResetError, ssl.SSLEOFError])
@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_mqtt_connect_send_error(
    max_wait: float | None,
    exc: type[Exception],
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_socket: Mock
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=MQTTHandshakeConnectState, error_state=ShutdownState)

    MQTTHandshakeConnectState.enter(fsm, state_data, env, params)
    mock_socket.send.side_effect = exc("TEST")
    ret = MQTTHandshakeConnectState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState
    assert isinstance(env.connect_error, exc)

    callbacks.assert_not_called()


@pytest.mark.parametrize("address", ["mqtt://testhost", "ws://testhost"])
@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_mqtt_connack_happy_path(
    address: str,
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    decoder: Mock,
    mock_select: Mock,
    mock_socket: Mock,
    mock_timeout: Mock,
    ws_decoder: Mock,
) -> None:
    params = ConnectParams(address=Address(address))
    fsm = FSM(env=env, init_state=MQTTHandshakeConnAckState, error_state=ShutdownState)
    mock_timeout.get_timeout.return_value = 1 if max_wait is None else max_wait

    # Enter state.
    MQTTHandshakeConnAckState.enter(fsm, state_data, env, params)
    assert fsm.state is MQTTHandshakeConnAckState

    # First handle, blocked read.
    decoder.decode.return_value = None
    ws_decoder.decode.return_value = None
    mock_select.select.return_value = (True, False)
    ret = MQTTHandshakeConnAckState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    mock_select.select.assert_called_once_with(read=True, timeout=1 if max_wait is None else max_wait)
    mock_select.reset_mock()
    assert fsm.state is MQTTHandshakeConnAckState

    # Second handle, complete.
    connack = MQTTConnAckPacket(properties=MQTTConnAckProps(ServerKeepAlive=23))
    decoder.decode.return_value = connack
    ws_decoder.decode.return_value = (OpCode.BINARY, connack.encode())
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
    assert isinstance(env.connect_error, TimeoutError)

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
    assert isinstance(env.connect_error, ClosedSocketError)

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
    assert isinstance(env.connect_error, MQTTError)
    assert env.connect_error.reason_code == MQTTReasonCode.ProtocolError

    callbacks.assert_not_called()


@pytest.mark.parametrize("reason_code", [MQTTReasonCode.UnspecifiedError, MQTTReasonCode.NotAuthorized])
@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_mqtt_connack_refused(
    max_wait: float | None,
    reason_code: MQTTReasonCode,
    callbacks: EnvironmentCallbacks,
    decoder: Mock,
    state_data: StateData,
    env: StateEnvironment
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"))
    fsm = FSM(env=env, init_state=MQTTHandshakeConnAckState, error_state=ShutdownState)

    MQTTHandshakeConnAckState.enter(fsm, state_data, env, params)
    state_data.connack = None
    decoder.decode.return_value = MQTTConnAckPacket(reason_code=reason_code)
    ret = MQTTHandshakeConnAckState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState
    assert isinstance(env.connect_error, MQTTError)
    assert env.connect_error.reason_code == reason_code
    assert state_data.connack is None

    callbacks.assert_not_called()


@pytest.mark.parametrize("address", ["mqtt://testhost", "ws://testhost"])
@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_mqtt_connack_malformed(
    address: str,
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    decoder: Mock,
    ws_decoder: Mock,
    state_data: StateData,
    env: StateEnvironment
) -> None:
    params = ConnectParams(address=Address(address))
    fsm = FSM(env=env, init_state=MQTTHandshakeConnAckState, error_state=ShutdownState)

    MQTTHandshakeConnAckState.enter(fsm, state_data, env, params)
    decoder.decode.side_effect = MQTTError("TEST", MQTTReasonCode.MalformedPacket)
    ws_decoder.decode.return_value = (OpCode.BINARY, b"\x20\x01\x00")
    ret = MQTTHandshakeConnAckState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState
    assert isinstance(env.connect_error, MQTTError)

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_mqtt_connack_ws_pong(
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    ws_decoder: Mock,
    state_data: StateData,
    env: StateEnvironment,
    mock_select: Mock,
) -> None:
    params = ConnectParams(address=Address("ws://testhost"))
    fsm = FSM(env=env, init_state=MQTTHandshakeConnAckState, error_state=ShutdownState)

    MQTTHandshakeConnAckState.enter(fsm, state_data, env, params)
    ws_decoder.decode.return_value = (OpCode.PONG, b"")
    ret = MQTTHandshakeConnAckState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    assert fsm.state is MQTTHandshakeConnAckState
    mock_select.select.assert_called_once()

    ws_decoder.decode.return_value = (OpCode.BINARY, MQTTConnAckPacket().encode())
    ret = MQTTHandshakeConnAckState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ConnectedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_mqtt_connack_ws_protocol_error(
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    ws_decoder: Mock,
    state_data: StateData,
    env: StateEnvironment
) -> None:
    params = ConnectParams(address=Address("ws://testhost"))
    fsm = FSM(env=env, init_state=MQTTHandshakeConnAckState, error_state=ShutdownState)

    MQTTHandshakeConnAckState.enter(fsm, state_data, env, params)
    ws_decoder.decode.return_value = (OpCode.TEXT, b"invalid data")
    ret = MQTTHandshakeConnAckState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState
    assert isinstance(env.connect_error, WebsocketError)

    callbacks.assert_not_called()


@pytest.mark.parametrize("address", ["mqtt://testhost", "ws://testhost"])
@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_connected_happy_path(
    address: str,
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
    params = ConnectParams(address=Address(address))
    fsm = FSM(env=env, init_state=ConnectedState, error_state=ShutdownState)
    mock_keepalive.get_next_timeout.return_value = 1 if max_wait is None else max_wait
    fsm.env.packet_buffer = deque()

    # Enter state.
    ConnectedState.enter(fsm, state_data, env, params)
    mock_keepalive.mark_init.assert_called_once()
    callbacks.packet.assert_called_once_with(state_data.connack)
    callbacks.reset()
    assert state_data.open_called is True
    assert state_data.was_connected is True
    assert fsm.state is ConnectedState

    # First handle, nothing to read.
    mock_select.select.return_value = (False, False)
    ret = ConnectedState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    mock_select.select.assert_called_once_with(read=True, write=False, timeout=1 if max_wait is None else max_wait)
    mock_select.reset_mock()
    assert fsm.state is ConnectedState

    # Second handle, send data in write buffer.
    packet = MQTTPublishPacket(topic="test/topic", payload=b"test_payload")
    fsm.env.packet_buffer.append(packet)
    mock_select.select.return_value = (False, True)
    if params.address.is_websocket():
        mock_socket.send.return_value = len(frame_ws_data(OpCode.BINARY, packet.encode()))
    else:
        mock_socket.send.return_value = len(packet.encode())
    ret = ConnectedState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    mock_select.select.assert_called_once_with(read=True, write=True, timeout=1 if max_wait is None else max_wait)
    mock_select.reset_mock()
    mock_socket.send.assert_called_once_with(state_data.write_buffer)
    mock_socket.reset_mock()
    assert len(state_data.write_buffer) == 0
    mock_keepalive.mark_send.assert_called_once()
    assert fsm.state is ConnectedState

    # Third handle, read data.
    mock_select.select.return_value = (True, False)
    mock_read.side_effect = [True, False]
    ret = ConnectedState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    mock_select.select.assert_called_once_with(read=True, write=False, timeout=1 if max_wait is None else max_wait)
    mock_select.reset_mock()
    assert mock_read.call_count == 1

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_connected_tls_pending(
    max_wait: float | None,
    state_data: StateData,
    env: Mock,
    mock_keepalive: Mock,
    mock_select: Mock,
    mock_socket: Mock,
    mock_read: Mock,
) -> None:
    params = ConnectParams(address=Address("mqtts://testhost"))
    fsm = FSM(env=env, init_state=ConnectedState, error_state=ShutdownState)
    mock_keepalive.get_next_timeout.return_value = 1 if max_wait is None else max_wait
    fsm.env.packet_buffer = deque()
    ConnectedState.enter(fsm, state_data, env, params)

    mock_socket.pending.return_value = 10
    mock_select.select.return_value = (False, False)
    ret = ConnectedState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    mock_select.select.assert_called_once_with(read=True, write=False, timeout=0)
    assert mock_read.call_count == 1


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
    mock_socket.send.assert_called_once_with(state_data.write_buffer)
    mock_socket.reset_mock()
    assert len(state_data.write_buffer) == 0

    # Second handle, keepalive timeout.
    mock_keepalive.should_send_ping.return_value = False
    mock_keepalive.should_close.return_value = True
    ret = ConnectedState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("block_exc", [ssl.SSLWantReadError, ssl.SSLWantWriteError, BlockingIOError])
@pytest.mark.parametrize("fatal_exc", [BrokenPipeError, ConnectionResetError, TimeoutError, ssl.SSLEOFError, OSError])
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

    fsm.env.packet_buffer.append(MQTTPublishPacket(topic="test/topic", payload=b"test_payload"))
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


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_connected_read_websocket_error(
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
    mock_read.side_effect = WebsocketError("TEST")
    ret = ConnectedState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert state_data.disconnect_rc is None
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
    ConnectedState.read_packet(fsm, state_data, env, params)
    decoder.decode.assert_called_once()
    decoder.reset_mock()
    assert fsm.state is ConnectedState

    # Handle complete PUBLISH packet.
    pub_packet = MQTTPublishPacket(topic="test/topic", payload=b"test_payload")
    decoder.decode.return_value = pub_packet
    ConnectedState.read_packet(fsm, state_data, env, params)
    decoder.decode.assert_called_once()
    decoder.reset_mock()
    callbacks.packet.assert_called_once_with(pub_packet)
    callbacks.reset()
    assert fsm.state is ConnectedState

    # Handle complete PINGRESP packet.
    pong_packet = MQTTPingRespPacket()
    decoder.decode.return_value = pong_packet
    ConnectedState.read_packet(fsm, state_data, env, params)
    decoder.decode.assert_called_once()
    decoder.reset_mock()
    mock_keepalive.mark_pong.assert_called_once()
    callbacks.assert_not_called()
    assert fsm.state is ConnectedState

    # Handle complete PINGREQ packet.
    ping_packet = MQTTPingReqPacket()
    decoder.decode.return_value = ping_packet
    ConnectedState.read_packet(fsm, state_data, env, params)
    decoder.decode.assert_called_once()
    decoder.reset_mock()
    assert fsm.env.packet_buffer.popleft() == MQTTPingRespPacket()
    fsm.env.packet_buffer.clear()
    callbacks.assert_not_called()
    assert fsm.state is ConnectedState

    # Handle complete DISCONNECT packet.
    dc_packet = MQTTDisconnectPacket()
    decoder.decode.return_value = dc_packet
    ConnectedState.read_packet(fsm, state_data, env, params)
    decoder.decode.assert_called_once()
    decoder.reset_mock()
    callbacks.assert_not_called()
    assert fsm.state is ClosingState

    # Handle invalid packets.
    for pt in (MQTTConnectPacket, MQTTSubscribePacket, MQTTUnsubscribePacket):
        packet: MQTTPacket = pt()
        decoder.decode.return_value = packet
        with pytest.raises(MQTTError) as excinfo:
            ConnectedState.read_packet(fsm, state_data, env, params)
        assert excinfo.value.reason_code == MQTTReasonCode.ProtocolError


def test_states_connected_read_packet_ws(
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    ws_decoder: Mock,
    mock_keepalive: Mock
) -> None:
    params = ConnectParams(address=Address("ws://testhost"))
    fsm = FSM(env=env, init_state=ConnectedState, error_state=ShutdownState)

    # Handle incomplete packet.
    ws_decoder.decode.return_value = None
    ConnectedState.read_packet(fsm, state_data, env, params)
    ws_decoder.decode.assert_called_once()
    ws_decoder.reset_mock()
    assert fsm.state is ConnectedState

    # Handle complete PUBLISH packet.
    pub_packet = MQTTPublishPacket(topic="test/topic", payload=b"test_payload")
    ws_decoder.decode.return_value = OpCode.BINARY, pub_packet.encode()
    ConnectedState.read_packet(fsm, state_data, env, params)
    ws_decoder.decode.assert_called_once()
    ws_decoder.reset_mock()
    callbacks.packet.assert_called_once_with(pub_packet)
    callbacks.reset()
    assert fsm.state is ConnectedState

    # Handle complete WebSocket PING frame.
    ws_decoder.decode.return_value = OpCode.PING, b"\x88"
    ConnectedState.read_packet(fsm, state_data, env, params)
    ws_decoder.decode.assert_called_once()
    ws_decoder.reset_mock()
    assert state_data.write_buffer[0] == 0x80 | OpCode.PONG.value
    assert state_data.write_buffer[1] == 0x81
    mask = state_data.write_buffer[2:6]
    payload = state_data.write_buffer[6:]
    unmasked_payload = bytearray(b ^ mask[i % 4] for i, b in enumerate(payload))
    assert unmasked_payload == b"\x88"
    state_data.write_buffer.clear()
    callbacks.assert_not_called()
    assert fsm.state is ConnectedState

    # Ignore unsolicited WebSocket PONG frame.
    ws_decoder.decode.return_value = OpCode.PONG, b""
    ConnectedState.read_packet(fsm, state_data, env, params)
    assert not state_data.write_buffer
    callbacks.assert_not_called()
    assert fsm.state is ConnectedState

    # Handle WebSocket CLOSE frame.
    ws_decoder.decode.return_value = OpCode.CLOSE, b""
    with pytest.raises(ClosedSocketError):
        ConnectedState.read_packet(fsm, state_data, env, params)

    # Handle bad opcode in WebSocket frame.
    ws_decoder.decode.return_value = OpCode.TEXT, b"asdf"
    with pytest.raises(WebsocketError):
        ConnectedState.read_packet(fsm, state_data, env, params)


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


@pytest.mark.parametrize("request_change", [False, True])
def test_states_reconnect_wait_race(
    request_change: bool,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mocker: MockerFixture
) -> None:
    """Check for a race condition between state changes or requests and waiting for state changes."""
    params = ConnectParams(address=Address("mqtt://testhost"), reconnect_delay=5)
    fsm = FSM(env=env, init_state=ReconnectWaitState, error_state=ShutdownState)
    mock_cond = mocker.MagicMock(spec=Condition)
    mock_cond.wait.return_value = True
    mock_cond.__enter__.return_value = mock_cond
    fsm.cond = mock_cond

    ReconnectWaitState.enter(fsm, state_data, env, params)
    if request_change:
        fsm.request_state(ClosingState)
    else:
        fsm.change_state(ConnectingState)
    ret = ReconnectWaitState.handle(fsm, state_data, env, params, None)
    assert ret is True
    mock_cond.wait.assert_not_called()

    callbacks.assert_not_called()


@pytest.mark.parametrize("init_dc_rc", [MQTTReasonCode.UnspecifiedError, None])
@pytest.mark.parametrize("address", ["mqtt://testhost", "mqtts://testhost"])
@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_closing_happy_path(
    init_dc_rc: MQTTReasonCode | None,
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
    state_data.disconnect_rc = init_dc_rc

    # Start from ConnectedState to test transition through ClosingState to ClosedState.
    fsm.previous_state = ConnectedState
    ClosingState.enter(fsm, state_data, env, params)
    assert mock_timeout.interval == params.connect_timeout
    mock_timeout.mark.assert_called_once()
    mock_timeout.reset_mock()
    assert state_data.disconnect_rc == MQTTReasonCode.NormalDisconnection if init_dc_rc is None else init_dc_rc
    if not params.address.use_tls:
        mock_socket.shutdown.assert_called_once_with(socket.SHUT_RD)
    else:
        mock_socket.shutdown.assert_not_called()
    mock_socket.reset_mock()
    assert fsm.state is ClosingState

    state_data.write_buffer.extend(b"test_data")

    # First handle, write blocked.
    mock_select.select.return_value = (False, False)
    ret = ClosingState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    mock_select.select.assert_called_once_with(write=True, timeout=1 if max_wait is None else max_wait)
    mock_select.reset_mock()
    mock_timeout.exceeded.assert_called_once()
    mock_timeout.reset_mock()
    assert fsm.state is ClosingState

    # Second handle, write all.
    mock_select.select.return_value = (False, True)
    mock_socket.send.return_value = len(state_data.write_buffer)
    ret = ClosingState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    mock_select.select.assert_called_once_with(write=True, timeout=1 if max_wait is None else max_wait)
    mock_select.reset_mock()
    mock_socket.send.assert_called_once_with(state_data.write_buffer)
    mock_socket.reset_mock()
    mock_timeout.exceeded.assert_called_once()
    mock_timeout.reset_mock()
    assert fsm.state is ClosingState

    # Third handle, done writing, transition to ClosedState.
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
@pytest.mark.parametrize("fatal_exc", [BrokenPipeError, ConnectionResetError, ssl.SSLEOFError])
@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_closing_send_error(
    max_wait: float | None,
    exc: type[Exception],
    fatal_exc: type[Exception],
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

    state_data.write_buffer.extend(b"test_data")
    mock_select.select.return_value = (False, True)
    mock_socket.send.side_effect = exc("TEST")
    ret = ClosingState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    assert fsm.state is ClosingState

    mock_socket.send.side_effect = fatal_exc("TEST")
    ret = ClosingState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState

    callbacks.assert_not_called()


@pytest.mark.parametrize("address", ["mqtt://testhost", "ws://testhost"])
@pytest.mark.parametrize("open_called", [True, False])
@pytest.mark.parametrize("was_connected", [True, False])
@pytest.mark.parametrize("requested_state", [ConnectingState, ClosingState])
@pytest.mark.parametrize("rc", [None, MQTTReasonCode.NormalDisconnection])
@pytest.mark.parametrize("reconn_delay", [0, 1])
def test_states_closed_happy_path(
    address: str,
    open_called: bool,
    was_connected: bool,
    requested_state: type[FSMState],
    rc: MQTTReasonCode | None,
    reconn_delay: int,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    decoder: Mock,
    mock_socket: Mock
) -> None:
    params = ConnectParams(address=Address(address), reconnect_delay=reconn_delay)
    fsm = FSM(env=env, init_state=ClosedState, error_state=ShutdownState)

    fsm.requested_state = requested_state
    state_data.disconnect_rc = rc
    state_data.open_called = open_called
    state_data.was_connected = was_connected
    ClosedState.enter(fsm, state_data, env, params)
    if open_called:
        if rc is not None:
            expected_data = MQTTDisconnectPacket(rc).encode()
            mock_socket.send.assert_called_once()
            data = mock_socket.send.call_args[0][0]
            if params.address.is_websocket():
                assert data[0] == OpCode.BINARY.value | 0x80
                assert data[1] == len(expected_data) | 0x80
                mask = data[2:6]
                masked_data = data[6:]
                unmasked_data = apply_mask(mask, masked_data)
                assert unmasked_data == expected_data
            else:
                assert data == expected_data
    mock_socket.shutdown.assert_called_once_with(socket.SHUT_RDWR)
    mock_socket.close.assert_called_once()
    decoder.reset.assert_called_once()
    # We will assert that the write buffer is empty in another test.
    if reconn_delay > 0 and was_connected and requested_state is ConnectingState:
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
    state_data.write_buffer.extend(b"test_data")
    ClosedState.enter(fsm, state_data, env, params)
    mock_socket.send.assert_not_called()
    assert len(state_data.write_buffer) == 0
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

    mock_write_buffer = mocker.Mock()
    state_data.write_buffer = mock_write_buffer
    state_data.open_called = open_called
    mock_socket.close.side_effect = close_exc("TEST") if close_exc else None
    ShutdownState.enter(fsm, state_data, env, params)
    if open_called:
        pass
    mock_socket.close.assert_called_once()
    decoder.reset.assert_called_once()
    mock_write_buffer.clear.assert_called_once()
    assert fsm.state is ShutdownState

    callbacks.assert_not_called()


@pytest.mark.parametrize(("address", "host"), [
    ("ws://test_host", "test_host"),
    ("ws://test_host:1234", "test_host:1234"),
    ("ws://127.0.0.1:1234", "127.0.0.1:1234"),
    ("ws://[::1]", "[::1]"),
    ("ws://[::1]:1234", "[::1]:1234"),
    ("wss://[2001:db8::1]", "[2001:db8::1]"),
])
def test_states_websocket_handshake_request_host(
    address: str,
    host: str,
    state_data: StateData,
    env: StateEnvironment,
) -> None:
    params = ConnectParams(address=Address(address))
    fsm = FSM(env=env, init_state=WebsocketHandshakeRequestState, error_state=ShutdownState)

    WebsocketHandshakeRequestState.enter(fsm, state_data, env, params)
    assert f"\r\nHost: {host}\r\n" in state_data.write_buffer.decode()


@pytest.mark.parametrize(
    "address", [
        "ws://test_host:1234",
        "wss://test_host:1234",
    ],
)
@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_websocket_handshake_request_happy_path(
    address: str,
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: Mock,
    mock_select: Mock,
    mock_socket: Mock,
    mock_timeout: Mock,
    mocker: MockerFixture,
) -> None:
    mock_write_buffer = mocker.Mock()
    state_data.write_buffer = mock_write_buffer
    params = ConnectParams(address=Address(address))
    fsm = FSM(env=env, init_state=WebsocketHandshakeRequestState, error_state=ShutdownState)
    mock_timeout.get_timeout.return_value = 1 if max_wait is None else max_wait

    # Enter state.
    WebsocketHandshakeRequestState.enter(fsm, state_data, env, params)
    assert state_data.ws_nonce
    mock_write_buffer.clear.assert_called_once()
    mock_write_buffer.extend.assert_called_once()
    data = mock_write_buffer.extend.call_args[0][0]
    mock_write_buffer.reset_mock()
    state_data.write_buffer = bytearray(data)
    body = data.decode()
    assert "GET / HTTP/1.1\r" in body
    assert "Host: test_host:1234\r" in body
    assert "Upgrade: websocket" in body
    assert "Connection: Upgrade" in body
    assert f"Sec-WebSocket-Key: {state_data.ws_nonce}" in body
    assert "Sec-WebSocket-Version: 13" in body
    assert fsm.state is WebsocketHandshakeRequestState

    # First handle, blocked write.
    mock_socket.send.side_effect = BlockingIOError
    ret = WebsocketHandshakeRequestState.handle(fsm, state_data, env, params, max_wait=max_wait)
    mock_socket.send.assert_called_once_with(state_data.write_buffer)
    mock_socket.reset_mock()
    assert ret is False
    if max_wait:
        mock_select.select.assert_called_once_with(write=True, timeout=1)
        mock_select.reset_mock()

    # Second handle, partial write.
    mock_socket.send.side_effect = None
    mock_socket.send.return_value = 10
    ret = WebsocketHandshakeRequestState.handle(fsm, state_data, env, params, max_wait=max_wait)
    mock_socket.send.assert_called_once_with(state_data.write_buffer)
    mock_socket.reset_mock()
    assert ret is False
    if max_wait:
        mock_select.select.assert_called_once_with(write=True, timeout=1)
        mock_select.reset_mock()
    # Check remaining contents of write buffer.
    assert state_data.write_buffer == bytearray(data[10:])

    # Third handle, complete.
    mock_socket.send.side_effect = None
    mock_socket.send.return_value = len(data) - 10
    ret = WebsocketHandshakeRequestState.handle(fsm, state_data, env, params, max_wait=max_wait)
    mock_socket.send.assert_called_once_with(state_data.write_buffer)
    mock_socket.reset_mock()
    assert ret is True

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_websocket_handshake_request_timeout(
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_timeout: Mock
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"), connect_timeout=5)
    fsm = FSM(env=env, init_state=WebsocketHandshakeRequestState, error_state=ShutdownState)

    fsm.previous_state = ConnectingState
    WebsocketHandshakeRequestState.enter(fsm, state_data, env, params)
    mock_timeout.exceeded.return_value = True
    ret = WebsocketHandshakeRequestState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState
    assert isinstance(env.connect_error, TimeoutError)

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_websocket_handshake_request_no_send(
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_socket: Mock,
    mock_timeout: Mock
) -> None:
    params = ConnectParams(address=Address("mqtt://testhost"), connect_timeout=5)
    fsm = FSM(env=env, init_state=WebsocketHandshakeRequestState, error_state=ShutdownState)

    fsm.previous_state = ConnectingState
    WebsocketHandshakeRequestState.enter(fsm, state_data, env, params)
    mock_socket.send.return_value = 0
    ret = WebsocketHandshakeRequestState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState
    assert isinstance(env.connect_error, ConnectionError)

    callbacks.assert_not_called()


@pytest.mark.parametrize("exc", [BrokenPipeError, ConnectionResetError, ssl.SSLEOFError])
@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_websocket_handshake_request_send_error(
    max_wait: float | None,
    exc: type[Exception],
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_socket: Mock,
) -> None:
    params = ConnectParams(address=Address("ws://testhost"), connect_timeout=5)
    fsm = FSM(env=env, init_state=WebsocketHandshakeRequestState, error_state=ShutdownState)

    fsm.previous_state = ConnectingState
    WebsocketHandshakeRequestState.enter(fsm, state_data, env, params)
    mock_socket.send.side_effect = exc("TEST")
    ret = WebsocketHandshakeRequestState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState
    assert isinstance(env.connect_error, exc)

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_websocket_handshake_response_happy_path(
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_socket: Mock,
    mock_timeout: Mock,
) -> None:
    params = ConnectParams(address=Address("ws://testhost"))
    fsm = FSM(env=env, init_state=WebsocketHandshakeResponseState, error_state=ShutdownState)
    state_data.ws_nonce = generate_nonce()
    expected_key = generate_handshake_key(state_data.ws_nonce)

    WebsocketHandshakeResponseState.enter(fsm, state_data, env, params)
    mock_socket.recv.return_value = (
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Accept: {expected_key}\r\n"
        "Sec-WebSocket-Protocol: mqtt\r\n"
        "\r\n"
    ).encode()
    ret = WebsocketHandshakeResponseState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is MQTTHandshakeConnectState

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_websocket_handshake_response_invalid(
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_select: Mock,
    mock_socket: Mock,
    mock_timeout: Mock,
) -> None:
    params = ConnectParams(address=Address("ws://testhost"))
    fsm = FSM(env=env, init_state=WebsocketHandshakeResponseState, error_state=ShutdownState)
    state_data.ws_nonce = generate_nonce()
    expected_key = generate_handshake_key(state_data.ws_nonce)

    WebsocketHandshakeResponseState.enter(fsm, state_data, env, params)

    # Invalid response status line.
    mock_socket.recv.return_value = (
        "HTTP/1.1 400 Bad Request\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Accept: {expected_key}\r\n"
        "Sec-WebSocket-Protocol: mqtt\r\n"
        "\r\n"
    ).encode()
    ret = WebsocketHandshakeResponseState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState
    assert isinstance(env.connect_error, WebsocketError)
    fsm = FSM(env=env, init_state=WebsocketHandshakeResponseState, error_state=ShutdownState)
    WebsocketHandshakeResponseState.enter(fsm, state_data, env, params)
    mock_socket.reset_mock()

    # Missing Sec-WebSocket-Accept header.
    mock_socket.recv.return_value = (
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "\r\n"
    ).encode()
    ret = WebsocketHandshakeResponseState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState
    assert isinstance(env.connect_error, WebsocketError)
    fsm = FSM(env=env, init_state=WebsocketHandshakeResponseState, error_state=ShutdownState)
    WebsocketHandshakeResponseState.enter(fsm, state_data, env, params)
    mock_socket.reset_mock()

    # Invalid Sec-WebSocket-Accept header.
    mock_socket.recv.return_value = (
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Accept: invalid_key\r\n"
        "\r\n"
    ).encode()
    ret = WebsocketHandshakeResponseState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState
    assert isinstance(env.connect_error, WebsocketError)
    fsm = FSM(env=env, init_state=WebsocketHandshakeResponseState, error_state=ShutdownState)
    WebsocketHandshakeResponseState.enter(fsm, state_data, env, params)
    mock_socket.reset_mock()

    # Missing Sec-WebSocket-Protocol header.
    mock_socket.recv.return_value = (
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Accept: {expected_key}\r\n"
        "\r\n"
    ).encode()
    ret = WebsocketHandshakeResponseState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState
    assert isinstance(env.connect_error, WebsocketError)
    fsm = FSM(env=env, init_state=WebsocketHandshakeResponseState, error_state=ShutdownState)
    WebsocketHandshakeResponseState.enter(fsm, state_data, env, params)
    mock_socket.reset_mock()

    # Invalid Sec-WebSocket-Protocol header.
    mock_socket.recv.return_value = (
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Accept: {expected_key}\r\n"
        "Sec-WebSocket-Protocol: invalid_protocol\r\n"
        "\r\n"
    ).encode()
    ret = WebsocketHandshakeResponseState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState
    assert isinstance(env.connect_error, WebsocketError)
    fsm = FSM(env=env, init_state=WebsocketHandshakeResponseState, error_state=ShutdownState)
    WebsocketHandshakeResponseState.enter(fsm, state_data, env, params)
    mock_socket.reset_mock()

    # Malformed status line.
    mock_socket.recv.return_value = b"SSH-2.0-OpenSSH\r\n\r\n"
    ret = WebsocketHandshakeResponseState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState
    assert isinstance(env.connect_error, WebsocketError)
    fsm = FSM(env=env, init_state=WebsocketHandshakeResponseState, error_state=ShutdownState)
    WebsocketHandshakeResponseState.enter(fsm, state_data, env, params)
    mock_socket.reset_mock()

    # Incomplete response.
    mock_socket.recv.return_value = "HTTP/1.1".encode()
    ret = WebsocketHandshakeResponseState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    assert fsm.state is WebsocketHandshakeResponseState
    if max_wait:
        mock_select.select.assert_called_once_with(read=True, timeout=1)

    callbacks.assert_not_called()


@pytest.mark.parametrize(("headers", "valid"), [
    pytest.param("Upgrade: websocket\r\nConnection: Upgrade\r\n", True, id="canonical"),
    pytest.param("upgrade: WebSocket\r\nconnection: keep-alive, upgrade\r\n", True, id="case_and_token_list"),
    pytest.param("Connection: Upgrade\r\n", False, id="missing_upgrade"),
    pytest.param("Upgrade: h2c\r\nConnection: Upgrade\r\n", False, id="wrong_upgrade"),
    pytest.param("Upgrade: websocket\r\n", False, id="missing_connection"),
    pytest.param("Upgrade: websocket\r\nConnection: keep-alive\r\n", False, id="wrong_connection"),
])
def test_states_websocket_handshake_response_upgrade_headers(
    headers: str,
    valid: bool,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_socket: Mock,
    mock_timeout: Mock,
) -> None:
    params = ConnectParams(address=Address("ws://testhost"))
    fsm = FSM(env=env, init_state=WebsocketHandshakeResponseState, error_state=ShutdownState)
    state_data.ws_nonce = generate_nonce()

    WebsocketHandshakeResponseState.enter(fsm, state_data, env, params)
    mock_socket.recv.return_value = (
        "HTTP/1.1 101 Switching Protocols\r\n"
        f"{headers}"
        f"Sec-WebSocket-Accept: {generate_handshake_key(state_data.ws_nonce)}\r\n"
        "Sec-WebSocket-Protocol: mqtt\r\n"
        "\r\n"
    ).encode()
    ret = WebsocketHandshakeResponseState.handle(fsm, state_data, env, params, None)
    assert ret is True
    if valid:
        assert fsm.state is MQTTHandshakeConnectState
    else:
        assert fsm.state is ClosedState
        assert isinstance(env.connect_error, WebsocketError)

    callbacks.assert_not_called()


@pytest.mark.parametrize("split", [1, 12, 100, -1])
def test_states_websocket_handshake_response_split(
    split: int,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_select: Mock,
    loopback_socket: socket.socket,
) -> None:
    params = ConnectParams(address=Address("ws://testhost"))
    fsm = FSM(env=env, init_state=WebsocketHandshakeResponseState, error_state=ShutdownState)
    loopback_socket.setblocking(False)
    state_data.sock = loopback_socket
    state_data.ws_decoder = WebsocketDecoder()
    state_data.ws_nonce = generate_nonce()
    response = (
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Accept: {generate_handshake_key(state_data.ws_nonce)}\r\n"
        "Sec-WebSocket-Protocol: mqtt\r\n"
        "\r\n"
    ).encode()
    trailing = frame_ws_data(OpCode.PING, b"ping", do_mask=False)

    WebsocketHandshakeResponseState.enter(fsm, state_data, env, params)
    loopback_socket.test_sendall(response[:split])  # type: ignore[attr-defined]
    assert WebsocketHandshakeResponseState.handle(fsm, state_data, env, params, None) is False
    assert fsm.state is WebsocketHandshakeResponseState

    loopback_socket.test_sendall(response[split:] + trailing[:3])  # type: ignore[attr-defined]
    assert WebsocketHandshakeResponseState.handle(fsm, state_data, env, params, None) is True
    assert fsm.state is MQTTHandshakeConnectState

    loopback_socket.test_sendall(trailing[3:])  # type: ignore[attr-defined]
    assert state_data.ws_decoder.decode(loopback_socket) == (OpCode.PING, b"ping")

    callbacks.assert_not_called()


@pytest.mark.parametrize("recv", [
    pytest.param(ConnectionResetError("TEST"), id="reset"),
    pytest.param(ssl.SSLEOFError("TEST"), id="ssl_eof"),
    pytest.param(b"", id="closed"),
])
@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_websocket_handshake_response_closed(
    max_wait: float | None,
    recv: Exception | bytes,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_select: Mock,
    mock_socket: Mock,
) -> None:
    params = ConnectParams(address=Address("ws://testhost"), connect_timeout=5)
    fsm = FSM(env=env, init_state=WebsocketHandshakeResponseState, error_state=ShutdownState)

    fsm.previous_state = WebsocketHandshakeRequestState
    WebsocketHandshakeResponseState.enter(fsm, state_data, env, params)
    if isinstance(recv, Exception):
        mock_socket.recv.side_effect = recv
    else:
        mock_socket.recv.return_value = recv
    ret = WebsocketHandshakeResponseState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState
    if isinstance(recv, Exception):
        assert env.connect_error is recv
    else:
        assert isinstance(env.connect_error, ConnectionError)
    mock_select.select.assert_not_called()

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
def test_states_websocket_handshake_response_timeout(
    max_wait: float | None,
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_timeout: Mock
) -> None:
    params = ConnectParams(address=Address("ws://testhost"), connect_timeout=5)
    fsm = FSM(env=env, init_state=WebsocketHandshakeResponseState, error_state=ShutdownState)

    fsm.previous_state = WebsocketHandshakeRequestState
    WebsocketHandshakeResponseState.enter(fsm, state_data, env, params)
    mock_timeout.exceeded.return_value = True
    ret = WebsocketHandshakeResponseState.handle(fsm, state_data, env, params, max_wait)
    assert ret is True
    assert fsm.state is ClosedState
    assert isinstance(env.connect_error, TimeoutError)

    callbacks.assert_not_called()


@pytest.mark.parametrize("max_wait", [None, 0.0])
@pytest.mark.parametrize("exc", [ssl.SSLWantReadError, ssl.SSLWantWriteError, BlockingIOError])
def test_states_websocket_handshake_response_errors(
    max_wait: float | None,
    exc: type[Exception],
    callbacks: EnvironmentCallbacks,
    state_data: StateData,
    env: StateEnvironment,
    mock_select: Mock,
    mock_socket: Mock,
    mock_timeout: Mock,
) -> None:
    params = ConnectParams(address=Address("ws://testhost"), connect_timeout=5)
    fsm = FSM(env=env, init_state=WebsocketHandshakeResponseState, error_state=ShutdownState)

    fsm.previous_state = WebsocketHandshakeRequestState
    WebsocketHandshakeResponseState.enter(fsm, state_data, env, params)

    mock_select.select.return_value = (True, False)
    mock_socket.recv.side_effect = exc("TEST")
    ret = WebsocketHandshakeResponseState.handle(fsm, state_data, env, params, max_wait)
    assert ret is False
    assert fsm.state is WebsocketHandshakeResponseState

    callbacks.assert_not_called()
