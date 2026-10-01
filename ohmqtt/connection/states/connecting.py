from __future__ import annotations

import errno
import socket
from typing import Final, TYPE_CHECKING

from .base import FSMState
from .closed import ClosedState
from .mqtt_handshake_connect import MQTTHandshakeConnectState
from .tls_handshake import TLSHandshakeState
from .websocket_handshake_request import WebsocketHandshakeRequestState
from ..types import ConnectParams, StateData, StateEnvironment
from ...logger import get_logger
from ...platform import AF_UNIX, HAS_AF_UNIX

if TYPE_CHECKING:
    from ..fsm import FSM

logger: Final = get_logger("connection.states.connecting")


def _get_socket(family: socket.AddressFamily) -> socket.socket:
    """Get a socket object.

    This is patched in tests to use a mock or loopback socket."""
    return socket.socket(family, socket.SOCK_STREAM)


class ConnectingState(FSMState):
    """Connecting to the broker."""
    @classmethod
    def enter(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams) -> None:
        state_data.keepalive.keepalive_interval = params.keepalive_interval
        state_data.timeout.interval = params.connect_timeout
        state_data.timeout.mark()
        state_data.connack = None
        state_data.disconnect_rc = None
        state_data.sock = _get_socket(params.address.family)
        state_data.sock.setblocking(False)
        state_data.connect_called = False
        if params.address.family in (socket.AF_INET, socket.AF_INET6):
            state_data.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, params.tcp_nodelay)
        state_data.decoder.reset()
        state_data.ws_decoder.reset()
        state_data.ws_handshake_buffer.clear()
        state_data.ws_nonce = ""
        state_data.open_called = False
        if fsm.previous_state is ClosedState:
            state_data.was_connected = False
        with fsm.selector:
            fsm.selector.change_sock(state_data.sock)

    @classmethod
    def handle(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams, max_wait: float | None) -> bool:

        if state_data.timeout.exceeded():
            logger.debug("Connect timeout")
            fsm.change_state(ClosedState)
            return True

        if not state_data.connect_called:
            state_data.connect_called = True
            if not cls.start_connect(state_data, params):
                fsm.change_state(ClosedState)
                return True

        with fsm.selector:
            timeout = state_data.timeout.get_timeout(max_wait)
            _, writable = fsm.selector.select(write=True, timeout=timeout)
        if writable:
            error = state_data.sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if error:
                logger.error("Failed to connect to broker: %s", errno.errorcode.get(error, error))
                fsm.change_state(ClosedState)
                return True
            logger.debug("Socket connected to broker")
            if params.address.use_tls:
                fsm.change_state(TLSHandshakeState)
            elif params.address.is_websocket():
                fsm.change_state(WebsocketHandshakeRequestState)
            else:
                fsm.change_state(MQTTHandshakeConnectState)
            return True
        return False

    @classmethod
    def start_connect(cls, state_data: StateData, params: ConnectParams) -> bool:
        """Start a non-blocking connect.  Name resolution happens here.

        :return: False if the connection failed immediately."""
        try:
            address = params.address
            if HAS_AF_UNIX and address.family == AF_UNIX:
                state_data.sock.connect(address.host)
            else:
                state_data.sock.connect((address.host, address.port))
        except BlockingIOError:
            pass  # Connection in progress.
        except OSError as exc:
            logger.error("Failed to connect to broker: %s", exc)
            return False
        return True
