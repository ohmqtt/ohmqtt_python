from __future__ import annotations

import socket
from typing import Final, TYPE_CHECKING

from .base import FSMState
from .closed import ClosedState
from .mqtt_handshake_connect import MQTTHandshakeConnectState
from .tls_handshake import TLSHandshakeState
from ..types import ConnectParams, StateData, StateEnvironment
from ...logger import get_logger
from ...platform import AF_UNIX, HAS_AF_UNIX

if TYPE_CHECKING:
    from ..fsm import FSM

logger: Final = get_logger("ohmqtt.connection.states.connecting")


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
        if params.address.family in (socket.AF_INET, socket.AF_INET6):
            state_data.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, params.tcp_nodelay)
        state_data.decoder.reset()
        state_data.open_called = False
        with fsm.selector:
            fsm.selector.change_sock(state_data.sock)

    @classmethod
    def handle(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams, max_wait: float | None) -> bool:

        if state_data.timeout.exceeded():
            logger.debug("Connect timeout")
            fsm.change_state(ClosedState)
            return True

        try:
            address = params.address
            if HAS_AF_UNIX and address.family == AF_UNIX:
                state_data.sock.connect(address.host)
            else:
                state_data.sock.connect((address.host, address.port))
        except ConnectionError as exc:
            logger.error("Failed to connect to broker: %s", exc)
            fsm.change_state(ClosedState)
            return True
        except (BlockingIOError, OSError):
            pass  # Either in progress or already connected, select will reveal which.

        with fsm.selector:
            timeout = state_data.timeout.get_timeout(max_wait)
            _, writable = fsm.selector.select(write=True, timeout=timeout)
        if writable:
            logger.debug("Socket connected to broker")
            if params.address.use_tls:
                fsm.change_state(TLSHandshakeState)
            else:
                fsm.change_state(MQTTHandshakeConnectState)
            state_data.sock.setblocking(False)
            return True
        return False
