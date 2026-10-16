from __future__ import annotations

import errno
import socket
from typing import Any, Final, TYPE_CHECKING

from .base import FSMState
from .closed import ClosedState
from .mqtt_handshake_connect import MQTTHandshakeConnectState
from .tls_handshake import TLSHandshakeState
from .websocket_handshake_request import WebsocketHandshakeRequestState
from ..address import Address
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


def _resolve(address: Address) -> list[tuple[socket.AddressFamily, Any]]:
    """Resolve an address to the socket addresses to try connecting to, in order.

    This is patched in tests to avoid name resolution.

    :raises OSError: Name resolution failed."""
    if HAS_AF_UNIX and address.family == AF_UNIX:
        return [(AF_UNIX, address.host)]
    infos = socket.getaddrinfo(address.host, address.port, type=socket.SOCK_STREAM)
    return [(family, sockaddr) for family, _, _, _, sockaddr in infos]


class ConnectingState(FSMState):
    """Connecting to the broker."""
    @classmethod
    def enter(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams) -> None:
        state_data.keepalive.keepalive_interval = params.keepalive_interval
        state_data.timeout.interval = params.connect_timeout
        state_data.timeout.mark()
        state_data.connack = None
        state_data.disconnect_rc = None
        state_data.connect_called = False
        state_data.connect_addresses.clear()
        env.connect_error = None
        state_data.decoder.reset()
        state_data.ws_decoder.reset()
        state_data.ws_handshake_buffer.clear()
        state_data.ws_nonce = ""
        state_data.open_called = False
        if fsm.previous_state is ClosedState:
            state_data.was_connected = False

    @classmethod
    def handle(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams, max_wait: float | None) -> bool:

        if state_data.timeout.exceeded():
            logger.debug("Connect timeout")
            env.connect_error = TimeoutError("Timed out connecting to the broker")
            fsm.change_state(ClosedState)
            return True

        if not state_data.connect_called:
            state_data.connect_called = True
            if (exc := cls.start_connect(fsm, state_data, params)) is not None:
                env.connect_error = exc
                fsm.change_state(ClosedState)
                return True

        with fsm.selector:
            timeout = state_data.timeout.get_timeout(max_wait)
            _, writable = fsm.selector.select(write=True, timeout=timeout)
        if writable:
            error = state_data.sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if error:
                logger.error("Failed to connect to broker: %s", errno.errorcode.get(error, error))
                state_data.sock.close()
                exc = OSError(error, errno.errorcode.get(error, str(error)))
                if (exc := cls.connect_next(fsm, state_data, params, exc)) is None:
                    return False  # Trying the next address.
                env.connect_error = exc
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
    def start_connect(cls, fsm: FSM, state_data: StateData, params: ConnectParams) -> OSError | None:
        """Resolve the broker address and start a non-blocking connect.  Name resolution happens here.

        :return: The error if name resolution or every connection attempt failed immediately, otherwise None."""
        try:
            state_data.connect_addresses = _resolve(params.address)
        except OSError as exc:
            logger.error("Failed to resolve broker address: %s", exc)
            return exc
        return cls.connect_next(fsm, state_data, params, None)

    @classmethod
    def connect_next(cls, fsm: FSM, state_data: StateData, params: ConnectParams, error: OSError | None) -> OSError | None:
        """Start a non-blocking connect to the next resolved address, skipping any which fail immediately.

        :param error: The error from the previous address, if any.
        :return: The last error if no connection attempt is in progress, otherwise None."""
        while state_data.connect_addresses:
            family, sockaddr = state_data.connect_addresses.pop(0)
            state_data.sock = _get_socket(family)
            state_data.sock.setblocking(False)
            if family in (socket.AF_INET, socket.AF_INET6):
                state_data.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, params.tcp_nodelay)
            with fsm.selector:
                fsm.selector.change_sock(state_data.sock)
            try:
                state_data.sock.connect(sockaddr)
            except BlockingIOError:
                pass  # Connection in progress.
            except OSError as exc:
                logger.error("Failed to connect to broker at %s: %s", sockaddr, exc)
                state_data.sock.close()
                error = exc
                continue
            return None
        return error
