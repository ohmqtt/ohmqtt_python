from __future__ import annotations

import ssl
from typing import cast, Final, TYPE_CHECKING

from .base import FSMState
from .closed import ClosedState
from .mqtt_handshake_connect import MQTTHandshakeConnectState
from .websocket_handshake_request import WebsocketHandshakeRequestState
from ..types import ConnectParams, StateData, StateEnvironment
from ...logger import get_logger

if TYPE_CHECKING:
    from ..fsm import FSM

logger: Final = get_logger("ohmqtt.connection.states.tls_handshake")


class TLSHandshakeState(FSMState):
    """Performing TLS handshake with the broker."""
    @classmethod
    def enter(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams) -> None:
        tls_context = params.tls_context if params.tls_context else ssl.create_default_context()
        state_data.sock = tls_context.wrap_socket(
            state_data.sock,
            server_hostname=params.tls_hostname if params.tls_hostname else params.address.host,
            do_handshake_on_connect=False,
        )
        with fsm.selector:
            fsm.selector.change_sock(state_data.sock)

    @classmethod
    def handle(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams, max_wait: float | None) -> bool:
        if state_data.timeout.exceeded():
            logger.debug("TLS handshake timeout")
            fsm.change_state(ClosedState)
            return True

        timeout = state_data.timeout.get_timeout(max_wait)
        sock = cast(ssl.SSLSocket, state_data.sock)
        try:
            logger.debug("trying TLS handshake")
            sock.do_handshake()
            if params.address.is_websocket():
                fsm.change_state(WebsocketHandshakeRequestState)
            else:
                fsm.change_state(MQTTHandshakeConnectState)
            return True
        except ssl.SSLWantReadError:
            logger.debug("TLS handshake wants read")
            with fsm.selector:
                fsm.selector.select(read=True, timeout=timeout)
        except ssl.SSLWantWriteError:
            logger.debug("TLS handshake wants write")
            with fsm.selector:
                fsm.selector.select(write=True, timeout=timeout)
        return False
