from __future__ import annotations

import ssl
from typing import Final, TYPE_CHECKING

from .base import FSMState
from .closed import ClosedState
from .websocket_handshake_response import WebsocketHandshakeResponseState
from ..types import ConnectParams, StateData, StateEnvironment
from ..wslib import generate_nonce
from ...logger import get_logger

if TYPE_CHECKING:
    from ..fsm import FSM

logger: Final = get_logger("ohmqtt.connection.states.websocket_handshake_request")


class WebsocketHandshakeRequestState(FSMState):
    """Sending Websocket handshake request to the broker."""
    @classmethod
    def enter(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams) -> None:
        state_data.ws_nonce = generate_nonce()
        host = f"{params.address.host}:{params.address.port}" if not params.address.is_default_port() else params.address.host
        req = f"""
        GET {params.address.path} HTTP/1.1\r
        Host: {host}\r
        Upgrade: websocket\r
        Connection: Upgrade\r
        Sec-WebSocket-Key: {state_data.ws_nonce}\r
        Sec-WebSocket-Version: 13\r
        Sec-WebSocket-Protocol: mqtt\r
        \r
        """
        logger.debug("---> %s", req)
        state_data.write_buffer.clear()
        state_data.write_buffer.extend(req.encode("utf-8"))

    @classmethod
    def handle(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams, max_wait: float | None) -> bool:
        if state_data.timeout.exceeded():
            logger.debug("Websocket handshake request keepalive timeout")
            fsm.change_state(ClosedState)
            return True

        try:
            with fsm.selector:
                num_sent = state_data.sock.send(state_data.write_buffer)
                if num_sent == 0:
                    logger.error("Websocket handshake request send returned 0 bytes, closing connection")
                    fsm.change_state(ClosedState)
                    return True
                if num_sent < len(state_data.write_buffer):
                    # Not all data was sent, wait for writable again.
                    logger.debug("Not all Websocket handshake request data was sent, waiting for writable again: wrote: %d", num_sent)
                    del state_data.write_buffer[:num_sent]
                    return False
                state_data.write_buffer.clear()
            fsm.change_state(WebsocketHandshakeResponseState)
            return True
        except (BlockingIOError, ssl.SSLWantReadError, ssl.SSLWantWriteError):
            # The write was blocked, wait for the socket to be writable.
            if max_wait is None or max_wait > 0.0:
                with fsm.selector:
                    timeout = state_data.timeout.get_timeout(max_wait)
                    fsm.selector.select(write=True, timeout=timeout)
        return False
