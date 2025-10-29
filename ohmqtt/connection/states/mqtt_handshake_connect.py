from __future__ import annotations

import ssl
from typing import Final, TYPE_CHECKING

from .base import FSMState
from .closed import ClosedState
from .mqtt_handshake_connack import MQTTHandshakeConnAckState
from ..types import ConnectParams, StateData, StateEnvironment
from ...logger import get_logger
from ...packet import MQTTConnectPacket

if TYPE_CHECKING:
    from ..fsm import FSM

logger: Final = get_logger("ohmqtt.connection.states.mqtt_handshake_connect")


class MQTTHandshakeConnectState(FSMState):
    """Sending MQTT CONNECT packet to the broker."""
    @classmethod
    def enter(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams) -> None:
        connect_packet = MQTTConnectPacket(
            client_id=params.client_id,
            protocol_version=params.protocol_version,
            clean_start=params.clean_start,
            keep_alive=params.keepalive_interval,
            properties=params.connect_properties,
            will_topic=params.will_topic,
            will_payload=params.will_payload,
            will_qos=params.will_qos,
            will_retain=params.will_retain,
            will_props=params.will_properties,
            username=params.address.username,
            password=params.address.password.encode() if params.address.password else None,
        )
        logger.debug("---> %s", connect_packet)
        with fsm.selector:
            env.write_buffer.clear()
            env.write_buffer.extend(connect_packet.encode())

    @classmethod
    def handle(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams, max_wait: float | None) -> bool:
        if state_data.timeout.exceeded():
            logger.debug("MQTT CONNECT keepalive timeout")
            fsm.change_state(ClosedState)
            return True

        try:
            with fsm.selector:
                num_sent = state_data.sock.send(env.write_buffer)
                if num_sent == 0:
                    logger.error("MQTT CONNECT send returned 0 bytes, closing connection")
                    fsm.change_state(ClosedState)
                    return True
                if num_sent < len(env.write_buffer):
                    # Not all data was sent, wait for writable again.
                    logger.debug("Not all CONNECT data was sent, waiting for writable again: wrote: %d", num_sent)
                    del env.write_buffer[:num_sent]
                    return False
                env.write_buffer.clear()
            fsm.change_state(MQTTHandshakeConnAckState)
            return True
        except (BlockingIOError, ssl.SSLWantReadError, ssl.SSLWantWriteError):
            # The write was blocked, wait for the socket to be writable.
            if max_wait is None or max_wait > 0.0:
                with fsm.selector:
                    timeout = state_data.timeout.get_timeout(max_wait)
                    fsm.selector.select(write=True, timeout=timeout)
        return False
