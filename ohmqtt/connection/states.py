from __future__ import annotations

import socket
import ssl
from typing import cast, Final

from .types import ConnectParams, StateData, StateEnvironment
from .fsm import FSM, FSMState
from .decoder import ClosedSocketError, WantRead
from ..error import MQTTError
from ..logger import get_logger
from ..mqtt_spec import MQTTPacketType, MQTTReasonCode
from ..packet import MQTTConnectPacket, MQTTConnAckPacket, MQTTDisconnectPacket, PING, PONG

logger: Final = get_logger("connection.states")


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
        state_data.disconnect_rc = -1
        state_data.sock = _get_socket(params.address.family)
        if params.address.family in (socket.AF_INET, socket.AF_INET6):
            state_data.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, params.tcp_nodelay)
        state_data.decoder.reset()

    @classmethod
    def handle(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams, block: bool) -> bool:
        if state_data.timeout.exceeded():
            logger.debug("Connect timeout")
            fsm.change_state(ClosedState)
            return True

        try:
            address = params.address
            if address.family == socket.AF_UNIX:
                state_data.sock.connect(address.host)
            else:
                state_data.sock.connect((address.host, address.port))
        except (BlockingIOError, OSError):
            pass  # Either in progress or already connected, select will reveal which.
        except ConnectionError as exc:
            logger.error(f"Failed to connect to broker: {exc}")
            fsm.change_state(ClosedState)
            return True

        with fsm.selector:
            timeout = state_data.timeout.get_timeout() if block else 0
            _, wlist, _ = fsm.selector.select([], [state_data.sock], [], timeout)
        if state_data.sock in wlist:
            logger.debug("Socket connected to broker")
            if params.address.use_tls:
                fsm.change_state(TLSHandshakeState)
            else:
                fsm.change_state(MQTTHandshakeConnectState)
            state_data.sock.setblocking(False)
            return True
        return False


class TLSHandshakeState(FSMState):
    """Performing TLS handshake with the broker."""
    @classmethod
    def enter(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams) -> None:
        state_data.sock = params.tls_context.wrap_socket(
            state_data.sock,
            server_hostname=params.tls_hostname if params.tls_hostname else params.address.host,
            do_handshake_on_connect=False,
        )

    @classmethod
    def handle(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams, block: bool) -> bool:
        if state_data.timeout.exceeded():
            logger.debug("TLS handshake timeout")
            fsm.change_state(ClosedState)
            return True
        
        timeout = state_data.timeout.get_timeout() if block else 0
        sock = cast(ssl.SSLSocket, state_data.sock)
        try:
            logger.debug("trying TLS handshake")
            sock.do_handshake()
            fsm.change_state(MQTTHandshakeConnectState)
            return True
        except ssl.SSLWantReadError:
            logger.debug("TLS handshake wants read")
            with fsm.selector:
                fsm.selector.select([sock], [], [], timeout)
        except ssl.SSLWantWriteError:
            logger.debug("TLS handshake wants write")
            with fsm.selector:
                fsm.selector.select([], [sock], [], timeout)
        return False


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
        )
        logger.debug(f"---> {str(connect_packet)}")
        with fsm.selector:
            env.write_buffer.clear()
            env.write_buffer.extend(connect_packet.encode())

    @classmethod
    def handle(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams, block: bool) -> bool:
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
                elif num_sent < len(env.write_buffer):
                    # Not all data was sent, wait for writable again.
                    logger.debug(f"Not all CONNECT data was sent, waiting for writable again: {num_sent=}")
                    del env.write_buffer[:num_sent]
                    return False
                env.write_buffer.clear()
            fsm.change_state(MQTTHandshakeConnAckState)
            return True
        except (BlockingIOError, ssl.SSLWantWriteError):
            # The write was blocked, wait for the socket to be writable.
            if block:
                with fsm.selector:
                    timeout = state_data.timeout.get_timeout()
                    fsm.selector.select([], [state_data.sock], [], timeout)
        return False


class MQTTHandshakeConnAckState(FSMState):
    """Receiving MQTT CONNACK packet from the broker."""
    @classmethod
    def handle(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams, block: bool) -> bool:
        if state_data.timeout.exceeded():
            logger.debug("MQTT CONNACK keepalive timeout")
            fsm.change_state(ClosedState)
            return True

        want_read = False
        try:
            packet = state_data.decoder.decode(state_data.sock)
            if packet is None:
                want_read = True
        except ClosedSocketError:
            logger.exception("Socket was closed")
            fsm.change_state(ClosedState)
            return True
        except WantRead:
            want_read = True
        
        if want_read:
            # Incomplete packet, wait for more data.
            with fsm.selector:
                timeout = state_data.timeout.get_timeout() if block else 0
                fsm.selector.select([state_data.sock], [], [], timeout)
            return False

        if packet is not None and packet.packet_type == MQTTPacketType.CONNACK:
            packet = cast(MQTTConnAckPacket, packet)
            logger.debug(f"<--- {str(packet)}")
            state_data.connack = packet
            if packet.properties.ServerKeepAlive is not None:
                state_data.keepalive.keepalive_interval = packet.properties.ServerKeepAlive
            fsm.change_state(ConnectedState)
            return True
        else:
            logger.error(f"Unexpected packet while waiting for CONNACK: {str(packet)}")
            fsm.change_state(ClosedState)
            return True


class ConnectedState(FSMState):
    """Connected to the broker."""
    @classmethod
    def enter(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams) -> None:
        state_data.keepalive.mark_init()
        with fsm.selector:
            env.write_buffer.clear()
        assert state_data.connack is not None
        env.open_callback(state_data.connack)

    @classmethod
    def handle(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams, block: bool) -> bool:
        if state_data.keepalive.should_close():
            logger.error("Keepalive timeout, closing socket")
            fsm.change_state(ClosedState)
            return True

        if state_data.keepalive.should_send_ping():
            logger.debug("---> PING")
            with fsm.selector:
                env.write_buffer.extend(PING)
            state_data.keepalive.mark_ping()

        timeout = state_data.keepalive.get_next_timeout() if block else 0
        with fsm.selector:
            write_check = [state_data.sock] if env.write_buffer else []
            rlist, wlist, _ = fsm.selector.select([state_data.sock], write_check, [], timeout)

        if state_data.sock in wlist:
            try:
                with fsm.selector:
                    sent = state_data.sock.send(env.write_buffer)
                    del env.write_buffer[:sent]
                state_data.keepalive.mark_send()
            except (BlockingIOError, ssl.SSLWantWriteError):
                pass
            except BrokenPipeError:
                logger.debug("MQTT connection was closed")
                fsm.change_state(ClosedState)
                return True

        if state_data.sock in rlist:
            want_read = True
            while want_read:  # Read all available packets.
                try:
                    want_read = cls.read_packet(fsm, state_data, env, params)
                except ClosedSocketError:
                    logger.debug("Connection closed")
                    fsm.change_state(ClosedState)
                    return True
                except MQTTError as exc:
                    logger.error(f"There was a problem with data from broker, closing connection: {exc}")
                    state_data.disconnect_rc = exc.reason_code
                    fsm.change_state(ClosedState)
                    return True

        return False

    @classmethod
    def read_packet(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams) -> bool:
        """Called by the underlying SocketWrapper when the socket is ready to read.
        
        Incrementally reads and decodes a packet from the socket.
        Complete packets are passed up to the read callback.
        
        Returns False if an incomplete packet was read, True if a complete packet was read."""
        packet = state_data.decoder.decode(state_data.sock)
        if packet is None:
            # No complete packet available yet.
            return False

        # Ping requests and responses are handled at this layer.
        if packet.packet_type == MQTTPacketType.PINGRESP:
            logger.debug("<--- PONG")
            state_data.keepalive.mark_pong()
        elif packet.packet_type == MQTTPacketType.PINGREQ:
            logger.debug("<--- PING PONG --->")
            with fsm.selector:
                env.write_buffer.extend(PONG)
        elif packet.packet_type == MQTTPacketType.DISCONNECT:
            logger.debug(f"<--- {str(packet)}")
            #fsm.change_state(ClosedState)
        else:
            # All other packets are passed to the read callback.
            env.read_callback(packet)
        return True


class ReconnectWaitState(FSMState):
    """Waiting to reconnect to the broker."""
    @classmethod
    def enter(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams) -> None:
        state_data.timeout.interval = params.reconnect_delay
        state_data.timeout.mark()

    @classmethod
    def handle(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams, block: bool) -> bool:
        # Here we repurpose the keepalive timer to wait for a reconnect.
        if state_data.timeout.exceeded():
            fsm.change_state(ConnectingState)
            return True
        elif block:
            timeout = state_data.timeout.get_timeout()
            fsm.wait(timeout)
        return False


class ClosingState(FSMState):
    """Gracefully closing the connection.

    The socket will be shutdown for reading, but existing buffers will be flushed."""
    @classmethod
    def enter(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams) -> None:
        state_data.timeout.interval = params.connect_timeout
        state_data.timeout.mark()

        if fsm.previous_state == ConnectedState:
            logger.debug("Gracefully closing connection")
            state_data.disconnect_rc = MQTTReasonCode.NormalDisconnection
        else:
            logger.debug("Skipping ClosingState")
            fsm.change_state(ClosedState)
            return

    @classmethod
    def handle(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams, block: bool) -> bool:
        # Wait for the socket to be writable.
        if state_data.timeout.exceeded():
            logger.error("ClosingState timed out")
            fsm.change_state(ClosedState)
            return True

        # Read all buffered packets without waiting for a select.
        want_read = True
        try:
            while want_read:  # Read all available packets.
                want_read = ConnectedState.read_packet(fsm, state_data, env, params)
        except MQTTError as exc:
            # Bad data during closing should still forcefully close the connection.
            logger.error(f"There was a problem with data from broker, closing connection: {exc}")
            state_data.disconnect_rc = exc.reason_code
            fsm.change_state(ClosedState)
            return True
        except (BlockingIOError, OSError):
            pass

        with fsm.selector:
            if not env.write_buffer:
                logger.debug("No more data to send, connection closed")
                fsm.change_state(ClosedState)
                return True
            timeout = state_data.timeout.get_timeout() if block else 0
            _, wlist, _ = fsm.selector.select([], [state_data.sock], [], timeout)

        if state_data.sock in wlist:
            try:
                with fsm.selector:
                    sent = state_data.sock.send(env.write_buffer)
                    del env.write_buffer[:sent]
                state_data.keepalive.mark_send()
            except (BlockingIOError, ssl.SSLWantWriteError):
                pass
            except BrokenPipeError:
                logger.error("Socket lost while closing")
                fsm.change_state(ClosedState)
                return True
        return False


class ClosedState(FSMState):
    """Connection is closed.

    All buffers will be flushed and the socket will be closed after conditionally sending a DISCONNECT."""
    @classmethod
    def enter(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams) -> None:
        if fsm.previous_state in (ClosingState, ConnectedState):
            env.close_callback()
            with fsm.selector:
                if state_data.disconnect_rc >= 0 and not env.write_buffer:
                    disconnect_packet = MQTTDisconnectPacket(reason_code=state_data.disconnect_rc)
                    # Try to send a DISCONNECT packet, but no problem if we can't.
                    try:
                        state_data.sock.send(disconnect_packet.encode())
                        logger.debug(f"---> {str(disconnect_packet)}")
                    except (BlockingIOError, ssl.SSLWantWriteError, OSError):
                        logger.debug("Failed to send DISCONNECT packet")
            if params.reconnect_delay > 0 and fsm.requested_state == ConnectingState:
                fsm.change_state(ReconnectWaitState)
        try:
            state_data.sock.shutdown(socket.SHUT_RDWR)
        except OSError as exc:
            logger.debug(f"Error while shutting down socket: {exc}")
        try:
            state_data.sock.close()
        except OSError as exc:
            logger.debug(f"Error while closing socket: {exc}")
        state_data.decoder.reset()
        with fsm.selector:
            env.write_buffer.clear()


class ShutdownState(FSMState):
    """The final state.

    All buffers will be flushed and the socket will be closed immediately on entry."""
    @classmethod
    def enter(cls, fsm: FSM, state_data: StateData, env: StateEnvironment, params: ConnectParams) -> None:
        # We can enter this state from any other state.
        # Free up as many resources as possible.
        if fsm.previous_state in (ClosingState, ConnectedState):
            env.close_callback()
        state_data.sock.close()
        state_data.decoder.reset()
        with fsm.selector:
            env.write_buffer.clear()
            fsm.selector.close()


# Hook up transitions.
ConnectingState.transitions_to = (ClosingState, ClosedState, ShutdownState, TLSHandshakeState, MQTTHandshakeConnectState)
ConnectingState.can_request_from = (ClosedState, ReconnectWaitState)

TLSHandshakeState.transitions_to = (ClosingState, ClosedState, ShutdownState, MQTTHandshakeConnectState)

MQTTHandshakeConnectState.transitions_to = (ClosingState, ClosedState, ShutdownState, MQTTHandshakeConnAckState)

MQTTHandshakeConnAckState.transitions_to = (ClosingState, ClosedState, ShutdownState, ConnectedState)

ConnectedState.transitions_to = (ClosingState, ClosedState, ShutdownState)

ClosingState.transitions_to = (ClosedState, ShutdownState)
ClosingState.can_request_from = (
    ConnectingState,
    TLSHandshakeState,
    MQTTHandshakeConnectState,
    MQTTHandshakeConnAckState,
    ConnectedState,
    ReconnectWaitState,
)
ClosedState.transitions_to = (ConnectingState, ShutdownState, ReconnectWaitState)
ReconnectWaitState.transitions_to = (ClosedState, ShutdownState, ConnectingState)
ShutdownState.can_request_from = (
    ConnectingState,
    TLSHandshakeState,
    MQTTHandshakeConnectState,
    MQTTHandshakeConnAckState,
    ConnectedState,
    ReconnectWaitState,
    ClosingState,
    ClosedState,
)
