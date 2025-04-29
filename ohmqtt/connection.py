from __future__ import annotations

from dataclasses import dataclass, field
import select
import socket
import ssl
import threading
from typing import Callable, cast, Final
from types import TracebackType

from .decoder import IncrementalDecoder
from .error import MQTTError
from .keepalive import KeepAlive
from .logger import get_logger
from .mqtt_spec import MQTTPacketType, MQTTReasonCode
from .packet import (
    MQTTPacket,
    MQTTConnectPacket,
    MQTTConnAckPacket,
    MQTTDisconnectPacket,
    PING,
    PONG,
)
from .property import MQTTPropertyDict

logger: Final = get_logger("connection")

ConnectionCloseCallback = Callable[[], None]
ConnectionOpenCallback = Callable[[MQTTConnAckPacket], None]
ConnectionReadCallback = Callable[[MQTTPacket], None]


def _get_socket() -> socket.socket:
    """Helper function to get a socket.

    This mostly helps us mock sockets in tests."""
    return socket.socket(socket.AF_INET, socket.SOCK_STREAM)


# Connection has finite states.
STATE_SHUTDOWN: Final = 0
STATE_CLOSED: Final = 1
STATE_CLOSING: Final = 2
STATE_CONNECTING: Final = 3
STATE_TLS_HANDSHAKE: Final = 4
STATE_MQTT_CONNECT: Final = 5
STATE_MQTT_CONNACK: Final = 6
STATE_CONNECTED: Final = 7


@dataclass(kw_only=True, slots=True)
class StateData:
    """State data for the connection.

    This should contain any attributes needed by multiple states.

    The data in this class should never be accessed from outside the state methods."""
    sock: socket.socket | ssl.SSLSocket = field(init=False, default_factory=_get_socket)
    disconnect_rc: int = field(init=False, default=MQTTReasonCode.NormalDisconnection)
    keepalive: KeepAlive = field(init=False, default_factory=KeepAlive)
    decoder: IncrementalDecoder = field(init=False, default_factory=IncrementalDecoder)


_InitAddress: Final[tuple[str, int]] = ("", -1)


class ConnectionCloseCondition(Exception):
    """Any exception which should cause the socket to close."""
    pass


class KeepAliveTimeout(Exception):
    """This exception means a PINGRESP has not been received within the interval."""
    pass


@dataclass(slots=True, match_args=True, frozen=True)
class ConnectParams:
    host: str
    port: int
    client_id: str = ""
    reconnect_delay: float = 0.0
    keepalive_interval: int = 0
    tcp_nodelay: bool = True
    use_tls: bool = False
    tls_context: ssl.SSLContext = field(default_factory=ssl.create_default_context)
    tls_hostname: str = ""
    protocol_version: int = 5
    clean_start: bool = False
    will_topic: str = ""
    will_payload: bytes = b""
    will_qos: int = 0
    will_retain: bool = False
    will_properties: MQTTPropertyDict = field(default_factory=lambda: MQTTPropertyDict())
    connect_properties: MQTTPropertyDict = field(default_factory=lambda: MQTTPropertyDict())


class Connection:
    """Non-blocking socket wrapper with TLS and application keepalive support."""
    __slots__ = (
        "_params",
        "_address",
        "_close_callback",
        "_open_callback",
        "_read_callback",
        "_write_buffer",
        "_write_buffer_lock",
        "_interrupt_r",
        "_interrupt_w",
        "_in_read",
        "_thread",
        "_state",
        "_previous_state",
        "_state_changed",
        "_state_data",
        "_state_cond",
        "_state_do_map",
        "_state_enter_map",
    )

    def __init__(
        self,
        close_callback: ConnectionCloseCallback,
        open_callback: ConnectionOpenCallback,
        read_callback: ConnectionReadCallback,
    ) -> None:
        self._close_callback = close_callback
        self._open_callback = open_callback
        self._read_callback = read_callback
        self._params = ConnectParams(*_InitAddress)
        self._address = _InitAddress

        self._write_buffer = bytearray()
        self._write_buffer_lock = threading.Lock()
        self._interrupt_r, self._interrupt_w = socket.socketpair()
        self._in_read = False
        self._thread: threading.Thread | None = None

        self._state = STATE_CLOSED
        self._previous_state = STATE_CLOSED
        self._state_changed = False
        self._state_data = StateData()
        self._state_cond = threading.Condition()
        self._state_do_map: dict[int, Callable[[StateData], bool]] = {
            STATE_SHUTDOWN: self._do_state_shutdown,
            STATE_CLOSED: self._do_state_closed,
            STATE_CONNECTING: self._do_state_connecting,
            STATE_TLS_HANDSHAKE: self._do_state_tls_handshake,
            STATE_MQTT_CONNECT: self._do_state_mqtt_connect,
            STATE_MQTT_CONNACK: self._do_state_mqtt_connack,
            STATE_CONNECTED: self._do_state_connected,
        }
        self._state_enter_map: dict[int, Callable[[StateData], None]] = {
            STATE_SHUTDOWN: self._enter_state_shutdown,
            STATE_CLOSED: self._enter_state_closed,
            STATE_CONNECTING: self._enter_state_connecting,
            STATE_TLS_HANDSHAKE: self._enter_state_tls_handshake,
            STATE_MQTT_CONNECT: self._enter_state_mqtt_connect,
            STATE_MQTT_CONNACK: self._enter_state_mqtt_connack,
            STATE_CONNECTED: self._enter_state_connected,
        }

    def __enter__(self) -> Connection:
        self.start_loop()
        return self

    def __exit__(self, exc_type: type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        self.shutdown()

    def connect(self, params: ConnectParams) -> None:
        """Connect to the broker.

        This method is non-blocking and will return immediately. The connection will be established in the background."""
        with self._state_cond:
            self._params = params
            self._address = params.host, params.port
            self._request_state(STATE_CONNECTING)

    def wait_for_connect(self, timeout: float | None = None) -> None:
        """Wait for the connection to be established.

        This method will block until the connection is established or the timeout is reached."""
        with self._state_cond:
            self._state_cond.wait_for(lambda: self._state >= STATE_CONNECTED, timeout=timeout)
            if self._state < STATE_CONNECTED:
                raise TimeoutError("Socket did not connect in time")

    def disconnect(self) -> None:
        """Close the connection.

        This method does not guarantee pending reads or writes will be completed."""
        self._address = _InitAddress
        self._request_state(STATE_CLOSED)

    def wait_for_disconnect(self, timeout: float | None = None) -> None:
        """Wait for the connection to be closed.

        This method will block until the connection is closed or the timeout is reached."""
        with self._state_cond:
            self._state_cond.wait_for(lambda: self._state < STATE_CONNECTING, timeout=timeout)
            if self._state >= STATE_CONNECTING:
                raise TimeoutError("Socket did not close in time")
            
    def is_connected(self) -> bool:
        """Check if the connection is established.

        This method will return True if the connection is established, False otherwise."""
        return self._state >= STATE_CONNECTED

    def shutdown(self) -> None:
        """Shutdown the connection, finalizing the instance.

        This method does not guarantee pending reads or writes will be completed."""
        self._request_state(STATE_SHUTDOWN)

    def send(self, data: bytes) -> None:
        """Write data to the broker."""
        with self._state_cond:
            if self._state < STATE_CONNECTED:
                logger.debug("Connection not established, ignoring send")
                return
        with self._write_buffer_lock:
            do_interrupt = not self._in_read and not self._write_buffer
            self._write_buffer.extend(data)
        if do_interrupt:
            self._interrupt_w.send(b"\x00")

    def _try_write(self, state_data: StateData) -> None:
        """Try to flush the write buffer to the socket."""
        try:
            with self._write_buffer_lock:
                sent = state_data.sock.send(self._write_buffer)
                state_data.keepalive.mark_send()
                del self._write_buffer[:sent]
        except ssl.SSLWantWriteError:
            pass
        except BrokenPipeError:
            raise ConnectionCloseCondition("Broken pipe")

    def _try_read(self, state_data: StateData) -> None:
        """Try to read data from the socket."""
        self._in_read = True
        try:
            self._read_packet(state_data)
        except ssl.SSLWantReadError:
            pass
        finally:
            self._in_read = False

    def _read_packet(self, state_data: StateData) -> None:
        """Called by the underlying SocketWrapper when the socket is ready to read.
        
        Incrementally reads and decodes a packet from the socket.
        Complete packets are passed up to the read callback."""
        packet = state_data.decoder.decode(state_data.sock)
        if packet is None:
            # No complete packet available yet.
            return

        # Ping requests and responses are handled at this layer.
        if packet.packet_type == MQTTPacketType.PINGRESP:
            logger.debug("<--- PONG")
            state_data.keepalive.mark_pong()
        elif packet.packet_type == MQTTPacketType.PINGREQ:
            logger.debug("<--- PING PONG --->")
            with self._write_buffer_lock:
                self._write_buffer.extend(PONG)
        elif packet.packet_type == MQTTPacketType.DISCONNECT:
            # DISCONNECT is purely informational.
            logger.debug(f"<--- {str(packet)}")
        else:
            # All other packets are passed to the read callback.
            self._read_callback(packet)

    def start_loop(self) -> None:
        """Start the connection loop in a new thread."""
        if self._thread is not None:
            raise RuntimeError("Connection loop already started")
        self._thread = threading.Thread(target=self.loop_forever, daemon=True)
        self._thread.start()

    def join(self, timeout: float | None = None) -> None:
        """Join the connection thread."""
        if self._thread is None:
            raise RuntimeError("Connection loop not started")
        self._thread.join(timeout)

    def is_alive(self) -> bool:
        """Check if the connection thread is alive."""
        if self._thread is None:
            return False
        return self._thread.is_alive()

    def loop_forever(self) -> None:
        """Run the state machine.

        This method will run in a loop until the connection is closed or shutdown.
        It will call the appropriate state methods based on the current state."""
        while True:
            state_data = self._state_data
            state_done = self._do_state(state_data)
            with self._state_cond:
                if state_done and not self._state_changed:
                    # State is finished, wait for a change.
                    self._state_cond.wait()
                if self._state == STATE_SHUTDOWN:
                    # We are shutting down, break the loop.
                    logger.debug("Shutting down loop")
                    break

    def _do_state(self, state_data: StateData) -> bool:
        """Do the current state.

        State transition will be run if needed.

        Returns True if the state is finished and the calling thread should wait for a change."""
        with self._state_cond:
            state = self._state
            state_changed = self._state_changed
            if state_changed:
                self._state_changed = False
                #logger.debug(f"Entering {state=}")
                self._state_enter_map[state](state_data)
                return False  # Run the state on the next loop, unless it has changed.
        #logger.debug(f"Running state {state}")
        return self._state_do_map[state](state_data)

    def _request_state(self, state: int) -> None:
        """Called from the public interface to request a state change."""
        if state in (STATE_TLS_HANDSHAKE, STATE_MQTT_CONNECT, STATE_MQTT_CONNACK, STATE_CONNECTED):
            raise RuntimeError(f"Cannot request {state=} directly")
        # Interrupt select before entering the Condition.
        logger.debug(f"Requesting {state=}")
        self._interrupt_w.send(b"\x00")
        with self._state_cond:
            if self._state == state:
                return
            self._change_state(state)

    def _change_state(self, state: int) -> None:
        """Transition into a new state.

        This method can be called directly from a state method to transition to a new state.

        Requests from the public Connection interface must use _request_state instead."""
        with self._state_cond:
            if state == self._state:
                return
            if self._state == STATE_SHUTDOWN:
                raise RuntimeError("Cannot change state after shutdown")
            self._state_changed = True
            self._previous_state = self._state
            self._state = state
            self._state_cond.notify_all()

    def _enter_state_connecting(self, state_data: StateData) -> None:
        """Transition into CONNECTING state."""
        state_data.keepalive.keepalive_interval = self._params.keepalive_interval
        state_data.disconnect_rc = MQTTReasonCode.NormalDisconnection
        if self._params.tcp_nodelay:
            state_data.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def _do_state_connecting(self, state_data: StateData) -> bool:
        """Run CONNECTING state."""
        state_data.sock.connect(self._address)
        state_data.sock.setblocking(False)

        if self._params.use_tls:
            self._change_state(STATE_TLS_HANDSHAKE)
            return True
        else:
            self._change_state(STATE_MQTT_CONNECT)
            return True

    def _enter_state_tls_handshake(self, state_data: StateData) -> None:
        """Transition into TLS handshake state."""
        state_data.sock = self._params.tls_context.wrap_socket(
            state_data.sock,
            server_hostname=self._params.tls_hostname,
            do_handshake_on_connect=False,
        )

    def _do_state_tls_handshake(self, state_data: StateData) -> bool:
        """Run TLS handshake."""
        sock = cast(ssl.SSLSocket, state_data.sock)
        try:
            sock.do_handshake()
            self._change_state(STATE_MQTT_CONNECT)
            return True
        except ssl.SSLWantReadError:
            readable, _, _ = select.select([sock, self._interrupt_r], [], [])
            if self._interrupt_r in readable:
                logger.debug("TLS handshake was interrupted")
                self._interrupt_r.recv(1024)
                return False
        except ssl.SSLWantWriteError:
            readable, _, _ = select.select([self._interrupt_r], [sock], [])
            if self._interrupt_r in readable:
                logger.debug("TLS handshake was interrupted")
                self._interrupt_r.recv(1024)
                return False
        return False

    def _enter_state_mqtt_connect(self, state_data: StateData) -> None:
        """Transition into MQTT CONNECT handshake state."""
        state_data.keepalive.mark_init()
        connect_packet = MQTTConnectPacket(
            client_id=self._params.client_id,
            protocol_version=self._params.protocol_version,
            clean_start=self._params.clean_start,
            keep_alive=self._params.keepalive_interval,
            properties=self._params.connect_properties,
            will_topic=self._params.will_topic,
            will_payload=self._params.will_payload,
            will_qos=self._params.will_qos,
            will_retain=self._params.will_retain,
            will_props=self._params.will_properties,
        )
        logger.debug(f"---> {str(connect_packet)}")
        self._write_buffer.clear()
        self._write_buffer.extend(connect_packet.encode())

    def _do_state_mqtt_connect(self, state_data: StateData) -> bool:
        """Send CONNECT for MQTT handshake."""
        timeout = state_data.keepalive.get_next_timeout()
        readable, writable, _ = select.select([self._interrupt_r], [state_data.sock], [], timeout)

        if self._interrupt_r in readable:
            logger.debug("MQTT CONNECT handshake was interrupted")
            self._interrupt_r.recv(1024)
            return False
        if state_data.sock in writable:
            try:
                num_sent = state_data.sock.send(self._write_buffer)
                state_data.keepalive.mark_send()
                if num_sent < len(self._write_buffer):
                    # Not all data was sent, wait for writable again.
                    logger.debug(f"Not all data was sent, waiting for writable again: {num_sent=}")
                    del self._write_buffer[:num_sent]
                    return False
                self._write_buffer.clear()
                self._change_state(STATE_MQTT_CONNACK)
                return True
            except ssl.SSLWantWriteError:
                # Socket is not writable, wait for it to be writable.
                logger.debug("MQTT CONNECT waiting for SSL")
                return False
        if state_data.keepalive.should_close():
            logger.debug("MQTT CONNECT keepalive timeout")
            self._change_state(STATE_CLOSED)
            return True
        return False

    def _enter_state_mqtt_connack(self, state_data: StateData) -> None:
        """Transition into waiting for MQTT CONNACK state."""
        pass

    def _do_state_mqtt_connack(self, state_data: StateData) -> bool:
        """Wait for MQTT CONNACK."""
        timeout = state_data.keepalive.get_next_timeout()
        readable, _, _ = select.select([state_data.sock, self._interrupt_r], [], [], timeout)

        if self._interrupt_r in readable:
            logger.debug("MQTT CONNACK handshake was interrupted")
            self._interrupt_r.recv(1024)
            return False
        if state_data.sock in readable:
            try:
                packet = state_data.decoder.decode(state_data.sock)
                if packet is None:
                    # No complete packet available yet.
                    return False
                if packet.packet_type == MQTTPacketType.CONNACK:
                    logger.debug(f"<--- {str(packet)}")
                    self._open_callback(cast(MQTTConnAckPacket, packet))
                    self._change_state(STATE_CONNECTED)
                    return True
                else:
                    logger.error(f"Unexpected packet while waiting for CONNACK: str({packet})")
                    self._change_state(STATE_CLOSED)
                    return True
            except ssl.SSLWantReadError:
                # Socket is not readable, wait for it to be readable.
                logger.debug("MQTT CONNACK waiting for SSL")
                return False
        if state_data.keepalive.should_close():
            logger.debug("MQTT CONNACK keepalive timeout")
            self._change_state(STATE_CLOSED)
            return True
        return False

    def _enter_state_connected(self, state_data: StateData) -> None:
        """Transition into CONNECTED state."""
        pass

    def _do_state_connected(self, state_data: StateData) -> bool:
        """Run the CONNECTED state."""
        try:
            next_timeout = state_data.keepalive.get_next_timeout()
            write_check = (state_data.sock,) if self._write_buffer else tuple()
            readable, writable, _ = select.select([state_data.sock, self._interrupt_r], write_check, [], next_timeout)
            #logger.debug(f"select() returned {readable=}, {writable=}")

            if self._interrupt_r in readable:
                self._interrupt_r.recv(1024)

            if state_data.sock in writable:
                self._try_write(state_data)

            if state_data.sock in readable:
                self._try_read(state_data)

            # Check keepalive
            if state_data.keepalive.should_close():
                raise KeepAliveTimeout("Keepalive timeout")
            if state_data.keepalive.should_send_ping():
                with self._write_buffer_lock:
                    logger.debug("---> PING")
                    self._write_buffer.extend(PING)
                state_data.keepalive.mark_ping()

            return False
        except MQTTError as exc:
            logger.error(f"There was a problem with data from broker, closing connection: {exc}")
            state_data.disconnect_rc = exc.reason_code
            self._change_state(STATE_CLOSED)
            return True
        except ConnectionCloseCondition as exc:
            logger.debug(f"Closing socket: {exc}")
            self._change_state(STATE_CLOSED)
            return True
        except KeepAliveTimeout:
            logger.error("Keepalive timeout, closing socket")
            state_data.disconnect_rc = -1  # Do not send DISCONNECT
            self._change_state(STATE_CLOSED)
            return True

    def _enter_state_closed(self, state_data: StateData) -> None:
        """Transition into CLOSED state."""
        if self._previous_state == STATE_CONNECTED:
            try:
                self._close_callback()
            except Exception:
                logger.exception("Error while calling close callback")
            if state_data.disconnect_rc >= 0:
                disconnect_packet = MQTTDisconnectPacket(reason_code=state_data.disconnect_rc)
                try:
                    state_data.sock.send(disconnect_packet.encode())
                    logger.debug(f"---> {str(disconnect_packet)}")
                except (BlockingIOError, ssl.SSLWantWriteError, OSError):
                    pass
        state_data.sock.close()
        state_data.decoder.reset()
        with self._write_buffer_lock:
            self._write_buffer.clear()

    def _do_state_closed(self, state_data: StateData) -> bool:
        """Run the CLOSED state."""
        return True  # Fix reconnect later
        """
        if self._params.reconnect_delay > 0:
            t0 = _time()
            try:
                logger.debug(f"Waiting {self._params.reconnect_delay}s for reconnect")
                with self._state_cond:
                    was_cancelled = self._state_cond.wait_for(self._check_connect, timeout=self._params.reconnect_delay)
            except ConnectionCloseCondition:
                # In shutdown.
                logger.debug("Reconnect cancelled by shutdown")
                return True
            else:
                if was_cancelled:
                    logger.debug("Reconnect timeout cancelled by close")
                    return True
                else:
                    logger.debug("Reconnecting to broker")
                    self._change_state(STATE_CONNECTED)
                    return True
        return True
        """

    def _enter_state_shutdown(self, state_data: StateData) -> None:
        """Transition into SHUTDOWN state."""
        # We can enter this state from any other state.
        # Free up as many resources as possible.
        state_data.sock.close()
        state_data.decoder.reset()
        with self._write_buffer_lock:
            self._write_buffer.clear()

    def _do_state_shutdown(self, state_data: StateData) -> bool:
        """Run the SHUTDOWN state."""
        return True
