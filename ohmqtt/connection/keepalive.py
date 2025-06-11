"""
The Keep Alive is a Two Byte Integer (a required field in CONNECT packets)
which is a time interval measured in seconds.

It is the maximum time interval that is permitted to elapse between the point
at which the Client finishes transmitting one MQTT Control Packet and the
point it starts sending the next.

It is the responsibility of the Client to ensure that the interval between
MQTT Control Packets being sent does not exceed the Keep Alive value.

If Keep Alive is non-zero and in the absence of sending any other MQTT Control
Packets, the Client MUST send a PINGREQ packet.

If the Server returns a Server Keep Alive on the CONNACK packet, the Client
MUST use that value instead of the value it sent as the Keep Alive.

If the Server does not send the Server Keep Alive, the Server MUST use the
Keep Alive value set by the Client on CONNECT.

If the Keep Alive value is non-zero and the Server does not receive an MQTT
Control Packet from the Client within one and a half times the Keep Alive time
period, it MUST close the Network Connection to the Client as if the network
had failed.

If a Client does not receive a PINGRESP packet within a reasonable amount of
time after it has sent a PINGREQ, it SHOULD close the Network Connection to
the Server.

A Keep Alive value of 0 has the effect of turning off the Keep Alive
mechanism. If Keep Alive is 0 the Client is not obliged to send MQTT Control
Packets on any particular schedule.
"""

from time import monotonic as _time
from typing import Final


MIN_TIMEOUT: Final = 0


class KeepAlive:
    """Tracks the keep alive timer for a connection."""
    __slots__ = (
        "_keepalive_interval",
        "_last_send",
        "_pong_deadline",
    )

    def __init__(self) -> None:
        self._keepalive_interval = 0
        self._last_send = 0.0
        self._pong_deadline = 0.0

    @property
    def keepalive_interval(self) -> int:
        """Get the keep alive interval in seconds."""
        return self._keepalive_interval

    @keepalive_interval.setter
    def keepalive_interval(self, value: int) -> None:
        """Set the keep alive interval in seconds.

        :raises ValueError: The value is out of range."""
        if value < 0:
            raise ValueError("Keep alive interval must be non-negative")
        if value > 65535:
            raise ValueError("Keep alive interval must be <= 65535")
        if value != self._keepalive_interval:
            self._keepalive_interval = value
            # Reset PINGRESP expectation.
            self._pong_deadline = 0.0

    def should_close(self) -> bool:
        """Check if the keep alive timer has expired."""
        if self._keepalive_interval == 0:
            # Keep alive is disabled
            return False
        now = _time()
        if self._pong_deadline > 0.0 and now >= self._pong_deadline:
            # No PINGRESP received after sending PINGREQ
            return True
        return False

    def should_send_ping(self) -> bool:
        """Check if a PINGREQ packet should be sent."""
        if self._keepalive_interval == 0:
            # Keep alive is disabled
            return False
        now = _time()
        if now - self._last_send >= self._keepalive_interval and self._pong_deadline == 0.0:
            # No data sent for keepalive_interval and no PINGREQ inflight
            return True
        return False

    def get_next_timeout(self, max_wait: float | None = None) -> float | None:
        """Check how long until the next next ping or closure check is due.

        max_wait is the maximum time to wait for a timeout. If this is set,
        the timeout will be the minimum of the calculated timeout and max_wait.

        If the keep alive interval is 0, this will return None unless max_wait is set."""
        if self._keepalive_interval == 0:
            # Keep alive is disabled
            if max_wait is not None:
                # If max_wait is set, we should wait for that time.
                return max_wait
            return None
        now = _time()
        if self._pong_deadline > 0.0:
            # After this amount of time, we should close the connection.
            to = max(MIN_TIMEOUT, self._pong_deadline - now)
        else:
            # After this amount of time, we should send a PINGREQ.
            to = max(MIN_TIMEOUT, self._last_send + self._keepalive_interval - now)
        if max_wait is not None:
            to = min(to, max_wait)
        return to

    def mark_init(self) -> None:
        """Initialize the keep alive timer for a new connection."""
        self._last_send = _time()
        self._pong_deadline = 0.0

    def mark_send(self) -> None:
        """Mark that data has been sent."""
        self._last_send = _time()

    def mark_ping(self) -> None:
        """Mark that a PINGREQ was sent.

        This also marks that data has been sent."""
        now = _time()
        self._pong_deadline = now + self._keepalive_interval
        self._last_send = now

    def mark_pong(self) -> None:
        """Mark that a PINGRESP was received.

        This also marks that data has been received."""
        self._pong_deadline = 0.0
