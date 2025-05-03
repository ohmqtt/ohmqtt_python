import time


_time = time.monotonic

class Timeout:
    """A simple timer class for getting timeouts since the last event."""
    __slots__ = ("interval", "_mark")

    def __init__(self, interval: float | None = None) -> None:
        self.interval = interval
        self._mark = _time()

    def mark(self) -> None:
        """Mark an event."""
        self._mark = _time()

    def get_timeout(self) -> float | None:
        """Get the difference between the interval and the last mark.

        If the value would be negative, returns 0.

        If the interval is None, returns None."""
        if self.interval is None:
            return None
        return max(0, self.interval - (_time() - self._mark))

    def exceeded(self) -> bool:
        """Check if the timeout has been exceeded.

        If the interval is None, always returns False."""
        if self.interval is None:
            return False
        return self.interval - (_time() - self._mark) <= 0
