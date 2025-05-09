import time


_time = time.monotonic

class Timeout:
    """A simple timer class for getting timeouts since the last event."""
    __slots__ = ("_mark", "interval")

    def __init__(self, interval: float | None = None) -> None:
        self.interval = interval
        self.mark()

    def mark(self) -> None:
        """Mark an event."""
        self._mark = _time()

    def get_timeout(self, max_wait: float | None = None) -> float | None:
        """Get the difference between the interval and the last mark.

        If the value would be negative, returns 0.

        If the interval is None, returns None.

        If max_wait is not None, returns the minimum of max_wait and the interval."""
        now = _time()
        if max_wait is not None:
            if self.interval is None:
                return max_wait
            return min(max_wait, self.interval - (now - self._mark))
        if self.interval is None:
            return None
        return max(0, self.interval - (now - self._mark))

    def exceeded(self) -> bool:
        """Check if the timeout has been exceeded.

        If the interval is None, always returns False."""
        if self.interval is None:
            return False
        return self.interval - (_time() - self._mark) <= 0
