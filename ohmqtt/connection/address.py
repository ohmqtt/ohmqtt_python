from dataclasses import dataclass
import socket
from typing import Final, Mapping
from urllib.parse import urlparse, ParseResult


DEFAULT_PORTS: Final[Mapping[str, int]] = {
    "mqtt": 1883,
    "mqtts": 8883,
    "unix": 0,
}


def is_ipv6(hostname: str) -> bool:
    """Check if the hostname is an IPv6 address."""
    try:
        socket.inet_pton(socket.AF_INET6, hostname)
        return True
    except socket.error:
        return False


def _get_family(parsed: ParseResult) -> socket.AddressFamily:
    """Get the address family based on the parsed URL scheme."""
    if parsed.scheme == "unix":
        return socket.AF_UNIX
    elif parsed.scheme in ("mqtt", "mqtts"):
        if not parsed.hostname:
            raise ValueError("Hostname is required for mqtt and mqtts schemes")
        if is_ipv6(parsed.hostname):
            return socket.AF_INET6
        return socket.AF_INET
    else:
        raise ValueError(f"Unsupported scheme: {parsed.scheme}")


@dataclass(slots=True, init=False, frozen=True)
class Address:
    scheme: str
    family: socket.AddressFamily
    host: str
    port: int
    username: str | None
    password: str | None

    def __init__(self, address: str = "") -> None:
        """Parse the address string into family, host, port, username, and password."""
        # Special case: empty address is allowed, but slots will be empty.
        if not address:
            return
        if "//" not in address and not address.startswith("unix:"):
            # urlparse may choke on some network address we wish to support, unless we guarantee a //.
            address = "//" + address
        parsed = urlparse(address, scheme="mqtt")
        object.__setattr__(self, "scheme", parsed.scheme)
        object.__setattr__(self, "family", _get_family(parsed))
        object.__setattr__(self, "host", parsed.hostname or parsed.path)
        if not self.host:
            raise ValueError("No path in address")
        if self.family == socket.AF_UNIX and self.host == "/":
            raise ValueError("'/' is not a valid Unix socket path")
        object.__setattr__(self, "port", parsed.port if parsed.port is not None else DEFAULT_PORTS[parsed.scheme])
        object.__setattr__(self, "username", parsed.username)
        object.__setattr__(self, "password", parsed.password)

    @property
    def use_tls(self) -> bool:
        """Check if the address uses TLS."""
        return self.scheme == "mqtts"
