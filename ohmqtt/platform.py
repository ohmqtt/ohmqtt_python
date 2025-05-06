import socket
from typing import Final


HAS_AF_UNIX: Final = hasattr(socket, "AF_UNIX")
