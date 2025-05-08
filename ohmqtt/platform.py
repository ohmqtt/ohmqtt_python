import socket
import sys
from typing import cast, Final


HAS_AF_UNIX: Final = sys.platform != "win32"

if sys.platform == "win32":
    AF_UNIX: Final = cast(socket.AddressFamily, 1)
else:
    AF_UNIX: Final = socket.AF_UNIX
