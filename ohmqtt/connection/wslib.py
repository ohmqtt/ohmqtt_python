"""RFC 6455 Websocket protocol library."""

import base64
import uuid


def generate_nonce() -> str:
    """Generate a random base64-encoded nonce for Websocket handshake."""

    random_bytes = uuid.uuid4().bytes
    return base64.b64encode(random_bytes).decode("utf-8")
