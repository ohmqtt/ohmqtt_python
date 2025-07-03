#!/usr/bin/env python3
"""Examples of authentication.  Three relevant features:

* TLS connection.
    Specify that you want to use TLS by using protocol mqtts:// in the server address.
    This authenticates the server and encrypts all MQTT traffic.

* Simple username/password in CONNECT.
    Specify the username and password in the server address.
    Use of TLS is strongly encouraged, otherwise the authenticating information can be captured and replayed.

* AUTH control packet exchange.
    Exchange AUTH packets to implement challenge/response authentication.
"""

import threading
from typing import Any

from ohmqtt import Client, MQTTAuthPacket, MQTTReasonCode


class AuthClient(Client):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.auth_done = threading.Event()

    def handle_auth(self, packet: MQTTAuthPacket) -> None:
        """Handle an AUTH packet from the server.

        You may send back AUTH packets from this handler."""
        method = packet.properties.AuthenticationMethod
        data = packet.properties.AuthenticationData
        rc = packet.reason_code
        print(f"auth {rc=} {method=} {data=}")
        if rc == MQTTReasonCode.ContinueAuthentication:
            self.auth(authentication_method=method, authentication_data=b"some auth data")
        else:
            self.auth_done.set()


def main() -> None:
    with AuthClient() as client:
        client.connect(
            # Use mqtts:// to specify TLS.
            # Use user:password@ to specify username and password.
            # Password will be encoded with UTF-8.
            "mqtts://user:password@localhost:8883",
            # Optionally provide your own TLS context.
            tls_context=None,
            # Optionally specify a hostname other than the one in the address for TLS.
            tls_hostname="localhost",
        )
        client.auth_done.wait(timeout=10)


if __name__ == "__main__":
    main()
