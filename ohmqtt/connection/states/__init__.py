from .closed import ClosedState as ClosedState
from .closing import ClosingState as ClosingState
from .connected import ConnectedState as ConnectedState
from .connecting import ConnectingState as ConnectingState
from .mqtt_handshake_connack import MQTTHandshakeConnAckState as MQTTHandshakeConnAckState
from .mqtt_handshake_connect import MQTTHandshakeConnectState as MQTTHandshakeConnectState
from .reconnect_wait import ReconnectWaitState as ReconnectWaitState
from .shutdown import ShutdownState as ShutdownState
from .tls_handshake import TLSHandshakeState as TLSHandshakeState
from .websocket_handshake_request import WebsocketHandshakeRequestState as WebsocketHandshakeRequestState
from .websocket_handshake_response import WebsocketHandshakeResponseState as WebsocketHandshakeResponseState


# Hook up transitions.
ConnectingState.transitions_to = (ClosingState, ClosedState, ShutdownState, TLSHandshakeState, WebsocketHandshakeRequestState, MQTTHandshakeConnectState)
ConnectingState.can_request_from = (ClosedState, ReconnectWaitState, WebsocketHandshakeRequestState)

TLSHandshakeState.transitions_to = (ClosingState, ClosedState, ShutdownState, MQTTHandshakeConnectState, WebsocketHandshakeRequestState)

WebsocketHandshakeRequestState.transitions_to = (ClosingState, ClosedState, ShutdownState, WebsocketHandshakeResponseState)

WebsocketHandshakeResponseState.transitions_to = (ClosingState, ClosedState, ShutdownState, MQTTHandshakeConnectState)

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

ReconnectWaitState.transitions_to = (ClosingState, ClosedState, ShutdownState, ConnectingState)

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
