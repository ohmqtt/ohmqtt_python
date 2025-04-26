# OhmQTT

A fast and reliable MQTT client library for Python.

## TODO for 0.1

* Client pubsub interfaces
* Client connect interfaces
* Client auth interfaces
* Examples
* Instructions
* Error handling and validation

## TODO for 1.0

* Autodoc
* Reconnect
* Wills
* Subscription identifiers
* Shared subscriptions
* Session lifecycle
* Persistence
* Portability (Windows)
* Publish automation

## Development

* uv
* nox

### Running the Tests

```bash
nox
```

## Client Structure

### Client

* High-level interface
* Subscription mapping

### ClientSession

* Persistence
* Replay

### ClientConnection

* Socket
* Thread ownership and keepalive
* TLS
* Serialization

```
 ┌────────────────────────┐      ┌────────────────────────┐
 │                        │      │                        │
 │   Client               ├──────┤   Subscriptions        │
 │                        │      │                        │
 └───────────┬────────────┘      └────────────────────────┘
             │
             │
 ┌───────────┴────────────┐      ┌────────────────────────┐
 │                        │      │                        │
 │   Session              ├──────┤   Retention            │
 │                        │      │                        │
 └───────────┬────────────┘      └────────────────────────┘
             │
             │
 ┌───────────┴────────────┐      ┌────────────────────────┐
 │                        │      │                        │
 │   Connection           ├──────┤   SocketWrapper        │
 │                        │      │                        │
 └────────────────────────┘      └────────────────────────┘
```