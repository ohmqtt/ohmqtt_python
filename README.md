# OhmQTT

A lightweight MQTT client library for Python.

## TODO for 0.1

* Instructions
* Error handling and validation

## TODO for 1.0

* Autodoc
* Reconnect
* Wills
* Subscription identifiers
* Shared subscriptions
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
 │   Connection           ├──────┤   IncrementalDecoder   │
 │                        │      │                        │
 └────────────────────────┘      └────────────────────────┘
```