# OhmQTT

A fast and reliable MQTT client library for Python.

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
