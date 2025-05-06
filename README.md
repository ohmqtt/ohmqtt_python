# ΩQTT

*pron. "ohm cue tee tee" or "ohm cutie"*

A reliable and persistent MQTT 5.0 client library for Python.

## Features

### QoS and Persistence

ΩQTT supports publish and subscribing all QoS levels with optional persistence to disk for QoS >0.
When not persisting QoS >0 messages, a fast (but volatile) in memory store is used.
Either way, publishing a message returns a handle with a method to wait for the message to be fully acknowledged by the broker.

### TLS

Optionally use TLS and provide your own TLS context when connecting to a broker.

### Properties

ΩQTT gives you complete access to all PUBLISH, SUBSCRIBE, AUTH and CONNECT optional properties.

### Automatic Topic Alias

Set an alias policy when publishing a message and a topic alias will be generated, if allowed by the broker.

### Toolkit

ΩQTT is built on a toolkit for efficiently serializing MQTT control messages.
Use it to build your own custom implementation, or to serialize your own payloads.

### Compatibility

ΩQTT is tested on Linux and Windows with CPython versions 3.10-3.13.
It should work on any platform that CPython runs on.

### Reliability

ΩQTT has been implemented to a high standard of test coverage and static analysis, from the beginning.

### Performance

Every drop of pure Python performance has been squeezed out of serialization and the event loop.
You're not using Python because it's fast, but it can't hurt.

## TODO for 0.1

* Auth
* Instructions
* Error handling and validation
* Refactor Session

## TODO for 1.0

* E2E Tests
* Autodoc
* Publish automation

## Development

* uv
* nox

### Running the Tests

```bash
nox
```
