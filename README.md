# ΩQTT

*pron. "ohm cue tee tee" or "omega cutie"*

A reliable and persistent MQTT 5.0 client library for Python.

## Features

### QoS and Persistence

ΩQTT supports publish and subscribing all QoS levels with optional persistence to disk for QoS >0.
When not persisting QoS >0 messages, a fast (but volatile) in memory store is used.
Either way, publishing a message returns a handle with a method to wait for the message to be fully acknowledged by the broker.

### Connectivity

Connect to a remote broker with optional TLS over TCP over IPV4 or IPV6.
Connect to a local broker with either TCP or Unix domain socket.

### Properties

Access all optional properties of all MQTT control packet types.
If you ever wanted to check the user properties of SUBACK and UNSUBACK (among others), welcome home.

### Automatic Topic Alias

Set an alias policy when publishing a message and a topic alias will be generated, if allowed by the broker.
If bandwidth is tight, set your QoS 0 publications to require a topic alias.
In this case, an error will be raised if the server does not offer enough alias values.

### Toolkit

ΩQTT is built on a toolkit for efficiently serializing MQTT control messages.
Use it to build your own custom implementation, or to serialize your own payloads.

### Portability

ΩQTT is tested on Linux, Windows and MacOS with CPython versions 3.10-3.13.
It should work on any platform that CPython runs on.

### Reliability

ΩQTT has been implemented to a high standard of test coverage and static analysis, from the beginning.
It continues to improve.

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

This project uses `nox` and `uv` to run the tests against all supported Python versions.

To do all of this in a venv:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install nox uv
nox
```

