# ΩQTT

*pronounced "ohm cue tee tee" or "ohm cutie"*

A lightweight and persistent MQTT 5.0 client library for Python 3.10+.

## Features

### QoS and Persistence

ΩQTT supports publish and subscribing all QoS levels with optional persistence to disk for QoS >0.
When not persisting QoS >0 messages, a fast (but volatile) in memory store is used.
Either way, publishing a message returns a handle with a method to wait for the message to be fully acknowledged by the broker.

### TLS and Authentication

Optionally use TLS and provide a TLS context when connecting to a broker.
Attach an auth callback to the client to authenticate the connection.

### Diminutive Profile

ΩQTT is obsessively optimized for low memory use and high throughput.
ΩQTT is obsessively tested and analyzed by a suite of tests and static analysis tools.
ΩQTT has no runtime dependencies.

### Properties

ΩQTT gives you easy access to all PUBLISH, SUBSCRIBE, AUTH and CONNECT optional properties.

## TODO for 0.1

* Instructions
* Error handling and validation

## TODO for 1.0

* Autodoc
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