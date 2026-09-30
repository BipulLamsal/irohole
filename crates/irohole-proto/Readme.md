### irohole-proto

`no_std`-compatible, transport-agnostic protocol crate. Defines how messages are framed and dispatched, not how they travel.

Frame layout:
[wire_id:u8] [len:u32] BE [payload: can't tell you decide]

* `wire_id`: 1-byte plugin address (`0x01` = tcp, `0x02` = input).
* `len`: big-endian payload length, max 16 MiB.
* `payload`: plugin-defined bytes (can be BE/LE).

Plugins implement `Plugin { id, wire_id, handle }` and register in `irohole-core::Registry`. 
Core routes inbound frames by `wire_id`. Same frame rides iroh-QUIC, TCP, UART, or WebSocket.
