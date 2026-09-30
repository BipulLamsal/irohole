# Plugins

Each plugin lives in its own crate here. Depends only on `irohole-proto`.

To add one (e.g. `input`):

1. `cargo new plugins/irohole-plugin-input --lib`
2. Implement `irohole_proto::Plugin` with a unique `wire_id`
   (`0x01` = tcp taken, use `0x02` next).
3. Re-export + register in `plugins/irohole-plugins/src/lib.rs`:

```rust
pub use irohole_plugin_input as input;
reg.register(input::InputPlugin)?;
```

Hosts call `irohole_plugins::builtin_registry()` — no per-host wiring.
