# dart_bwk

A Dart/Flutter capability package exposing **Silent Payments** (BIP352) over the Rust
[`bwk`](https://github.com/pythcoiner/bwk) (Bitcoin Wallet Kit) crates, via
[`flutter_rust_bridge`](https://github.com/fzyzcjy/flutter_rust_bridge).

The Rust crate is named `dart_bwk` (to avoid clashing with the upstream `bwk` crate). It wraps
`bwk-sp` / `bwk` / `bwk-tx` and surfaces a Silent Payments account API: scanning (user-triggered
only), balances, coin/payment views, PSBT prepare/sign, and broadcast.

## How it is consumed

This package is aggregated by [`bull_sdk`](https://github.com/SatoshiPortal/bull_sdk) alongside
the other capabilities (`lwk`, `boltz`, `ark`, `bbqr`, …). Apps depend on `bull_sdk` and import
the SP bindings through its barrel:

```dart
import 'package:bull_sdk/bwk.dart';
```

The single generated `BullSdk` entrypoint initializes all capabilities, including this one:

```dart
await BullSdk.init();
```

When built under the `bull_sdk` feature (`features = ["bull_sdk"]`), this crate's own
`frb_generated` module is cfg'd out and `bull_sdk`'s codegen owns the bindings. The standalone
config (`flutter_rust_bridge.yaml`, entrypoint `BwkCore`) exists only for building this crate in
isolation.

## Invariant: scan is user-triggered only

Silent Payments scanning never auto-starts. The rust-side checks live in
`scripts/audit-sp-invariant.sh` and `rust/tests/{no_auto_scan_in_rust,no_continuous_in_dart}.rs`:
exactly one `start_scan()` call site (inside `scan_once()`), zero `scan_blocks()` calls, and no
continuous-scan mode in the generated Dart. The `continuous-scan` cargo feature is guarded by a
`compile_error!`.

## Layout

- `rust/src/api/` — the FFI surface: `sp_account.rs`, `types.rs`, `regtest.rs`, `ping.rs`.
- `rust/tests/` — invariant + regtest integration tests (`SP_NETWORK_TESTS=1`-gated).
- `scripts/audit-sp-invariant.sh` — rust-side no-autoscan audit.

## Build (standalone)

```bash
cd rust
cargo build --features bull_sdk   # how the aggregator builds it
cargo test  --features bull_sdk
```
