## 0.1.0

- Initial version: Silent Payments (BIP352) capability over the `bwk` Rust crates
  (`bwk-sp` / `bwk` / `bwk-tx`), aggregated by `bull_sdk`.
- SP account API: user-triggered scan, balances, coin/payment views, PSBT
  prepare/sign, broadcast.
- No-autoscan invariant enforced by `scripts/audit-sp-invariant.sh` and the
  `rust/tests/` scan checks.
