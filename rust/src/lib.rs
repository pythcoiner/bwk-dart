#![allow(unexpected_cfgs)]

pub mod api;

#[cfg(not(feature = "bull_sdk"))]
mod frb_generated;

// When building under the `bull_sdk` feature the full flutter_rust_bridge
// generated module (`frb_generated.rs`) is intentionally cfg'd out (the
// aggregator regenerates all wire code). The API layer still references
// `crate::frb_generated::StreamSink`, so we provide a minimal shim.
//
// Why an alias to `StreamSinkBase` instead of re-running the FRB macro:
// `frb_generated_boilerplate!` emits a *per-crate* `StreamSink` newtype that
// wraps `StreamSinkBase` (its inner field is private). The bull_sdk aggregator
// emits its OWN such newtype in its generated wire function and constructs it
// there, so a newtype here would be a DISTINCT nominal type from the one the
// aggregator passes to `SpAccount::init` — the generated call would not type
// check, and the dependency cannot name the aggregator's type (no cyclic dep).
//
// Instead we alias `StreamSink` to the FRB crate's shared `StreamSinkBase`,
// which is the single canonical type re-exported by `flutter_rust_bridge`.
// The aggregator's post-gen `fix_frb_generated.sh` reconstructs a
// `StreamSinkBase` (via its public `::deserialize`, wire-identical to what the
// newtype does internally) for `init`'s argument, so both sides agree on one
// type. The codec is `DcoCodec` to match the aggregator's `full_dep` default
// (`default_stream_sink_codec = DcoCodec`); note this differs from a standalone
// bb-mobile build, whose own codegen defaults to `SseCodec`. `StreamSinkBase`
// only has the codec-level `add_raw`, so the typed `add(SpNotification)` used by
// `sp_account.rs` is provided via the `SpStreamSinkExt` extension trait below.
#[cfg(feature = "bull_sdk")]
mod frb_generated {
    use crate::api::types::SpNotification;
    use flutter_rust_bridge::for_generated::{DcoCodec, StreamSinkBase};
    use flutter_rust_bridge::Rust2DartSendError;

    pub type StreamSink<T, C = DcoCodec> = StreamSinkBase<T, C>;

    pub trait SpStreamSinkExt {
        fn add(&self, value: SpNotification) -> Result<(), Rust2DartSendError>;
    }

    impl SpStreamSinkExt for StreamSinkBase<SpNotification, DcoCodec> {
        fn add(&self, value: SpNotification) -> Result<(), Rust2DartSendError> {
            use flutter_rust_bridge::for_generated::Rust2DartAction;
            use flutter_rust_bridge::IntoIntoDart;
            self.add_raw(DcoCodec::encode(
                Rust2DartAction::Success,
                value.into_into_dart(),
            ))
        }
    }

    use flutter_rust_bridge::for_generated::DartAbi;
    use flutter_rust_bridge::{IntoDart, IntoIntoDart};

    impl IntoDart for SpNotification {
        fn into_dart(self) -> DartAbi {
            unreachable!()
        }
    }

    impl IntoIntoDart<SpNotification> for SpNotification {
        fn into_into_dart(self) -> SpNotification {
            self
        }
    }
}
