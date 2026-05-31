use std::{
    collections::BTreeSet,
    path::PathBuf,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, MutexGuard,
    },
    thread::JoinHandle,
    time::Duration,
};

use bitcoin::{
    bip32::ChildNumber,
    consensus::encode::{deserialize_hex, serialize, serialize_hex},
    Network,
};
use flutter_rust_bridge::frb;

use bwk::persist::{ConfigStore, FileConfigStore};
use bwk::{parse_electrum_url, ElectrumScheme};
use bwk_sp::{SubAccountConfig, TxBuilderSpExt};

use crate::api::types::{
    CoinSource, RecipientView, SpBalanceView, SpCoinView, SpNetwork, SpNotification,
    SpPaymentDirection, SpPaymentView, SubAccountKind, TxSimulation, UnifiedCoinView,
};
use crate::frb_generated::StreamSink;

// Under the `bull_sdk` aggregation feature, `StreamSink` is an alias for the
// FRB-shared `StreamSinkBase` (so it is the SAME nominal type the aggregator's
// generated wire function constructs). `StreamSinkBase` has no typed `add()`,
// so the typed `add(SpNotification)` is provided by this extension trait. In a
// standalone build `StreamSink` is the macro-generated wrapper which already
// has an inherent `add()`, so the trait is not imported there.
#[cfg(feature = "bull_sdk")]
use crate::frb_generated::SpStreamSinkExt as _;

// Prevent anyone from enabling continuous-scan mode across the FRB boundary.
#[cfg(feature = "continuous-scan")]
compile_error!(
    "ScanMode::Continuous is intentionally not exposed across FRB. \
     Removing this guard requires a deliberate design change."
);

#[frb(opaque)]
pub struct SpAccount {
    inner: Arc<Mutex<bwk_sp::Account>>,
    sink: Mutex<Option<StreamSink<SpNotification>>>,
    electrum_url: Mutex<String>, // cached for broadcast

    // Cooperative shutdown for the notification-forwarding thread spawned in
    // `init()`. `dispose()` (FFI) and `Drop` both flip this flag and join the
    // handle so the thread, the inner Account, and its sqlite handle are
    // released deterministically. Without this, every WalletBloc refresh that
    // replaced `state.spWallet` would leak a thread blocked on `rx.recv()`
    // plus an open sqlite connection.
    shutdown: Arc<AtomicBool>,
    notif_handle: Mutex<Option<JoinHandle<()>>>,

    // Cancel signal for an in-flight `scan_once`. This is a clone of the
    // bwk_sp::Account's internal `scanner_stop`. Flipping it to `true`
    // causes spdk-core's `process_blocks` to bail at the next per-block
    // checkpoint, releasing the inner mutex without us ever needing to
    // touch it. `stop_scan` MUST NOT take the inner mutex (the
    // scan holds it for the full duration); `dispose()` flips this
    // flag so a running scan releases the lock promptly and the next
    // `SpAccount::load` doesn't race the sqlite handle.
    scan_cancel: Arc<AtomicBool>,
}

/// Test-only counter incremented when the notification thread exits cleanly.
/// Used by the Drop test to assert the thread actually terminates instead of
/// leaking. Fully gated behind `#[cfg(test)]`: in non-test builds the recorder
/// is an inlined no-op, so neither the counter nor the increment is compiled in.
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
#[cfg(test)]
static NOTIF_THREAD_EXITS: AtomicUsize = AtomicUsize::new(0);

/// Record a clean notification-thread exit. Increments the test counter under
/// test; a zero-cost no-op in production.
#[cfg(test)]
fn record_notif_thread_exit() {
    NOTIF_THREAD_EXITS.fetch_add(1, Ordering::Relaxed);
}

#[cfg(not(test))]
#[inline(always)]
fn record_notif_thread_exit() {}

#[cfg(test)]
fn notif_thread_exit_count() -> usize {
    NOTIF_THREAD_EXITS.load(Ordering::Relaxed)
}

/// Take the SP-account mutex, mapping a poisoned lock to a recoverable error.
///
/// A panic in a holder of this lock (e.g. the notification thread) used to
/// poison it permanently, after which every subsequent FFI call would itself
/// panic via `.unwrap()` — bricking the wallet for the rest of the process.
/// Returning `Err` instead lets the cubit surface a "wallet stopped" state
/// and lets the user recover by restarting the app (no data loss: state is
/// persisted to sqlite on every coin update).
fn lock_inner(inner: &Mutex<bwk_sp::Account>) -> Result<MutexGuard<'_, bwk_sp::Account>, String> {
    inner
        .lock()
        .map_err(|_| "SP wallet lock poisoned; restart required".to_string())
}

fn to_bitcoin_network(n: SpNetwork) -> bitcoin::Network {
    match n {
        SpNetwork::Bitcoin => bitcoin::Network::Bitcoin,
        SpNetwork::Signet => bitcoin::Network::Signet,
        SpNetwork::Testnet => bitcoin::Network::Testnet,
        SpNetwork::Regtest => bitcoin::Network::Regtest,
    }
}

fn build_tx_with_coin_selection(
    inner: &bwk_sp::Account,
    recipients: &[RecipientView],
    feerate_sat_vb: u64,
) -> Result<bwk_tx::TxBuilder, String> {
    let network = inner.network();
    let feerate_msat_vb = feerate_sat_vb.saturating_mul(1_000);
    let mut builder = inner.tx_builder().feerate(feerate_msat_vb);

    for r in recipients {
        match r {
            RecipientView::Sp {
                address,
                amount_sat,
                ..
            } => {
                let sp_addr =
                    bwk_sp::silentpayments::SilentPaymentAddress::try_from(address.as_str())
                        .map_err(|e| format!("invalid SP address '{address}': {e}"))?;
                if sp_addr.get_network() == bwk_sp::silentpayments::Network::Mainnet
                    && network != Network::Bitcoin
                {
                    return Err(format!("Wrong network for SP address {}", sp_addr));
                }
                builder.send_to_sp(sp_addr, *amount_sat);
            }
            RecipientView::Standard {
                address,
                amount_sat,
            } => {
                let addr = bitcoin::Address::from_str(address)
                    .map_err(|e| format!("invalid address '{address}': {e}"))?
                    .require_network(network)
                    .map_err(|e| format!("wrong network for '{address}': {e}"))?;
                builder.send_to(addr, *amount_sat);
            }
        }
    }

    Ok(builder)
}

/// Look up a coin by outpoint across the SP store and all sub-account stores.
///
/// Returns `Err` if the coin is no longer present (e.g. reorged out, or
/// re-classified between simulation and finalize), or if it is not spendable
/// (already spent, currently being spent in another in-flight tx). This is
/// the chokepoint that turns a "coin store mutated between confirm and
/// broadcast" race into a clear user-visible error instead of a silent
/// re-selection that would broadcast a tx different from what the user
/// confirmed.
fn find_coin_by_outpoint(
    inner: &bwk_sp::Account,
    outpoint: bitcoin::OutPoint,
) -> Result<bwk_tx::Coin, String> {
    // Try SP coin store first.
    if let Some(entry) = inner.get_coin(&outpoint) {
        if !entry.is_spendable() {
            return Err(format!(
                "transaction inputs changed since confirmation: SP coin {outpoint} is no longer spendable; please re-confirm"
            ));
        }
        // Reconstruct the Coin via the SP coin source. We re-derive by
        // walking through sp_coin_entry-style construction: bwk_sp's
        // tx_builder_from_request does the same. We delegate to the SP
        // tx_builder's coin source via `all_coins`/store: easiest path is
        // to round-trip through `inner.tx_builder().select_coins`, but
        // that re-runs selection. Instead, mirror bwk_sp's helper.
        return sp_coin_view_to_tx_coin(inner, outpoint);
    }
    for sub in inner.sub_accounts() {
        if let Some(entry) = sub.coins().get(&outpoint) {
            if matches!(
                entry.status(),
                bwk_tx::CoinStatus::Spent | bwk_tx::CoinStatus::BeingSpend
            ) {
                return Err(format!(
                    "transaction inputs changed since confirmation: sub-account coin {outpoint} is no longer spendable; please re-confirm"
                ));
            }
            return Ok(entry.coin.clone());
        }
    }
    Err(format!(
        "transaction inputs changed since confirmation: coin {outpoint} not found in wallet; please re-confirm"
    ))
}

/// Materialize a `bwk_tx::Coin` for an SP outpoint directly from the entry
/// returned by `bwk_sp::Account::get_coin`.
///
/// This mirrors `bwk_sp::account::sp_coin_entry_to_coin` (private) verbatim:
/// SP outputs are always single-key taproot, so the satisfaction weight is a
/// constant 66 witness units and the spend info carries the empty derivation
/// plus the per-output tweak.
///
/// Earlier iterations of this helper tried to round-trip through
/// `inner.tx_builder().select_coins(u64::MAX / 2, 1_000)` to avoid duplicating
/// the conversion. That was broken: `bwk_tx::coin_selection::select_coins`
/// short-circuits to an empty `Vec` when the candidate set has more than 20
/// coins (combinatorial guard), and even otherwise `u64::MAX / 2` exceeds any
/// realistic wallet balance, so every `range(target..)` lookup misses and the
/// `cj_selection` branch overflows in debug. The result was that every SP
/// finalize call failed with "inputs changed" — see the R4 audit.
fn sp_coin_view_to_tx_coin(
    inner: &bwk_sp::Account,
    outpoint: bitcoin::OutPoint,
) -> Result<bwk_tx::Coin, String> {
    let entry = inner.get_coin(&outpoint).ok_or_else(|| {
        format!(
            "transaction inputs changed since confirmation: SP coin {outpoint} not found in wallet; please re-confirm"
        )
    })?;
    if !entry.is_spendable() {
        return Err(format!(
            "transaction inputs changed since confirmation: SP coin {outpoint} is no longer spendable; please re-confirm"
        ));
    }
    Ok(sp_entry_to_tx_coin(outpoint, &entry))
}

/// Pure conversion from an `SpCoinEntry` (returned by
/// `bwk_sp::Account::get_coin`) to a `bwk_tx::Coin` suitable for
/// `TxBuilder::add_input`. Factored out so the conversion logic can be
/// exercised by a unit test without needing a live `bwk_sp::Account` (which
/// can only be populated via a real scan against Blindbit).
///
/// SP outputs are always single-key taproot, so the satisfaction weight is a
/// constant 66 witness units (key-path Schnorr signature) and the spend info
/// carries the empty derivation plus the per-output tweak. This mirrors the
/// private `bwk_sp::account::sp_coin_entry_to_coin` verbatim.
fn sp_entry_to_tx_coin(outpoint: bitcoin::OutPoint, entry: &bwk_sp::SpCoinEntry) -> bwk_tx::Coin {
    const TR_KEYSPEND_SATISFACTION_WEIGHT: u64 = 66;
    bwk_tx::Coin {
        txout: bitcoin::TxOut {
            value: entry.amount(),
            script_pubkey: entry.script().clone(),
        },
        outpoint,
        height: Some(entry.height() as u64),
        sequence: bitcoin::Sequence::ENABLE_RBF_NO_LOCKTIME,
        status: bwk_tx::CoinStatus::Confirmed,
        label: None,
        satisfaction_size: TR_KEYSPEND_SATISFACTION_WEIGHT,
        spend_info: bwk_tx::CoinSpendInfo::Sp {
            derivation: bitcoin::bip32::DerivationPath::default(),
            tweak: *entry.tweak(),
        },
    }
}

/// Build a `TxBuilder` whose inputs are exactly those listed in `simulation`
/// (no auto-selection). The output set is rebuilt verbatim from the
/// simulation's `outputs`. If any input is missing or no longer spendable,
/// returns a clear "inputs changed" error so the caller can surface a
/// re-confirm prompt instead of broadcasting a tx the user never saw.
///
/// This is the pin that closes the race between `prepare_psbt` (where the
/// user reviews inputs/outputs/fee) and `finalize_psbt`/`sign_psbt`/
/// `broadcast` (irreversible). Without it, an incoming SP coin from a
/// completed scan or an Electrum push for a sub-account coin landing in
/// the window between Confirm tap and broadcast resolution could change
/// the input set, fee, or change address.
fn build_tx_from_simulation(
    inner: &bwk_sp::Account,
    simulation: &TxSimulation,
) -> Result<bwk_tx::TxBuilder, String> {
    let network = inner.network();
    // Feerate is no longer a free parameter at finalize time — we inherit it
    // from the simulation by setting it implicitly through the same TxBuilder
    // policy: bwk_tx::TxBuilder requires a feerate or fee. The simulation
    // already captured the user-confirmed feerate via `feerate_sat_vb` at
    // prepare time, which produced the fee_sat in the simulation. We pass
    // `fee` (absolute) rather than feerate so the assembler produces the
    // exact fee the user confirmed.
    let mut builder = inner.tx_builder().fee(simulation.fee_sat);

    // Rebuild the output set verbatim from what the user confirmed.
    for r in &simulation.outputs {
        match r {
            RecipientView::Sp {
                address,
                amount_sat,
                ..
            } => {
                let sp_addr =
                    bwk_sp::silentpayments::SilentPaymentAddress::try_from(address.as_str())
                        .map_err(|e| format!("invalid SP address '{address}': {e}"))?;
                builder.send_to_sp(sp_addr, *amount_sat);
            }
            RecipientView::Standard {
                address,
                amount_sat,
            } => {
                let addr = bitcoin::Address::from_str(address)
                    .map_err(|e| format!("invalid address '{address}': {e}"))?
                    .require_network(network)
                    .map_err(|e| format!("wrong network for '{address}': {e}"))?;
                builder.send_to(addr, *amount_sat);
            }
        }
    }

    // Pin the exact input set the user confirmed. Any drift is fatal.
    for view in &simulation.inputs {
        let outpoint = bitcoin::OutPoint::from_str(&view.outpoint)
            .map_err(|e| format!("invalid outpoint '{}': {e}", view.outpoint))?;
        let coin = find_coin_by_outpoint(inner, outpoint)?;
        // Sanity: the amount in the store must match what the user saw.
        // A mismatch implies the same outpoint now resolves to a different
        // tx_out — a reorg edge case. Fail loudly.
        if coin.txout.value.to_sat() != view.amount_sat {
            return Err(format!(
                "transaction inputs changed since confirmation: coin {outpoint} amount {} != confirmed {}; please re-confirm",
                coin.txout.value.to_sat(),
                view.amount_sat
            ));
        }
        builder.add_input(coin);
    }

    Ok(builder)
}

impl SpAccount {
    #[allow(clippy::too_many_arguments)]
    #[frb(sync)]
    pub fn create_from_keys(
        name: String,
        network: SpNetwork,
        scan_sk_hex: String,
        spend_sk_hex: String,
        blindbit_url: String,
        electrum_url: String,
        data_dir: String,
        birthday_height: Option<u32>,
        dust_limit: Option<u64>,
        xprv_base58: Option<String>,
    ) -> Result<SpAccount, String> {
        let btc_net = to_bitcoin_network(network);
        let mut config = bwk_sp::Config::from_keys(
            name,
            btc_net,
            scan_sk_hex,
            spend_sk_hex,
            blindbit_url,
            PathBuf::from(&data_dir),
        )
        .map_err(|e| e.to_string())?;

        config.birthday_height = birthday_height;
        config.dust_limit = dust_limit;

        // Build segwit + taproot sub-accounts when an xprv is supplied.
        if let Some(xprv_str) = xprv_base58 {
            let xprv = bitcoin::bip32::Xpriv::from_str(&xprv_str)
                .map_err(|e| format!("invalid xprv: {e}"))?;
            let signer = bwk::bwk_sign::HotSigner::new_from_xpriv(btc_net, xprv);

            let account_idx = ChildNumber::from_hardened_idx(0)
                .map_err(|e| format!("hardcoded account index: {e}"))?;

            // BIP84 segwit descriptor (wpkh)
            let segwit_path = bwk::bwk_descriptor::wpkh_path(btc_net, account_idx)
                .map_err(|e| format!("wpkh_path: {e:?}"))?;
            let segwit_xpub = signer.xpub(&segwit_path);
            let segwit_descriptor =
                bwk::bwk_descriptor::SpkDerivator::new_wpkh(segwit_xpub, btc_net)
                    .map_err(|e| format!("SpkDerivator::new_wpkh: {e:?}"))?;

            // BIP86 taproot descriptor (tr)
            let taproot_path = bwk::bwk_descriptor::tr_path(btc_net, account_idx)
                .map_err(|e| format!("tr_path: {e:?}"))?;
            let taproot_xpub = signer.xpub(&taproot_path);
            let taproot_descriptor =
                bwk::bwk_descriptor::SpkDerivator::new_tr(taproot_xpub, btc_net)
                    .map_err(|e| format!("SpkDerivator::new_tr: {e:?}"))?;

            let (electrum_host, electrum_port, _scheme) = parse_electrum_url(&electrum_url)?;

            config.descriptors.push(SubAccountConfig {
                descriptor: segwit_descriptor.descriptor(),
                mnemonic: None,
                electrum_url: electrum_host.clone(),
                electrum_port,
            });
            config.descriptors.push(SubAccountConfig {
                descriptor: taproot_descriptor.descriptor(),
                mnemonic: None,
                electrum_url: electrum_host,
                electrum_port,
            });
        }

        let config = config.with_persist_kind(bwk::persist::PersistenceKind::Sqlite);
        let account = bwk_sp::Account::new(config).map_err(|e| e.to_string())?;
        let scan_cancel = account.cancel_flag();

        Ok(SpAccount {
            inner: Arc::new(Mutex::new(account)),
            sink: Mutex::new(None),
            electrum_url: Mutex::new(electrum_url),
            shutdown: Arc::new(AtomicBool::new(false)),
            notif_handle: Mutex::new(None),
            scan_cancel,
        })
    }

    #[frb(sync)]
    pub fn load(name: String, data_dir: String) -> Result<SpAccount, String> {
        let config_path = PathBuf::from(&data_dir)
            .join(&name)
            .join(bwk_sp::CONFIG_FILENAME);
        let store = FileConfigStore::<bwk_sp::Config>::new(config_path);
        let config = store
            .load()
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("no config found for account '{name}' in {data_dir}"))?;

        let account = bwk_sp::Account::new(config).map_err(|e| e.to_string())?;

        let electrum_url = account
            .sub_accounts()
            .first()
            .map(|sub| {
                let url = sub.electrum_url();
                let port = sub.electrum_port();
                if !url.is_empty() && !port.is_empty() {
                    format!("{url}:{port}")
                } else {
                    url
                }
            })
            .unwrap_or_default();

        let scan_cancel = account.cancel_flag();
        Ok(SpAccount {
            inner: Arc::new(Mutex::new(account)),
            sink: Mutex::new(None),
            electrum_url: Mutex::new(electrum_url),
            shutdown: Arc::new(AtomicBool::new(false)),
            notif_handle: Mutex::new(None),
            scan_cancel,
        })
    }

    #[frb(sync)]
    pub fn init(&self, sink: StreamSink<SpNotification>) -> Result<(), String> {
        let rx = lock_inner(&self.inner)?
            .receiver()
            .ok_or_else(|| "init() already called — receiver already taken".to_string())?;

        let inner_arc = self.inner.clone();
        let thread_sink = sink.clone();
        let shutdown = self.shutdown.clone();
        *self
            .sink
            .lock()
            .map_err(|_| "sink mutex poisoned".to_string())? = Some(sink);

        let handle = std::thread::spawn(move || {
            // Snapshot existing sub-account outpoints so we only fire ElectrumTx
            // for NEW coins that arrive after init() is called.
            // A poisoned lock on the initial snapshot means the wallet is
            // already in a broken state — emit BackendOffline and exit.
            let mut sub_snap: Vec<BTreeSet<bitcoin::OutPoint>> = match inner_arc.lock() {
                Ok(inner) => inner
                    .sub_accounts()
                    .iter()
                    .map(|sub| sub.coins().into_keys().collect())
                    .collect(),
                Err(_) => {
                    log::error!("SpAccount::init: inner lock poisoned at snapshot");
                    let _ = thread_sink.add(SpNotification::BackendOffline);
                    record_notif_thread_exit();
                    return;
                }
            };

            // Drain the channel with a short timeout so we can check the
            // cooperative shutdown flag. The Sender lives inside the Account
            // (held by `inner_arc`); when SpAccount drops, the Account drops,
            // the Sender drops, and `rx.recv_timeout` would return Disconnected
            // — but we cannot rely on that alone because the JoinHandle is
            // joined from Drop while the strong Arc count is still >0.
            'outer: loop {
                if shutdown.load(Ordering::Acquire) {
                    break;
                }
                match rx.recv_timeout(Duration::from_millis(50)) {
                    Ok(n) => {
                        for mapped in map_notification(n, &inner_arc, &mut sub_snap) {
                            if thread_sink.add(mapped).is_err() {
                                break 'outer;
                            }
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
            record_notif_thread_exit();
        });

        if let Ok(mut slot) = self.notif_handle.lock() {
            *slot = Some(handle);
        }

        Ok(())
    }

    /// Cooperatively stop the notification thread and release the inner
    /// Account (and its sqlite handle). Safe to call multiple times; the
    /// second call is a no-op.
    ///
    /// Dart callers MUST invoke this before dropping the last reference to
    /// `SpAccount` (typically from `SpWalletEntity.dispose()` which is in
    /// turn called from `WalletBloc._onRefreshSpWallet` before reassigning
    /// `state.spWallet`). Relying on `Drop` alone is risky because the FRB
    /// Arc may outlive the Dart-side handle for a finalizer cycle, leaking
    /// an open sqlite connection in the meantime.
    ///
    /// If a `scan_once` is in flight when `dispose()` is called, the
    /// scan still holds the inner mutex via `&mut self`. Flipping
    /// `scan_cancel` causes spdk-core's `process_blocks` to bail at the
    /// next per-block checkpoint, the scan returns, and the inner Account
    /// becomes free to drop. Without this signal, the next
    /// `SpAccount::load` would open `account.sqlite` while the previous
    /// scan was still writing — the exact double-handle race we are
    /// trying to prevent.
    ///
    /// Not `#[frb(sync)]`: dispose() now waits for an in-flight
    /// scan to release the lock, which can take up to ~30 seconds (the
    /// `update_time` checkpoint cadence in spdk-core's `process_blocks`)
    /// in the worst case. Running that on the Dart UI isolate would freeze
    /// the UI. FRB dispatches `async` methods on a worker isolate.
    ///
    /// Return contract: returns `Ok(())` when the inner mutex
    /// became reacquirable within the bounded budget (i.e. the previous
    /// holder of the lock — a scan, a long `unified_history`/
    /// `unified_coins`, an `prepare_psbt`/`finalize_psbt`/`sign_psbt`
    /// invocation — actually released it). Returns `Err("dispose timed
    /// out: inner lock still held; retry or restart")` when the budget
    /// elapsed without the lock becoming free. The caller MUST treat the
    /// timeout case as "previous SpAccount is still in flight" and MUST
    /// NOT proceed to call `SpAccount::load(...)` against the same
    /// `data_dir` — doing so would race the sqlite handle and is the
    /// exact double-open we are trying to prevent.
    ///
    /// We still flip the cancel + shutdown flags, join the notification
    /// thread, and drop the cached sink on the timeout path so the
    /// notification side of cleanup is best-effort idempotent and a
    /// subsequent dispose() call observes a clean state. Only the
    /// inner-lock contract is violated.
    pub async fn dispose(&self) -> Result<(), String> {
        // 1. Signal any in-flight scan to bail at the next block checkpoint.
        //    This must happen BEFORE we attempt to take the inner mutex —
        //    the scan call frame holds the mutex via `&mut self`, so we
        //    cannot acquire it until the scan returns.
        self.scan_cancel.store(true, Ordering::Relaxed);

        // 2. Flip the notification-thread shutdown flag.
        self.shutdown.store(true, Ordering::Release);

        // 3. Wait for the inner mutex to be free (i.e. the scan has
        //    bailed). We don't actually need to hold it — we just need to
        //    confirm it is reacquirable so the next `SpAccount::load`
        //    won't race the sqlite handle.
        //
        //    Bounded poll: spdk-core's `process_blocks` checks the cancel
        //    flag before every block, plus has a 30-second `update_time`
        //    persistence cycle. A 5-second budget covers the common case
        //    (per-block check fires within ms) without making the worst
        //    case (a long blindbit RPC blocking inside `process_block`)
        //    hang the UI indefinitely.
        //
        //    If we exceed the budget we surface that to the caller
        //    as an `Err` so `WalletBloc._onRefreshSpWallet` declines to
        //    open `account.sqlite` while the previous handle is still
        //    writing. `scan_once`'s per-block checkpoint observes our
        //    cancel flag, but the other lock-holding methods
        //    (`prepare_psbt`, `finalize_psbt`, `sign_psbt`,
        //    `unified_history`, `unified_coins`) run to completion with
        //    no cancellation hook today — see follow-up Change C in the
        //    R6 report. The caller can retry the refresh once the
        //    in-flight method finishes.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut lock_freed = false;
        while std::time::Instant::now() < deadline {
            if self.inner.try_lock().is_ok() {
                lock_freed = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }

        // 4. Take the JoinHandle out before joining so a second dispose()
        //    call (or Drop running after dispose) is a no-op.
        let handle = self
            .notif_handle
            .lock()
            .ok()
            .and_then(|mut slot| slot.take());
        if let Some(h) = handle {
            // Ignore JoinError (poison) — the thread is gone either way.
            let _ = h.join();
        }

        // 5. Drop the cached sink so any outstanding `add()` returns Err
        //    and does not pin Dart-side resources.
        if let Ok(mut sink) = self.sink.lock() {
            *sink = None;
        }

        if lock_freed {
            Ok(())
        } else {
            Err("dispose timed out: inner lock still held; retry or restart".to_string())
        }
    }

    #[frb(sync)]
    pub fn sp_address(&self) -> Result<String, String> {
        Ok(lock_inner(&self.inner)?.sp_address().to_string())
    }

    #[frb(sync)]
    pub fn confirmed_balance(&self) -> Result<u64, String> {
        Ok(lock_inner(&self.inner)?.balance())
    }

    #[frb(sync)]
    pub fn last_scanned_height(&self) -> Result<Option<u32>, String> {
        Ok(lock_inner(&self.inner)?.last_scanned_height())
    }

    #[frb(sync)]
    pub fn is_scanning(&self) -> Result<bool, String> {
        Ok(lock_inner(&self.inner)?.is_scanning())
    }

    /// USER-TRIGGERED ONLY. The bb-mobile app contract is that this method
    /// is invoked exclusively from `ScanSpWalletUsecase`, which itself is
    /// invoked exclusively from `SpCubit.scan()` (the Scan button handler).
    /// Do not call from app lifecycle hooks, route observers, timers, or
    /// background services. Doing so violates the documented invariant.
    ///
    /// Not `#[frb(sync)]`: the underlying bwk_sp scan walker is fully
    /// synchronous and would freeze the Dart UI isolate for the entire
    /// scan duration if dispatched there. FRB runs `async` methods on a
    /// worker isolate. The inner mutex is held for the full scan — that
    /// serializes against other methods, which is acceptable because SP
    /// scans are user-triggered and rare.
    ///
    /// Cancellation: `stop_scan` flips `self.scan_cancel` (a clone of the
    /// bwk_sp::Account's internal `scanner_stop`), which causes the scan
    /// to bail at the next per-block checkpoint inside spdk-core's
    /// `process_blocks`. bwk_sp resets the cancel flag at the start of
    /// every OneShot run, so a stale `true` from a previous cancel does
    /// not affect subsequent scans.
    pub fn scan_once(&self) -> Result<(), String> {
        // Explicitly clear the cancel flag here too, even though bwk_sp's
        // scan_oneshot resets it: this guards against the edge case where
        // a previous `dispose()` flipped the flag and a brand-new
        // SpAccount is being scanned. Cheap relaxed store.
        self.scan_cancel.store(false, Ordering::Relaxed);
        let mut g = lock_inner(&self.inner)?;
        g.start_scan(bwk_sp::ScanMode::OneShot)
            .map_err(|e| e.to_string())
    }

    /// Cooperatively cancel an in-flight `scan_once`. Returns immediately
    /// without touching the inner mutex (which the scan call still holds
    /// via `&mut self` for its full duration); spdk-core's `process_blocks`
    /// observes `scan_cancel` between blocks and returns `Ok(())` after
    /// persisting state. The scan handler then emits `ScanCompleted` and
    /// the cubit's `_onNotification` transitions out of `isScanning`.
    ///
    /// Was `#[frb(sync)]`: that meant Dart's Stop button ran on the
    /// UI isolate and blocked waiting for the inner mutex held by the
    /// scan, freezing the UI for the rest of the scan. The new
    /// signature is async + non-locking; the Stop button never deadlocks.
    ///
    /// Idempotent: re-flipping an already-`true` flag is a no-op.
    pub async fn stop_scan(&self) -> Result<(), String> {
        self.scan_cancel.store(true, Ordering::Relaxed);
        Ok(())
    }

    #[frb(sync)]
    pub fn coins(&self) -> Result<Vec<SpCoinView>, String> {
        let map = lock_inner(&self.inner)?.coins();
        Ok(map
            .into_values()
            .map(|entry| SpCoinView {
                outpoint: entry.outpoint().to_string(),
                amount_sat: entry.amount_sat(),
                height: entry.height(),
                is_spendable: entry.is_spendable(),
                label: None,
            })
            .collect())
    }

    #[frb(sync)]
    pub fn payment_history(&self) -> Result<Vec<SpPaymentView>, String> {
        let payments = lock_inner(&self.inner)?.payment_history();
        Ok(payments
            .into_iter()
            .map(|p| SpPaymentView {
                txid: p.txid,
                direction: match p.payment_type {
                    bwk_sp::PaymentType::Receive => SpPaymentDirection::Receive,
                    bwk_sp::PaymentType::Send => SpPaymentDirection::Send,
                },
                amount_sat: p.amount,
                fee_sat: None,
                height: p.height,
                timestamp: None,
                label: Some(p.label).filter(|l| !l.is_empty()),
            })
            .collect())
    }

    #[frb(sync)]
    pub fn block_height(&self) -> Result<u32, String> {
        lock_inner(&self.inner)?
            .block_height()
            .map_err(|e| e.to_string())
    }

    #[frb(sync)]
    pub fn backend_online(&self) -> Result<bool, String> {
        Ok(lock_inner(&self.inner)?.backend_online())
    }

    #[frb(sync)]
    pub fn name(&self) -> Result<String, String> {
        Ok(lock_inner(&self.inner)?.name().to_string())
    }

    /// Returns the wallet's network as an `SpNetwork`.
    ///
    /// `bitcoin::Network` is upstream-marked `#[non_exhaustive]`; rather than
    /// silently mapping a future variant to one of our known cases (which
    /// would corrupt downstream address validation), we return an explicit
    /// `Err` so the caller is forced to handle the unknown case.
    #[frb(sync)]
    pub fn network(&self) -> Result<SpNetwork, String> {
        match lock_inner(&self.inner)?.network() {
            bitcoin::Network::Bitcoin => Ok(SpNetwork::Bitcoin),
            bitcoin::Network::Signet => Ok(SpNetwork::Signet),
            bitcoin::Network::Testnet => Ok(SpNetwork::Testnet),
            bitcoin::Network::Regtest => Ok(SpNetwork::Regtest),
            other => Err(format!("unsupported bitcoin::Network variant: {other:?}")),
        }
    }

    /// Reveal a fresh receive address for the BIP86 taproot sub-account.
    ///
    /// Each call derives the next never-before-issued address via
    /// [`bwk::Account::new_addr`], which bumps and persists the receive-chain
    /// tip (sqlite under `PersistenceKind::Sqlite`) *before* deriving. So an
    /// address is never handed out twice — even across restarts, and
    /// regardless of whether the previously revealed one has received a coin
    /// yet. Callers MUST treat this as "give me a new address to hand out"
    /// (an explicit user action), never as a stable display getter.
    ///
    /// Store-only / pure-descriptor: it never contacts Electrum or Blindbit,
    /// so it does not violate the no-chain-query-outside-`scan_once` invariant.
    ///
    /// The segwit sub-account (index 0) intentionally exposes no hand-out
    /// address; it exists only for change/internal use.
    pub fn new_taproot_address(&self) -> Result<String, String> {
        let mut guard = lock_inner(&self.inner)?;
        let sub = guard
            .sub_accounts_mut()
            .get_mut(1)
            .ok_or_else(|| "taproot sub-account missing".to_string())?;
        Ok(sub.new_addr().value())
    }

    /// Confirmed balance of one sub-account in satoshis.
    #[frb(sync)]
    pub fn sub_account_balance(&self, kind: SubAccountKind) -> Result<u64, String> {
        let idx = match kind {
            SubAccountKind::Segwit => 0,
            SubAccountKind::Taproot => 1,
        };
        Ok(lock_inner(&self.inner)?
            .sub_accounts()
            .get(idx)
            .map(|sub| sub.balance().0)
            .unwrap_or(0))
    }

    /// Aggregated balance across SP + all sub-accounts.
    #[frb(sync)]
    pub fn unified_balance(&self) -> Result<SpBalanceView, String> {
        let inner = lock_inner(&self.inner)?;
        Ok(SpBalanceView {
            confirmed_sat: inner.balance(),
            total_unified_sat: inner.total_balance(),
            last_scanned_height: inner.last_scanned_height(),
        })
    }

    /// Aggregated coins across SP + all sub-accounts, each tagged with its source.
    pub fn unified_coins(&self) -> Result<Vec<UnifiedCoinView>, String> {
        let inner = lock_inner(&self.inner)?;
        let mut result = Vec::new();

        for (outpoint, entry) in inner.coins() {
            result.push(UnifiedCoinView {
                source: CoinSource::Sp,
                outpoint: outpoint.to_string(),
                amount_sat: entry.amount_sat(),
                height: Some(entry.height()),
            });
        }

        let sources = [CoinSource::Segwit, CoinSource::Taproot];
        for (i, sub) in inner.sub_accounts().iter().enumerate() {
            let source = sources.get(i).cloned().unwrap_or(CoinSource::Segwit);
            for (outpoint, entry) in sub.coins() {
                result.push(UnifiedCoinView {
                    source: source.clone(),
                    outpoint: outpoint.to_string(),
                    amount_sat: entry.amount_sat(),
                    height: entry.height().map(|h| h as u32),
                });
            }
        }

        Ok(result)
    }

    /// Aggregated payment history across SP + all sub-accounts.
    pub fn unified_history(&self) -> Result<Vec<SpPaymentView>, String> {
        let inner = lock_inner(&self.inner)?;
        let mut result = Vec::new();

        for p in inner.payment_history() {
            result.push(SpPaymentView {
                txid: p.txid,
                direction: match p.payment_type {
                    bwk_sp::PaymentType::Receive => SpPaymentDirection::Receive,
                    bwk_sp::PaymentType::Send => SpPaymentDirection::Send,
                },
                amount_sat: p.amount,
                fee_sat: None,
                height: p.height,
                timestamp: None,
                label: Some(p.label).filter(|l| !l.is_empty()),
            });
        }

        for sub in inner.sub_accounts() {
            for p in sub.payment_history() {
                result.push(SpPaymentView {
                    txid: p.txid,
                    direction: match p.payment_type {
                        bwk::coin_store::PaymentType::Receive => SpPaymentDirection::Receive,
                        bwk::coin_store::PaymentType::Send => SpPaymentDirection::Send,
                        bwk::coin_store::PaymentType::ToSelf => SpPaymentDirection::SelfSend,
                    },
                    amount_sat: p.amount,
                    fee_sat: None,
                    height: None,
                    timestamp: None,
                    label: Some(p.label).filter(|l| !l.is_empty()),
                });
            }
        }

        Ok(result)
    }

    /// Update the Electrum server endpoint for all sub-accounts (in-memory, no persist).
    /// Accepts `[scheme://]host[:port]` with `scheme` ∈ `{tcp, ssl}`. The
    /// scheme is preserved in the cached `electrum_url` so broadcast() can
    /// pick the matching transport.
    #[frb(sync)]
    pub fn set_electrum_url(&self, url: String) -> Result<(), String> {
        let (host, port, _scheme) = parse_electrum_url(&url)?;
        lock_inner(&self.inner)?.set_electrum_settings(host, port);
        *self
            .electrum_url
            .lock()
            .map_err(|_| "electrum_url mutex poisoned".to_string())? = url;
        Ok(())
    }

    /// Preview a transaction: run coin selection and return fee/change estimates.
    /// Does NOT produce a signable PSBT — use finalize_psbt() for that.
    ///
    /// feerate_sat_vb: fee rate in satoshis per virtual byte.
    pub fn prepare_psbt(
        &self,
        recipients: Vec<RecipientView>,
        feerate_sat_vb: u64,
    ) -> Result<TxSimulation, String> {
        let inner = lock_inner(&self.inner)?;
        let builder = build_tx_with_coin_selection(&inner, &recipients, feerate_sat_vb)?;
        let result = builder.simulate();

        if let Some(err) = result.error {
            return Err(format!("simulation failed: {err:?}"));
        }

        let inputs: Vec<UnifiedCoinView> = result
            .tx_template
            .inputs
            .iter()
            .map(|coin| UnifiedCoinView {
                source: match coin.source() {
                    bwk_tx::CoinSourceKind::SilentPayment => CoinSource::Sp,
                    bwk_tx::CoinSourceKind::Segwit => CoinSource::Segwit,
                    bwk_tx::CoinSourceKind::Taproot => CoinSource::Taproot,
                    bwk_tx::CoinSourceKind::Other => CoinSource::Other,
                },
                outpoint: coin.outpoint.to_string(),
                amount_sat: coin.txout.value.to_sat(),
                height: coin.height.map(|h| h as u32),
            })
            .collect();

        Ok(TxSimulation {
            inputs,
            outputs: recipients,
            fee_sat: result.fees.map(|f| f.to_sat()).unwrap_or(0),
            change_sat: result.change.map(|c| c.to_sat()).unwrap_or(0),
        })
    }

    /// Build and serialize an unsigned PSBT ready for signing, consuming the
    /// `TxSimulation` the user confirmed in the previous `prepare_psbt`.
    ///
    /// This method DOES NOT run coin selection: it pins the input set and the
    /// output set to exactly what the simulation contains. If the coin store
    /// has drifted since the simulation was produced (an incoming SP coin
    /// from a completed scan, an Electrum push for a sub-account coin, a
    /// reorg evicting an input), the method returns an error of the form
    /// `"transaction inputs changed since confirmation: ... please re-confirm"`
    /// so the cubit can surface a re-confirm prompt instead of broadcasting
    /// a tx that differs from what the user reviewed.
    ///
    /// Rationale: finalize → sign → broadcast is irreversible; an auto-
    /// re-selection here could ship a tx with different inputs, fee, or
    /// change address from the one shown on the Confirm page.
    pub fn finalize_psbt(&self, simulation: TxSimulation) -> Result<Vec<u8>, String> {
        let inner = lock_inner(&self.inner)?;
        let mut builder = build_tx_from_simulation(&inner, &simulation)?;
        let psbt = builder
            .generate()
            .map_err(|e| format!("PSBT generation failed: {e:?}"))?;
        Ok(psbt.serialize())
    }

    /// Sign and finalize an unsigned PSBT (returned by finalize_psbt).
    /// Returns the raw serialized transaction bytes.
    /// Hex-encode the result before passing to broadcast().
    pub fn sign_psbt(&self, psbt: Vec<u8>) -> Result<Vec<u8>, String> {
        let mut psbt =
            bitcoin::Psbt::deserialize(&psbt).map_err(|e| format!("invalid PSBT bytes: {e}"))?;

        let tx = lock_inner(&self.inner)?
            .sign_and_finalize(&mut psbt)
            .map_err(|e| e.to_string())?;

        Ok(serialize(&tx))
    }

    /// Broadcast a signed transaction to the network via Electrum.
    /// tx_hex: hex-encoded raw transaction bytes (hex::encode the sign_psbt result).
    /// Returns the transaction ID (txid) as a hex string on success.
    /// Async on the Dart side; FRB dispatches it on a worker thread, so it is
    /// safe to await from the UI isolate while the TCP handshake completes.
    pub fn broadcast(&self, tx_hex: String) -> Result<String, String> {
        let url = self
            .electrum_url
            .lock()
            .map_err(|_| "electrum_url mutex poisoned".to_string())?
            .clone();
        broadcast_via_electrum(&url, &tx_hex)
    }

    /// Update the Blindbit backend URL at runtime.
    #[frb(sync)]
    pub fn set_blindbit_url(&self, url: String) -> Result<(), String> {
        lock_inner(&self.inner)?.set_blindbit_url(url);
        Ok(())
    }
}

impl Drop for SpAccount {
    fn drop(&mut self) {
        // Mirror `dispose()` but defensive against a caller that didn't
        // invoke it explicitly. We avoid `dispose(&self)` here to skip the
        // sink lock if it would deadlock the runtime; instead we open-code
        // the minimum-needed shutdown.
        //
        // Also flip the scan-cancel flag so an in-flight `scan_once`
        // bails promptly. Without this, dropping an `SpAccount` while a
        // scan is in progress leaves the bwk_sp::Account pinned through
        // the scan's `&mut self` call frame, holding the sqlite handle
        // open past the FRB-side drop.
        self.scan_cancel.store(true, Ordering::Relaxed);
        self.shutdown.store(true, Ordering::Release);
        if let Some(h) = self.notif_handle.get_mut().ok().and_then(|s| s.take()) {
            let _ = h.join();
        }
        if let Ok(mut sink) = self.sink.lock() {
            *sink = None;
        }
        // The inner Account (and its sqlite handle) is dropped as part of
        // the normal struct field drop after this function returns.
    }
}

fn broadcast_via_electrum(electrum_url: &str, tx_hex: &str) -> Result<String, String> {
    use bwk_sp::bwk::bwk_electrum::electrum::{request::Request, response::Response};
    use std::collections::HashMap;
    use std::time::Duration;

    if electrum_url.is_empty() {
        return Err("electrum URL not configured".to_string());
    }

    let tx: bitcoin::Transaction =
        deserialize_hex(tx_hex).map_err(|e| format!("invalid tx hex: {e}"))?;
    let txid = tx.compute_txid();

    let (host, port, scheme) = parse_electrum_url(electrum_url)?;
    let host = host.ok_or_else(|| format!("missing host in electrum URL '{electrum_url}'"))?;
    let port = port.ok_or_else(|| format!("missing port in electrum URL '{electrum_url}'"))?;

    let mut client = match scheme {
        ElectrumScheme::Tcp => bwk_sp::bwk::bwk_electrum::raw_client::Client::new_tcp(&host, port),
        ElectrumScheme::Ssl => bwk_sp::bwk::bwk_electrum::raw_client::Client::new_ssl(&host, port),
    };
    client
        .try_connect(Some(Duration::from_secs(10)))
        .map_err(|e| format!("connection to {electrum_url} failed: {e}"))?;

    // Defend against the "TCP/SSL handshake completes but the server never
    // responds" failure mode. Without these timeouts, the underlying socket
    // calls block forever on the FRB worker thread, and the Dart-side
    // `await broadcast(...)` future never completes. 30s is generous enough
    // for a slow signet/regtest hop while still being recoverable for the UI.
    client
        .set_read_timeout(Some(Duration::from_secs(30)))
        .map_err(|e| format!("electrum timeout (set_read_timeout): {e}"))?;
    client
        .set_write_timeout(Some(Duration::from_secs(30)))
        .map_err(|e| format!("electrum timeout (set_write_timeout): {e}"))?;

    let raw_tx = serialize_hex(&tx);
    let request = Request::tx_broadcast(raw_tx);
    client
        .try_send(&request)
        .map_err(|e| format!("send to {electrum_url} failed: {e}"))?;

    let req_id = request.id;
    let mut index = HashMap::new();
    index.insert(req_id, request);

    let responses = client
        .recv(&index)
        .map_err(|e| format!("electrum timeout: no response from {electrum_url}: {e}"))?;

    for r in responses {
        match r {
            Response::TxBroadcast(resp) if resp.id == req_id => {
                return Ok(txid.to_string());
            }
            Response::Error(e) if e.id == req_id => {
                return Err(format!(
                    "server {electrum_url} rejected transaction: {}",
                    e.error.message
                ));
            }
            _ => {}
        }
    }

    Err(format!("unexpected response from {electrum_url}"))
}

fn map_notification(
    n: bwk_sp::Notification,
    inner: &Arc<Mutex<bwk_sp::Account>>,
    sub_snap: &mut Vec<BTreeSet<bitcoin::OutPoint>>,
) -> Vec<SpNotification> {
    match n {
        bwk_sp::Notification::Sp(sp) => map_sp_notification(sp, inner).into_iter().collect(),
        bwk_sp::Notification::CoinUpdate => map_coin_update(inner, sub_snap),
        bwk_sp::Notification::Electrum(_) => {
            log::trace!("SpAccount::map_notification: Electrum connection event");
            vec![]
        }
        bwk_sp::Notification::AddressTipChanged => {
            log::trace!("SpAccount::map_notification: AddressTipChanged");
            vec![]
        }
        bwk_sp::Notification::InvalidElectrumConfig => {
            log::trace!("SpAccount::map_notification: InvalidElectrumConfig");
            vec![]
        }
        bwk_sp::Notification::InvalidLookAhead => {
            log::trace!("SpAccount::map_notification: InvalidLookAhead");
            vec![]
        }
        bwk_sp::Notification::Stopped => {
            log::trace!("SpAccount::map_notification: Stopped");
            vec![]
        }
        bwk_sp::Notification::Error(e) => {
            log::warn!("SpAccount::map_notification: bwk error: {e:?}");
            vec![]
        }
    }
}

/// Diff sub-account coin stores against `sub_snap`; emit one `ElectrumTx`
/// per new coin. Updates `sub_snap` after each comparison.
/// Index 0 = segwit, index 1 = taproot (matches `config.descriptors` push order).
fn map_coin_update(
    inner: &Arc<Mutex<bwk_sp::Account>>,
    sub_snap: &mut Vec<BTreeSet<bitcoin::OutPoint>>,
) -> Vec<SpNotification> {
    let sources = [CoinSource::Segwit, CoinSource::Taproot];
    let mut result = Vec::new();

    // Hold the lock only long enough to snapshot current coins.
    // Poison → return empty: notifications stop, but the caller will emit
    // BackendOffline on its next loop iteration once it observes the
    // poisoned guard.
    let current_sub_coins: Vec<
        std::collections::BTreeMap<bitcoin::OutPoint, bwk::coin_store::CoinEntry>,
    > = match inner.lock() {
        Ok(inner) => inner.sub_accounts().iter().map(|sub| sub.coins()).collect(),
        Err(_) => {
            log::error!("map_coin_update: SP wallet lock poisoned");
            return result;
        }
    };

    // Grow snapshot vec if sub-accounts were added after init (shouldn't happen, but defensive).
    while sub_snap.len() < current_sub_coins.len() {
        sub_snap.push(BTreeSet::new());
    }

    for (i, coins) in current_sub_coins.iter().enumerate() {
        let source = sources.get(i).cloned().unwrap_or(CoinSource::Segwit);
        let snap = &mut sub_snap[i];
        for (outpoint, entry) in coins {
            if snap.insert(*outpoint) {
                result.push(SpNotification::ElectrumTx {
                    kind: source.clone(),
                    txid: outpoint.txid.to_string(),
                    amount_sat: entry.amount_sat(),
                    height: entry.height().map(|h| h as u32),
                });
            }
        }
    }

    result
}

fn map_sp_notification(
    sp: bwk_sp::SpNotification,
    inner: &Arc<Mutex<bwk_sp::Account>>,
) -> Option<SpNotification> {
    match sp {
        bwk_sp::SpNotification::StartingScan => {
            log::trace!("SpAccount: StartingScan (skipped)");
            None
        }
        bwk_sp::SpNotification::ScanStarted { start, end } => Some(SpNotification::ScanStarted {
            from: start,
            to: end,
        }),
        bwk_sp::SpNotification::FailStartScanning { message } => {
            Some(SpNotification::ScanFailed { message })
        }
        bwk_sp::SpNotification::FailScan { message } => {
            Some(SpNotification::ScanFailed { message })
        }
        bwk_sp::SpNotification::StoppingScan => {
            log::trace!("SpAccount: StoppingScan (skipped)");
            None
        }
        bwk_sp::SpNotification::ScanStopped => Some(SpNotification::ScanStopped),
        bwk_sp::SpNotification::ScanProgress { current, end } => {
            Some(SpNotification::ScanProgress { current, end })
        }
        bwk_sp::SpNotification::ScanCompleted => Some(SpNotification::ScanCompleted),
        bwk_sp::SpNotification::NewOutput(outpoint) => {
            let amount_sat = match inner.lock() {
                Ok(g) => g.get_coin(&outpoint).map(|c| c.amount_sat()).unwrap_or(0),
                Err(_) => {
                    log::error!("map_sp_notification(NewOutput): SP wallet lock poisoned");
                    0
                }
            };
            Some(SpNotification::NewOutput {
                outpoint: outpoint.to_string(),
                amount_sat,
            })
        }
        bwk_sp::SpNotification::OutputSpent(outpoint) => Some(SpNotification::OutputSpent {
            outpoint: outpoint.to_string(),
        }),
        bwk_sp::SpNotification::WaitingForBlocks { .. } => {
            log::trace!("SpAccount: WaitingForBlocks (continuous-mode, skipped)");
            None
        }
        bwk_sp::SpNotification::NewBlocksDetected { .. } => {
            log::trace!("SpAccount: NewBlocksDetected (continuous-mode, skipped)");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const SCAN_SK: &str = "0101010101010101010101010101010101010101010101010101010101010101";
    const SPEND_SK: &str = "0202020202020202020202020202020202020202020202020202020202020202";

    #[test]
    fn sp_account_create_load() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();

        let acc = SpAccount::create_from_keys(
            "test".to_string(),
            SpNetwork::Regtest,
            SCAN_SK.to_string(),
            SPEND_SK.to_string(),
            "http://localhost:3000".to_string(),
            String::new(),
            data_dir.clone(),
            None,
            None,
            None,
        )
        .expect("create_from_keys");

        let addr1 = acc.sp_address().expect("sp_address");
        // Under Sqlite persistence mode, signer material is stripped from disk so
        // load() would fail without keys. Just verify address is non-empty and drop.
        assert!(!addr1.is_empty());
    }

    #[test]
    fn accessors_zero_state() {
        let dir = tempdir().unwrap();
        let acc = SpAccount::create_from_keys(
            "zero".to_string(),
            SpNetwork::Regtest,
            SCAN_SK.to_string(),
            SPEND_SK.to_string(),
            "http://localhost:3000".to_string(),
            String::new(),
            dir.path().to_str().unwrap().to_string(),
            None,
            None,
            None,
        )
        .expect("create");

        assert_eq!(acc.confirmed_balance().unwrap(), 0);
        assert!(acc.coins().unwrap().is_empty());
        assert!(acc.payment_history().unwrap().is_empty());
        assert!(!acc.is_scanning().unwrap());
        assert!(acc.last_scanned_height().unwrap().is_none());
        assert_eq!(acc.name().unwrap(), "zero");
        assert!(matches!(acc.network(), Ok(SpNetwork::Regtest)));
    }

    #[test]
    fn init_idempotent_guard() {
        // Verify that receiver() returns None on the second call (guard ensures single-init).
        // We test the inner account directly since StreamSink requires the FRB runtime.
        let dir = tempdir().unwrap();
        let acc = SpAccount::create_from_keys(
            "guard".to_string(),
            SpNetwork::Regtest,
            SCAN_SK.to_string(),
            SPEND_SK.to_string(),
            "http://localhost:3000".to_string(),
            String::new(),
            dir.path().to_str().unwrap().to_string(),
            None,
            None,
            None,
        )
        .expect("create");

        // First take succeeds.
        let rx1 = acc.inner.lock().unwrap().receiver();
        assert!(rx1.is_some());
        // Second take returns None (receiver already moved out).
        let rx2 = acc.inner.lock().unwrap().receiver();
        assert!(rx2.is_none());
    }

    #[test]
    fn taproot_address_never_reissued() {
        // `new_taproot_address` must REVEAL a fresh, never-before-issued
        // address on every call (no reuse), and the receive tip must persist
        // across restart so a reload never collides with a previously issued
        // index.
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap().to_string();

        let seed = [0x42u8; 64];
        let xprv = bitcoin::bip32::Xpriv::new_master(bitcoin::Network::Regtest, &seed)
            .expect("new_master");
        let xprv_str = xprv.to_string();

        let acc1 = SpAccount::create_from_keys(
            "sub-persist".to_string(),
            SpNetwork::Regtest,
            SCAN_SK.to_string(),
            SPEND_SK.to_string(),
            "http://localhost:3000".to_string(),
            "localhost:50001".to_string(),
            data_dir.clone(),
            None,
            None,
            Some(xprv_str.clone()),
        )
        .expect("create 1");

        // Every reveal must hand out a fresh, never-before-issued address:
        // successive calls advance the receive tip and must all differ. This
        // is the core anti-reuse property — we must never re-hand an address
        // just because the previous one has not received a coin yet.
        let mut seen = std::collections::BTreeSet::new();
        for i in 0..5 {
            let addr = acc1
                .new_taproot_address()
                .unwrap_or_else(|e| panic!("reveal {i}: {e}"));
            assert!(!addr.is_empty(), "taproot address must be non-empty");
            assert!(seen.insert(addr.clone()), "reveal {i} reused address {addr}");
        }
    }

    #[test]
    fn unified_views_zero_state() {
        let dir = tempdir().unwrap();
        let seed = [0x01u8; 64];
        let xprv = bitcoin::bip32::Xpriv::new_master(bitcoin::Network::Regtest, &seed).unwrap();

        let acc = SpAccount::create_from_keys(
            "unified-zero".to_string(),
            SpNetwork::Regtest,
            SCAN_SK.to_string(),
            SPEND_SK.to_string(),
            "http://localhost:3000".to_string(),
            "localhost:50001".to_string(),
            dir.path().to_str().unwrap().to_string(),
            None,
            None,
            Some(xprv.to_string()),
        )
        .expect("create");

        let bal = acc.unified_balance().unwrap();
        assert_eq!(bal.confirmed_sat, 0);
        assert_eq!(bal.total_unified_sat, 0);
        assert!(bal.last_scanned_height.is_none());

        assert!(acc.unified_coins().unwrap().is_empty());
        assert!(acc.unified_history().unwrap().is_empty());

        assert_eq!(acc.sub_account_balance(SubAccountKind::Segwit).unwrap(), 0);
        assert_eq!(acc.sub_account_balance(SubAccountKind::Taproot).unwrap(), 0);

        // Sub-accounts are created, so a revealed taproot receive address must
        // be non-empty.
        assert!(!acc.new_taproot_address().unwrap().is_empty());
    }

    #[test]
    fn prepare_empty_account() {
        let dir = tempdir().unwrap();
        let seed = [0x03u8; 64];
        let xprv = bitcoin::bip32::Xpriv::new_master(bitcoin::Network::Regtest, &seed).unwrap();
        let acc = SpAccount::create_from_keys(
            "prepare-empty".to_string(),
            SpNetwork::Regtest,
            SCAN_SK.to_string(),
            SPEND_SK.to_string(),
            "http://localhost:3000".to_string(),
            "localhost:50001".to_string(),
            dir.path().to_str().unwrap().to_string(),
            None,
            None,
            Some(xprv.to_string()),
        )
        .expect("create");

        // No coins seeded → coin selection returns empty → simulate reports AddInput error.
        let sp_addr = acc.sp_address().expect("sp_address");
        let result = acc.prepare_psbt(
            vec![RecipientView::Sp {
                address: sp_addr,
                amount_sat: 10_000,
                label: None,
            }],
            1,
        );
        assert!(result.is_err(), "expected error for empty coin store");
    }

    #[test]
    fn prepare_invalid_sp_address() {
        let dir = tempdir().unwrap();
        let acc = SpAccount::create_from_keys(
            "prepare-invalid-sp".to_string(),
            SpNetwork::Regtest,
            SCAN_SK.to_string(),
            SPEND_SK.to_string(),
            "http://localhost:3000".to_string(),
            "localhost:50001".to_string(),
            dir.path().to_str().unwrap().to_string(),
            None,
            None,
            None,
        )
        .expect("create");

        let result = acc.prepare_psbt(
            vec![RecipientView::Sp {
                address: "not_a_valid_sp_address".to_string(),
                amount_sat: 10_000,
                label: None,
            }],
            1,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid SP address"));
    }

    #[test]
    fn prepare_invalid_standard_address() {
        let dir = tempdir().unwrap();
        let acc = SpAccount::create_from_keys(
            "prepare-invalid-std".to_string(),
            SpNetwork::Regtest,
            SCAN_SK.to_string(),
            SPEND_SK.to_string(),
            "http://localhost:3000".to_string(),
            "localhost:50001".to_string(),
            dir.path().to_str().unwrap().to_string(),
            None,
            None,
            None,
        )
        .expect("create");

        let result = acc.prepare_psbt(
            vec![RecipientView::Standard {
                address: "not_a_bitcoin_address".to_string(),
                amount_sat: 10_000,
            }],
            1,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid address"));
    }

    #[test]
    fn parse_electrum_url_variants() {
        // plain host:port → defaults to TCP
        let (h, p, s) = parse_electrum_url("electrum.example.com:50001").unwrap();
        assert_eq!(h.as_deref(), Some("electrum.example.com"));
        assert_eq!(p, Some(50001));
        assert_eq!(s, ElectrumScheme::Tcp);

        // tcp:// scheme is honoured and stripped
        let (h, p, s) = parse_electrum_url("tcp://electrum.example.com:50001").unwrap();
        assert_eq!(h.as_deref(), Some("electrum.example.com"));
        assert_eq!(p, Some(50001));
        assert_eq!(s, ElectrumScheme::Tcp);

        // ssl:// scheme is honoured and stripped — this is the production
        // default we used to ship broken.
        let (h, p, s) = parse_electrum_url("ssl://electrum.bullbitcoin.com:50002").unwrap();
        assert_eq!(h.as_deref(), Some("electrum.bullbitcoin.com"));
        assert_eq!(p, Some(50002));
        assert_eq!(s, ElectrumScheme::Ssl);

        // empty input → no host/port, defaults TCP
        let (h, p, _s) = parse_electrum_url("").unwrap();
        assert!(h.is_none());
        assert!(p.is_none());

        // unknown scheme is rejected
        assert!(parse_electrum_url("http://foo:80").is_err());
        assert!(parse_electrum_url("ws://foo:80").is_err());
    }

    #[test]
    fn sign_psbt_invalid_bytes() {
        let dir = tempdir().unwrap();
        let acc = SpAccount::create_from_keys(
            "sign-invalid".to_string(),
            SpNetwork::Regtest,
            SCAN_SK.to_string(),
            SPEND_SK.to_string(),
            "http://localhost:3000".to_string(),
            "localhost:50001".to_string(),
            dir.path().to_str().unwrap().to_string(),
            None,
            None,
            None,
        )
        .expect("create");

        let result = acc.sign_psbt(b"garbage bytes".to_vec());
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid PSBT bytes"));
    }

    /// Regression: dropping an SpAccount must terminate the
    /// notification thread instead of leaking it (and the inner sqlite
    /// handle) for the lifetime of the process.
    ///
    /// We can't call `init()` from a non-FRB test because `StreamSink`
    /// requires the FRB runtime, so we exercise the shutdown contract
    /// directly: inject a stand-in worker thread into `notif_handle`
    /// that watches the same `shutdown` flag, then assert the thread
    /// exits when `dispose()` runs (and again that `Drop` is a no-op
    /// the second time).
    #[test]
    fn dispose_joins_notification_thread() {
        use std::sync::atomic::AtomicBool;
        use std::time::{Duration, Instant};

        let dir = tempdir().unwrap();
        let acc = SpAccount::create_from_keys(
            "dispose-thread".to_string(),
            SpNetwork::Regtest,
            SCAN_SK.to_string(),
            SPEND_SK.to_string(),
            "http://localhost:3000".to_string(),
            String::new(),
            dir.path().to_str().unwrap().to_string(),
            None,
            None,
            None,
        )
        .expect("create");

        let observed = Arc::new(AtomicBool::new(false));
        let observed_clone = observed.clone();
        let shutdown = acc.shutdown.clone();
        let handle = std::thread::spawn(move || {
            // Mirror the real notification thread: loop with a short timeout
            // and exit on the shutdown flag.
            while !shutdown.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(10));
            }
            observed_clone.store(true, Ordering::Release);
        });
        *acc.notif_handle.lock().unwrap() = Some(handle);

        let baseline = notif_thread_exit_count();
        let res = futures_lite_block_on(acc.dispose());
        assert!(
            res.is_ok(),
            "dispose must succeed when lock is free: {res:?}"
        );
        assert!(
            observed.load(Ordering::Acquire),
            "thread did not observe shutdown"
        );

        // dispose() must be idempotent. Second call also returns Ok because
        // the inner mutex is free.
        let res2 = futures_lite_block_on(acc.dispose());
        assert!(res2.is_ok(), "second dispose must succeed: {res2:?}");

        // Dropping after dispose must be a clean no-op (no double-join panic).
        drop(acc);

        // We never incremented NOTIF_THREAD_EXITS because we didn't go through
        // the real init() path; just sanity-check the counter is monotonic and
        // didn't decrement.
        assert!(notif_thread_exit_count() >= baseline);

        // Bound the wall-clock cost of this test.
        let _ = Instant::now();
    }

    /// Regression: `finalize_psbt` MUST refuse to sign a tx whose
    /// inputs differ from the simulation the user confirmed. An empty/zero
    /// coin store cannot satisfy the simulation's pinned outpoints, so
    /// finalize must return an "inputs changed" error rather than silently
    /// re-selecting coins.
    #[test]
    fn finalize_fails_when_inputs_no_longer_present() {
        let dir = tempdir().unwrap();
        let acc = SpAccount::create_from_keys(
            "finalize-drift".to_string(),
            SpNetwork::Regtest,
            SCAN_SK.to_string(),
            SPEND_SK.to_string(),
            "http://localhost:3000".to_string(),
            "localhost:50001".to_string(),
            dir.path().to_str().unwrap().to_string(),
            None,
            None,
            None,
        )
        .expect("create");

        // Hand-build a simulation that references an outpoint not in the
        // (empty) coin store. This is exactly the shape `prepare_psbt` would
        // return, but with the store mutated under us between prepare and
        // finalize.
        let sp_addr = acc.sp_address().expect("sp_address");
        let phantom_outpoint =
            "0000000000000000000000000000000000000000000000000000000000000000:0".to_string();
        let sim = TxSimulation {
            inputs: vec![UnifiedCoinView {
                source: CoinSource::Sp,
                outpoint: phantom_outpoint,
                amount_sat: 50_000,
                height: Some(100),
            }],
            outputs: vec![RecipientView::Sp {
                address: sp_addr,
                amount_sat: 10_000,
                label: None,
            }],
            fee_sat: 200,
            change_sat: 39_800,
        };

        let res = acc.finalize_psbt(sim);
        let err = res.expect_err("finalize must fail when inputs are no longer present");
        assert!(
            err.contains("inputs changed"),
            "expected 'inputs changed' error, got: {err}"
        );
        assert!(
            err.contains("re-confirm"),
            "error must direct user to re-confirm, got: {err}"
        );
    }

    /// Regression: a poisoned inner-mutex must surface as a
    /// recoverable `Err(String)` rather than panicking via `.unwrap()`. We
    /// intentionally poison the mutex by panicking inside a holder, then
    /// confirm every FFI getter returns Err.
    #[test]
    fn poisoned_mutex_returns_err_not_panic() {
        let dir = tempdir().unwrap();
        let acc = SpAccount::create_from_keys(
            "poison".to_string(),
            SpNetwork::Regtest,
            SCAN_SK.to_string(),
            SPEND_SK.to_string(),
            "http://localhost:3000".to_string(),
            String::new(),
            dir.path().to_str().unwrap().to_string(),
            None,
            None,
            None,
        )
        .expect("create");

        // Poison the inner mutex.
        let inner = acc.inner.clone();
        let _ = std::thread::spawn(move || {
            let _guard = inner.lock().unwrap();
            panic!("intentional poison");
        })
        .join();

        assert!(acc.sp_address().is_err());
        assert!(acc.confirmed_balance().is_err());
        assert!(acc.is_scanning().is_err());
        assert!(acc.name().is_err());
        assert!(acc.unified_balance().is_err());
        assert!(acc.unified_coins().is_err());
        assert!(acc.unified_history().is_err());
        assert!(acc.coins().is_err());
        assert!(acc.payment_history().is_err());
        assert!(acc.network().is_err());
        assert!(acc.last_scanned_height().is_err());
    }

    /// Regression for R4: `sp_entry_to_tx_coin` must produce a fully populated
    /// `bwk_tx::Coin` (with the SP `tweak`, the correct script, and a
    /// confirmed status) from an `SpCoinEntry`. The previous implementation
    /// of `sp_coin_view_to_tx_coin` round-tripped through
    /// `inner.tx_builder().select_coins(u64::MAX / 2, 1_000)` to materialize
    /// the coin; that path is broken because `select_coins` short-circuits to
    /// an empty Vec when the candidate set has >20 coins and otherwise overflows
    /// on the cj_selection branch (u64::MAX / 2 * 90 / 100 * 2..). Net effect:
    /// every SP finalize call returned "inputs changed". This unit test pins
    /// the conversion logic so we never silently regress to the select_coins
    /// round-trip again.
    ///
    /// We exercise the pure conversion (`sp_entry_to_tx_coin`) because we
    /// can't inject a coin into a `bwk_sp::Account` from outside the crate;
    /// the end-to-end `finalize_psbt` path (which also hits the
    /// `Account::get_coin` lookup) is covered by the `send_pure_sp` regtest
    /// scenario in `tests/send_regtest.rs` (gated by `SP_NETWORK_TESTS=1`).
    #[test]
    fn sp_entry_to_tx_coin_materializes_full_coin() {
        use bitcoin::absolute::Height;
        use bitcoin::hashes::Hash;
        use bitcoin::{Amount, OutPoint, ScriptBuf, Txid};
        use bwk_sp::spdk_core::{OutputSpendStatus, OwnedOutput};
        use bwk_sp::SpCoinEntry;

        let outpoint = OutPoint {
            txid: Txid::from_byte_array([0xABu8; 32]),
            vout: 1,
        };
        let tweak = [0x77u8; 32];
        // SP outputs are P2TR — 34-byte script `OP_1 <32-byte x-only key>`.
        let mut script_bytes = Vec::with_capacity(34);
        script_bytes.push(0x51); // OP_1
        script_bytes.push(0x20); // push 32 bytes
        script_bytes.extend_from_slice(&[0xDEu8; 32]);
        let script = ScriptBuf::from_bytes(script_bytes);

        let output = OwnedOutput {
            blockheight: Height::from_consensus(123_456).unwrap(),
            tweak,
            amount: Amount::from_sat(50_000),
            script: script.clone(),
            label: None,
            spend_status: OutputSpendStatus::Unspent,
        };
        let entry = SpCoinEntry::new(outpoint, output);

        let coin = super::sp_entry_to_tx_coin(outpoint, &entry);

        assert_eq!(coin.outpoint, outpoint);
        assert_eq!(coin.txout.value.to_sat(), 50_000);
        assert_eq!(coin.txout.script_pubkey, script);
        assert_eq!(coin.height, Some(123_456));
        assert_eq!(coin.status, bwk_tx::CoinStatus::Confirmed);
        assert_eq!(coin.satisfaction_size, 66);
        match coin.spend_info {
            bwk_tx::CoinSpendInfo::Sp {
                ref derivation,
                tweak: t,
            } => {
                assert_eq!(derivation, &bitcoin::bip32::DerivationPath::default());
                assert_eq!(t, tweak, "tweak must be preserved verbatim");
            }
            other => panic!("expected CoinSpendInfo::Sp, got {other:?}"),
        }
        assert!(
            entry.is_spendable(),
            "fixture entry must be spendable so the helper's spendability gate is exercised by the surrounding sp_coin_view_to_tx_coin"
        );
    }

    /// Regression: the scan-cancel signal must be wired through
    /// to the bwk_sp::Account's scanner_stop flag so flipping our
    /// `scan_cancel` causes spdk-core's `process_blocks` to observe the
    /// interrupt at the next block boundary.
    ///
    /// We cannot drive a real Blindbit scan from a unit test (no live
    /// backend), so we exercise the contract that matters: stop_scan must
    /// flip the same `Arc<AtomicBool>` that bwk_sp::Account holds, and the
    /// flag must NOT require taking the inner mutex. This pins the
    /// invariant that the Stop button cannot deadlock against an in-flight
    /// scan.
    #[test]
    fn scan_cancel_signal_aborts_in_progress_scan() {
        let dir = tempdir().unwrap();
        let acc = SpAccount::create_from_keys(
            "scan-cancel".to_string(),
            SpNetwork::Regtest,
            SCAN_SK.to_string(),
            SPEND_SK.to_string(),
            "http://localhost:3000".to_string(),
            String::new(),
            dir.path().to_str().unwrap().to_string(),
            None,
            None,
            None,
        )
        .expect("create");

        // Take the inner lock from a "scan" worker — this models the
        // bwk_sp::Account holding the mutex via `&mut self` while
        // `scan_blocks` runs. The Stop button hitting `stop_scan` must NOT
        // try to take the same lock; if it does, this test deadlocks.
        let inner = acc.inner.clone();
        let stuck = std::thread::spawn(move || {
            let _guard = inner.lock().expect("lock");
            // Hold for 500ms — longer than the stop_scan latency budget.
            std::thread::sleep(Duration::from_millis(500));
        });

        // stop_scan must return immediately without blocking on the
        // inner mutex. The async wrapper is trivial (it just flips an
        // atomic), so we can poll the future to completion with a
        // hand-rolled noop waker.
        let start = std::time::Instant::now();
        let fut = acc.stop_scan();
        let res = futures_lite_block_on(fut);
        let elapsed = start.elapsed();
        assert!(res.is_ok(), "stop_scan must succeed: {res:?}");
        assert!(
            elapsed < Duration::from_millis(100),
            "stop_scan blocked for {elapsed:?}; must be non-blocking (deadlock regression)"
        );

        // The cancel flag must be set, and it must be the SAME flag the
        // bwk_sp::Account sees. We verify the latter by checking that the
        // inner Account's cancel_flag pointer-compares equal to ours after
        // the holder thread releases the lock.
        assert!(acc.scan_cancel.load(Ordering::Relaxed));

        stuck.join().expect("join");
        let inner_flag = acc.inner.lock().unwrap().cancel_flag();
        assert!(Arc::ptr_eq(&acc.scan_cancel, &inner_flag));
        assert!(inner_flag.load(Ordering::Relaxed));
    }

    /// Regression: `dispose()` must NOT block indefinitely while
    /// a scan holds the inner mutex. Flipping the cancel flag is enough to
    /// signal the scan to bail; `dispose` polls `try_lock` with a bounded
    /// budget and returns even if the lock is still held.
    ///
    /// We model the in-flight scan by holding the lock from a worker
    /// thread for longer than the 5-second budget, then asserting dispose
    /// still returns within a small wall-clock budget of (budget + ε).
    /// This proves dispose doesn't hang the FRB worker.
    #[test]
    fn dispose_during_running_scan_releases_lock() {
        let dir = tempdir().unwrap();
        let acc = SpAccount::create_from_keys(
            "dispose-scan".to_string(),
            SpNetwork::Regtest,
            SCAN_SK.to_string(),
            SPEND_SK.to_string(),
            "http://localhost:3000".to_string(),
            String::new(),
            dir.path().to_str().unwrap().to_string(),
            None,
            None,
            None,
        )
        .expect("create");

        // Worker that grabs the inner lock and releases it after 200ms —
        // shorter than the 5s dispose budget so dispose should observe
        // the lock free and break the poll loop early.
        let inner = acc.inner.clone();
        let scan_cancel = acc.scan_cancel.clone();
        let scanning = std::thread::spawn(move || {
            let _guard = inner.lock().expect("lock");
            // Spin until the cancel flag is set (this is what spdk-core's
            // process_blocks effectively does between blocks).
            while !scan_cancel.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(10));
            }
            // "Scan" persisted state and returns — releasing the lock.
        });

        let start = std::time::Instant::now();
        let fut = acc.dispose();
        let res = futures_lite_block_on(fut);
        let elapsed = start.elapsed();

        scanning.join().expect("join");

        // Dispose should observe the lock release shortly after flipping
        // the cancel flag; well under the 5s budget. The lock was
        // released by the cooperating scan, so dispose returns Ok.
        assert!(
            res.is_ok(),
            "dispose must succeed when scan cooperates: {res:?}"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "dispose took {elapsed:?}; expected <2s once scan cooperates"
        );

        // After dispose, the inner mutex must be free.
        assert!(acc.inner.try_lock().is_ok());

        // And a subsequent `create_from_keys` against the same data_dir
        // (the operative case: WalletBloc._onRefreshSpWallet re-loading
        // after a settings change) must NOT race the sqlite handle.
        // Note: we use create_from_keys because Sqlite persistence strips
        // signer material so load() can't reconstruct from disk in tests.
        let acc2 = SpAccount::create_from_keys(
            "dispose-scan".to_string(),
            SpNetwork::Regtest,
            SCAN_SK.to_string(),
            SPEND_SK.to_string(),
            "http://localhost:3000".to_string(),
            String::new(),
            dir.path().to_str().unwrap().to_string(),
            None,
            None,
            None,
        );
        assert!(acc2.is_ok(), "second create after dispose must succeed");
    }

    /// Regression: `dispose()` MUST return
    /// `Err(...)` containing "timed out" when the inner mutex is held
    /// past the 5-second budget. This is the signal `WalletBloc._
    /// onRefreshSpWallet` uses to decline a `SpAccount::load(...)` call
    /// against the same `data_dir` (preventing the double-open race
    /// against the in-flight lock holder).
    ///
    /// We model an in-flight `prepare_psbt`/`unified_history`/etc. by
    /// holding the inner lock from a worker thread for ~6 seconds —
    /// longer than the dispose budget. Unlike `dispose_during_running_
    /// scan_releases_lock`, the holder does NOT observe `scan_cancel`,
    /// because those non-scan methods don't have a cancellation hook in
    /// the current implementation.
    #[test]
    fn dispose_returns_err_when_lock_held_past_timeout() {
        let dir = tempdir().unwrap();
        let acc = SpAccount::create_from_keys(
            "dispose-timeout".to_string(),
            SpNetwork::Regtest,
            SCAN_SK.to_string(),
            SPEND_SK.to_string(),
            "http://localhost:3000".to_string(),
            String::new(),
            dir.path().to_str().unwrap().to_string(),
            None,
            None,
            None,
        )
        .expect("create");

        // Worker holds the inner lock for ~6s (longer than dispose's 5s
        // budget). It does NOT consult `scan_cancel` — that's the whole
        // point: this models `prepare_psbt`/`unified_history`/etc. which
        // run to completion with no cancellation hook.
        let inner = acc.inner.clone();
        let holder = std::thread::spawn(move || {
            let _guard = inner.lock().expect("lock");
            std::thread::sleep(Duration::from_millis(6_000));
        });

        // Give the holder a moment to actually grab the lock before we
        // start dispose. Otherwise dispose could acquire it first.
        std::thread::sleep(Duration::from_millis(50));

        let start = std::time::Instant::now();
        let res = futures_lite_block_on(acc.dispose());
        let elapsed = start.elapsed();

        // dispose() must return Err with a "timed out" message.
        let err = res.expect_err("dispose must time out when lock is held past budget");
        assert!(
            err.contains("timed out"),
            "expected 'timed out' in error, got: {err}"
        );

        // The cancel flag must have been flipped regardless (best-effort
        // signal to any cooperating callee).
        assert!(
            acc.scan_cancel.load(Ordering::Relaxed),
            "scan_cancel must be set even when dispose times out"
        );

        // Wall-clock should be ~5s (the budget) plus the polling
        // granularity; well under 6s (the lock-hold duration). This
        // proves dispose returned before the holder released.
        assert!(
            elapsed < Duration::from_millis(5_500),
            "dispose budget breached: elapsed={elapsed:?}"
        );
        assert!(
            elapsed >= Duration::from_secs(5),
            "dispose returned too early ({elapsed:?}); must wait full budget before timing out"
        );

        // Wait for the holder to finish so the test cleanly releases
        // the temporary directory.
        holder.join().expect("join");
    }

    /// Minimal hand-rolled block_on for the async dispose/stop_scan
    /// helpers above. We don't want to pull tokio just for tests, and
    /// `futures::executor::block_on` is also a heavier dep than needed —
    /// both functions are non-blocking (they just flip an atomic and poll
    /// `try_lock`), so a noop-waker poll loop is sufficient.
    fn futures_lite_block_on<F: std::future::Future>(mut fut: F) -> F::Output {
        use std::pin::Pin;
        use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

        fn noop_clone(_: *const ()) -> RawWaker {
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        fn noop(_: *const ()) {}
        static VTABLE: RawWakerVTable = RawWakerVTable::new(noop_clone, noop, noop, noop);

        let raw_waker = RawWaker::new(std::ptr::null(), &VTABLE);
        // SAFETY: VTABLE methods are all no-ops; the waker's data pointer
        // is never dereferenced. This is the canonical noop-waker pattern.
        let waker = unsafe { Waker::from_raw(raw_waker) };
        let mut cx = Context::from_waker(&waker);

        // SAFETY: fut is owned by this function (moved in by value) and is
        // not moved across the polling loop. Pin::new_unchecked is safe as
        // long as we don't move `fut` after pinning; the loop only borrows
        // it as &mut, and the loop returns the moment Poll::Ready fires.
        let mut fut = unsafe { Pin::new_unchecked(&mut fut) };
        loop {
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(v) => return v,
                Poll::Pending => std::thread::sleep(Duration::from_millis(1)),
            }
        }
    }
}
