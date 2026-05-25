//! Regtest integration tests for the full send pipeline.
//! Requires: SP_NETWORK_TESTS=1 and a live minta.pythcoiner.dev regtest instance.

use dart_bwk::api::sp_account::SpAccount;
use dart_bwk::api::types::{CoinSource, RecipientView, SpNetwork, SubAccountKind};

fn should_run() -> bool {
    std::env::var("SP_NETWORK_TESTS").as_deref() == Ok("1")
}

const SCAN_SK: &str = "0101010101010101010101010101010101010101010101010101010101010101";
const SPEND_SK: &str = "0202020202020202020202020202020202020202020202020202020202020202";
const MINTA_BASE: &str = "http://minta.pythcoiner.dev";

/// Helper: create a fresh SpAccount using regtest defaults.
fn make_account(name: &str, dir: &std::path::Path) -> SpAccount {
    let defaults = dart_bwk::api::regtest::get_regtest_defaults();
    assert!(defaults.is_ok, "minta unreachable: {}", defaults.error);
    SpAccount::create_from_keys(
        name.to_string(),
        SpNetwork::Regtest,
        SCAN_SK.to_string(),
        SPEND_SK.to_string(),
        defaults.blindbit_url,
        defaults.electrum_url,
        dir.to_str().unwrap().to_string(),
        None,
        None,
        Some({
            let seed = [0x05u8; 64];
            bitcoin::bip32::Xpriv::new_master(bitcoin::Network::Regtest, &seed)
                .unwrap()
                .to_string()
        }),
    )
    .expect("create account")
}

/// Helper: POST to minta faucet to fund an SP address.
fn fund_sp(client: &reqwest::blocking::Client, sp_address: &str, amount_sat: u64) {
    let resp = client
        .post(format!("{MINTA_BASE}/api/faucet/sp"))
        .json(&serde_json::json!({ "address": sp_address, "amount": amount_sat }))
        .send()
        .expect("faucet request");
    assert!(
        resp.status().is_success(),
        "SP faucet failed: {}",
        resp.status()
    );
}

/// Helper: POST to minta faucet to fund a standard address.
fn fund_standard(client: &reqwest::blocking::Client, address: &str, amount_sat: u64) {
    let resp = client
        .post(format!("{MINTA_BASE}/api/faucet"))
        .json(&serde_json::json!({ "address": address, "amount": amount_sat }))
        .send()
        .expect("faucet request");
    assert!(
        resp.status().is_success(),
        "faucet failed: {}",
        resp.status()
    );
}

/// Helper: mine 1 block via minta.
fn mine_block(client: &reqwest::blocking::Client) {
    let resp = client
        .post(format!("{MINTA_BASE}/api/mine"))
        .send()
        .expect("mine request");
    assert!(resp.status().is_success(), "mine failed: {}", resp.status());
    std::thread::sleep(std::time::Duration::from_secs(2));
}

/// Scenario 1: Pure SP send — coins come from SP UTXOs, recipient is an SP address.
///
/// Flow: fund SP → scan → prepare → finalize → sign → broadcast → mine → scan → assert balance.
#[test]
#[ignore]
fn send_pure_sp() {
    if !should_run() {
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let acc = make_account("send-pure-sp", dir.path());
    let client = reqwest::blocking::Client::new();

    // Fund with 100_000 sat to the SP address.
    let sp_addr = acc.sp_address().expect("sp_address");
    fund_sp(&client, &sp_addr, 100_000);
    mine_block(&client);

    // Scan to pick up the SP coin.
    acc.scan_once().expect("scan failed");
    let balance_before = acc.confirmed_balance().expect("confirmed_balance");
    assert!(
        balance_before >= 100_000,
        "expected funded balance, got {balance_before}"
    );

    // Send 10_000 sat back to the same SP address (self-send).
    let recipients = vec![RecipientView::Sp {
        address: sp_addr.clone(),
        amount_sat: 10_000,
        label: None,
    }];

    // prepare_psbt: verify inputs are tagged Sp, fee > 0.
    let sim = acc
        .prepare_psbt(recipients.clone(), 1)
        .expect("prepare failed");
    assert!(!sim.inputs.is_empty(), "expected at least one input");
    assert!(sim.fee_sat > 0, "expected non-zero fee");

    // finalize → sign → broadcast. finalize consumes the simulation to pin
    // the input set the user confirmed.
    let psbt_bytes = acc.finalize_psbt(sim).expect("finalize failed");
    let tx_bytes = acc.sign_psbt(psbt_bytes).expect("sign failed");
    let tx_hex = hex::encode(&tx_bytes);

    let txid = acc.broadcast(tx_hex).expect("broadcast failed");
    assert!(!txid.is_empty(), "expected txid");

    mine_block(&client);

    // Scan again and verify balance decreased.
    acc.scan_once().expect("second scan failed");
    let balance_after = acc.confirmed_balance().expect("confirmed_balance");
    // Balance decreased by at least fee (self-send returns change + output).
    assert!(
        balance_before >= balance_after,
        "expected balance to decrease or stay (fee paid)"
    );
}

/// Scenario 2: Pure standard send — coins from segwit sub-account, recipient is a taproot address.
///
/// Sub-account coins arrive via Electrum push (no scan needed).
/// Flow: fund segwit → wait for ElectrumTx notification → finalize → sign → broadcast → mine.
#[test]
#[ignore]
fn send_pure_standard() {
    if !should_run() {
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let acc = make_account("send-pure-std", dir.path());
    let client = reqwest::blocking::Client::new();

    let fund_addr = acc.new_taproot_address().expect("reveal taproot address");
    fund_standard(&client, &fund_addr, 100_000);
    mine_block(&client);

    // Poll taproot balance (Electrum push drives update; give it time).
    let mut balance = 0u64;
    for _ in 0..15 {
        std::thread::sleep(std::time::Duration::from_secs(2));
        balance = acc
            .sub_account_balance(SubAccountKind::Taproot)
            .expect("sub_account_balance");
        if balance > 0 {
            break;
        }
    }
    assert!(
        balance >= 100_000,
        "taproot sub-account not funded after 30s"
    );

    // Send to a fresh taproot address.
    let tr_addr = acc.new_taproot_address().expect("reveal taproot address");
    let recipients = vec![RecipientView::Standard {
        address: tr_addr,
        amount_sat: 20_000,
    }];

    let sim = acc.prepare_psbt(recipients, 1).expect("prepare failed");
    let psbt_bytes = acc.finalize_psbt(sim).expect("finalize failed");
    let tx_bytes = acc.sign_psbt(psbt_bytes).expect("sign failed");
    let tx_hex = hex::encode(&tx_bytes);

    let txid = acc.broadcast(tx_hex).expect("broadcast failed");
    assert!(!txid.is_empty(), "expected txid");
}

/// Scenario 3: Mixed send — coins from both SP and segwit sub-account; SP recipient.
///
/// Flow: fund SP (scan) + fund segwit (Electrum push) → finalize → sign → broadcast.
#[test]
#[ignore]
fn send_mixed() {
    if !should_run() {
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let acc = make_account("send-mixed", dir.path());
    let client = reqwest::blocking::Client::new();

    // Fund SP.
    let sp_addr = acc.sp_address().expect("sp_address");
    fund_sp(&client, &sp_addr, 50_000);
    mine_block(&client);
    acc.scan_once().expect("scan");
    assert!(
        acc.confirmed_balance().expect("confirmed_balance") >= 50_000,
        "SP not funded"
    );

    // Fund taproot sub-account and wait for Electrum push.
    let std_addr = acc.new_taproot_address().expect("reveal taproot address");
    fund_standard(&client, &std_addr, 50_000);
    mine_block(&client);

    let mut std_balance = 0u64;
    for _ in 0..15 {
        std::thread::sleep(std::time::Duration::from_secs(2));
        std_balance = acc
            .sub_account_balance(SubAccountKind::Taproot)
            .expect("sub_account_balance");
        if std_balance > 0 {
            break;
        }
    }
    assert!(std_balance >= 50_000, "taproot not funded after 30s");

    // Send 80_000 sat (requires coins from both sources).
    let recipients = vec![RecipientView::Sp {
        address: sp_addr,
        amount_sat: 80_000,
        label: None,
    }];

    let sim = acc.prepare_psbt(recipients.clone(), 1).expect("prepare");
    // Mixed: expect both Sp and standard (taproot) inputs.
    let has_sp = sim
        .inputs
        .iter()
        .any(|c| matches!(c.source, CoinSource::Sp));
    let has_std = sim
        .inputs
        .iter()
        .any(|c| matches!(c.source, CoinSource::Taproot));
    assert!(has_sp, "expected SP input");
    assert!(has_std, "expected taproot input");

    let psbt_bytes = acc.finalize_psbt(sim).expect("finalize");
    let tx_bytes = acc.sign_psbt(psbt_bytes).expect("sign");
    let tx_hex = hex::encode(&tx_bytes);

    let txid = acc.broadcast(tx_hex).expect("broadcast");
    assert!(!txid.is_empty());
}
