//! Regtest integration tests for a watch-only account built from keys, which
//! spends only with the keys `finalize_and_sign` lends it for one call.
//! Requires: SP_NETWORK_TESTS=1 and a live minta.pythcoiner.dev regtest instance.

use std::str::FromStr;

use bitcoin::bip32::{DerivationPath, Xpriv, Xpub};
use bitcoin::secp256k1::Secp256k1;
use bwk_sp::receiver::bip39::Mnemonic;
use dart_bwk::api::sp_account::SpAccount;
use dart_bwk::api::types::{CoinSource, RecipientView, SpNetwork, SubAccountKind};

fn should_run() -> bool {
    std::env::var("SP_NETWORK_TESTS").as_deref() == Ok("1")
}

const TEST_MNEMONIC: &str =
    "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
const OTHER_MNEMONIC: &str =
    "legal winner thank year wave sausage worth useful legal winner thank yellow";
const MINTA_BASE: &str = "http://minta.pythcoiner.dev";

/// What the custody side derives from a mnemonic, the BIP352 and BIP86 way,
/// on a test network (coin type 1).
struct Derived {
    scan_sk_hex: String,
    spend_pk_hex: String,
    b_spend_hex: String,
    taproot_descriptor: String,
    taproot_account_xprv: String,
}

fn derive(mnemonic: &str) -> Derived {
    let secp = Secp256k1::new();
    let seed = Mnemonic::from_str(mnemonic).unwrap().to_seed("");
    let master = Xpriv::new_master(bitcoin::Network::Regtest, &seed).unwrap();
    let at = |path: &str| {
        master
            .derive_priv(&secp, &DerivationPath::from_str(path).unwrap())
            .unwrap()
    };
    let scan = at("m/352'/1'/0'/1'/0").private_key;
    let spend = at("m/352'/1'/0'/0'/0").private_key;
    let taproot_account = at("m/86'/1'/0'");
    Derived {
        scan_sk_hex: hex::encode(scan.secret_bytes()),
        spend_pk_hex: spend.public_key(&secp).to_string(),
        b_spend_hex: hex::encode(spend.secret_bytes()),
        taproot_descriptor: format!(
            "tr([{}/86'/1'/0']{}/<0;1>/*)",
            master.fingerprint(&secp),
            Xpub::from_priv(&secp, &taproot_account)
        ),
        taproot_account_xprv: taproot_account.to_string(),
    }
}

fn make_watch_only(name: &str, dir: &std::path::Path, keys: &Derived) -> SpAccount {
    let defaults = dart_bwk::api::regtest::get_regtest_defaults();
    assert!(defaults.is_ok, "minta unreachable: {}", defaults.error);
    SpAccount::create_from_keys(
        name.to_string(),
        SpNetwork::Regtest,
        keys.scan_sk_hex.clone(),
        keys.spend_pk_hex.clone(),
        keys.taproot_descriptor.clone(),
        defaults.blindbit_url,
        defaults.electrum_url,
        dir.to_str().unwrap().to_string(),
        None,
        None,
        None,
        None,
    )
    .expect("create watch-only account")
}

fn make_hot(name: &str, dir: &std::path::Path) -> SpAccount {
    let defaults = dart_bwk::api::regtest::get_regtest_defaults();
    assert!(defaults.is_ok, "minta unreachable: {}", defaults.error);
    SpAccount::create_from_mnemonic(
        name.to_string(),
        SpNetwork::Regtest,
        TEST_MNEMONIC.to_string(),
        defaults.blindbit_url,
        defaults.electrum_url,
        dir.to_str().unwrap().to_string(),
        None,
        None,
    )
    .expect("create account")
}

fn fund(client: &reqwest::blocking::Client, path: &str, address: &str, amount_sat: u64) {
    let resp = client
        .post(format!("{MINTA_BASE}{path}"))
        .json(&serde_json::json!({ "address": address, "amount": amount_sat }))
        .send()
        .expect("faucet request");
    assert!(
        resp.status().is_success(),
        "faucet failed: {}",
        resp.status()
    );
}

fn mine_block(client: &reqwest::blocking::Client) {
    let resp = client
        .post(format!("{MINTA_BASE}/api/mine"))
        .send()
        .expect("mine request");
    assert!(resp.status().is_success(), "mine failed: {}", resp.status());
    std::thread::sleep(std::time::Duration::from_secs(2));
}

fn wait_for_taproot_balance(acc: &SpAccount, done: impl Fn(u64) -> bool) -> u64 {
    let mut balance = 0;
    for _ in 0..15 {
        std::thread::sleep(std::time::Duration::from_secs(2));
        balance = acc
            .sub_account_balance(SubAccountKind::Taproot)
            .expect("sub_account_balance");
        if done(balance) {
            break;
        }
    }
    balance
}

/// The same mnemonic gives the same SP address and taproot addresses whether
/// the account is built from it or from the keys derived outside bwk.
#[test]
#[ignore]
fn keys_give_the_mnemonic_accounts_addresses() {
    if !should_run() {
        return;
    }
    let hot_dir = tempfile::tempdir().unwrap();
    let watch_dir = tempfile::tempdir().unwrap();
    let hot = make_hot("from-mnemonic", hot_dir.path());
    let watch = make_watch_only("from-keys", watch_dir.path(), &derive(TEST_MNEMONIC));

    assert_eq!(watch.sp_address().unwrap(), hot.sp_address().unwrap());
    assert_eq!(
        watch.new_taproot_address().unwrap(),
        hot.new_taproot_address().unwrap()
    );
}

/// Mixed send — an SP coin and a taproot sub-account coin, to the account's
/// own SP address — signed with lent keys only.
#[test]
#[ignore]
fn a_watch_only_account_spends_with_lent_keys() {
    if !should_run() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let keys = derive(TEST_MNEMONIC);
    let acc = make_watch_only("send-from-keys", dir.path(), &keys);
    let client = reqwest::blocking::Client::new();

    let sp_addr = acc.sp_address().expect("sp_address");
    fund(&client, "/api/faucet/sp", &sp_addr, 50_000);
    mine_block(&client);
    acc.scan_once(None).expect("scan");
    assert!(
        acc.confirmed_balance().expect("confirmed_balance") >= 50_000,
        "the scan key alone finds the SP payment"
    );

    let std_addr = acc.new_taproot_address().expect("reveal taproot address");
    fund(&client, "/api/faucet", &std_addr, 50_000);
    mine_block(&client);
    let funded = wait_for_taproot_balance(&acc, |b| b > 0);
    assert!(funded >= 50_000, "taproot not funded after 30s");

    let recipients = vec![RecipientView::Sp {
        address: sp_addr,
        amount_sat: 80_000,
        label: None,
        is_max: false,
    }];
    let sim = acc.prepare_psbt(recipients, 1).expect("prepare");
    assert!(sim
        .inputs
        .iter()
        .any(|c| matches!(c.source, CoinSource::Sp)));
    assert!(sim
        .inputs
        .iter()
        .any(|c| matches!(c.source, CoinSource::Taproot)));

    // Without keys the account cannot build the spend at all.
    assert!(acc.finalize_psbt(sim.clone()).is_err());

    // Keys of another wallet are refused.
    let foreign = derive(OTHER_MNEMONIC);
    assert!(acc
        .finalize_and_sign(
            sim.clone(),
            foreign.b_spend_hex.clone(),
            keys.taproot_account_xprv.clone()
        )
        .is_err());
    assert!(acc
        .finalize_and_sign(
            sim.clone(),
            keys.b_spend_hex.clone(),
            foreign.taproot_account_xprv.clone()
        )
        .is_err());

    let tx_bytes = acc
        .finalize_and_sign(
            sim,
            keys.b_spend_hex.clone(),
            keys.taproot_account_xprv.clone(),
        )
        .expect("finalize and sign");

    // Before broadcast, the receiving side recognises the payment to the
    // account's own address and the SP change.
    let owned = acc.owned_outputs(tx_bytes.clone()).expect("owned outputs");
    assert_eq!(owned.len(), 2, "payment and change: {owned:?}");
    assert!(owned.iter().any(|o| !o.is_change && o.amount_sat == 80_000));
    assert!(owned.iter().any(|o| o.is_change && o.amount_sat > 0));

    acc.broadcast(hex::encode(&tx_bytes)).expect("broadcast");
    mine_block(&client);

    // The node accepted it: the taproot coin is spent and the self-sent SP
    // output is found by the scan key.
    let remaining = wait_for_taproot_balance(&acc, |b| b < funded);
    assert!(remaining < funded, "taproot coin still unspent after 30s");
    acc.scan_once(None).expect("second scan");
    assert!(acc.confirmed_balance().expect("confirmed_balance") >= 80_000);
}
