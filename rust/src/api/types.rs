use flutter_rust_bridge::frb;

#[frb(unignore)]
#[derive(Debug, Clone)]
pub enum SpNetwork {
    Bitcoin,
    Signet,
    Testnet,
    Regtest,
}

#[frb(unignore)]
#[derive(Debug, Clone)]
pub enum SubAccountKind {
    Segwit,
    Taproot,
}

#[frb(unignore)]
#[derive(Debug, Clone)]
pub enum CoinSource {
    Sp,
    Segwit,
    Taproot,
    Other,
}

#[frb(unignore)]
#[derive(Debug, Clone)]
pub enum SpNotification {
    ScanStarted {
        from: u32,
        to: u32,
    },
    ScanProgress {
        current: u32,
        end: u32,
    },
    ScanCompleted,
    ScanStopped,
    ScanFailed {
        message: String,
    },
    NewOutput {
        outpoint: String,
        amount_sat: u64,
    },
    OutputSpent {
        outpoint: String,
    },
    BackendOffline,
    ElectrumTx {
        kind: CoinSource,
        txid: String,
        amount_sat: u64,
        height: Option<u32>,
    },
}

#[frb(unignore)]
#[derive(Debug, Clone)]
pub struct SpCoinView {
    pub outpoint: String,
    pub amount_sat: u64,
    pub height: u32,
    pub is_spendable: bool,
    pub label: Option<String>,
}

/// Typed payment direction surfaced to Dart so consumers can `match` on
/// it exhaustively instead of string-comparing.
#[frb(unignore)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpPaymentDirection {
    Receive,
    Send,
    SelfSend,
}

#[frb(unignore)]
#[derive(Debug, Clone)]
pub struct SpPaymentView {
    pub txid: String,
    pub direction: SpPaymentDirection,
    pub amount_sat: u64,
    pub fee_sat: Option<u64>,
    pub height: Option<u32>,
    pub timestamp: Option<u64>,
    pub label: Option<String>,
}

#[frb(unignore)]
#[derive(Debug, Clone)]
pub struct UnifiedCoinView {
    pub source: CoinSource,
    pub outpoint: String,
    pub amount_sat: u64,
    pub height: Option<u32>,
}

#[frb(unignore)]
#[derive(Debug, Clone)]
pub enum RecipientView {
    Sp {
        address: String,
        amount_sat: u64,
        label: Option<u32>,
    },
    Standard {
        address: String,
        amount_sat: u64,
    },
}

#[frb(unignore)]
#[derive(Debug, Clone)]
pub struct TxSimulation {
    pub inputs: Vec<UnifiedCoinView>,
    pub outputs: Vec<RecipientView>,
    pub fee_sat: u64,
    pub change_sat: u64,
}

#[frb(unignore)]
#[derive(Debug, Clone)]
pub struct SpBalanceView {
    pub confirmed_sat: u64,
    pub total_unified_sat: u64,
    pub last_scanned_height: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct RegtestDefaults {
    pub is_ok: bool,
    pub error: String,
    pub blindbit_url: String,
    pub p2p_node: String,
    pub electrum_url: String,
}
