use flutter_rust_bridge::frb;

#[frb(sync)]
pub fn ping() -> u32 {
    42
}
