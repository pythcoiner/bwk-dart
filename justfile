gen:
    flutter_rust_bridge_codegen generate

lint:
    cd rust && cargo clippy --features bull_sdk --all-targets -- -D warnings

clean:
    cd rust && cargo clean

# vim:expandtab:sw=4:ts=4
