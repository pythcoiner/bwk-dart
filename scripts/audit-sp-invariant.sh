#!/usr/bin/env bash
# audit-sp-invariant.sh: Static analysis enforcing SP no-autoscan invariants
# on the Rust side of dart_bwk (the SP scan code now lives in this crate).
#
# Checks (rust-side only; the dart-side checks remain in bb-mobile):
#   * EXACTLY 1 start_scan( call site in rust/src/, inside `pub fn scan_once`
#     in rust/src/api/sp_account.rs;
#   * ZERO scan_blocks( call sites anywhere in rust/src/;
#   * no "Continuous" string in the standalone-generated Dart output
#     (lib/src/generated/), if that dir exists.
set -euo pipefail

# Run from the repo root regardless of caller's cwd.
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

fail=0

echo '=== Rust start_scan( call sites in rust/src/ (must be EXACTLY 1, inside pub fn scan_once in rust/src/api/sp_account.rs) ==='
start_scan_hits=$(grep -rnF 'start_scan(' rust/src/ || true)
printf '%s\n' "$start_scan_hits"
start_scan_count=$(printf '%s' "$start_scan_hits" | grep -c . || true)
if [ "$start_scan_count" -ne 1 ]; then
  echo "FAIL: expected exactly 1 start_scan( call site in rust/src/, found ${start_scan_count}."
  echo "Unexpected call sites listed above. SP rescan must be user-triggered only;"
  echo "every additional start_scan( risks bypassing the user-driven scan contract."
  fail=1
else
  expected_file='rust/src/api/sp_account.rs'
  if ! printf '%s' "$start_scan_hits" | grep -qF "$expected_file"; then
    echo "FAIL: the single start_scan( call site is NOT in ${expected_file}."
    fail=1
  else
    # Ensure the single call site is inside `pub fn scan_once` (not some
    # other FRB-exported method that would silently auto-start scans).
    enclosing=$(awk '
      /^[[:space:]]*pub fn [a-zA-Z0-9_]+/ { last_fn = $0 }
      /start_scan\(/ { print last_fn; exit }
    ' "$expected_file")
    if ! printf '%s' "$enclosing" | grep -q 'pub fn scan_once'; then
      echo "FAIL: start_scan( in ${expected_file} is not inside 'pub fn scan_once'."
      echo "Enclosing pub fn: ${enclosing}"
      fail=1
    else
      echo "PASS: single start_scan( call site is inside pub fn scan_once."
    fi
  fi
fi

echo ''
echo '=== Rust scan_blocks( call sites in rust/src/ (must be ZERO) ==='
scan_blocks_hits=$(grep -rnF 'scan_blocks(' rust/src/ || true)
if [ -n "$scan_blocks_hits" ]; then
  printf '%s\n' "$scan_blocks_hits"
  echo 'FAIL: scan_blocks( must not be called from rust/src/.'
  fail=1
else
  echo 'PASS: no scan_blocks( call sites in rust/src/.'
fi

echo ''
echo '=== ScanMode::Continuous in standalone-generated Dart (lib/src/generated/, must be empty) ==='
if [ -d lib/src/generated ]; then
  if grep -rnF 'Continuous' lib/src/generated/ 2>/dev/null; then
    echo "FAIL: 'Continuous' string present in FRB-generated Dart"
    fail=1
  else
    echo "PASS: 'Continuous' not found in generated Dart"
  fi
else
  echo 'SKIP: lib/src/generated/ absent (standalone codegen not run); nothing to check.'
fi

echo ''
if [ "$fail" -ne 0 ]; then
  echo "RESULT: SP invariant audit FAILED ($fail violation(s) found)."
  exit 1
fi
echo 'OK: SP invariant audit passed.'
