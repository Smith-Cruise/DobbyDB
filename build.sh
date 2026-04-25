#!/usr/bin/env bash
set -euo pipefail

arch="$(uname -m)"

if [[ "${arch}" == "x86_64" || "${arch}" == "amd64" ]]; then
    echo "Detected x86 architecture (${arch}), building release with target-cpu=native"
    RUSTFLAGS="-C target-cpu=native" cargo build --release
else
    echo "Detected non-x86 architecture (${arch}), building normal release"
    cargo build --release
fi
