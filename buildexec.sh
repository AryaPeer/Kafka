#!/bin/sh

set -e 

(
  cd "$(dirname "$0")" 
  cargo build --release --target-dir=/tmp/kafka-build --manifest-path ./Cargo.toml
)

exec /tmp/kafka-build/release/kafka-rust "$@"
