#!/usr/bin/env bash
echo "hi"
cargo r --bin manual-twitter --release -- --materialization client -n 1000 -u 100
cargo r --bin manual-twitter --release -- --materialization client -n 10000 -u 1000
cargo r --bin manual-twitter --release -- --materialization client -n 100000 -u 10000
cargo r --bin manual-twitter --release -- --materialization client -n 1000 -u 1000
cargo r --bin manual-twitter --release -- --materialization client -n 10000 -u 100
