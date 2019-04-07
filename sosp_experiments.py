import subprocess
import argparse
import matplotlib.pyplot as plt
import numpy as np



# Read txput experiments:
# partial basic
cmd = "/usr/bin/time perflock cargo run --release --bin=piazza --manifest-path noria-benchmarks/Cargo.toml -- -l 5000 -p 1000000 -u 5000 -c 1000 -m 10 --partial --policies noria-benchmarks/piazza/basic-policies.json > 5kpartialbasic.txt"
subprocess.call(cmd, shell=True)

cmd = "/usr/bin/time perflock cargo run --release --bin=piazza --manifest-path noria-benchmarks/Cargo.toml -- -l 1000 -p 1000000 -u 1000 -c 1000 -m 10 --partial --policies noria-benchmarks/piazza/basic-policies.json > 1kpartialbasic.txt"
subprocess.call(cmd, shell=True)

cmd = "/usr/bin/time perflock cargo run --release --bin=piazza --manifest-path noria-benchmarks/Cargo.toml -- -l 3000 -p 1000000 -u 3000 -c 1000 -m 10 --partial --policies noria-benchmarks/piazza/basic-policies.json > 3kpartialbasic.txt"
subprocess.call(cmd, shell=True)

# full basic
cmd = "/usr/bin/time perflock cargo run --release --bin=piazza --manifest-path noria-benchmarks/Cargo.toml -- -l 5000 -p 1000000 -u 5000 -c 1000 -m 10 --policies noria-benchmarks/piazza/basic-policies.json > 5kfullbasic.txt"
subprocess.call(cmd, shell=True)

cmd = "/usr/bin/time perflock cargo run --release --bin=piazza --manifest-path noria-benchmarks/Cargo.toml -- -l 1000 -p 1000000 -u 1000 -c 1000 -m 10 --policies noria-benchmarks/piazza/basic-policies.json > 1kfullbasic.txt"
subprocess.call(cmd, shell=True)

cmd = "/usr/bin/time perflock cargo run --release --bin=piazza --manifest-path noria-benchmarks/Cargo.toml -- -l 3000 -p 1000000 -u 3000 -c 1000 -m 10 --policies noria-benchmarks/piazza/basic-policies.json > 3kfullbasic.txt"
subprocess.call(cmd, shell=True)

# full complex
cmd = "/usr/bin/time perflock cargo run --release --bin=piazza --manifest-path noria-benchmarks/Cargo.toml -- -l 5000 -p 1000000 -u 5000 -c 1000 -m 10 --policies noria-benchmarks/piazza/complex-policies.json > 5kfullcomplex.txt"
subprocess.call(cmd, shell=True)

cmd = "/usr/bin/time perflock cargo run --release --bin=piazza --manifest-path noria-benchmarks/Cargo.toml -- -l 1000 -p 1000000 -u 1000 -c 1000 -m 10 --policies noria-benchmarks/piazza/complex-policies.json > 1kfullcomplex.txt"
subprocess.call(cmd, shell=True)

cmd = "/usr/bin/time perflock cargo run --release --bin=piazza --manifest-path noria-benchmarks/Cargo.toml -- -l 3000 -p 1000000 -u 3000 -c 1000 -m 10 --policies noria-benchmarks/piazza/complex-policies.json > 3kfullcomplex.txt"
subprocess.call(cmd, shell=True)

# partial complex
cmd = "/usr/bin/time perflock cargo run --release --bin=piazza --manifest-path noria-benchmarks/Cargo.toml -- -l 5000 -p 1000000 -u 5000 -c 1000 -m 10 --partial --policies noria-benchmarks/piazza/complex-policies.json > 5kpartialcomplex.txt"
subprocess.call(cmd, shell=True)

cmd = "/usr/bin/time perflock cargo run --release --bin=piazza --manifest-path noria-benchmarks/Cargo.toml -- -l 1000 -p 1000000 -u 1000 -c 1000 -m 10 --partial --policies noria-benchmarks/piazza/complex-policies.json > 1kpartialcomplex.txt"
subprocess.call(cmd, shell=True)

cmd = "/usr/bin/time perflock cargo run --release --bin=piazza --manifest-path noria-benchmarks/Cargo.toml -- -l 3000 -p 1000000 -u 3000 -c 1000 -m 10 --partial --policies noria-benchmarks/piazza/complex-policies.json > 3kpartialcomplex.txt"
subprocess.call(cmd, shell=True)
