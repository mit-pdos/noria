import subprocess
import argparse
import matplotlib.pyplot as plt
import numpy as np



# Read txput experiments:
# 5k users, 1mill posts, 1k classes. partially materialized. read txput & mem OH. 10 classes per user. basic policies.
cmd = "/usr/bin/time perflock cargo run --release --bin=piazza --manifest-path noria-benchmarks/Cargo.toml -- -l 5000 -p 1000000 -u 5000 -c 1000 -m 10 --policies noria-benchmarks/piazza/basic-policies.json --partial > exp1.txt"
subprocess.call(cmd, shell=True)

# 5k users, 1mill posts, 1k classes. partially materialized. read txput & mem OH. 10 classes per user. basic policies.
cmd = "/usr/bin/time perflock cargo run --release --bin=piazza --manifest-path noria-benchmarks/Cargo.toml -- -l 5000 -p 1000000 -u 5000 -c 1000 -m 10 --policies noria-benchmarks/piazza/basic-policies.json --partial > exp2.txt"
subprocess.call(cmd, shell=True)

# 5k users, 1mill posts, 1k classes. fully materialized. read txput & mem OH. 1000 classes per user. complex policies.
cmd = "/usr/bin/time perflock cargo run --release --bin=piazza --manifest-path noria-benchmarks/Cargo.toml -- -l 5000 -p 1000000 -u 5000 -c 1000 -m 1000 --policies noria-benchmarks/piazza/complex-policies.json > exp3.txt"
subprocess.call(cmd, shell=True)

# 5k users, 1mill posts, 1k classes. fully materialized. read txput & mem OH. 1000 classes per user. complex policies.
cmd = "/usr/bin/time perflock cargo run --release --bin=piazza --manifest-path noria-benchmarks/Cargo.toml -- -l 5000 -p 1000000 -u 5000 -c 1000 -m 1000 --policies noria-benchmarks/piazza/complex-policies.json > exp4.txt"
subprocess.call(cmd, shell=True)


# Write txput experiments:
# 5k users, 1mill posts, 1k classes. fully materialized. write txput & mem OH. 10 classes per user.
# 5k users, 1mill posts, 1k classes. fully materialized. write txput & mem OH. 10 classes per user.
# 5k users, 1mill posts, 1k classes. fully materialized. write txput & mem OH. 1000 classes per user.
# 5k users, 1mill posts, 1k classes. fully materialized. write txput & mem OH. 1000 classes per user.
