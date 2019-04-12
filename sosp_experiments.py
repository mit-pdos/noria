import subprocess
import argparse
import matplotlib.pyplot as plt
import numpy as np


# EXPERIMENT 1:
# read throughput as a function of the number of users. 1k to 5k users. 1 million posts, 1000 classes. 90% public posts. users enrolled in 10 classes each.
nusers = [1000, 2000, 3000, 4000, 5000]
policies = ["basic-policies", "complex-policies"]
queries = ["post"]

for users in nusers:
    for mode in policies:
        for query in queries:
            cmd = "gtime cargo run --release --bin=piazza --manifest-path noria-benchmarks/Cargo.toml -- -l {} -p 1000000 -u {} -c 1000 -m 10 --partial --policies noria-benchmarks/piazza/{}.json -q noria-benchmarks/piazza/{}-queries.sql > {}_users-{}-{}_queries-partial_READ.txt".format(users, users, mode, query, users, mode, query)
            subprocess.call(cmd, shell=True)
            cmd = "gtime cargo run --release --bin=piazza --manifest-path noria-benchmarks/Cargo.toml -- -l {} -p 1000000 -u {} -c 1000 -m 10 --policies noria-benchmarks/piazza/{}.json -q noria-benchmarks/piazza/{}-queries.sql > {}_users-{}-{}_queries-full_READ.txt".format(users, users, mode, query, users, mode, query)
            subprocess.call(cmd, shell=True)


# EXPERIMENT 2 (and 3):
# 2: write throughput as a function of the number of users.
# 3: write throughput as a function of query complexity. (no policies, basic policies, complex policies)
all_policies = ["basic-policies", "no"]
for users in nusers:
    for mode in all_policies:
        for query in queries:
            cmd = "gtime perflock cargo run --release --bin=piazza-write --manifest-path noria-benchmarks/Cargo.toml -- -l {} -p 1000000 -u {} -c 1000 -m 10 --partial --policies noria-benchmarks/piazza/{}.json -q noria-benchmarks/piazza/{}-queries.sql > {}_users-{}-{}_queries-partial_WRITE.txt".format(users, users, mode, query, users, mode, query)
            subprocess.call(cmd, shell=True)
            cmd = "gtime perflock cargo run --release --bin=piazza-write --manifest-path noria-benchmarks/Cargo.toml -- -l {} -p 1000000 -u {} -c 1000 -m 10 --policies noria-benchmarks/piazza/{}.json -q noria-benchmarks/piazza/{}-queries.sql > {}_users-{}-{}_queries-full_WRITE.txt".format(users, users, mode, query, users, mode, query)
            subprocess.call(cmd, shell=True)
            cmd = "gtime perflock cargo run --release --bin=piazza-write --manifest-path noria-benchmarks/Cargo.toml -- -l {} -p 1000000 -u {} -c 1000 -m 10 -q noria-benchmarks/piazza/{}-queries.sql > {}_users-{}_policies-{}_queries-full_WRITE.txt".format(users, users, query, users, mode, query)
            subprocess.call(cmd, shell=True)


# EXPERIMENT 4:
# memory overhead as a function of number of users. (with groups + without)
# for users in nusers:
#     for query in ["post"]:
#         for mode in policies:
#             cmd = "/usr/bin/time perflock cargo run --release --bin=piazza --manifest-path noria-benchmarks/Cargo.toml -- -l {} -p 1000000 -u {} -c 1000 -m 10 --policies noria-benchmarks/piazza/{}.json -q noria-benchmarks/piazza/{}-queries.sql > {}_users-{}-{}_queries-full_MEMORY.txt".format(users, users, mode, query, users, mode, query)
#             subprocess.call(cmd, shell=True)
