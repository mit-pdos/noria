            subprocess.call(cmd, shell=True)
            cmd = "gtime cargo run --release --bin=piazza-write --manifest-path noria-benchmarks/Cargo.toml -- -z post_count --private 0.1 -l {} -p 10000 -u {} -c 100 -m 10 -q noria-benchmarks/piazza/{}-queries.sql > {}_users-{}_policies-{}_queries-full_WRITE.txt".format(users, users, query, users, mode, query)
            subprocess.call(cmd, shell=True)


# EXPERIMENT 4:
# memory overhead as a function of number of users.
for users in nusers:
    for query in ["post"]:
        for mode in policies:
            cmd = "gtime cargo run --release --bin=piazza --manifest-path noria-benchmarks/Cargo.toml  -- -z posts --private 0.1 -l {} -p 1000000 -u {} -c 1000 -m 10 --policies noria-benchmarks/piazza/{}.json -q noria-benchmarks/piazza/{}-queries.sql > {}_users-{}-{}_queries-full_MEMORY.txt".format(users, users, mode, query, users, mode, query)
            subprocess.call(cmd, shell=True)
