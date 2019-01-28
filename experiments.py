import subprocess


nusers = [100, 1000, 10000]
nclasses = [100, 500, 1000]
nposts = [1, 10, 100]


for users in nusers:
    for classes in nclasses:
        for posts in nposts:
            postcount = posts * classes
            subprocess.call("/usr/bin/time cargo run --release --bin=piazza --manifest-path noria-benchmarks/Cargo.toml -- -l {} -p {} -u {} -c {} --policies noria-benchmarks/piazza/basic-policies.json > exp-{}-{}-{}.txt".format(users, postcount, users, classes, users, postcount, classes), shell=True)
