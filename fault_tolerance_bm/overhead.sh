HOME="/home/gina"
RESULTS_DIR="$HOME/noria/fault_tolerance_bm/results"

build() {
    BRANCH=$1
    BINARY=$2

    git checkout $BRANCH
    pls cargo b --release --bin vote && pls cp $HOME/noria/target/release/vote $HOME/noria/bin/$BINARY
    echo "built branch $BRANCH as binary $BINARY"
}

build_all()
{
    # need to create ft_no_clone by cherry-picking 3425981f onto wpbm_ft_sharded
    build wpbm_ft_sharded ft
    build wpbm_master_sharded master
}

run_localsoup()
{
    BINARY=$1          # "ft" or "ft_no_clone" or "master" in bin/$BINARY
    TARGET=$2          # target ops/s
    SHARDING=$3        # sharding
    TRUNCATE_EVERY=$4  # log truncation interval
    RESULTS_FILE="$RESULTS_DIR/$BINARY-$TARGET-$SHARDING-$TRUNCATE_EVERY.txt"

    # fixed parameters
    ARTICLES=1000000        # 1 mil articles
    WRITE_EVERY=2           # write-read ratio
    MAX_BATCH_TIME_US=1000  # send a batch every 1000 microseconds
    WARMUP=10                # warmup time, in seconds
    RUNTIME=30              # runtime, in seconds

    if test -f "$RESULTS_FILE"; then
        echo "skipping $RESULTS_FILE: already exists"
    else
        cmd="pls timeout $(( $WARMUP + $RUNTIME + 10 )) /home/gina/noria/bin/$BINARY "
        cmd+=" --no-early-exit"
        cmd+=" --max-batch-time-us $MAX_BATCH_TIME_US"
        cmd+=" --articles $ARTICLES"
        cmd+=" --runtime $RUNTIME"
        cmd+=" --warmup $WARMUP"
        cmd+=" --write-every $WRITE_EVERY"
        cmd+=" --target $TARGET "
        cmd+=" localsoup"
        cmd+=" --shards $SHARDING"
        cmd+=" --truncate-every $TRUNCATE_EVERY"
        echo "$cmd > $RESULTS_FILE"
        # $cmd
        $cmd > $RESULTS_FILE
    fi
}

# build wpbm_ft_sharded ft
# build wpbm_master_sharded master
for sharding in 20
do
    for target in $(seq 100 100 3000)
    do
        truncate_every=2
        run_localsoup ft $(($target * 1000)) $sharding $truncate_every
        run_localsoup master $(($target * 1000)) $sharding 1
        # run_localsoup ft_no_truncate $(($target * 1000)) $sharding 1
        run_localsoup ft_no_clone $(($target * 1000)) $sharding 1
    done
done

# for target in $(seq 20000 20000 400000)
# do
#     for sharding in 1 4 10
#     do
#         run_localsoup master $target $sharding 1
#         for truncate_every in 2
#         do
#             run_localsoup ft $target $sharding $truncate_every
#         done
#     done
# done

# for target in 1000000
# do
#     for sharding in 10 20
#     do
#         for truncate_every in 1 2 4 8
#         do
#             run_localsoup ft $target $sharding $truncate_every
#         done
#     done
# done

# for target in 100000
# do
#     for sharding in 1 2 4 8 10
#     do
#         for truncate_every in 2
#         do
#             run_localsoup ft $target $sharding $truncate_every
#         done
#     done
# done
