HOME="/home/gina"
RESULTS_DIR="$HOME/noria/fault_tolerance_bm/results_gina"

BINARY=$1          # "ft" or "ft_no_clone" or "master" in bin/$BINARY
TARGET=$2          # target ops/s
SHARDING=$3        # sharding
TRUNCATE_EVERY=2  # log truncation interval

# fixed parameters
ARTICLES=1000000        # 1 mil articles
WRITE_EVERY=2           # write-read ratio
MAX_BATCH_TIME_US=1000  # send a batch every 1000 microseconds
WARMUP=$4               # warmup time, in seconds
RUNTIME=30              # runtime, in seconds
RESULTS_FILE="$RESULTS_DIR/$BINARY-$TARGET-$SHARDING-$WARMUP.txt"

# if test -f "$RESULTS_FILE"; then
#     echo "skipping $RESULTS_FILE: already exists"
# else
    # cmd="perflock timeout $(( $WARMUP + $RUNTIME + 20 )) /home/gina/noria/bin/$BINARY "
    cmd="perflock /home/gina/noria/bin/$BINARY "
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
    echo $RESULTS_FILE
    echo $cmd
    # echo $cmd > $RESULTS_FILE
    # echo $RESULTS_FILE >> $RESULTS_FILE
    $cmd
    # $cmd >> $RESULTS_FILE
# fi

