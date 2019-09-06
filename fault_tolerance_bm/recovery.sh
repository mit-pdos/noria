HOME="/home/gina"
BM_DIR="$HOME/noria/fault_tolerance_bm"
LOG_DIR="$HOME/noria/fault_tolerance_bm/logs"

build()
{
	pls cargo b --release --bin noria-zk
	pls cargo b --release --bin noria-server
	pls cargo b --release --bin vote
	# cp target/release/noria-zk bin/noria-zk
	# cp target/release/noria-server bin/noria-server
	# cp target/release/vote bin/vote
}

clean()
{
	pls cargo r --release --bin noria-zk -- --clean --deployment gina
}

kill_deployment()
{
    kill $(ps aux | grep "\-\-deployment gina" | awk '{print $2}')
}

run_experiment()
{
	binary=$1
	shards=$2
	articles=$3
	num_trials=$4
	warmup=$5
	runtime=$6

	target=100000
	truncate_every=2
	max_batch_time_us=1000

	# starts at 0 (migration completed)
	# kill worker 0 at 15 (migration completed + 15)
	# failure detected (handled failure of...)

	# server_cmd="$HOME/noria/bin/noria-server -v"
	server_cmd="cargo r --release --bin noria-server -- -v"
	server_cmd+=" --deployment gina"
	server_cmd+=" --durability memory"
	server_cmd+=" --shards $shards"
	server_cmd+=" --no-partial"

	# client_cmd="perflock $HOME/noria/bin/vote"
	client_cmd="cargo r --release --bin vote --"
	client_cmd+=" --write-every 2"
	client_cmd+=" --target $target"
	client_cmd+=" --warmup $warmup"
	client_cmd+=" -r $runtime"
	client_cmd+=" --no-early-exit"
	client_cmd+=" --articles $articles"
	client_cmd+=" --max-batch-time-us $max_batch_time_us"
	client_cmd+=" netsoup"
	client_cmd+=" --deployment gina"

	kill_deployment
	build
	clean
	sleep 5s

	for trial in $(seq 1 $num_trials)
	do
		# Do not repeat the trial if it already exists
		CLIENT_FILENAME="$BM_DIR/$1/$2_$3_client_$trial"
		CONTROLLER_FILENAME="$BM_DIR/$1/$2_$3_controller_$trial"
		echo $CLIENT_FILENAME
		if test -f "$CLIENT_FILENAME"; then
	        echo "skipping $CLIENT_FILENAME: already exists"
	        continue
	    fi

		# Start a worker for each shard
		echo "running $binary $shards $articles trial $trial / $num_trials"
		echo "$server_cmd x$shards"
		for shard_i in $(seq 1 $(($shards)))
		do
			if [[ $shard_i -eq 1 ]]
			then
				perflock $server_cmd > "$LOG_DIR/worker$shard_i" 2>&1 &
				# perflock $server_cmd 2>&1 &
				sleep 5s  # sleep to make sure this server is elected controller
			else
				$server_cmd > "$LOG_DIR/worker$shard_i" 2>&1 &
			fi
		done

		# Start a worker for the shard merger and sleep to make sure it's the last one connected
		sleep 5s
		cmd="timeout $((warmup + 15 + 1))s $server_cmd"
		echo $cmd
		$cmd > "$LOG_DIR/worker$(($shards + 1))" 2>&1 &
		# $cmd 2>&1 &
		sleep 1s

		# Start the client
		cmd="$client_cmd"
		echo $cmd
		$cmd > "$LOG_DIR/client" 2>&1
		# $cmd 2>&1

		# Copy the logs of the controller and the vote client for parsing
		cp $LOG_DIR/client $CLIENT_FILENAME
		cp $LOG_DIR/worker1 $CONTROLLER_FILENAME

		kill_deployment
		sleep 5s
		clean
		sleep 5s
	done
}

# binary=master
shards=20
# articles=$1
num_trials=1
# warmup=$2
# runtime=$3

# run_experiment $binary $shards $articles $num_trials $warmup $runtime
git checkout wpbm_master_sharded
run_experiment master $shards 1000000 $num_trials 15 30
# run_experiment master $shards 10000000 $num_trials 30 30
# run_experiment master $shards 50000000 $num_trials 90 90

# git checkout wpbm_ft_sharded
# run_experiment ft $shards 1000000 $num_trials 15 30
# run_experiment ft $shards 10000000 $num_trials 30 30
# run_experiment ft $shards 50000000 $num_trials 90 90
