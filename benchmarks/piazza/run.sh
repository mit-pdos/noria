#!/bin/bash

if [ $# -lt 3 ]
  then
    echo "usage: ./run.sh [dir] [policies]"
    exit
fi

mkdir $1
policies=$2
queries=$3

case "$OSTYPE" in
  darwin*)  cmd="gtime" ;;
  linux*)   cmd="/usr/bin/time" ;;
esac

for i in {0..10}
do
    nuser=$(( $i * 50 ))

    reuse="full"
    $cmd -v cargo run --manifest-path benchmarks/Cargo.toml --bin=piazza --release -- \
        -l $nuser -p 10000 --populate --reuse $reuse --policies $policies -q $queries > $1/results-$nuser-$reuse.out 2> $1/results-$nuser-$reuse.log

    reuse="noreuse"
    $cmd -v cargo run --manifest-path benchmarks/Cargo.toml --bin=piazza --release -- \
        -l $nuser -p 10000 --populate --reuse $reuse --policies $policies -q $queries > $1/results-$nuser-$reuse.out 2> $1/results-$nuser-$reuse.log

    # all posts are private workload
    reuse="full"
    $cmd -v cargo run --manifest-path benchmarks/Cargo.toml --bin=piazza --release -- \
        -l $nuser -p 10000 --populate --private 1.0 --reuse $reuse --policies $policies -q $queries > $1/results-$nuser-$reuse-private.out 2> $1/results-$nuser-$reuse-private.log

    reuse="noreuse"
    $cmd -v cargo run --manifest-path benchmarks/Cargo.toml --bin=piazza --release -- \
        -l $nuser -p 10000 --populate --private 1.0 --reuse $reuse --policies $policies -q $queries > $1/results-$nuser-$reuse-private.out 2> $1/results-$nuser-$reuse-private.log

done