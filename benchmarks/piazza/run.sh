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

nuser=5000
reuse="full"
name=$nuser-$reuse
mkdir $1/$name
mkdir $1/$name/info

$cmd -v cargo run --manifest-path benchmarks/Cargo.toml --bin=piazza --release -- \
    -l $nuser -u $nuser -i $1/$name/info/info -p 100000 -c 1000 --populate --reuse $reuse --policies $policies -q $queries > $1/$name/results-$name.out 2> $1/$name/results-$name.log

reuse="noreuse"
name=$nuser-$reuse
mkdir $1/$name
mkdir $1/$name/info

$cmd -v cargo run --manifest-path benchmarks/Cargo.toml --bin=piazza --release -- \
    -l $nuser -u $nuser -i $1/$name/info/info -p 100000 -c 1000 --populate --reuse $reuse --policies $policies -q $queries > $1/$name/results-$name.out 2> $1/$name/results-$name.log

# all posts are private workload
reuse="full"
name=$nuser-$reuse-private
mkdir $1/$name
mkdir $1/$name/info

$cmd -v cargo run --manifest-path benchmarks/Cargo.toml --bin=piazza --release -- \
    -l $nuser -u $nuser -i $1/$name/info/info -p 100000 -c 1000 --populate --private 1.0 --reuse $reuse --policies $policies -q $queries > $1/$name/results-$name.out 2> $1/$name/results-$name.log

reuse="noreuse"
name=$nuser-$reuse-private
mkdir $1/$name
mkdir $1/$name/info

$cmd -v cargo run --manifest-path benchmarks/Cargo.toml --bin=piazza --release -- \
    -l $nuser -u $nuser -i $1/$name/info/info -p 100000 -c 1000 --populate --private 1.0 --reuse $reuse --policies $policies -q $queries > $1/$name/results-$name.out 2> $1/$name/results-$name.log
