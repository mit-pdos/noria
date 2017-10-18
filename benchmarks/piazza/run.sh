#!/bin/bash

mkdir $1
case "$OSTYPE" in
  darwin*)  cmd="gtime" ;;
  linux*)   cmd="/usr/bin/time" ;;
esac

for i in {0..10}
do
    nuser=$(( $i * 100 ))
    $cmd -v cargo run --features=binaries --bin=piazza --release -- -l $nuser -p 10000 --populate > $1/results-$nuser.out 2> $1/results-$nuser.log
done