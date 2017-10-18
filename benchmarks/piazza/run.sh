#!/bin/bash

case "$OSTYPE" in
  darwin*)  cmd="gtime" ;;
  linux*)   cmd="/usr/bin/time" ;;
esac

for i in {0..10}
do
    nuser=$(( $i * 100 ))
    $cmd -v cargo run --features=binaries --bin=piazza --release -- -u $nuser -p 10000 --populate > results-$nuser.out 2> results-$nuser.log
done