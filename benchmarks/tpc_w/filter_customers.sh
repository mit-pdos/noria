#!/bin/bash

cat tests/tpc-w-queries.txt | egrep "(customer|CREATE)" | grep -v "#" > tests/tpc-w-customer-queries.txt
for i in `seq 11 20`; do
	head -n $i tests/tpc-w-customer-queries.txt > tests/tpc-w-customer-queries-$((i-10)).txt
done
