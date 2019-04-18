#!/bin/bash
if ! command -v pls >/dev/null 2>&1; then
	function pls() {
		"$@"
	}
	function perflock() {
		"$@"
	}
fi

pls cargo b --release --bin piazza || exit

for l in 0.25 0.5 0.75 1.0; do
for setup in "partial" "shallow-readers" "full"; do
	prefix="piazza-${setup}-$(echo "$l" | sed 's/\./_/')l"
	echo "==> $setup (logged-in fraction: $l)"
	perflock cargo r -q --release --bin piazza -- -m "$setup" --iter 3 -u 1000 -l $l -g "${prefix}.gv" > "${prefix}.log" 2> "${prefix}.err"
done
done
