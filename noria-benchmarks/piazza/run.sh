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
<<<<<<< HEAD
	perflock cargo r -q --release --bin piazza -- -m "$setup" --iter 5 -u 1000 -l $l -g "${prefix}.gv" -v > "${prefix}.log" 2> "${prefix}.err"
=======
	perflock cargo r -q --release --bin piazza -- -m "$setup" --iter 3 -u 1000 -l $l -g "${prefix}.gv" > "${prefix}.log" 2> "${prefix}.err"
>>>>>>> Run multiple iterations at once
done
done

for l in 0.25 0.5 0.75 1.0; do
	prefix="piazza-shallow-readers-noreuse-$(echo "$l" | sed 's/\./_/')l"
	echo "==> no-reuse (logged-in fraction: $l)"
	perflock cargo r -q --release --bin piazza -- -m "shallow-readers" --iter 5 -u 1000 -l $l --reuse no -g "${prefix}.gv" -v > "${prefix}.log" 2> "${prefix}.err"
done
