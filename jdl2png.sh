#!/bin/bash
#
# USAGE
#
#      ./jdl2png.sh [-p] IN-JDL [OUT-PNG]
#
# The -p option indicates that the output PNG should be opened in a GUI
# preview application. If OUT-PNG is omitted, a temporary file is
# created, and -p is implied.

set -o errexit
set -o pipefail

open() {
  if command -v xdg-open >/dev/null 2>&1; then
    xdg-open "$@"
  elif command -v open >/dev/null 2>&1; then
    command open -a /Applications/Preview.app "$@"
  else
    echo 'unable to find `xdg-open` or `open` on path' >&2
    exit 1
  fi
}

do_preview=false
while getopts ":p" opt; do
  case "$opt" in
    p) do_preview=true ;;
    *) echo "Invalid option '$OPTARG'." >&2; exit 1 ;;
  esac
done
shift $((OPTIND-1))

infile="$1"
outfile="$2"

[[ "$outfile" ]] || {
  do_preview=true
  outfile="$(mktemp).png"
}

cargo run --features=jdl --bin=jdl2dot -- "$infile" | dot -Gdpi=300 -Tpng > "$outfile"

$do_preview && {
  open "$outfile"
}
