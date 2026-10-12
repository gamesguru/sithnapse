#!/usr/bin/env bash
# Compare bounded and exhaustive mtxdb collection scans.
#
# Usage:
#   scripts-dev/benchmark_mtxdb_scan.sh DATABASE COLLECTION [SAMPLES]
#
# Example:
#   scripts-dev/benchmark_mtxdb_scan.sh \
#     /run/media/shane/shane4tb-ent/.mtxdb \
#     0xA3C568666DC7494C681D68CCB1C0C667 5

set -euo pipefail

database_dir="${1:?database directory required}"
collection="${2:?collection id required}"
samples="${3:-5}"

if ! [[ "$samples" =~ ^[1-9][0-9]*$ ]]; then
	echo "samples must be a positive integer" >&2
	exit 2
fi
command -v mtxdb >/dev/null || {
	echo "mtxdb is not on PATH" >&2
	exit 127
}

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/mtxdb-scan-bench.XXXXXX")"
trap 'rm -rf "$tmp_dir"' EXIT

run_scan() {
	local label="$1"
	local limit="$2"
	local sample="$3"
	local time_file elapsed

	time_file="$tmp_dir/${label}.${sample}.time"
	# Keep terminal output out of the measurement. The scan still parses the
	# same records; only rendering is discarded.
	if ! /usr/bin/time -f '%e' -o "$time_file" \
		mtxdb --dir "$database_dir" scan -l "$limit" "$collection" >/dev/null; then
		echo "${label} sample ${sample}: command failed" >&2
		exit 1
	fi
	elapsed="$(<"$time_file")"
	printf '%-9s sample %2d/%-2d %ss\n' "$label" "$sample" "$samples" "$elapsed"
	printf '%s\n' "$elapsed" >>"$tmp_dir/$label.values"
}

# The first invocation is intentionally reported separately: it includes
# process startup and whatever page-cache state the host currently has.
echo "database=$database_dir collection=$collection samples=$samples"
echo "(output discarded; timings are wall-clock seconds)"
: >"$tmp_dir/bounded.values"
: >"$tmp_dir/full.values"

for sample in $(seq 1 "$samples"); do
	run_scan bounded 5 "$sample"
	run_scan full 0 "$sample"
done

median() {
	sort -n "$1" | awk '
    { values[NR] = $1 }
    END {
      if (NR == 0) exit 1
      if (NR % 2) print values[(NR + 1) / 2]
      else print (values[NR / 2] + values[NR / 2 + 1]) / 2
    }'
}

echo
echo "median bounded(5): $(median "$tmp_dir/bounded.values")s"
echo "median full(0):     $(median "$tmp_dir/full.values")s"
