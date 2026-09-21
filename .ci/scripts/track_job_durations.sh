#!/usr/bin/env bash
# Append per-job AND per-step wall-clock durations from the "Tests" workflow
# to a CSV, so we can spot regressions (e.g. hybrid TiKV modes getting slower)
# over time instead of noticing by hand months later.
#
# Usage:
#   .ci/scripts/track_job_durations.sh [branch] [csv_path]
#
# Defaults to the current branch and docs/development-gg/ci-job-durations.csv.
# Safe to re-run: it skips (run_id, job_id, step_name) rows already recorded.
#
# Each job produces one row with an empty step_name (the whole-job duration,
# including fixed per-job overhead like image sanity checks and log upload)
# plus one additional row per step named in $STEP_NAMES (the actual
# test-execution time). Job-level duration is NOT simply "sum of steps" --
# it also includes setup/checkout/upload overhead that doesn't scale with
# backend choice, so it dilutes cross-backend percentage comparisons. Use
# the step-level rows (non-empty step_name) when comparing engines/backends;
# use job-level rows (empty step_name) when accounting for total CI wall time.
#
# Rows are kept sorted ascending by run_started_at (column 3) on every run --
# gh run list/view return newest-first, and appending in fetch order silently
# leaves the file in newest-first order. Splitting or trending that as if it
# were chronological order inverts the result (a real regression reads as an
# improvement, and vice versa) without erroring -- do not skip the sort step
# below, and don't assume row order without checking column 3 yourself.

set -euo pipefail

branch=${1:-$(git rev-parse --abbrev-ref HEAD)}
csv_path=${2:-docs/development-gg/ci-job-durations.csv}
workflow=${WORKFLOW_NAME:-tests.yml}
limit=${RUN_LIMIT:-20}
# Steps worth tracking individually, across all job types in this workflow.
IFS=',' read -r -a step_names <<<"${STEP_NAMES:-Run Complement Tests,Run in-repo Complement Tests}"
header="branch,run_id,run_started_at,job_id,job_name,step_name,duration_seconds,conclusion"

mkdir -p "$(dirname "$csv_path")"
if [ ! -f "$csv_path" ]; then
	echo "$header" >"$csv_path"
fi

echo "Fetching last $limit '$workflow' runs on branch '$branch'..." >&2

run_ids=$(gh run list --workflow "$workflow" --branch "$branch" --limit "$limit" \
	--json databaseId -q '.[].databaseId')

# Build a jq array literal of the step names we care about.
step_names_json=$(printf '%s\n' "${step_names[@]}" | jq -R . | jq -s .)

for run_id in $run_ids; do
	run_started_at=$(gh run view "$run_id" --json createdAt -q '.createdAt')

	gh run view "$run_id" --json jobs | jq -r --argjson wanted_steps "$step_names_json" '
		.jobs[]
		| select(
			.startedAt != null and .completedAt != null
			and .startedAt != "0001-01-01T00:00:00Z"
			and .completedAt != "0001-01-01T00:00:00Z"
		)
		| . as $job
		| (
			# Whole-job row. Use a sentinel (not "") for the missing step_name:
			# bash `read -d $'"'"'\t'"'"'` treats runs of IFS-whitespace
			# (tab included) as a single delimiter and silently drops empty
			# fields between two tabs, which would corrupt the row.
			[
				$job.databaseId,
				$job.name,
				"-",
				(($job.completedAt | fromdateiso8601) - ($job.startedAt | fromdateiso8601)),
				$job.conclusion
			]
		),
		(
			# One row per matched step, actual test-execution time.
			($job.steps // [])[]
			| select(
				(.name as $n | $wanted_steps | index($n)) != null
				and .startedAt != null and .completedAt != null
				and .startedAt != "0001-01-01T00:00:00Z"
				and .completedAt != "0001-01-01T00:00:00Z"
			)
			| [
				$job.databaseId,
				$job.name,
				.name,
				((.completedAt | fromdateiso8601) - (.startedAt | fromdateiso8601)),
				(.conclusion // $job.conclusion)
			]
		)
		| @tsv
	' | while IFS=$'\t' read -r job_id job_name step_name duration_seconds conclusion; do
		# job_name/step_name may contain commas (e.g. "trial (3.10, postgres, 14, all)");
		# quote them for CSV safety.
		if [ "$step_name" = "-" ]; then
			prefix="${run_id},${run_started_at},${job_id},\"${job_name}\",,"
		else
			prefix="${run_id},${run_started_at},${job_id},\"${job_name}\",\"${step_name}\","
		fi
		# Skip if this exact (run_id, job_id, step_name) row is already recorded.
		if grep -qF "$prefix" "$csv_path" 2>/dev/null; then
			continue
		fi
		printf '%s,%s%s,%s\n' \
			"$branch" "$prefix" "$duration_seconds" "$conclusion" >>"$csv_path"
	done
done

# Re-sort ascending by run_started_at (column 3). Fields 5/6 (job_name/step_name)
# are quoted and may themselves contain commas, but they sort after our key
# column so quoting doesn't confuse `sort -t,`.
{
	echo "$header"
	tail -n +2 "$csv_path" | sort -t, -k3,3
} >"${csv_path}.sorted"
mv "${csv_path}.sorted" "$csv_path"

echo "Updated $csv_path (sorted ascending by run_started_at)" >&2
