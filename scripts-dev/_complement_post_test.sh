#!/usr/bin/env bash
#
# Wired in via complement's COMPLEMENT_POST_TEST_SCRIPT hook (see
# executePostScript/Destroy in complement's internal/docker/deployer.go):
# Complement runs this once per homeserver, per test, while that
# homeserver's container is *still up* -- specifically before it stops or
# force-removes it -- and passes exactly:
#   $1  container id
#   $2  test name
#   $3  "true"/"false" -- whether the test failed
#
# Two things happen here, both meant to catch state Complement would
# otherwise destroy before anyone can look at it:
#
#   1. Dump a few cheap Postgres stats (only meaningful for
#      Postgres-backed runs; a no-op, not an error, on SQLite ones) to
#      tests/complement/pg_stats.log so DB activity/locks/deadlocks can be
#      compared test-to-test rather than only inspected live.
#
#   2. On failure, `docker commit` the container to a locally-tagged image
#      before Complement's Destroy() force-removes it. This can snapshot a
#      very large writable layer, so it must not happen for passing tests.
#      The saved image can later be inspected with, e.g.:
#        docker run --rm -it --entrypoint sh <tag>
#        docker cp <a-container-from-that-image>:/data/embedded_hamt ./out
#
# Failures in here are deliberately non-fatal (Complement only logs
# executePostScript's error, it doesn't fail the test run on our account)
# but we still want them visible, so everything below goes to stderr/the
# stats log rather than being silently swallowed.

set -uo pipefail

container_id="${1:?missing container id}"
test_name="${2:?missing test name}"
failed="${3:-false}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
runtime="${CONTAINER_RUNTIME:-docker}"

stats_dir="${repo_root}/.tmp/complement"
stats_log="${stats_dir}/pg_stats.log"
mkdir -p "$stats_dir"

# Synapse's own application log (not the Go test's output) -- the only
# place the closure-cache's diagnostic logger.warning() calls end up.
# Not captured anywhere else: Complement's own logs.jsonl is the *test*
# framework's output, and COMPLEMENT_ALWAYS_PRINT_SERVER_LOGS isn't set.
"$runtime" logs "$container_id" >"${stats_dir}/container.${container_id:0:12}.${test_name//[^A-Za-z0-9_.-]/_}.log" 2>&1 || true

{
	echo "=== $(date -u +%FT%TZ) test=${test_name} failed=${failed} container=${container_id} ==="
	if "$runtime" exec -u postgres "$container_id" pg_isready -q 2>/dev/null; then
		echo "--- pg_stat_database ---"
		"$runtime" exec -u postgres "$container_id" psql -X -q -At -c "
      SELECT datname, numbackends, xact_commit, xact_rollback, deadlocks, conflicts, blks_hit, blks_read
      FROM pg_stat_database
      WHERE datname NOT IN ('template0', 'template1');" 2>&1
		echo "--- pg_stat_activity (non-idle) ---"
		"$runtime" exec -u postgres "$container_id" psql -X -q -At -c "
      SELECT pid, state, wait_event_type, wait_event, now() - query_start AS running_for, left(query, 120)
      FROM pg_stat_activity
      WHERE state IS NOT NULL AND state != 'idle';" 2>&1
		echo "--- pg_locks (not granted) ---"
		"$runtime" exec -u postgres "$container_id" psql -X -q -At -c "
      SELECT pid, mode, locktype, relation::regclass, granted
      FROM pg_locks
      WHERE NOT granted;" 2>&1
	else
		echo "(postgres not up in this container -- SQLite run, or not ready yet; skipping)"
	fi
} >>"$stats_log" 2>&1

safe_name="$(printf '%s' "$test_name" | tr -c 'A-Za-z0-9_.-' '_')"

# With dirty runs, Complement invokes this once at package teardown with
# failed=false, so there is deliberately no image snapshot in that mode.
# For non-dirty runs, preserve only containers belonging to a failed test.
if [[ "$failed" == "true" ]]; then
tag="complement-failed-debug:${safe_name}-$(date +%s)"
if "$runtime" commit "$container_id" "$tag" >/dev/null 2>&1; then
	echo "saved container ${container_id} (test ${test_name}, failed=${failed}) as image ${tag}" >&2
	echo "${tag}" >>"${stats_dir}/failed_images.txt"
else
	echo "WARN: failed to ${runtime} commit ${container_id} for ${test_name}" >&2
fi
fi

# The one thing that actually discriminates between "the closure walk
# under-reports a chain that's really there", "the chain was never built
# at persist time", and "event_auth rows are missing outright": every
# m.room.member event's own direct one-hop auth edges, across every room
# in this run, in order. Postgres-only; harmless no-op on SQLite.
if "$runtime" exec -u postgres "$container_id" pg_isready -q 2>/dev/null; then
	"$runtime" exec -u postgres "$container_id" psql -X -q -At -d synapse -c "
      SELECT e.room_id, e.type, e.state_key, e.stream_ordering, ea.event_id, ea.auth_id
      FROM event_auth ea JOIN events e ON e.event_id = ea.event_id
      WHERE e.type = 'm.room.member'
      ORDER BY e.room_id, e.stream_ordering;" \
		>"${stats_dir}/event_auth.${safe_name}.tsv" 2>&1
	echo "saved event_auth dump for ${test_name} to ${stats_dir}/event_auth.${safe_name}.tsv" >&2
fi

exit 0
