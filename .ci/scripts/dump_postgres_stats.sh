#!/usr/bin/env bash

set -euo pipefail

stats_file=${1:-/tmp/postgres-stats.txt}
: >"$stats_file"

psql_args=(-h "${SYNAPSE_POSTGRES_HOST:-localhost}"
	-U "${SYNAPSE_POSTGRES_USER:-postgres}" -v ON_ERROR_STOP=1)

# Check if PostgreSQL is reachable before attempting statistics collection
if ! psql "${psql_args[@]}" -d postgres -c "SELECT 1;" >/dev/null 2>&1; then
	echo "PostgreSQL is not reachable at ${SYNAPSE_POSTGRES_HOST:-localhost}; skipping statistics collection." | tee "$stats_file"
	exit 0
fi

{
	echo "PostgreSQL diagnostics"
	date --iso-8601=seconds
	echo
	echo "== database sizes =="
	psql "${psql_args[@]}" -d postgres -c \
		"SELECT datname, pg_size_pretty(pg_database_size(datname)) AS size
       FROM pg_catalog.pg_database
      WHERE datallowconn AND NOT datistemplate
      ORDER BY pg_database_size(datname) DESC;" || true

	echo
	echo "== database activity =="
	psql "${psql_args[@]}" -d postgres -c \
		"SELECT datname, xact_commit, xact_rollback, blks_read, blks_hit,
            tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted,
            temp_files, temp_bytes, deadlocks
       FROM pg_catalog.pg_stat_database
      WHERE datname <> 'postgres' AND datname NOT LIKE 'template%'
      ORDER BY (tup_fetched + tup_inserted + tup_updated + tup_deleted) DESC;" || true

	# The suite can create hundreds of databases. Keep detailed table output
	# bounded, while printing database-level counters for all of them above.
	mapfile -t databases < <(psql "${psql_args[@]}" -d postgres -Atc \
		"SELECT datname FROM pg_catalog.pg_stat_database
      WHERE datname <> 'postgres' AND datname NOT LIKE 'template%'
      ORDER BY (tup_fetched + tup_inserted + tup_updated + tup_deleted) DESC
      LIMIT 10;" || true)

	for database in "${databases[@]}"; do
		[ -n "$database" ] || continue
		echo
		echo "================================================================"
		echo "DATABASE: $database"
		echo "================================================================"

		echo
		echo "-- Largest by size"
		psql "${psql_args[@]}" -d "$database" <<'SQL' || true
SELECT schemaname || '.' || relname AS table_name,
       pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
       pg_size_pretty(pg_relation_size(relid)) AS table_size,
       pg_size_pretty(pg_indexes_size(relid)) AS index_size
FROM pg_catalog.pg_stat_user_tables
ORDER BY pg_total_relation_size(relid) DESC
LIMIT 10;
SQL

		echo
		echo "-- Most row reads"
		psql "${psql_args[@]}" -d "$database" <<'SQL' || true
SELECT schemaname || '.' || relname AS table_name,
       (seq_tup_read + idx_tup_fetch) AS total_rows_read,
       seq_scan, seq_tup_read, idx_scan, idx_tup_fetch
FROM pg_catalog.pg_stat_user_tables
ORDER BY (seq_tup_read + idx_tup_fetch) DESC
LIMIT 10;
SQL

		echo
		echo "-- Most read cache misses"
		psql "${psql_args[@]}" -d "$database" <<'SQL' || true
SELECT schemaname || '.' || relname AS table_name,
       (heap_blks_read + idx_blks_read) AS total_disk_blocks_read
FROM pg_catalog.pg_statio_user_tables
ORDER BY (heap_blks_read + idx_blks_read) DESC
LIMIT 10;
SQL

		echo
		echo "-- Most INSERT/UPDATE/DELETE ops"
		psql "${psql_args[@]}" -d "$database" <<'SQL' || true
SELECT schemaname || '.' || relname AS table_name,
       (n_tup_ins + n_tup_upd + n_tup_del) AS total_writes,
       n_tup_ins AS inserts, n_tup_upd AS updates, n_tup_del AS deletes
FROM pg_catalog.pg_stat_user_tables
ORDER BY (n_tup_ins + n_tup_upd + n_tup_del) DESC
LIMIT 10;
SQL

		echo
	done
} 2>&1 | tee "$stats_file"
