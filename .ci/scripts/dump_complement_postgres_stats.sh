#!/usr/bin/env bash

# Called by Complement before it stops each homeserver container. PostgreSQL is
# integrated into that container, so host-side diagnostics cannot inspect it.
set -euo pipefail

container_id=$1
test_name=$2
stats_dir=${COMPLEMENT_POSTGRES_STATS_DIR:-/tmp/complement-postgres-stats}
stats_file="$stats_dir/$container_id.txt"
target_db="synapse"
psql=(docker exec -e PGPASSWORD=somesecret "$container_id" psql -h localhost -U postgres -v ON_ERROR_STOP=1)

mkdir -p "$stats_dir"

if ! "${psql[@]}" -d "$target_db" -c "SELECT 1;" >/dev/null 2>&1; then
    if "${psql[@]}" -d postgres -c "SELECT 1;" >/dev/null 2>&1; then
        target_db="postgres"
    else
        {
            echo "PostgreSQL unavailable for container $container_id (test: $test_name)"
            echo "Connection output:"
            "${psql[@]}" -d "$target_db" -c "SELECT 1;" 2>&1 || true
        } >"$stats_file"
        exit 0
    fi
fi

run_query() {
    "${psql[@]}" -d "$target_db" -c "$1" || true
}

{
    echo "Complement PostgreSQL diagnostics"
    echo "test: $test_name"
    echo "container: $container_id"
    echo "timestamp: $(date --iso-8601=seconds)"
    echo
    echo "== database size =="
    run_query "SELECT pg_size_pretty(pg_database_size(current_database())) AS size;"

    echo
    echo "== database activity =="
    run_query "SELECT xact_commit, xact_rollback, blks_read, blks_hit, tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted, temp_files, temp_bytes, deadlocks FROM pg_catalog.pg_stat_database WHERE datname = current_database();"

    echo
    echo "== largest tables =="
    run_query "SELECT schemaname || '.' || relname AS table_name, pg_size_pretty(pg_total_relation_size(relid)) AS total_size, pg_size_pretty(pg_relation_size(relid)) AS table_size, pg_size_pretty(pg_indexes_size(relid)) AS index_size FROM pg_catalog.pg_stat_user_tables ORDER BY pg_total_relation_size(relid) DESC LIMIT 10;"

    echo
    echo "== most row reads =="
    run_query "SELECT schemaname || '.' || relname AS table_name, (seq_tup_read + idx_tup_fetch) AS total_rows_read, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch FROM pg_catalog.pg_stat_user_tables ORDER BY (seq_tup_read + idx_tup_fetch) DESC LIMIT 10;"

    echo
    echo "== most read cache misses =="
    run_query "SELECT schemaname || '.' || relname AS table_name, (heap_blks_read + idx_blks_read) AS total_disk_blocks_read FROM pg_catalog.pg_statio_user_tables ORDER BY (heap_blks_read + idx_blks_read) DESC LIMIT 10;"

    echo
    echo "== most INSERT/UPDATE/DELETE ops =="
    run_query "SELECT schemaname || '.' || relname AS table_name, (n_tup_ins + n_tup_upd + n_tup_del) AS total_writes, n_tup_ins AS inserts, n_tup_upd AS updates, n_tup_del AS deletes FROM pg_catalog.pg_stat_user_tables ORDER BY (n_tup_ins + n_tup_upd + n_tup_del) DESC LIMIT 10;"
} >"$stats_file" 2>&1
