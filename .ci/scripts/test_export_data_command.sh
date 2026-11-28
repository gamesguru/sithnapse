#!/usr/bin/env bash

# Test for the export-data admin command against sqlite and postgres

# Expects Synapse to have been already installed with `uv sync --extra postgres` (or similar).
# Expects `uv` to be available on the `PATH`.

set -xe
cd "$(dirname "$0")/../.."

# `synapse.app.admin_cmd` now requires mtxdb. Keep the SQL-only configs for
# schema setup and synapse_port_db, then create export-only configs that share
# one fresh mtxdb fixture. The SQLite database is ported to Postgres, so their
# event and state IDs match and the embedded fixture can serve both exports.
MTXDB_PATH="$(mktemp -d)"
SQLITE_EXPORT_CONFIG="$(mktemp .ci/sqlite-export-config.XXXXXX.yaml)"
POSTGRES_EXPORT_CONFIG="$(mktemp .ci/postgres-export-config.XXXXXX.yaml)"
cleanup() {
  rm -f "$SQLITE_EXPORT_CONFIG" "$POSTGRES_EXPORT_CONFIG"
  rm -rf "$MTXDB_PATH"
}
trap cleanup EXIT

cp .ci/sqlite-config.yaml "$SQLITE_EXPORT_CONFIG"
cp .ci/postgres-config.yaml "$POSTGRES_EXPORT_CONFIG"
cat >> "$SQLITE_EXPORT_CONFIG" <<EOF

embedded_db:
  engine: mtxdb
  path: "$MTXDB_PATH"
EOF
cat >> "$POSTGRES_EXPORT_CONFIG" <<EOF

embedded_db:
  engine: mtxdb
  path: "$MTXDB_PATH"
EOF

echo "--- Generate the signing key"

# Generate the server's signing key.
uv run synapse_homeserver --generate-keys -c .ci/sqlite-config.yaml

echo "--- Prepare test database"

# Upgrade the fixture with mtxdb enabled so the state HAMT backfill is written
# to the embedded store rather than completed only in SQL.
uv run update_synapse_database --database-config "$SQLITE_EXPORT_CONFIG" --run-background-updates

echo "--- Prepare the mtxdb fixture for export"

# The fixture predates embedded state and event adjacency. Backfill state HAMT
# roots, migrate SQL auth edges, and import prev-event adjacency explicitly.
uv run synapse_auth_edges_migrate --config "$SQLITE_EXPORT_CONFIG"
uv run python .ci/scripts/prepare_export_data_mtxdb.py --config "$SQLITE_EXPORT_CONFIG"

# Event status is recorded in the EventDag locator headers, and admin_cmd's
# startup gate refuses a store that predates the status migration. The fixture
# is older than that gate, so migrate it before the first export.
uv run synapse_event_status_migrate --config "$SQLITE_EXPORT_CONFIG"

# Run the export-data command on the sqlite test database
uv run python -m synapse.app.admin_cmd -c "$SQLITE_EXPORT_CONFIG" export-data @anon-20191002_181700-832:localhost:8800 \
--output-directory /tmp/export_data

# Test that the output directory exists and contains the rooms directory
dir_r="/tmp/export_data/rooms"
dir_u="/tmp/export_data/user_data"
if [ -d "$dir_r" ] && [ -d "$dir_u" ]; then
  echo "Command successful, this test passes"
else
  echo "No output directories found, the command fails against a sqlite database."
  exit 1
fi

# Create the PostgreSQL database.
psql -c "CREATE DATABASE synapse"

# Port the SQLite databse to postgres so we can check command works against postgres
echo "+++ Port SQLite3 databse to postgres"
uv run synapse_port_db --sqlite-database .ci/test_db.db --postgres-config .ci/postgres-config.yaml

# synapse_port_db intentionally uses the SQL-only config. It may requeue the
# state-root backfill when those roots live in the shared mtxdb, so finish that
# update under the export config before starting admin_cmd.
uv run update_synapse_database --database-config "$POSTGRES_EXPORT_CONFIG" --run-background-updates

# The completion marker lives in SQL: confirm it survived the SQLite -> Postgres
# port before admin_cmd's startup gate reads it.
uv run synapse_event_status_migrate --config "$POSTGRES_EXPORT_CONFIG"

# Run the export-data command on postgres database
uv run python -m synapse.app.admin_cmd -c "$POSTGRES_EXPORT_CONFIG" export-data @anon-20191002_181700-832:localhost:8800 \
--output-directory /tmp/export_data2

# Test that the output directory exists and contains the rooms directory
dir_r2="/tmp/export_data2/rooms"
dir_u2="/tmp/export_data2/user_data"
if [ -d "$dir_r2" ] && [ -d "$dir_u2" ]; then
  echo "Command successful, this test passes"
else
  echo "No output directories found, the command fails against a postgres database."
  exit 1
fi
