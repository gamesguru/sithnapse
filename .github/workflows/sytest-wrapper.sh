#!/bin/bash
# Centralized wrapper script to download, patch, and execute SyTest cleanly with uv.
set -e

MODE="$1" # "frozen", "upgrade", or "offline"
shift
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SYTEST_DIR="${SYTEST_DIR:-$REPO_ROOT/.sytest}"
SYTEST_VENV_DIR="${SYTEST_VENV_DIR:-$REPO_ROOT/.venv}"
SYTEST_WORK_DIR="${SYTEST_WORK_DIR:-$SYTEST_DIR/.work}"
SYTEST_LOG_DIR="${SYTEST_LOG_DIR:-$SYTEST_DIR/.logs}"
SYTEST_SOURCE_DIR="${SYTEST_SOURCE_DIR:-$REPO_ROOT}"
SYTEST_RUNTIME_DIR="${SYTEST_RUNTIME_DIR:-$SYTEST_DIR/.runtime}"
SYTEST_PERL_DIR="${SYTEST_PERL_DIR:-$SYTEST_DIR/.perl5}"
export SYTEST_DIR
export SYTEST_VENV_DIR
export SYTEST_WORK_DIR
export SYTEST_LOG_DIR
export SYTEST_SOURCE_DIR
export SYTEST_RUNTIME_DIR
export SYTEST_PERL_DIR
export SYTEST_COVERAGE="${SYTEST_COVERAGE:-1}"
export PATH="$SYTEST_PERL_DIR/bin:$PATH"
export COVERAGE_FILE="$SYTEST_WORK_DIR/.coverage"
export PERL5LIB="$SYTEST_PERL_DIR/lib/perl5${PERL5LIB:+:$PERL5LIB}"
export PERL_LOCAL_LIB_ROOT="$SYTEST_PERL_DIR${PERL_LOCAL_LIB_ROOT:+:$PERL_LOCAL_LIB_ROOT}"
export PERL_MB_OPT="--install_base \"$SYTEST_PERL_DIR\""
export PERL_MM_OPT="INSTALL_BASE=$SYTEST_PERL_DIR"

if [ -z "$MODE" ]; then
	echo "Usage: $0 [frozen|upgrade|offline]"
	exit 1
fi

case "$MODE" in
frozen)
	UV_ARGS="--frozen"
	;;
upgrade)
	UV_ARGS="--upgrade"
	;;
offline)
	UV_ARGS="--offline"
	;;
*)
	echo "Unknown mode: $MODE"
	exit 1
	;;
esac

export UV_ARGS
export UV_PROJECT_ENVIRONMENT="$SYTEST_VENV_DIR"

# Pin a known-good commit of SyTest for determinism in CI.
# To update, run: git ls-remote https://github.com/matrix-org/sytest.git refs/heads/develop
SYTEST_PINNED_REV="c4da260e19a25d4ef86e07409ffd0fda5b2c2eb8"

if [ -n "$SYTEST_BRANCH" ]; then
	branch_name="$SYTEST_BRANCH"
else
	# Use the pinned commit instead of tracking a moving branch
	branch_name="$SYTEST_PINNED_REV"
fi
# Strip refs/heads/ if present
branch_name="${branch_name#refs/heads/}"

echo "--- Downloading SyTest revision: $branch_name"
if ! wget -q "https://github.com/matrix-org/sytest/archive/$branch_name.tar.gz" -O sytest.tar.gz; then
	echo "Using pinned revision $SYTEST_PINNED_REV instead..."
	wget -q "https://github.com/matrix-org/sytest/archive/$SYTEST_PINNED_REV.tar.gz" -O sytest.tar.gz
fi

mkdir -p "$SYTEST_DIR"
tar -C "$SYTEST_DIR" --strip-components=1 -xf sytest.tar.gz

# Set necessary environment variables that would normally be set by bootstrap.sh
export SYTEST_LIB="$SYTEST_DIR/lib"

echo "--- Patching SyTest's SQLite DB clearing to remove WAL and SHM files"
sed -i "s/unlink \$db if -f \$db;/unlink \$db if -f \$db;\n    unlink \"\$db-wal\" if -f \"\$db-wal\";\n    unlink \"\$db-shm\" if -f \"\$db-shm\";/g" "$SYTEST_DIR/lib/SyTest/Homeserver.pm"

echo "--- Patching /sytest/scripts/synapse_sytest.sh to pre-create sytest_template database"
# Create sytest_template database to quiet speculative DBI connect noise and errors
"$SYTEST_VENV_DIR/bin/python" <<'PY'
import os
import shutil

with open(os.environ['SYTEST_DIR'] + '/scripts/synapse_sytest.sh', 'r') as f:
    content = f.read()

content = content.replace('/work', os.environ['SYTEST_WORK_DIR'])
content = content.replace('/logs', os.environ['SYTEST_LOG_DIR'])
content = content.replace('/src', os.environ['SYTEST_SOURCE_DIR'])
content = content.replace('/synapse', os.environ['SYTEST_RUNTIME_DIR'])
content = content.replace('/sytest', os.environ['SYTEST_DIR'])
runtime_dir = os.environ['SYTEST_RUNTIME_DIR']
content = content.replace(
    f'cp -r "$SYNAPSE_SOURCE" {runtime_dir}',
    f'rsync -a --exclude=.git/ --exclude=target/ --exclude=.sytest/ '
    f'--exclude=.venv/ --exclude=.mypy_cache/ --exclude=.tmp/ '
    f'--exclude=.sytest-runtime/ '
    f'"$SYNAPSE_SOURCE"/ {runtime_dir}/',
)

content = content.replace(
    'su -c \'psql -c \"CREATE DATABASE pg2;\"\' postgres',
    'su -c \'psql -c \"CREATE DATABASE pg2;\"\' postgres\n    su -c \'psql -c \"CREATE DATABASE sytest_template;\"\' postgres'
)
content = content.replace(
    'CREATE DATABASE pg2_state;',
    'CREATE DATABASE pg2_state;\nCREATE DATABASE sytest_template;'
)

with open(os.environ['SYTEST_DIR'] + '/scripts/synapse_sytest.sh', 'w') as f:
    f.write(content)
PY

echo "--- Patching /sytest/scripts/synapse_sytest.sh to use uv sync"
# Patch synapse_sytest.sh to run 'uv sync' instead of legacy pip/poetry install
# We use the absolute path /venv/bin/python to avoid any container PATH issues
"$SYTEST_VENV_DIR/bin/python" <<'PY'
import os
import shutil

with open(os.environ['SYTEST_DIR'] + '/scripts/synapse_sytest.sh', 'r') as f:
    content = f.read()

uv_args = os.environ.get('UV_ARGS', '')
# Assertions to ensure we are patching the expected script and have not drifted silently
assert 'poetry install -vv --extras all' in content or 'pip install' in content, 'Upstream synapse_sytest.sh has drifted: expected installation commands not found'

venv_dir = os.environ['SYTEST_VENV_DIR']
uv_path = shutil.which('uv')
if not uv_path:
    raise RuntimeError('uv was not found on PATH')
content = content.replace(f'{venv_dir}/bin/uv', uv_path)
content = content.replace('/venv', venv_dir)
content = content.replace('/bin/pip ', '/bin/uv pip ')
content = content.replace(f'{venv_dir}/bin/uv', uv_path)
content = content.replace('--upgrade --upgrade-strategy eager', '--upgrade')
content = content.replace('poetry install -vv --extras all', f'{uv_path} sync --all-extras {uv_args}')
content = content.replace('/venv/bin/pip install -q --upgrade --upgrade-strategy eager --no-cache-dir /synapse[all]', f'(cd /synapse && {uv_path} sync --all-extras {uv_args})')
content = content.replace('/venv/bin/pip install --no-deps --no-index --find-links /pypi-offline-cache /synapse', f'(cd /synapse && {uv_path} sync --all-extras {uv_args})')

# Confirm replacements actually succeeded
assert 'poetry install -vv --extras all' not in content, 'Failed to replace poetry install command'
assert '/synapse[all]' not in content, 'Failed to replace legacy pip install command'

# SyTest's --coverage flag makes each homeserver run through ``coverage run``.
# Keep coverage enabled by default for CI, but allow fast local runs to use the
# interpreter directly (and avoid the large first-startup delay).
coverage_switch = '''
if [ "$SYTEST_COVERAGE" = "0" ]; then
    RUN_TESTS_WITHOUT_COVERAGE=()
    for RUN_TEST_ARG in "${RUN_TESTS[@]}"; do
        if [ "$RUN_TEST_ARG" != "--coverage" ]; then
            RUN_TESTS_WITHOUT_COVERAGE+=("$RUN_TEST_ARG")
        fi
    done
    RUN_TESTS=("${RUN_TESTS_WITHOUT_COVERAGE[@]}")
    unset COVERAGE_PROCESS_START
fi
'''
anchor = 'if [ -n "$ASYNCIO_REACTOR" ]; then'
assert anchor in content, 'Could not find SyTest run-tests options anchor'
content = content.replace(anchor, coverage_switch + '\n' + anchor, 1)

with open(os.environ['SYTEST_DIR'] + '/scripts/synapse_sytest.sh', 'w') as f:
    f.write(content)
PY

echo "--- Patching /sytest/lib/SyTest/Homeserver/Synapse.pm to inject config"
# When SYNAPSE_EMBEDDED_DB_ENGINE/SYNAPSE_EMBEDDED_DB_PATH are set, add an
# `embedded_db` block to the homeserver config that sytest generates, so
# the HAMT state backend runs against a real mtxdb database (mirrors
# trial-mtxdb / complement-mtxdb). Synapse also reads these as plain
# environment variables directly (see synapse/config/database.py), but
# sytest-spawned processes aren't guaranteed to inherit the host
# environment, so this config injection is the same belt-and-braces
# approach the old TiKV wiring used here.
"$SYTEST_VENV_DIR/bin/python" <<'PY'
import os

with open(os.environ['SYTEST_DIR'] + '/lib/SyTest/Homeserver/Synapse.pm', 'r') as f:
    content = f.read()

anchor = '        databases => \\%db_configs,'
content = content.replace(
    '"PATH" => $ENV{PATH},',
    '"PATH" => $ENV{PATH},\n'
    '      "COVERAGE_FILE" => $ENV{COVERAGE_FILE},',
)
injection = '''        databases => \\%db_configs,
        # SyTest deliberately fires rapid-succession failure/retry scenarios
        # (e.g. tests/50federation/01keys.pl) against the same reused fake
        # federation server within a single test file. MSC4499's negative-cache
        # backoff for key fetches (see KeyFetchBackoffCache in
        # synapse/crypto/keyring.py) would otherwise make a later, unrelated
        # test in the same file see a stale backoff window and fail fast
        # instead of attempting its fetch. Force the floor to 0s so this
        # doesn't leak between tests; override via SYNAPSE_KEY_FETCH_BACKOFF_FLOOR
        # if a test specifically wants to exercise backoff behaviour.
        key_fetch_backoff_floor => ( length( $ENV{SYNAPSE_KEY_FETCH_BACKOFF_FLOOR} // '' ) ? $ENV{SYNAPSE_KEY_FETCH_BACKOFF_FLOOR} : "0s" ),
        ( do {
            my $engine = $ENV{SYNAPSE_EMBEDDED_DB_ENGINE} // '';
            my $path = $ENV{SYNAPSE_EMBEDDED_DB_PATH} // '';
            ( length($engine) && length($path) )
                ? ( embedded_db => { engine => $engine, path => $path } )
                : ();
        } ),'''

assert anchor in content, 'Could not find databases anchor in Synapse.pm'
content = content.replace(anchor, injection, 1)

with open(os.environ['SYTEST_DIR'] + '/lib/SyTest/Homeserver/Synapse.pm', 'w') as f:
    f.write(content)
PY

echo "--- Executing SyTest via synapse_sytest.sh"
exec "$SYTEST_DIR/scripts/synapse_sytest.sh" "$@"
