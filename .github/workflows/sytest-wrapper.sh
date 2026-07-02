#!/bin/bash
# Centralized wrapper script to download, patch, and execute SyTest cleanly with uv.
set -e

MODE="$1" # "frozen", "upgrade", or "offline"

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

# Determine dynamic branch name (replicating original bootstrap.sh logic)
export SYTEST_DEFAULT_BRANCH="${SYTEST_DEFAULT_BRANCH:-develop}"
if [ -n "$SYTEST_BRANCH" ]; then
	branch_name="$SYTEST_BRANCH"
else
	branch_name="$(git --git-dir=/src/.git symbolic-ref HEAD 2>/dev/null)" || branch_name="${SYTEST_DEFAULT_BRANCH}"
fi
# Strip refs/heads/ if present
branch_name="${branch_name#refs/heads/}"

echo "--- Downloading SyTest branch: $branch_name"
if ! wget -q "https://github.com/matrix-org/sytest/archive/$branch_name.tar.gz" -O sytest.tar.gz; then
	echo "Using ${SYTEST_DEFAULT_BRANCH} instead..."
	wget -q "https://github.com/matrix-org/sytest/archive/${SYTEST_DEFAULT_BRANCH}.tar.gz" -O sytest.tar.gz
fi

mkdir -p /sytest
tar -C /sytest --strip-components=1 -xf sytest.tar.gz

# Set necessary environment variables that would normally be set by bootstrap.sh
export SYTEST_LIB="/sytest/lib"

echo "--- Patching SyTest's SQLite DB clearing to remove WAL and SHM files"
sed -i 's/unlink $db if -f $db;/unlink $db if -f $db;\n    unlink "$db-wal" if -f "$db-wal";\n    unlink "$db-shm" if -f "$db-shm";/g' /sytest/lib/SyTest/Homeserver.pm

echo "--- Patching /sytest/scripts/synapse_sytest.sh with uv sync and sytest_template"
# Patch synapse_sytest.sh to run 'uv sync' instead of legacy pip/poetry install,
# and pre-create the sytest_template database to avoid speculative DBI connection warnings.
# We use the absolute path /venv/bin/python to avoid any container PATH issues
/venv/bin/python -c "
with open('/sytest/scripts/synapse_sytest.sh', 'r') as f:
    content = f.read()

# Assertions to ensure we are patching the expected script and have not drifted silently (Violation 2)
assert 'poetry install -vv --extras all' in content or 'pip install' in content, 'Upstream synapse_sytest.sh has drifted: expected installation commands not found'

content = content.replace('poetry install -vv --extras all', '/venv/bin/uv sync --all-extras $UV_ARGS && /venv/bin/uv pip install pyopenssl==26.0.0 cryptography==46.0.7 service-identity==24.2.0')
content = content.replace('/venv/bin/pip install -q --upgrade --upgrade-strategy eager --no-cache-dir /synapse[all]', '(cd /synapse && /venv/bin/uv sync --all-extras $UV_ARGS && /venv/bin/uv pip install pyopenssl==26.0.0 cryptography==46.0.7 service-identity==24.2.0)')
content = content.replace('/venv/bin/pip install --no-deps --no-index --find-links /pypi-offline-cache /synapse', '(cd /synapse && /venv/bin/uv sync --all-extras $UV_ARGS && /venv/bin/uv pip install pyopenssl==26.0.0 cryptography==46.0.7 service-identity==24.2.0)')

# Create sytest_template database to quiet speculative DBI connect noise and errors
content = content.replace(
    'su -c \'psql -c \"CREATE DATABASE pg2;\"\' postgres',
    'su -c \'psql -c \"CREATE DATABASE pg2;\"\' postgres\n    su -c \'psql -c \"CREATE DATABASE sytest_template;\"\' postgres'
)
content = content.replace(
    'CREATE DATABASE pg2_state;',
    'CREATE DATABASE pg2_state;\nCREATE DATABASE sytest_template;'
)

# Confirm replacements actually succeeded (Violation 2)
assert 'poetry install -vv --extras all' not in content, 'Failed to replace poetry install command'
assert '/synapse[all]' not in content, 'Failed to replace legacy pip install command'

with open('/sytest/scripts/synapse_sytest.sh', 'w') as f:
    f.write(content)
"

echo "--- Executing SyTest via synapse_sytest.sh"
exec /sytest/scripts/synapse_sytest.sh
