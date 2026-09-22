#!/usr/bin/env bash
# This script is designed for developers who want to test their code
# against Complement.
#
# It makes a Synapse image which represents the current checkout,
# builds a synapse-complement image on top, then runs tests with it.
#
# By default the script will fetch the latest Complement main branch and
# run tests with that. This can be overridden to use a custom Complement
# checkout by setting the COMPLEMENT_DIR environment variable to the
# filepath of a local Complement checkout or by setting the COMPLEMENT_REF
# environment variable to pull a different branch or commit.
#
# To use the 'podman' command instead 'docker', set the PODMAN environment
# variable. Example:
#
# PODMAN=1 ./complement.sh
#
# By default Synapse is run in monolith mode. This can be overridden by
# setting the WORKERS environment variable.
#
# You can optionally give a "-f" argument (for "fast") before any to skip
# rebuilding the docker images, if you just want to rerun the tests.
#
# Remaining commandline arguments are passed through to `go test`. For example,
# you can supply a regular expression of test method names via the "-run"
# argument:
#
# ./complement.sh -run "TestOutboundFederation(Profile|Send)"
#
# Specifying TEST_ONLY_SKIP_DEP_HASH_VERIFICATION=1 will cause `poetry export`
# to not emit any hashes when building the Docker image. This then means that
# you can use 'unverifiable' sources such as git repositories as dependencies.

# Exit if a line returns a non-zero exit code
set -e

# Tag local builds with a dummy registry namespace so that later builds may reference
# them exactly instead of accidentally pulling from a remote registry.
#
# This is important as some Docker storage drivers/types prefer remote images over local
# (like `containerd`) which causes problems as we're testing against some remote image
# that doesn't include all of the changes that we're trying to test (be it locally or in
# a PR in CI). This is spawning from a real-world problem where the GitHub runners were
# updated to use Docker Engine 29.0.0+ which uses `containerd` by default for new
# installations.
#
# XXX: If the Docker image name changes, don't forget to update
# `.github/workflows/push_complement_image.yml` as well
LOCAL_IMAGE_NAMESPACE=localhost

# The image tags for how these images will be stored in the registry
SYNAPSE_IMAGE_PATH="$LOCAL_IMAGE_NAMESPACE/synapse"
SYNAPSE_WORKERS_IMAGE_PATH="$LOCAL_IMAGE_NAMESPACE/synapse-workers"
# XXX: If the Docker image name changes, don't forget to update
# `.github/workflows/push_complement_image.yml` as well
COMPLEMENT_SYNAPSE_IMAGE_PATH="$LOCAL_IMAGE_NAMESPACE/complement-synapse"

SYNAPSE_EDITABLE_IMAGE_PATH="$LOCAL_IMAGE_NAMESPACE/synapse-editable"
SYNAPSE_WORKERS_EDITABLE_IMAGE_PATH="$LOCAL_IMAGE_NAMESPACE/synapse-workers-editable"
COMPLEMENT_SYNAPSE_EDITABLE_IMAGE_PATH="$LOCAL_IMAGE_NAMESPACE/complement-synapse-editable"

# Helper to emit annotations that collapse portions of the log in GitHub Actions
echo_if_github() {
  if [[ -n "$GITHUB_WORKFLOW" ]]; then
    printf '%s\n' "$*" >&2
  fi
}

# Helper to print out the usage instructions
usage() {
    cat >&2 <<EOF
Usage: $0 [-f] <go test arguments>...
Run the complement test suite on Synapse.
  --in-repo
        Whether to run the in-repo suite of Complement tests (see ./complement in this project)
        vs the Complement tests from the Complement repo.

  -f, --fast
        Skip rebuilding the docker images, and just use the most recent
        'localhost/complement-synapse:latest' image.
        Conflicts with --build-only.

  --build-only
        Only build the Docker images. Don't actually run Complement.
        Conflicts with -f/--fast.

  -e, --editable
        Use an editable build of Synapse, rebuilding the image if necessary.
        This is suitable for use in development where a fast turn-around time
        is important.
        Not suitable for use in CI in case the editable environment is impure.

  --rebuild-editable
        Force a rebuild of the editable build of Synapse.
        This is occasionally useful if the built-in rebuild detection with
        --editable fails, e.g. when changing configure_workers_and_start.py.

Environment variables:
  COMPLEMENT_ENABLE_DIRTY_RUNS=0
        Disable reuse of containers between runs (recommended when debugging).

  COMPLEMENT_CLEANUP_STALE_RESOURCES=0
        Disable the startup sweep when sharing a container daemon with other
        Complement runners. Current-run cleanup remains enabled.

Only one complement.sh run may execute at a time. If the lock message appears,
inspect the holder on the host with:
  lslocks -o PID,COMMAND,PATH | grep synapse-complement
or:
  fuser -v "${TMPDIR:-/tmp}/synapse-complement.lock"
The lock is descriptor-based; deleting the lock file does not release it.

For help on arguments to 'go test', run 'go help testflag'.
EOF
}

# We use a function to wrap the script logic so that we can use `return` to exit early
# if needed. This is particularly useful so that this script can be sourced by other
# scripts without exiting the calling subshell (composable). This allows us to share
# variables like `SYNAPSE_SUPPORTED_COMPLEMENT_TEST_PACKAGES` with other scripts.
#
# Returns an exit code of 0 on success, or 1 on failure.
main() {
  # parse our arguments
  skip_docker_build=""
  skip_complement_run=""
  use_in_repo_tests=""
  while [ $# -ge 1 ]; do
    arg=$1
    case "$arg" in
      "-h")
        usage
        return 1
        ;;
      "--in-repo")
        use_in_repo_tests=1
        ;;
      "-f"|"--fast")
        skip_docker_build=1
        ;;
      "--build-only")
        skip_complement_run=1
        ;;
      "-e"|"--editable")
        use_editable_synapse=1
        ;;
      "--rebuild-editable")
        rebuild_editable_synapse=1
        ;;
      *)
        # unknown arg: presumably an argument to gotest. break the loop.
        break
    esac
    shift
  done

  # enable buildkit for the docker builds
  export DOCKER_BUILDKIT=1

  # Determine whether to use the docker or podman container runtime.
  if [ -n "$PODMAN" ]; then
    export CONTAINER_RUNTIME=podman
    export DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock
    export BUILDAH_FORMAT=docker
    export COMPLEMENT_HOSTNAME_RUNNING_COMPLEMENT=host.containers.internal
  else
    export CONTAINER_RUNTIME=docker
  fi

  # Complement deployments use a shared container daemon. Serialize this
  # script so one invocation cannot clean up resources belonging to another
  # invocation between deployment and container attachment. `flock` releases
  # the lock automatically if the shell is killed.
  acquire_complement_run_lock

  # Change to the repository root. Resolve it once, here, to an absolute
  # path and reuse that below -- $0 is never re-anchored after this cd, so
  # re-deriving "$(dirname "$0")/.." again later (once CWD has already
  # moved here) resolves relative to the new CWD instead of the original
  # invocation directory, producing a doubled/invalid path (this is what
  # broke `realpath: synapse/scripts-dev/..: No such file or directory` in
  # CI, where complement.sh is invoked as `synapse/scripts-dev/complement.sh`
  # from a parent directory).
  cd "$(dirname "$0")/.."
  repo_root="$(pwd)"

  # Check for a user-specified Complement checkout
  if [[ -z "$COMPLEMENT_DIR" ]]; then
    COMPLEMENT_REF=${COMPLEMENT_REF:-main}
    COMPLEMENT_REPO=${COMPLEMENT_REPO:-matrix-org/complement}
    echo "COMPLEMENT_DIR not set. Fetching ${COMPLEMENT_REPO} at ${COMPLEMENT_REF}..." >&2

    # Download the Complement checkout at the specified ref.
    wget -q -O "${COMPLEMENT_REF}.tar.gz" "https://github.com/${COMPLEMENT_REPO}/archive/${COMPLEMENT_REF}.tar.gz"

    # Delete the existing complement checkout. Otherwise we'll end up with stale
    # test files after they're deleted server-side, and `tar` will not delete
    # old files.
    complement_repo_name="${COMPLEMENT_REPO##*/}"
    complement_repo_name="${complement_repo_name%.git}"
    COMPLEMENT_DIR="${complement_repo_name}-${COMPLEMENT_REF}"
    rm -rf "$COMPLEMENT_DIR"

    # Extract the checkout.
    tar -xzf "${COMPLEMENT_REF}.tar.gz"

    echo "Checkout available at '$COMPLEMENT_DIR'" >&2
  fi

  if [[ -z "$use_in_repo_tests" ]] && [[ "$(realpath "$COMPLEMENT_DIR")" == "$(realpath ./complement)" ]]; then
    echo "COMPLEMENT_DIR points at this repository's in-repo Complement tests." >&2
    echo "Use --in-repo with COMPLEMENT_DIR=./complement, or unset COMPLEMENT_DIR to test against upstream Complement." >&2
    return 1
  fi

  # Compute this before deciding whether to rebuild images. The version-check
  # test also runs with --fast and --editable, where the standard-image build
  # branch below is skipped.
  pkg_version="$(sed -n 's/^version = "\(.*\)"$/\1/p' pyproject.toml | head -n1)"
  git_branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
  if [ -n "$git_branch" ]; then git_branch="b=$git_branch"; fi
  git_tag="$(git describe --exact-match 2>/dev/null || true)"
  if [ -n "$git_tag" ]; then git_tag="t=$git_tag"; fi
  git_commit="$(git rev-parse --short HEAD 2>/dev/null || true)"
  git_dirty=""
  if git describe --dirty=-this_is_a_dirty_checkout 2>/dev/null | grep -q -- '-this_is_a_dirty_checkout$'; then
    git_dirty="dirty"
  fi
  git_version="$(IFS=,; echo "${git_branch:+$git_branch,}${git_tag:+$git_tag,}${git_commit:+$git_commit,}${git_dirty:+$git_dirty,}" | sed 's/,$//')"
  if [ -n "$git_version" ]; then
    synapse_version_string="$pkg_version ($git_version)"
  else
    synapse_version_string="$pkg_version"
  fi
  export SYNAPSE_VERSION_STRING="$synapse_version_string"

  if [ -n "$use_editable_synapse" ]; then
    if [[ -e synapse/synapse_rust.abi3.so ]]; then
      # In an editable install, back up the host's compiled Rust module to prevent
      # inconvenience; the container will overwrite the module with its own copy.
      mv -n synapse/synapse_rust.abi3.so synapse/synapse_rust.abi3.so~host
      # And restore it on exit:
      synapse_pkg=$(realpath synapse)
      trap 'mv -f "$synapse_pkg/synapse_rust.abi3.so~host" "$synapse_pkg/synapse_rust.abi3.so"' EXIT
    fi

    editable_mount="$(realpath .):/editable-src:z"
    if [ -n "$rebuild_editable_synapse" ]; then
      unset skip_docker_build
    elif $CONTAINER_RUNTIME inspect "$COMPLEMENT_SYNAPSE_EDITABLE_IMAGE_PATH" &>/dev/null; then
      # complement-synapse-editable already exists: see if we can still use it:
      # - The Rust module must still be importable; it will fail to import if the Rust source has changed.
      # - The uv lock file must be the same (otherwise we assume dependencies have changed)

      # First set up the module in the right place for an editable installation.
      $CONTAINER_RUNTIME run --rm -v "$editable_mount" --entrypoint 'cp' "$COMPLEMENT_SYNAPSE_EDITABLE_IMAGE_PATH" -- /synapse_rust.abi3.so.bak /editable-src/synapse/synapse_rust.abi3.so

      if ($CONTAINER_RUNTIME run --rm -v "$editable_mount" --entrypoint 'python' "$COMPLEMENT_SYNAPSE_EDITABLE_IMAGE_PATH" -c 'import synapse.synapse_rust' \
        && $CONTAINER_RUNTIME run --rm -v "$editable_mount" --entrypoint 'diff' "$COMPLEMENT_SYNAPSE_EDITABLE_IMAGE_PATH" --brief /editable-src/uv.lock /uv.lock.bak); then
        skip_docker_build=1
      else
        echo "Editable Synapse image is stale. Will rebuild." >&2
        unset skip_docker_build
      fi
    fi
  fi

  if [ -z "$skip_docker_build" ]; then
    # Shell words in this environment variable are Docker build options.
    # Convert them once to an array so each option remains a distinct argv item.
    read -r -a docker_build_args <<<"${DOCKER_BUILD_ARGS:-}"
    if [ -n "$use_editable_synapse" ]; then

      # Build a special image designed for use in development with editable
      # installs.
      $CONTAINER_RUNTIME build "${docker_build_args[@]}" \
        -t "$SYNAPSE_EDITABLE_IMAGE_PATH" \
        -f "docker/editable.Dockerfile" .

      $CONTAINER_RUNTIME build "${docker_build_args[@]}" \
        -t "$SYNAPSE_WORKERS_EDITABLE_IMAGE_PATH" \
        --build-arg FROM="$SYNAPSE_EDITABLE_IMAGE_PATH" \
        -f "docker/Dockerfile-workers" .

      $CONTAINER_RUNTIME build "${docker_build_args[@]}" \
        -t "$COMPLEMENT_SYNAPSE_EDITABLE_IMAGE_PATH" \
        --build-arg FROM="$SYNAPSE_WORKERS_EDITABLE_IMAGE_PATH" \
        -f "docker/complement/Dockerfile" "docker/complement"

      # Prepare the Rust module
      $CONTAINER_RUNTIME run --rm -v "$editable_mount" --entrypoint 'cp' "$COMPLEMENT_SYNAPSE_EDITABLE_IMAGE_PATH" -- /synapse_rust.abi3.so.bak /editable-src/synapse/synapse_rust.abi3.so

    else
      # We remove the `egg-info` as it can contain outdated information which won't line
      # up with our current reality.
      rm -rf matrix_synapse.egg-info/
      # Build the base Synapse image from the local checkout
      echo_if_github "::group::Build Docker image: matrixdotorg/synapse"
      $CONTAINER_RUNTIME build "${docker_build_args[@]}" \
        -t "$SYNAPSE_IMAGE_PATH" \
        --build-arg SYNAPSE_VERSION_STRING="$synapse_version_string" \
        --build-arg TEST_ONLY_SKIP_DEP_HASH_VERIFICATION \
        --build-arg TEST_ONLY_IGNORE_LOCKFILE \
        -f "docker/Dockerfile" .
      echo_if_github "::endgroup::"

      # Build the workers docker image (from the base Synapse image we just built).
      echo_if_github "::group::Build Docker image: matrixdotorg/synapse-workers"
      $CONTAINER_RUNTIME build "${docker_build_args[@]}" \
        -t "$SYNAPSE_WORKERS_IMAGE_PATH" \
        --build-arg FROM="$SYNAPSE_IMAGE_PATH" \
        -f "docker/Dockerfile-workers" .
      echo_if_github "::endgroup::"

      # Build the unified Complement image (from the worker Synapse image we just built).
      echo_if_github "::group::Build Docker image: complement/Dockerfile"
      $CONTAINER_RUNTIME build "${docker_build_args[@]}" \
        -t "$COMPLEMENT_SYNAPSE_IMAGE_PATH" \
        --build-arg FROM="$SYNAPSE_WORKERS_IMAGE_PATH" \
        -f "docker/complement/Dockerfile" "docker/complement"
      echo_if_github "::endgroup::"

    fi
  
    echo "Docker images built." >&2
  else
    echo "Skipping Docker image build as requested." >&2
  fi

  if [ -n "$skip_complement_run" ]; then
    echo "Docker images built; skipping Complement tests as requested." >&2
    return 0
  fi

  # Default set of Complement tests to run from the Complement repo
  #
  # We pick and choose the specific MSC's that Synapse supports.
  default_complement_test_packages=(
    ./tests/csapi
    ./tests
    ./tests/msc3874
    ./tests/msc3890
    ./tests/msc3391
    ./tests/msc3757
    ./tests/msc3930
    ./tests/msc3902
    ./tests/msc3967
    ./tests/msc4140
    ./tests/msc4155
    ./tests/msc4306
    ./tests/msc4222
    ./tests/msc4429
    ./tests/msc4499
  )

  available_complement_test_packages=()
  for test_package in "${default_complement_test_packages[@]}"; do
    if [[ -d "$COMPLEMENT_DIR/$test_package" ]]; then
      available_complement_test_packages+=("$test_package")
    else
      echo "Skipping unavailable Complement test package: $test_package" >&2
    fi
  done

  # Export the list of test packages as a space-separated environment variable, so other
  # scripts can use it.
  export SYNAPSE_SUPPORTED_COMPLEMENT_TEST_PACKAGES="${available_complement_test_packages[*]}"

  # Default set of Complement tests to run when using the in-repo test suite. Most
  # likely, this should be all tests.
  #
  # Relative to the `./complement` repo in this project
  default_in_repo_complement_test_packages=(
    ./tests/...
  )

  export COMPLEMENT_BASE_IMAGE="$COMPLEMENT_SYNAPSE_IMAGE_PATH"
  if [ -n "$use_editable_synapse" ]; then
    export COMPLEMENT_BASE_IMAGE="$COMPLEMENT_SYNAPSE_EDITABLE_IMAGE_PATH"
    export COMPLEMENT_HOST_MOUNTS="$editable_mount"
  fi

  # Enable dirty runs, so tests will reuse the same container where possible.
  # This significantly speeds up tests, but increases the possibility of test pollution.
  export COMPLEMENT_ENABLE_DIRTY_RUNS="${COMPLEMENT_ENABLE_DIRTY_RUNS:-1}"

  # Reclaim resources left by older failed runs. The sweep can be disabled
  # when this daemon is shared with Complement runners outside this script;
  # current-run, token-scoped cleanup remains enabled in either case.
  if [ "${COMPLEMENT_CLEANUP_STALE_RESOURCES:-1}" != "0" ]; then
    cleanup_stale_complement_containers
    # The grace period prevents a freshly-created, not-yet-attached network
    # from being mistaken for stale state.
    cleanup_stale_complement_networks
  fi

  # All environment variables starting with PASS_ will be shared.
  # (The prefix is stripped off before reaching the container.)
  export COMPLEMENT_SHARE_ENV_PREFIX=PASS_

  # Identify Synapse to Complement's runtime skip registry by default. Set
  # COMPLEMENT_NO_BLACKLIST=1 to run a diagnostic pass without that registry.
  test_tags=""
  if [ -z "${COMPLEMENT_NO_BLACKLIST:-}" ]; then
    test_tags="synapse_blacklist"
  fi

  # It takes longer than 10m to run the whole suite.
  test_timeout="60m"

  # Number of packages to run in parallel. Default 2 matches congruent's
  # COMPLEMENT_PARALLEL=2 — go test defaults to GOMAXPROCS which can spin up
  # enough containers simultaneously to cause 502s on registration.
  test_parallel="${COMPLEMENT_PARALLEL:-2}"

  if [[ -n "$WORKERS" ]]; then
    # Use workers.
    export PASS_SYNAPSE_COMPLEMENT_USE_WORKERS=true

    # Pass through the workers defined. If none, it will be an empty string
    export PASS_SYNAPSE_WORKER_TYPES="$WORKER_TYPES"

    # Workers can only use Postgres as a database.
    export PASS_SYNAPSE_COMPLEMENT_DATABASE=postgres

    # And provide some more configuration to complement.

    # Fail unhealthy worker deployments promptly rather than spending up to
    # three minutes retrying each one. Callers can raise this when diagnosing
    # a genuinely slow host.
    export COMPLEMENT_SPAWN_HS_TIMEOUT_SECS=${COMPLEMENT_SPAWN_HS_TIMEOUT_SECS:-30}
  else
    export PASS_SYNAPSE_COMPLEMENT_USE_WORKERS=
    # Prefer the SYNAPSE_TEST_POSTGRES name used by tests/utils.py's
    # in-process trial runner, falling back to the bare POSTGRES on-switch.
    POSTGRES="${SYNAPSE_TEST_POSTGRES:-${SYNAPSE_POSTGRES:-${POSTGRES:-}}}"
    if [[ -n "$POSTGRES" ]]; then
      export PASS_SYNAPSE_COMPLEMENT_DATABASE=postgres
    else
      export PASS_SYNAPSE_COMPLEMENT_DATABASE=sqlite
    fi
  fi

  if [[ -n "$ASYNCIO_REACTOR" ]]; then
    # Enable the Twisted asyncio reactor
    export PASS_SYNAPSE_COMPLEMENT_USE_ASYNCIO_REACTOR=true
  fi

  if [[ -n "$UNIX_SOCKETS" ]]; then
    # Enable full on Unix socket mode for Synapse, Redis and Postgresql
    export PASS_SYNAPSE_USE_UNIX_SOCKET=1
  fi

  if [[ -n "$SYNAPSE_TEST_LOG_LEVEL" ]]; then
    # Set the log level to what is desired
    export PASS_SYNAPSE_LOG_LEVEL="$SYNAPSE_TEST_LOG_LEVEL"

    # Allow logging sensitive things (currently SQL queries & parameters).
    # (This won't have any effect if we're not logging at DEBUG level overall.)
    # Since this is just a test suite, this is fine and won't reveal anyone's
    # personal information
    export PASS_SYNAPSE_LOG_SENSITIVE=1
  fi

  # Log a few more useful things for a developer attempting to debug something
  # particularly tricky.
  export PASS_SYNAPSE_LOG_TESTING=1

  # Only TEST-scoped controls may enter Complement containers. In particular,
  # never inherit a developer's production embedded-HAMT path: a path without
  # its engine is an invalid Synapse config, and a path with its engine could
  # mutate a real local store.
  SYNAPSE_MTXDB="${SYNAPSE_TEST_MTXDB:-}"
  SYNAPSE_EMBEDDED_HAMT_ENGINE="${SYNAPSE_TEST_EMBEDDED_HAMT_ENGINE:-}"
  SYNAPSE_EMBEDDED_HAMT_PATH="${SYNAPSE_TEST_EMBEDDED_HAMT_PATH:-}"

  if [[ -n "${SYNAPSE_MTXDB:-}" && -z "$SYNAPSE_EMBEDDED_HAMT_ENGINE" ]]; then
    SYNAPSE_EMBEDDED_HAMT_ENGINE="mtxdb"
  fi

  if [[ -n "$SYNAPSE_EMBEDDED_HAMT_ENGINE" ]]; then
    export PASS_SYNAPSE_EMBEDDED_HAMT_ENGINE="$SYNAPSE_EMBEDDED_HAMT_ENGINE"
    # SYNAPSE_EMBEDDED_HAMT_PATH is read inside the Complement container, not
    # on the host -- a caller who just wants to turn mtxdb on shouldn't have
    # to know or care about that. Default it to a path that's always
    # writable there (the image's WORKDIR) rather than making them supply an
    # in-container path themselves.
    SYNAPSE_EMBEDDED_HAMT_PATH="${SYNAPSE_EMBEDDED_HAMT_PATH:-/data/embedded_hamt}"
    export PASS_SYNAPSE_EMBEDDED_HAMT_PATH="$SYNAPSE_EMBEDDED_HAMT_PATH"
  fi

  # Test-only durability escape hatch, matching the engine/path controls
  # above: Complement containers are destroyed after every test, so the
  # durable fsync path buys nothing, but the engine deliberately defaults
  # durability ON for production. Only the TEST_-scoped variable is
  # honoured, so a developer's production SYNAPSE_MTXDB_NO_SYNC cannot leak
  # into containers. Same value semantics as tests/utils.py: falsey
  # (0/false/no/off/empty) leaves sync on, a truthy value disables it.
  case "${SYNAPSE_TEST_MTXDB_NO_SYNC:-}" in
    "" | 0 | false | False | no | No | off | Off) ;;
    *) export PASS_SYNAPSE_MTXDB_NO_SYNC=1 ;;
  esac

  # synapse/config/workers.py requires the write-ahead journal whenever the
  # embedded engine is on *and* the deployment is multi-process (worker_app
  # set or a non-empty instance_map -- i.e. WORKERS=1 runs, not every
  # Complement run): with no SQL fallback for the data it owns, a committed
  # write can be reported absent by a read-only worker until a checkpoint
  # rewrite refreshes that worker's index, and that rewrite can be deferred.
  # The WAL's read-committed overlay is the only read path that closes that
  # window independently of the checkpoint rewrite. This is a visibility
  # requirement, not a durability one -- neither mode fsyncs a write before
  # the coalescer's next sync -- so a single-process run never actually needs
  # it. Default WAL on whenever the engine is on anyway (single-process
  # included), purely so every embedded-engine Complement run exercises the
  # same journal path production uses by default. SYNAPSE_TEST_MTXDB_WAL can
  # still force it off; under WORKERS=1 that now makes the container refuse
  # to start (exercising the workers.py validation), but a non-worker run
  # with it forced off is a legitimately supported single-process WAL-off
  # configuration, not just a way to trigger the rejection. Same
  # truthy/falsey semantics as above.
  _default_mtxdb_wal=""
  if [[ -n "$SYNAPSE_EMBEDDED_HAMT_ENGINE" ]]; then
    _default_mtxdb_wal=1
  fi
  case "${SYNAPSE_TEST_MTXDB_WAL:-$_default_mtxdb_wal}" in
    "" | 0 | false | False | no | No | off | Off) ;;
    *) export PASS_SYNAPSE_MTXDB_WAL=1 ;;
  esac

  # THROWAWAY DIAGNOSTIC: force a synchronous EVENT_DAG fsync after every
  # event_json write (see embedded_event_json._FORCE_SYNC_EVENT_JSON). This is
  # the "forced sync" arm of the publication-timing experiment matrix; it
  # answers whether the cross-process miss is a writer publication race or a
  # persistent reader-refresh miss. Remove with the flag it forwards.
  case "${SYNAPSE_TEST_MTXDB_FORCE_SYNC_EVENT_JSON:-}" in
    "" | 0 | false | False | no | No | off | Off) ;;
    *) export PASS_SYNAPSE_MTXDB_FORCE_SYNC_EVENT_JSON=1 ;;
  esac

  # Forward the diagnostic stats switch into the containers: with sync
  # disabled the report is empty, so this is only useful on a sync-on lane,
  # but it must reach the container or there is no way to size a sync's
  # fsync/checkpoint split from a Complement run.
  if [[ -n "${SYNAPSE_TEST_MTXDB_STATS:-}" ]]; then
    export PASS_SYNAPSE_MTXDB_STATS=1
  fi

  # Record the exact checkout that produced the image alongside the effective
  # test configuration. `--dirty` makes a locally modified build explicit,
  # which is essential when comparing Complement timings or failures later.
  local synapse_revision
  synapse_revision="$(git -C "$repo_root" describe --tags --always --dirty 2>/dev/null || echo '<unknown>')"
  echo "Synapse revision: ${synapse_revision}" >&2
  echo "Database: ${PASS_SYNAPSE_COMPLEMENT_DATABASE} (workers: ${PASS_SYNAPSE_COMPLEMENT_USE_WORKERS:-false}) | Embedded HAMT engine: ${PASS_SYNAPSE_EMBEDDED_HAMT_ENGINE:-<none>}${PASS_SYNAPSE_EMBEDDED_HAMT_ENGINE:+ at ${PASS_SYNAPSE_EMBEDDED_HAMT_PATH:-<not set>}}${PASS_SYNAPSE_MTXDB_NO_SYNC:+ (no_sync)}${PASS_SYNAPSE_MTXDB_WAL:+ (wal)}${PASS_SYNAPSE_MTXDB_STATS:+ (stats)}${PASS_SYNAPSE_MTXDB_FORCE_SYNC_EVENT_JSON:+ (force-sync-event-json)}" >&2

  # Complement's Destroy() force-removes every homeserver container
  # unconditionally, pass or fail -- there is no "keep failed containers"
  # option, so this hook (which runs while the container is still up,
  # per executePostScript in complement's deployer.go) is the only place
  # that can save anything from a failing run for later inspection, and
  # it also dumps Postgres stats test-to-test along the way. Don't clobber
  # a caller who has already set their own COMPLEMENT_POST_TEST_SCRIPT.
  export COMPLEMENT_POST_TEST_SCRIPT="${COMPLEMENT_POST_TEST_SCRIPT:-${repo_root}/scripts-dev/_complement_post_test.sh}"

  if [[ -n "${SYNAPSE_PG_TIMINGS:-}" ]]; then
    export PASS_SYNAPSE_PG_TIMINGS=1
    # Pass setup_timings_path="-" into the container so each Synapse process
    # prints its Databases.__init__ breakdown to stderr immediately after
    # setup() completes -- before any SIGTERM, so timing output is never lost
    # to an instant SIGKILL.  "-" is the sentinel meaning stderr-only (no file
    # write inside the container, which would be destroyed before retrieval).
    export PASS_SYNAPSE_DB_SETUP_TIMINGS_PATH=-
    # NOTE: we do NOT force COMPLEMENT_ALWAYS_PRINT_SERVER_LOGS /
    # COMPLEMENT_STOP_TIMEOUT_SECS here. Doing so forces every container in
    # the run through a graceful SIGTERM stop (Postgres runs a full shutdown
    # CHECKPOINT, flushing every dirty page) instead of an instant SIGKILL.
    # That's fine for a single targeted `-run TestFoo` invocation, but it is
    # actively harmful across a full/parallel suite run: hundreds of
    # containers all doing a graceful multi-second shutdown at once causes
    # real disk/CPU contention that measurably slows down and destabilizes
    # unrelated, timing-sensitive tests -- confirmed against a full
    # `make complement` run producing new failures and heavy sustained
    # containerd disk writes, while still not reliably producing any timing
    # output (the docker-log-watcher's start/scan race gets worse, not
    # better, at that container-count scale). If you want a timing report,
    # set these two vars yourself for a narrow `-run` invocation rather than
    # enabling them here unconditionally for every run.
  fi

  # Complement's blueprint cache key is only (package namespace, blueprint
  # name). A blueprint is a committed container image, so it also captures the
  # base-image contents and every PASS_* variable passed into its homeservers.
  # Without varying the namespace, a later SQLite/no-mtxdb run can reuse a
  # blueprint built by an earlier Postgres/mtxdb run and silently boot with
  # that old environment. Include the immutable base-image ID and effective
  # forwarded configuration in the namespace to make such reuse impossible.
  local _base_image_id _cache_config_hash _namespace_prefix
  _base_image_id="$($CONTAINER_RUNTIME image inspect --format '{{.Id}}' "$COMPLEMENT_BASE_IMAGE")"
  _cache_config_hash="$(
    {
      printf '%s\n' "$_base_image_id"
      env | LC_ALL=C sort | sed -n '/^PASS_/p'
    } | sha256sum | cut -c1-16
  )"
  _namespace_prefix="${COMPLEMENT_PACKAGE_NAMESPACE_PREFIX:-synapse}"
  export COMPLEMENT_PACKAGE_NAMESPACE_PREFIX="${_namespace_prefix}_cfg_${_cache_config_hash}"
  echo "Complement blueprint cache namespace: ${COMPLEMENT_PACKAGE_NAMESPACE_PREFIX}" >&2

  # ── Run-filter and extra-tags from remaining args ───────────────────────────
  # RUN_TESTS=. means "run everything" (the default).
  # -run PATTERN and -run=PATTERN are extracted for package narrowing + anchoring.
  # -tags TAG and -tags=TAG are merged into test_tags (never forwarded as a
  # second -tags flag which go test would silently clobber the first with).
  # Everything else goes into extra_args and is forwarded verbatim.
  RUN_TESTS="${COMPLEMENT_RUN:-.}"
  local -a extra_args=()
  local _i=1
  while [ $_i -le $# ]; do
    local _arg="${!_i}"
    if [[ "$_arg" == "-run" || "$_arg" == "--run" ]]; then
      local _next=$((_i+1))
      RUN_TESTS="${!_next}"
      _i=$((_i+2))
    elif [[ "$_arg" =~ ^--?run=(.+) ]]; then
      RUN_TESTS="${BASH_REMATCH[1]}"
      _i=$((_i+1))
    elif [[ "$_arg" == "-tags" ]]; then
      local _next=$((_i+1))
      test_tags="${test_tags:+${test_tags},}${!_next}"
      _i=$((_i+2))
    elif [[ "$_arg" =~ ^-tags=(.+) ]]; then
      test_tags="${test_tags:+${test_tags},}${BASH_REMATCH[1]}"
      _i=$((_i+1))
    else
      extra_args+=("$_arg")
      _i=$((_i+1))
    fi
  done

  # ── Staged result / log files (timestamped, never overwrite) ────────────────
  # repo_root was already resolved (once, correctly) right after the cd near
  # the top of this function -- don't re-derive it from $0 here.
  results_dir="${RESULTS_DIR:-tests/complement}"
  main_results_file="${repo_root}/${results_dir}/results.jsonl"
  main_log_file="${repo_root}/${results_dir}/logs.jsonl"
  mkdir -p "$(dirname "$main_results_file")"
  touch "$main_results_file" "$main_log_file"

  if [ "$RUN_TESTS" = "." ]; then
    run_suffix="all"
  else
    run_suffix="$(echo "$RUN_TESTS" | sed 's/[^a-zA-Z0-9]/_/g' | cut -c1-32)"
    run_suffix="${run_suffix:-all}"
  fi
  run_stamp="$(date +%s%N)"
  staging_dir="${repo_root}/.tmp/complement"
  mkdir -p "$staging_dir"
  staged_log_file="${staging_dir}/logs.${run_suffix}.${run_stamp}.jsonl"
  staged_results_file="${staging_dir}/test_results.${run_suffix}.${run_stamp}.jsonl"
  : >"$staged_log_file"
  : >"$staged_results_file"

  echo "" >&2
  echo "running go test with:" >&2
  echo "\$COMPLEMENT_DIR: ${COMPLEMENT_DIR:-<auto>}" >&2
  echo "\$COMPLEMENT_BASE_IMAGE: $COMPLEMENT_BASE_IMAGE" >&2
  echo "\$staged_results_file (staging): $staged_results_file" >&2
  echo "\$main_results_file: $main_results_file" >&2
  echo "\$staged_log_file: $staged_log_file" >&2
  echo "\$RUN_TESTS: $RUN_TESTS" >&2
  echo "" >&2

  # ── anchor_one: per-segment ^ anchoring so -run TestFoo doesn't match TestFooBar ──
  anchor_one() {
    local pattern="$1"
    local -a anchored=()
    local -a segments
    IFS='/' read -r -a segments <<<"$pattern"
    local last=$(( ${#segments[@]} - 1 ))
    local idx=0
    for segment in "${segments[@]}"; do
      if [[ "$segment" =~ ^\^ || "$segment" =~ .*[][()?.+*|$] ]]; then
        anchored+=("$segment")
      elif [ "$idx" -eq "$last" ]; then
        anchored+=("^${segment}")
      else
        anchored+=("^${segment}\$")
      fi
      idx=$((idx+1))
    done
    (IFS='/'; echo "${anchored[*]}")
  }

  # Split top-level | into separate go test invocations (go test's -run re-splits
  # on every /, silently dropping one side of alternations with differing depth).
  #
  # That depth-mismatch bug can only fire when the alternatives being OR'd
  # together have differing "/" depth (e.g. `TestFoo|TestBar/SomeSubtest`).
  # When every alternative is a single flat segment (no "/" at all -- no
  # subtest is being targeted by any of them), depth is uniformly 1 and the
  # bug cannot trigger, so there is nothing to protect against by splitting.
  # In that case, combine everything into one `^(a|b|c)$`-style alternation
  # and run go test once instead of once per name -- this is the common case
  # for a targeted top-level test-name batch and the split's per-invocation
  # process/container-churn overhead is otherwise paid for nothing.
  ALT_PATTERNS=()
  if [ "$RUN_TESTS" = "." ]; then
    ALT_PATTERNS=(".")
  else
    local -a raw_alts
    IFS='|' read -r -a raw_alts <<<"$RUN_TESTS"
    local _all_flat=1
    for alt in "${raw_alts[@]}"; do
      if [[ "$alt" == */* ]]; then
        _all_flat=0
        break
      fi
    done
    if [ "${#raw_alts[@]}" -gt 1 ] && [ "$_all_flat" -eq 1 ]; then
      local _combined
      _combined="$(IFS='|'; echo "${raw_alts[*]}")"
      ALT_PATTERNS=("^(${_combined})\$")
      echo "All alternatives are flat top-level names; combined into one go test invocation:" >&2
      echo "  ${ALT_PATTERNS[0]}" >&2
    else
      for alt in "${raw_alts[@]}"; do
        ALT_PATTERNS+=("$(anchor_one "$alt")")
      done
      if [ "${#ALT_PATTERNS[@]}" -gt 1 ]; then
        echo "Anchored run regexes (one go test invocation each):" >&2
        for alt in "${ALT_PATTERNS[@]}"; do echo "  $alt" >&2; done
      else
        echo "Anchored run regex: ${ALT_PATTERNS[0]}" >&2
      fi
    fi
  fi

  # ── Container token + cleanup trap ──────────────────────────────────────────
  export COMPLEMENT_WRAPPER_TOKEN="${COMPLEMENT_WRAPPER_TOKEN:-"complement-$$-$(date +%s%N)"}"
  export PASS_COMPLEMENT_WRAPPER_TOKEN="$COMPLEMENT_WRAPPER_TOKEN"
  export COMPLEMENT_SHARE_ENV_PREFIX=PASS_
  # Complement retries a homeserver deploy up to 3x. Keep the default
  # per-attempt startup timeout short so an unhealthy deployment fails
  # promptly (about 90s worst-case), while allowing callers to override it.
  export COMPLEMENT_SPAWN_HS_TIMEOUT_SECS=${COMPLEMENT_SPAWN_HS_TIMEOUT_SECS:-30}
  # Keep genuinely stalled requests from consuming a minute and a half of a
  # test run. Callers can still raise this for intentionally slow scenarios.
  export COMPLEMENT_CLIENT_TIMEOUT_SECS=${COMPLEMENT_CLIENT_TIMEOUT_SECS:-30}
  # Placeholder until merge_and_report exists below; replaced with the real
  # combined EXIT trap once it's defined, so merging is never optional --
  # it happens on literal end-of-script, an explicit `exit`, a `set -e`
  # abort, or any trapped signal, from one single codepath instead of being
  # duplicated across call sites that can drift out of sync.
  trap cleanup_complement_containers EXIT

  return 0
}

# Keep only one local complement.sh deployment active at a time. This is
# deliberately process-scoped rather than runtime-scoped: Docker/Podman do
# not provide a transaction covering resource discovery and deployment.
acquire_complement_run_lock() {
  local lock_file="${TMPDIR:-/tmp}/synapse-complement.lock"
  if ! command -v flock &>/dev/null; then
    echo "ERROR: flock is required to safely clean up stale Complement resources" >&2
    return 1
  fi

  exec {COMPLEMENT_RUN_LOCK_FD}>"$lock_file"
  if ! flock -n "$COMPLEMENT_RUN_LOCK_FD"; then
    echo "Another complement.sh run is active; refusing to clean up shared resources" >&2
    return 1
  fi
}

# Invoked by the EXIT trap installed in main.
# shellcheck disable=SC2329
cleanup_complement_containers() {
  local runtime="${CONTAINER_RUNTIME:-docker}"
  local container_label="COMPLEMENT_WRAPPER_TOKEN=$COMPLEMENT_WRAPPER_TOKEN"
  local containers container network
  local -a ours=() networks=()
  if command -v "$runtime" &>/dev/null; then
    mapfile -t containers < <("$runtime" ps -aq --filter "name=complement" 2>/dev/null || true)
    for container in "${containers[@]:-}"; do
      if "$runtime" inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$container" 2>/dev/null \
          | grep -Fxq "$container_label"; then
        ours+=("$container")
        while IFS= read -r network; do
          [ -n "$network" ] || continue
          if [[ ! " ${networks[*]} " == *" $network "* ]]; then
            networks+=("$network")
          fi
        done < <(
          # shellcheck disable=SC2016 # Docker/Podman expands this Go template.
          "$runtime" inspect --format '{{range $name, $config := .NetworkSettings.Networks}}{{println $name}}{{end}}' \
            "$container" 2>/dev/null || true
        )
      fi
    done
    if [ "${#ours[@]}" -gt 0 ]; then
      echo "Cleaning up Complement containers spawned by this run..." >&2
      printf '%s\n' "${ours[@]}" | xargs -r "$runtime" rm -f
    fi

    # Only remove networks which were attached to containers carrying this
    # run's token. An unscoped name-based sweep can delete a network another
    # Complement invocation has just created but not attached yet.
    for network in "${networks[@]:-}"; do
      echo "Cleaning up Complement network $network..." >&2
      "$runtime" network rm "$network" >/dev/null 2>&1 || true
    done
  fi
}

# Stop the PostgreSQL timing watcher and every process it spawned. The watcher
# contains `docker events`, a tail pipeline, and one `docker logs -f` process
# per container; process-group membership is not reliable when this script is
# launched from an interactive shell, so walk the actual child tree instead.
cleanup_pg_log_watcher() {
  local pid="${_pg_log_watcher_pid:-}"
  local timing_dir="${_pg_timing_dir:-}"
  local child

  if [ -n "$pid" ]; then
    for child in $(pgrep -P "$pid" 2>/dev/null || true); do
      _kill_process_tree "$child"
    done
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi

  # The watcher contains a pipeline.  Its tail and log followers can become
  # reparented when the pipeline is torn down, so they are no longer visible
  # below _pg_log_watcher_pid.  The timing directory is unique to this run;
  # use it to reap those otherwise-detached processes without matching another
  # Complement invocation.
  if [ -n "$timing_dir" ]; then
    while IFS= read -r child; do
      [ -n "$child" ] || continue
      _kill_process_tree "$child"
    done < <(pgrep -f -- "$timing_dir" 2>/dev/null || true)
  fi

  _pg_log_watcher_pid=""
  _pg_timing_dir=""
}

_kill_process_tree() {
  local pid="$1"
  local child
  for child in $(pgrep -P "$pid" 2>/dev/null || true); do
    _kill_process_tree "$child"
  done
  kill "$pid" 2>/dev/null || true
}

# A crashed Complement process can leave running containers or pods behind.
# They have no reliable token we can recover after the shell dies, so this
# startup sweep is protected by the run lock and removes only Complement-named
# resources carrying Complement's ownership labels from this runtime.
cleanup_stale_complement_containers() {
  local runtime="${CONTAINER_RUNTIME:-docker}"
  local container pod
  local -a containers=() pods=()

  if ! command -v "$runtime" &>/dev/null; then
    return 0
  fi

  mapfile -t containers < <("$runtime" ps -aq --filter "label=complement_pkg" 2>/dev/null || true)
  if [ "${#containers[@]}" -gt 0 ]; then
    echo "Cleaning up stale Complement containers..." >&2
    printf '%s\n' "${containers[@]}" | xargs -r "$runtime" rm -f
  fi

  if [ "$runtime" = "podman" ]; then
    mapfile -t pods < <("$runtime" pod ps -aq --filter "label=complement_pkg" 2>/dev/null || true)
    for pod in "${pods[@]:-}"; do
      [ -n "$pod" ] || continue
      echo "Cleaning up stale Complement pod $pod..." >&2
      "$runtime" pod rm -f "$pod" >/dev/null 2>&1 || true
    done
  fi
}

# Remove only old, empty Complement networks. This handles deployments which
# died after creating a network but before creating a token-labelled container.
# The age check is intentional: network creation and container attachment are
# separate daemon operations, so an unscoped zero-container check alone has a
# startup race with another Complement invocation.
cleanup_stale_complement_networks() {
  local runtime="${CONTAINER_RUNTIME:-docker}"
  local network created created_epoch now age attached
  local stale_after="${COMPLEMENT_STALE_NETWORK_AGE_SECS:-600}"
  local -a networks=()

  if ! command -v "$runtime" &>/dev/null; then
    return 0
  fi

  # Docker exposes network membership in `network inspect`. Podman does not,
  # so do not run this stale-resource sweep there until its membership query
  # is implemented using `podman ps --filter network=...` and verified.
  if [ "$runtime" != "docker" ]; then
    return 0
  fi

  now=$(date +%s)
  mapfile -t networks < <("$runtime" network ls -q --filter "label=complement_pkg" 2>/dev/null || true)
  for network in "${networks[@]:-}"; do
    [ -n "$network" ] || continue

    # Do not remove a network if the runtime cannot describe its age.
    created=$("$runtime" network inspect --format '{{.Created}}' "$network" 2>/dev/null || true)
    created_epoch=$(date -d "$created" +%s 2>/dev/null || true)
    [[ "$created_epoch" =~ ^[0-9]+$ ]] || continue
    age=$((now - created_epoch))
    [ "$age" -ge "$stale_after" ] || continue

    # Docker exposes Containers as a map. A failure deliberately keeps the
    # network rather than risking deletion.
    attached=$("$runtime" network inspect "$network" 2>/dev/null \
      | jq -r '.[0].Containers // {} | length' 2>/dev/null || echo 1)
    [[ "$attached" =~ ^[0-9]+$ ]] || continue
    [ "$attached" -eq 0 ] || continue

    echo "Cleaning up stale Complement network $network (${age}s old)..." >&2
    "$runtime" network rm "$network" >/dev/null 2>&1 || true
  done
}

# ── record_result: one summary line + append to staged results ───────────────
record_result() {
  local action="$1" test_name="$2" elapsed="$3"
  jq -nc --arg Action "$action" --arg Test "$test_name" \
    '{Action: $Action, Test: $Test}' >>"$staged_results_file"

  if [ "$action" != "skip" ]; then
    # Truncate only the printed name (the full name is still recorded
    # above) so a long subtest path doesn't wrap the summary line.
    local _display_name="$test_name"
    if [ "${#_display_name}" -gt 80 ]; then
      _display_name="${_display_name:0:79}…"
    fi
    # `printf %-80s` measures UTF-8 bytes, not terminal characters. A name
    # containing `§` (as in the MSC4499 tests) would therefore make the
    # duration appear one column early. Bash's `${#var}` is character-based
    # under the UTF-8 locale used by the test runner, so pad explicitly.
    local _name_padding=$((80 - ${#_display_name}))
    printf '%-6s  %s%*s  %8s\n' \
      "${action^^}" "$_display_name" "$_name_padding" "" "$elapsed" >&2
  fi
}

# ── run_one_pattern: one go test invocation per -run alternative ─────────────
run_one_pattern() {
  local pattern="$1"

  # Narrow packages to where the requested test lives.
  local -a packages
  if [ -n "$use_in_repo_tests" ]; then
    packages=("${default_in_repo_complement_test_packages[@]}")
  else
    packages=("${available_complement_test_packages[@]}")
  fi

  # A single flat name (`^TestFoo$`) or the combined-flat-batch form
  # (`^(TestFoo|TestBar|...)$`, produced above when every alternative in the
  # batch is a top-level name) both narrow packages the same way: union the
  # package(s) each individual name's `func TestX` lives in.
  local -a _batch_names=()
  if [[ "$pattern" != "." ]] && [[ "$pattern" =~ ^\^\((Test[[:alnum:]_|]+)\)\$$ ]]; then
    IFS='|' read -r -a _batch_names <<<"${BASH_REMATCH[1]}"
  elif [[ "$pattern" != "." ]] && [[ "$pattern" =~ ^\^?(Test[[:alnum:]_]+)(/.*)?$ ]]; then
    _batch_names=("${BASH_REMATCH[1]}")
  fi

  if [ "${#_batch_names[@]}" -gt 0 ]; then
    local _base_dir="$COMPLEMENT_DIR"
    if [ -n "$use_in_repo_tests" ]; then _base_dir="${repo_root}/complement"; fi
    if command -v rg &>/dev/null; then
      local -a matched_pkgs=()
      mapfile -t matched_pkgs < <(
        cd "$_base_dir" \
          && rg -l --glob '*_test.go' "^func[[:space:]]+(${_batch_names[0]}$(printf '|%s' "${_batch_names[@]:1}"))\\b" tests 2>/dev/null \
          | xargs -r -n1 dirname | sed 's#^#./#' | sort -u || true
      )
      if [ "${#matched_pkgs[@]}" -gt 0 ]; then
        packages=("${matched_pkgs[@]}")
        echo "Selected package(s) for $pattern: ${packages[*]}" >&2
      fi
    fi
  fi

  local -a flags=(
    -tags "$test_tags"
    -v
    -count=1
    -timeout "$test_timeout"
    -p "$test_parallel"
    -parallel "$test_parallel"
    "${extra_args[@]}"
  )
  if [[ "$pattern" != "." ]]; then flags+=(-run "$pattern"); fi

  local _events_dir
  _events_dir="$(mktemp -d "${staged_results_file}.events.XXXXXX")"
  local _events_fifo="${_events_dir}/events"
  mkfifo "$_events_fifo"

  # ── Real-time docker log capture for PG timings ────────────────────────────
  # Complement removes containers during test teardown, so we cannot docker-cp
  # files after go test exits.  Instead, watch for container starts via
  # docker-events and follow their logs; timing sections land in the captured
  # files when the SIGTERM/exit handlers in Synapse flush them to stderr.
  _pg_timing_dir=""
  _pg_log_watcher_pid=""
  if [[ -n "${SYNAPSE_PG_TIMINGS:-}" ]]; then
    # Use whichever runtime the harness was configured with (podman under
    # `PODMAN=1`); plain `docker` may not even be installed there, and the
    # podman CLI talks to the podman socket directly.
    local _rt="${CONTAINER_RUNTIME:-docker}"
    _pg_timing_dir="$(mktemp -d "${staged_results_file}.pgtimings.XXXXXX")"
    local _container_label="COMPLEMENT_WRAPPER_TOKEN=$COMPLEMENT_WRAPPER_TOKEN"
    # Follow logs from complement containers as they start.  Subscribe to
    # container-events *before* scanning already-running containers, then feed
    # both container-id sources through one loop body; a `seen` set stops a
    # container appearing in both from being followed twice. This narrows
    # the start/scan race but does not fully close it: the background
    # `events` process below is not guaranteed to be connected to
    # the daemon before the scan runs, so a container starting in that
    # small window could still be missed by both paths.
    #
    # Every process here -- the `events` reader, the merge/dedupe
    # pipeline, and each `logs -f` follower it forks -- is a
    # grandchild (or deeper) of this function, so plain `wait` on their
    # pids cannot reap them. Instead, `set -m` gives this whole subshell
    # its own process group, so it can be torn down as a unit with
    # `kill -- -PGID` below (same technique as the go-test launch further
    # down this function).
    (
      # Do not let the watcher keep the process-wide flock alive if the
      # parent is interrupted. In particular, `tail -f` can outlive this
      # subshell and would otherwise retain the inherited lock FD.
      if [ -n "${COMPLEMENT_RUN_LOCK_FD:-}" ]; then
        eval "exec ${COMPLEMENT_RUN_LOCK_FD}>&-"
      fi
      set -m
      "$_rt" events --filter 'event=start' --format '{{.ID}}' 2>/dev/null >"${_pg_timing_dir}/.events_stream" &
      declare -A _seen
      { "$_rt" ps -q 2>/dev/null; tail -n +1 -f "${_pg_timing_dir}/.events_stream" 2>/dev/null; } \
        | while IFS= read -r _cid; do
        [[ -n "${_seen[$_cid]:-}" ]] && continue
        _seen[$_cid]=1
        if "$_rt" inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$_cid" 2>/dev/null \
            | grep -Fxq "$_container_label"; then
          "$_rt" logs -f "$_cid" >>"${_pg_timing_dir}/${_cid}.log" 2>&1 &
        fi
      done
    ) &
    _pg_log_watcher_pid=$!
  fi

  local _go_exit=0
  set +e
  # Enable job control just for this launch so the subshell (and the
  # go test/tee/jq pipeline it forks) gets its own process group. That
  # lets the INT/TERM/HUP traps below kill the whole group with
  # `kill -- -PGID` instead of only the subshell PID, which would leave
  # go test/tee/jq running as orphans past container cleanup.
  set -m
  (
    # Background test processes must not inherit the run lock either. The
    # parent shell remains responsible for holding and releasing it.
    if [ -n "${COMPLEMENT_RUN_LOCK_FD:-}" ]; then
      eval "exec ${COMPLEMENT_RUN_LOCK_FD}>&-"
    fi
    set -o pipefail
    if [ -n "$use_in_repo_tests" ]; then
      cd "${repo_root}/complement"
    else
      cd "$COMPLEMENT_DIR"
    fi
    go test -json "${flags[@]}" "${packages[@]}" \
      | tee -a "$staged_log_file" \
      | jq --unbuffered -r \
        'select((.Action == "pass" or .Action == "fail" or .Action == "skip") and .Test != null)
         | (.Elapsed // 0) as $e
         | [.Action, .Test,
            (if $e == 0 then "0s"
             else ((($e * 100 | round) / 100) | tostring) + "s" end)
           ] | @tsv' \
      >"$_events_fifo"
  ) &
  local _producer=$!
  set +m
  _active_producer=$_producer

  while IFS=$'\t' read -r _action _tname _elapsed; do
    [ -n "$_action" ] || continue
    record_result "$_action" "$_tname" "$_elapsed"
  done <"$_events_fifo"

  wait "$_producer"
  _go_exit=$?
  _active_producer=""
  set -e
  rm -rf "$_events_dir"

  cleanup_pg_log_watcher
  # Accumulate every pattern invocation's timing dir instead of overwriting,
  # so `finish` extracts timings from *all* -run patterns, not just the last.
  if [[ -n "${_pg_timing_dir:-}" ]]; then
    _PG_TIMING_DIRS="${_PG_TIMING_DIRS:+$_PG_TIMING_DIRS }$_pg_timing_dir"
  fi

  return "$_go_exit"
}

main "$@"

test_start_seconds=$SECONDS
TEST_EXIT_CODE=0
_active_producer=""
# Accrued PG-timing capture dirs, one per `run_one_pattern` invocation
# (only populated under `SYNAPSE_PG_TIMINGS=1`); see `finish` below.
_PG_TIMING_DIRS=""

# Merges staged results into the main ledger and prints a summary. Called
# from the EXIT trap below so it runs no matter how the script stops --
# reaching the end, an explicit `exit`, a `set -e` abort, or a signal --
# instead of only on the happy path. Guarded against running twice (a
# signal's `exit` still triggers this same trap).
_reported=""
finish() {
  [ -n "$_reported" ] && return 0
  _reported=1

  cleanup_pg_log_watcher

  merge_script="${repo_root}/scripts-dev/merge_complement_results.py"
  if [ -f "$staged_results_file" ] && [ -s "$staged_results_file" ]; then
    if [ "$RUN_TESTS" = "." ]; then
      python3 "$merge_script" --dedupe-in-place "$staged_results_file" \
        || echo "WARN: dedupe of staged results failed ($staged_results_file); keeping raw rows" >&2
      python3 "$merge_script" --sort-in-place "$staged_results_file" \
        || echo "WARN: sort of staged results failed ($staged_results_file); keeping arrival order" >&2
      if cp "$staged_results_file" "$main_results_file"; then
        echo "refreshed $main_results_file from $(wc -l <"$staged_results_file") staged results" >&2
      else
        echo "MERGE FAILED: refreshing $main_results_file from staged results" >&2
        TEST_EXIT_CODE=1
      fi
    else
      tmp_merge="$(mktemp "${main_results_file}.merge.XXXXXX")"
      if python3 "$merge_script" "$main_results_file" "$staged_results_file" "$tmp_merge"; then
        if mv "$tmp_merge" "$main_results_file"; then
          echo "merged $(wc -l <"$staged_results_file") staged results into $main_results_file" >&2
        else
          echo "MERGE FAILED: moving merged results into $main_results_file" >&2
          TEST_EXIT_CODE=1
        fi
      else
        echo "WARN: merge into $main_results_file failed; appending staged results" >&2
        cat "$staged_results_file" >>"$main_results_file"
        rm -f "$tmp_merge"
      fi
    fi
  else
    echo "Warning: $staged_results_file is missing or empty. No results processed." >&2
    if [ "${TEST_EXIT_CODE:-0}" -eq 0 ]; then
      TEST_EXIT_CODE=1
    fi
  fi

  # Log: point-in-time snapshot, straight copy (not a merge -- no history to preserve).
  if [ -f "$staged_log_file" ]; then
    cp "$staged_log_file" "$main_log_file"
    echo "refreshed $main_log_file from staged log" >&2
  fi

  _pass=$(grep -c '"pass"' "$staged_results_file" 2>/dev/null || true)
  _fail=$(grep -c '"fail"' "$staged_results_file" 2>/dev/null || true)
  _skip=$(grep -c '"skip"' "$staged_results_file" 2>/dev/null || true)
  test_duration_seconds=$((SECONDS - test_start_seconds))

  echo "" >&2
  echo "RESULTS: ${_pass:-0} pass / ${_fail:-0} fail / ${_skip:-0} skip" >&2
  echo "TIME: $(printf '%d:%02d' $((test_duration_seconds / 60)) $((test_duration_seconds % 60))) min" >&2
  echo "" >&2
  echo "complement logs saved at $staged_log_file" >&2
  echo "complement results staged at $staged_results_file" >&2
  echo "complement results merged into $main_results_file" >&2
  echo "" >&2

  # ── Stats: slowest tests + time by suite ───────────────────────────────────
  if [ -f "$staged_log_file" ] && [ -s "$staged_log_file" ]; then
    python3 -c "
import json, sys
from collections import defaultdict

results = []
for line in open(sys.argv[1]):
    line = line.strip()
    if not line:
        continue
    try:
        r = json.loads(line)
    except json.JSONDecodeError:
        continue
    if r.get('Action') not in ('pass', 'fail'):
        continue
    if not r.get('Test'):
        continue
    elapsed = r.get('Elapsed', 0) or 0
    results.append((r['Test'], r['Action'], elapsed))

if not results:
    sys.exit(0)

# go test reports both a parent aggregate event and each of its subtests'
# events (e.g. TestX plus TestX/foo). Summing both would double-count the
# parent's elapsed time (which already includes its subtests), so keep only
# leaf results: a result is a leaf unless some other reported test name is
# <name> + '/'.
names = {test for test, _, _ in results}
def is_leaf(name):
    return not any(other.startswith(name + '/') for other in names)

leaf_results = [r for r in results if is_leaf(r[0])]

# Slowest 10 tests
print('--- Slowest tests ---')
for test, action, elapsed in sorted(leaf_results, key=lambda x: -x[2])[:10]:
    print(f'  {elapsed:7.2f}s  {action.upper():6s}  {test}')

# Time by suite (first path component after Test)
suite_times = defaultdict(float)
suite_counts = defaultdict(int)
for test, action, elapsed in leaf_results:
    suite = test.split('/')[0]
    suite_times[suite] += elapsed
    suite_counts[suite] += 1

print()
print('--- Time by suite ---')
for suite, total in sorted(suite_times.items(), key=lambda x: -x[1]):
    print(f'  {total:8.2f}s  {suite_counts[suite]:4d} tests  {suite}')
" "$staged_log_file" >&2
    echo "" >&2
  fi

  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    {
      echo "### Complement results"
      echo "**${_pass:-0}** pass / **${_fail:-0}** fail / **${_skip:-0}** skip"
      echo ""
      echo "Duration: \`${test_duration_seconds}s\` (in_repo=\`${use_in_repo_tests:-0}\`)"
    } >> "$GITHUB_STEP_SUMMARY"
  fi

  # ── Extract timing from captured docker logs ─────────────────────────────
  if [[ -n "${SYNAPSE_PG_TIMINGS:-}" ]] && [[ -n "${_PG_TIMING_DIRS:-}" ]]; then
    local _found_timing=0
    for _pg_dir in $_PG_TIMING_DIRS; do
      if [[ ! -d "$_pg_dir" ]]; then
        continue
      fi
      for _f in "${_pg_dir}"/*.log; do
        [ -f "$_f" ] || continue
        # Extract the timing sections from the captured log.
        local _sections
        _sections=$(awk '
          /^=== Per-table SQL timing/ { p=1 }
          /^=== State store mtxdb-vs-SQL timings/ { p=1 }
          /^=== Postgres test-DB lifecycle timings/ { p=1 }
          /^=== END SYNAPSE PG TIMINGS ===/ { p=0 }
          /^================================/ { if(p) { print; p=0; next } }
          { if(p) print }
        ' "$_f" 2>/dev/null)
        if [[ -n "$_sections" ]]; then
          if [ "$_found_timing" -eq 0 ]; then
            echo "" >&2
            echo "=== SYNAPSE PG TIMINGS (from containers) ===" >&2
            _found_timing=1
          fi
          echo "--- ${_f##*/} ---" >&2
          echo "$_sections" >&2
        fi
      done
    done
    if [ "$_found_timing" -eq 1 ]; then
      echo "=== END SYNAPSE PG TIMINGS ===" >&2
    fi
  fi

  cleanup_complement_containers
  # Also sweep resources left by an older interrupted invocation. Keep this
  # on the EXIT path as well as startup so an invocation which is interrupted
  # before Complement's normal teardown still gets cleaned up immediately.
  if [ "${COMPLEMENT_CLEANUP_STALE_RESOURCES:-1}" != "0" ]; then
    cleanup_stale_complement_containers
    cleanup_stale_complement_networks
  fi
}
trap finish EXIT

# Bash only runs the EXIT trap for a signal that's itself trapped -- an
# untrapped INT/TERM/HUP kills the process directly and skips EXIT (and
# `finish` above) entirely. This is what used to discard everything staged
# on Ctrl+C or a dropped terminal. `exit` from here still runs `finish` via
# the EXIT trap, so these just need the right conventional exit code.
# Terminate any active go-test pipeline so it does not outlive container
# cleanup. Clear _active_producer after a successful wait to avoid
# signaling a recycled PID later.
# shellcheck disable=SC2329 # invoked indirectly by the signal traps below.
_kill_active_producer() {
  if [ -n "$_active_producer" ]; then
    # Negative PID targets the whole process group (see `set -m` above),
    # so go test/tee/jq are all signaled, not just the subshell.
    kill -- "-$_active_producer" 2>/dev/null || kill -- "$_active_producer" 2>/dev/null || true
    wait "$_active_producer" 2>/dev/null || true
    _active_producer=""
  fi
}
trap '_kill_active_producer; cleanup_pg_log_watcher; exit 130' INT
trap '_kill_active_producer; cleanup_pg_log_watcher; exit 143' TERM
trap '_kill_active_producer; cleanup_pg_log_watcher; exit 129' HUP

# ── Run all patterns ──────────────────────────────────────────────────────────
for _pattern in "${ALT_PATTERNS[@]}"; do
  set +e
  run_one_pattern "$_pattern"
  _pexit=$?
  set -e
  if [ "$_pexit" -ne 0 ]; then
    TEST_EXIT_CODE="$_pexit"
  fi
done

# Run finish before selecting the final exit status so that persistence
# failures (e.g. copy/move errors in the merge step) can update
# TEST_EXIT_CODE and are not silently lost.
finish

if [ "$TEST_EXIT_CODE" -ne 0 ]; then
  exit "$TEST_EXIT_CODE"
fi

exit 0
