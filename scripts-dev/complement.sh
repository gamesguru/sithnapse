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
    COMPLEMENT_REPO=${COMPLEMENT_REPO:-gamesguru/complement}
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
  export COMPLEMENT_ENABLE_DIRTY_RUNS=1

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

    # It can take quite a while to spin up a worker-mode Synapse for the first
    # time (the main problem is that we start 14 python processes for each test,
    # and complement likes to do two of them in parallel).
    export COMPLEMENT_SPAWN_HS_TIMEOUT_SECS=120
  else
    export PASS_SYNAPSE_COMPLEMENT_USE_WORKERS=
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

  # SYNAPSE_MDBX=1 is the concise production on-switch (see
  # config/database.py) but was never actually forwarded into the
  # container here -- treat it the same as SYNAPSE_EMBEDDED_HAMT_ENGINE=mdbx
  # so it does something locally too.
  if [[ -n "${SYNAPSE_MDBX:-}" && -z "$SYNAPSE_EMBEDDED_HAMT_ENGINE" ]]; then
    SYNAPSE_EMBEDDED_HAMT_ENGINE="mdbx"
    SYNAPSE_EMBEDDED_HAMT_PATH="${SYNAPSE_EMBEDDED_HAMT_PATH:-${SYNAPSE_MDBX_PATH:-}}"
  fi

  if [[ -n "$SYNAPSE_EMBEDDED_HAMT_ENGINE" ]]; then
    export PASS_SYNAPSE_EMBEDDED_HAMT_ENGINE="$SYNAPSE_EMBEDDED_HAMT_ENGINE"
    # SYNAPSE_EMBEDDED_HAMT_PATH is read inside the Complement container, not
    # on the host -- a caller who just wants to turn mdbx on shouldn't have
    # to know or care about that. Default it to a path that's always
    # writable there (the image's WORKDIR) rather than making them supply an
    # in-container path themselves.
    SYNAPSE_EMBEDDED_HAMT_PATH="${SYNAPSE_EMBEDDED_HAMT_PATH:-/data/embedded_hamt}"
  fi
  if [[ -n "$SYNAPSE_EMBEDDED_HAMT_PATH" ]]; then
    export PASS_SYNAPSE_EMBEDDED_HAMT_PATH="$SYNAPSE_EMBEDDED_HAMT_PATH"
  fi

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
    if [[ "$_arg" == "-run" ]]; then
      local _next=$((_i+1))
      RUN_TESTS="${!_next}"
      _i=$((_i+2))
    elif [[ "$_arg" =~ ^-run=(.+) ]]; then
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
  ALT_PATTERNS=()
  if [ "$RUN_TESTS" = "." ]; then
    ALT_PATTERNS=(".")
  else
    local -a raw_alts
    IFS='|' read -r -a raw_alts <<<"$RUN_TESTS"
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

  # ── Container token + cleanup trap ──────────────────────────────────────────
  export COMPLEMENT_WRAPPER_TOKEN="${COMPLEMENT_WRAPPER_TOKEN:-"complement-$$-$(date +%s%N)"}"
  export PASS_COMPLEMENT_WRAPPER_TOKEN="$COMPLEMENT_WRAPPER_TOKEN"
  export COMPLEMENT_SHARE_ENV_PREFIX=PASS_
  export COMPLEMENT_SPAWN_HS_TIMEOUT_SECS=${COMPLEMENT_SPAWN_HS_TIMEOUT_SECS:-120}
  # Placeholder until merge_and_report exists below; replaced with the real
  # combined EXIT trap once it's defined, so merging is never optional --
  # it happens on literal end-of-script, an explicit `exit`, a `set -e`
  # abort, or any trapped signal, from one single codepath instead of being
  # duplicated across call sites that can drift out of sync.
  trap cleanup_complement_containers EXIT

  return 0
}

# Invoked by the EXIT trap installed in main.
# shellcheck disable=SC2329
cleanup_complement_containers() {
  local container_label="COMPLEMENT_WRAPPER_TOKEN=$COMPLEMENT_WRAPPER_TOKEN"
  local containers container ours=()
  if command -v docker &>/dev/null; then
    mapfile -t containers < <(docker ps -aq --filter "name=complement" 2>/dev/null || true)
    for container in "${containers[@]:-}"; do
      if docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$container" 2>/dev/null \
          | grep -Fxq "$container_label"; then
        ours+=("$container")
      fi
    done
    if [ "${#ours[@]}" -gt 0 ]; then
      echo "Cleaning up Complement containers spawned by this run..." >&2
      printf '%s\n' "${ours[@]}" | xargs -r docker rm -f
    fi
  fi
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
    printf '%s\t%s\t%s\n' "${action^^}" "$_display_name" "$elapsed" >&2
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

  if [[ "$pattern" != "." ]] && [[ "$pattern" =~ ^\^?(Test[[:alnum:]_]+)(/.*)?$ ]]; then
    local _test_name="${BASH_REMATCH[1]}"
    local _base_dir="$COMPLEMENT_DIR"
    if [ -n "$use_in_repo_tests" ]; then _base_dir="${repo_root}/complement"; fi
    if command -v rg &>/dev/null; then
      local -a matched_pkgs=()
      mapfile -t matched_pkgs < <(
        cd "$_base_dir" \
          && rg -l --glob '*_test.go' "^func[[:space:]]+${_test_name}" tests 2>/dev/null \
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

  local _go_exit=0
  set +e
  # Enable job control just for this launch so the subshell (and the
  # go test/tee/jq pipeline it forks) gets its own process group. That
  # lets the INT/TERM/HUP traps below kill the whole group with
  # `kill -- -PGID` instead of only the subshell PID, which would leave
  # go test/tee/jq running as orphans past container cleanup.
  set -m
  (
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
  return "$_go_exit"
}

main "$@"

test_start_seconds=$SECONDS
TEST_EXIT_CODE=0
_active_producer=""

# Merges staged results into the main ledger and prints a summary. Called
# from the EXIT trap below so it runs no matter how the script stops --
# reaching the end, an explicit `exit`, a `set -e` abort, or a signal --
# instead of only on the happy path. Guarded against running twice (a
# signal's `exit` still triggers this same trap).
_reported=""
finish() {
  [ -n "$_reported" ] && return 0
  _reported=1

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

  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    {
      echo "### Complement results"
      echo "**${_pass:-0}** pass / **${_fail:-0}** fail / **${_skip:-0}** skip"
      echo ""
      echo "Duration: \`${test_duration_seconds}s\` (in_repo=\`${use_in_repo_tests:-0}\`)"
    } >> "$GITHUB_STEP_SUMMARY"
  fi

  cleanup_complement_containers
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
_kill_active_producer() {
  if [ -n "$_active_producer" ]; then
    # Negative PID targets the whole process group (see `set -m` above),
    # so go test/tee/jq are all signaled, not just the subshell.
    kill -- "-$_active_producer" 2>/dev/null || kill -- "$_active_producer" 2>/dev/null || true
    wait "$_active_producer" 2>/dev/null || true
    _active_producer=""
  fi
}
trap '_kill_active_producer; exit 130' INT
trap '_kill_active_producer; exit 143' TERM
trap '_kill_active_producer; exit 129' HUP

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
