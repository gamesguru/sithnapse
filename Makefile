SHELL=/bin/bash
.DEFAULT_GOAL=_help

# [ENUM] Styling / Colors
STYLE_CYAN := $(shell tput setaf 6 2>/dev/null || echo -e "\033[36m")
STYLE_RESET := $(shell tput sgr0 2>/dev/null || echo -e "\033[0m")

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Linting, formatting
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.PHONY: format
format: ##H Format with ruff
	uv run --no-sync ruff format .
	uv run --no-sync ruff check --fix .
	cargo +nightly fmt

.PHONY: lint
lint: ##H Lint the code with mypy
	uv run --no-sync mypy
	cargo +nightly clippy --all-targets --all-features


.PHONY: sync
sync:	##H Sync deps (uv) then build the Rust extension (maturin develop)
	# Install/refresh dependencies without touching the project package: uv
	# and `maturin develop` otherwise fight over who owns the editable
	# `matrix-synapse` install, so every subsequent `uv run` would uninstall
	# and reinstall it. `--no-install-project` leaves the package to maturin;
	# `--inexact` keeps it from being treated as extraneous and removed.
	uv sync --no-install-project --inexact
	@rm -f target/maturin/libsynapse.so target/release/libsynapse.so
	@RUSTC_WRAPPER= uv run --no-sync maturin develop --release
	@test -s target/maturin/libsynapse.so || { \
		echo "maturin produced an empty libsynapse.so" >&2; \
		exit 1; \
	}
	@file target/maturin/libsynapse.so | grep -q 'ELF .*shared object' || { \
		echo "maturin produced a non-ELF libsynapse.so" >&2; \
		exit 1; \
	}

p ?=

# When the test target is invoked as `make -jN test`, pass the same worker
# count through to Trial. GNU Make normalizes both `-j N` and `-jN` to `-jN`
# in MAKEFLAGS. An unbounded `make -j` has no numeric value to propagate.
#
# Deliberately do NOT default this to nproc for a bare `make test`: passing
# `-j` at all switches trial_ctrlc.py onto Twisted's distributed-trial
# runner, which has no custom Ctrl+C handling and can hang indefinitely
# (reactor.run() never returns) rather than exit cleanly -- e.g. an empty
# or tiny suite starts a worker pool sized `min(len(testCases), maxWorkers)`,
# which is 0 workers for 0 tests, so nothing ever signals completion. Plain
# `make test` stays on trial_ctrlc.py's non-distributed path (config["jobs"]
# is None), which installs its own SIGINT handler and always exits cleanly.
TRIAL_JOBS_REQUESTED := $(shell printf '%s\n' "$(MAKEFLAGS)" | sed -n 's/.*-j\([0-9][0-9]*\).*/\1/p')
# Clamp to (nproc - 1) so a `-jN` from MAKEFLAGS can never oversubscribe the
# machine: each Trial worker drives its own rapid Postgres CREATE/DROP DATABASE
# cycle plus the embedded storage engine, so requesting more workers than
# spare cores starves the desktop (observed: a 6-core machine hard-froze
# under 7 workers) rather than just running slower.
TRIAL_MAX_JOBS := $(shell n=$$(nproc 2>/dev/null || echo 1); echo $$(( n > 1 ? n - 1 : 1 )))
TRIAL_JOBS := $(shell if [ -n "$(TRIAL_JOBS_REQUESTED)" ]; then \
	if [ "$(TRIAL_JOBS_REQUESTED)" -gt "$(TRIAL_MAX_JOBS)" ]; then echo "$(TRIAL_MAX_JOBS)"; \
	else echo "$(TRIAL_JOBS_REQUESTED)"; fi; \
	fi)

.PHONY: test
test: ##H Run tests, e.g., on tests/storage/
	cargo +nightly test
	if [ -n "$$SYNAPSE_POSTGRES" ] && [ -z "$$SYNAPSE_POSTGRES_HOST" ]; then eval "$$(scripts-dev/start_test_postgres.sh)" || exit 1; fi; \
	uv run --no-sync python scripts-dev/trial_ctrlc.py $(if $(TRIAL_JOBS),-j $(TRIAL_JOBS),) $(p)

# Match Complement's package and in-package parallelism to an explicit GNU
# Make -jN value. A plain `make complement` keeps the script's conservative
# default of 2; callers can also override COMPLEMENT_PARALLEL directly.
COMPLEMENT_MAKE_JOBS := $(shell printf '%s\n' "$(MAKEFLAGS)" | sed -n 's/.*-j\([0-9][0-9]*\).*/\1/p')
COMPLEMENT_DEFAULT_PARALLEL := $(if $(COMPLEMENT_MAKE_JOBS),$(COMPLEMENT_MAKE_JOBS),2)

.PHONY: complement
complement: ##H Run Complement tests (use -jN to set Complement parallelism)
	COMPLEMENT_PARALLEL=$${COMPLEMENT_PARALLEL:-$(COMPLEMENT_DEFAULT_PARALLEL)} ./scripts-dev/complement.sh $(COMPLEMENT_ARGS)

.PHONY: _complement/cleanup
_complement/cleanup: ##H Stop Complement and remove its labeled containers/networks
	@set -euo pipefail; \
	lock_file="$${TMPDIR:-/tmp}/synapse-complement.lock"; \
	if command -v fuser >/dev/null 2>&1 && [ -e "$$lock_file" ]; then \
		fuser -TERM "$$lock_file" 2>/dev/null || true; \
		for _ in 1 2 3 4 5; do \
			if ! fuser "$$lock_file" >/dev/null 2>&1; then break; fi; \
			sleep 1; \
		done; \
		fuser -KILL "$$lock_file" 2>/dev/null || true; \
	fi; \
	exec 9>"$$lock_file"; \
	flock -n 9 || { echo "Complement lock is still owned; refusing cleanup" >&2; exit 1; }; \
	runtime="$${CONTAINER_RUNTIME:-docker}"; \
	if command -v "$$runtime" >/dev/null 2>&1; then \
		"$$runtime" ps -aq --filter label=complement_pkg | xargs -r "$$runtime" rm -f; \
		"$$runtime" network ls -q --filter label=complement_pkg | xargs -r "$$runtime" network rm || true; \
	fi


.PHONY: build
build: ##H Build the package
	uv build


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Install
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Defaults for `make install` / `make install/gen-config` / `make install/server`.
# Override them in .env (sourced below) or in the environment, e.g.
# `make install INSTALL_USER=deploy`. The virtualenv lives at INSTALL_DIR/.venv.
# INSTALL_EXTRAS is an optional comma-separated list of extras to install,
# e.g. `postgres` -> pip install "INSTALL_DIR[postgres]".
INSTALL_USER ?= sith
INSTALL_DIR ?= /opt/sithnapse
INSTALL_PYTHON ?= python3
INSTALL_EXTRAS ?=

# Defaults for `make install/gen-config` / `make install/server`.
SERVER_NAME ?= sith.nutra.tk
CONFIG_PATH ?= /etc/sithnapse/homeserver.yaml
REPORT_STATS ?= no

.PHONY: install
install: ##H Create the venv and pip-install the package as INSTALL_USER
	set -euo pipefail; \
	if [ -f .env ]; then set -a; . ./.env; set +a; fi; \
	install_user="$${INSTALL_USER:-$(INSTALL_USER)}"; \
	install_dir="$${INSTALL_DIR:-$(INSTALL_DIR)}"; \
	install_python="$${INSTALL_PYTHON:-$(INSTALL_PYTHON)}"; \
	install_extras="$${INSTALL_EXTRAS:-$(INSTALL_EXTRAS)}"; \
	venv="$$install_dir/.venv"; \
	if [ -n "$$install_extras" ]; then spec="$$install_dir[$$install_extras]"; else spec="$$install_dir"; fi; \
	echo "Creating venv at $$venv as $$install_user"; \
	sudo -u "$$install_user" -H "$$install_python" -m venv "$$venv"; \
	echo "Installing $$spec into $$venv"; \
	sudo -u "$$install_user" -H "$$venv/bin/pip" install "$$spec"

.PHONY: install/gen-config
install/gen-config: ##H Generate the homeserver config as INSTALL_USER
	set -euo pipefail; \
	if [ -f .env ]; then set -a; . ./.env; set +a; fi; \
	install_user="$${INSTALL_USER:-$(INSTALL_USER)}"; \
	install_dir="$${INSTALL_DIR:-$(INSTALL_DIR)}"; \
	venv="$$install_dir/.venv"; \
	server_name="$${SERVER_NAME:-$(SERVER_NAME)}"; \
	config_path="$${CONFIG_PATH:-$(CONFIG_PATH)}"; \
	report_stats="$${REPORT_STATS:-$(REPORT_STATS)}"; \
	echo "Generating $$config_path for $$server_name"; \
	sudo -u "$$install_user" -H "$$venv/bin/python" \
		-m synapse.app.homeserver \
		--server-name "$$server_name" \
		--config-path "$$config_path" \
		--generate-config \
		--report-stats="$$report_stats"

.PHONY: install/server
install/server: ##H Run the homeserver as INSTALL_USER with CONFIG_PATH
	set -euo pipefail; \
	if [ -f .env ]; then set -a; . ./.env; set +a; fi; \
	install_user="$${INSTALL_USER:-$(INSTALL_USER)}"; \
	install_dir="$${INSTALL_DIR:-$(INSTALL_DIR)}"; \
	config_path="$${CONFIG_PATH:-$(CONFIG_PATH)}"; \
	echo "Starting homeserver from $$config_path as $$install_user"; \
	sudo -u "$$install_user" -H "$$install_dir/.venv/bin/python" \
		-m synapse.app.homeserver \
		--config-path "$$config_path"

.PHONY: all
all:	##H Run the main targets
all: sync format lint test


.PHONY: publish
publish: build ##H Upload the package to PyPI using twine
	uv run --with twine twine upload dist/*


.PHONY: clean
clean: ##H Clean the virtual environment and caches
	cargo clean
	#rm -rf $(VENV)
	find . -type f -name '*.pyc' -delete
	find . -type d -name '__pycache__' -exec rm -rf {} +
	rm -rf .mypy_cache


.PHONY: _help
_help: ##H Show this help, list available targets
	@grep -hE '^[a-zA-Z0-9_\/-]+:[[:space:]]*##H .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":[[:space:]]*##H "}; {printf "$(STYLE_CYAN)%-15s$(STYLE_RESET) %s\n", $$1, $$2}'
