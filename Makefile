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
	npx --yes --package=prettier@3.6.2 prettier --prose-wrap always --print-width 80 --write docs/development-gg/
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
	uv sync --no-install-project --inexact --group core_dev
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

# Defaults for the `install/*` targets. Override them in .env (sourced below)
# or in the environment, e.g. `make install/server INSTALL_USER=deploy`.
# INSTALL_DIR is the virtualenv; INSTALL_SRC is the source checkout to build
# and install from; INSTALL_EXTRAS is an optional comma-separated extra list.
INSTALL_USER ?= sith
INSTALL_DIR ?= /opt/sithnapse
INSTALL_SRC ?= $(CURDIR)
INSTALL_EXTRAS ?= [postgres]

# Defaults for `make install/gen-config` / `make install/server`.
SERVER_NAME ?= sith.nutra.tk
CONFIG_PATH ?= /etc/sithnapse/homeserver.yaml
REPORT_STATS ?= no

.PHONY: install/build
install/build: ##H Build, pip-install $(INSTALL_SRC) to $(INSTALL_DIR)
	# set -euo pipefail
	# if [ -f .env ]; then set -a; . ./.env; set +a; fi
	@echo "Installing $(INSTALL_SRC)$(INSTALL_EXTRAS) into $(INSTALL_DIR)"
	sudo "$(INSTALL_DIR)/bin/pip" install "$(INSTALL_SRC)$(INSTALL_EXTRAS)"
	@# A pip-installed copy has no git checkout, so record the branch and commit
	@# it was built from; synapse/util/__init__.py reads this for the version.
	@# Use the installed interpreter in isolated mode so the checkout's
	@# synapse/ package, egg-info and PYTHONPATH cannot shadow it on sys.path.
	@set -euo pipefail; \
	install_python="$$(cd "$(INSTALL_DIR)" && pwd)/bin/python"; \
	ver="$$("$$install_python" -I -c 'import importlib.metadata as m; print(m.version("matrix-synapse"))')"; \
	branch="$$(git -C "$(INSTALL_SRC)" rev-parse --abbrev-ref HEAD)"; \
	rev="$$(git -C "$(INSTALL_SRC)" rev-parse --short HEAD)"; \
	dirty=""; \
	if [ -n "$$(git -C "$(INSTALL_SRC)" status --porcelain --untracked-files=no)" ]; then dirty=",dirty"; fi; \
	pkg="$$("$$install_python" -I -c 'import importlib.util as u; print(u.find_spec("synapse").submodule_search_locations[0])')"; \
	echo "Recording version $$ver (b=$$branch,$$rev$$dirty) in $$pkg"; \
	echo "$$ver (b=$$branch,$$rev$$dirty)" | sudo tee "$$pkg/_build_version.txt" >/dev/null

.PHONY: install/gen-config
install/gen-config: ##H Generate homeserver config as $(INSTALL_USER)
	# set -euo pipefail
	# if [ -f .env ]; then set -a; . ./.env; set +a; fi
	@echo "Generating $(CONFIG_PATH) for $(SERVER_NAME)"
	sudo -u "$(INSTALL_USER)" -H "$(INSTALL_DIR)/bin/python" \
		-m synapse.app.homeserver \
		--server-name "$(SERVER_NAME)" \
		--config-path "$(CONFIG_PATH)" \
		--generate-config \
		--report-stats="$(REPORT_STATS)"


# Output of `make install/merge-config`. It holds your secrets, so keep it out
# of version control (`/homeserver*.yaml` is already gitignored).
MERGED_PATH ?= homeserver.merged.yaml

# Defaults for the offline `make merge-config` target, which works on a config
# copied into the checkout (e.g. `scp server:/etc/sithnapse/homeserver.yaml .`).
LOCAL_CONFIG ?= homeserver.yaml
LOCAL_MERGED_PATH ?= homeserver.merged.yaml

.PHONY: merge-config
merge-config: ##H Merge local homeserver.yaml onto the schema (offline) -> homeserver.merged.yaml
	python3 scripts-dev/merge_yaml.py \
		-c "$(LOCAL_CONFIG)" \
		-s schema/synapse-config.schema.yaml \
		-o "$(LOCAL_MERGED_PATH)"

.PHONY: install/merge-config
install/merge-config: ##H Merge CONFIG_PATH onto the full documented template -> MERGED_PATH
	@echo "Merging $(CONFIG_PATH) into $(MERGED_PATH)"
	uv run --no-sync python scripts-dev/merge_yaml.py -c "$(CONFIG_PATH)" -o "$(MERGED_PATH)"


.PHONY: _publish
_publish: build ##H Upload the package to PyPI using twine
	uv run --with twine twine upload dist/*

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Others / Misc
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.PHONY: all
all:	##H Run the main targets
all: sync format lint test


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
