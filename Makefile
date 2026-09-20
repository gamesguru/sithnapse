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
	uv run ruff format .
	uv run ruff check --fix .
	cargo +nightly fmt

.PHONY: lint
lint: ##H Lint the code with mypy
	uv run mypy
	cargo +nightly clippy --all-targets --all-features


.PHONY: sync
sync:	##H Runs: uv run maturin develop
	@rm -f target/maturin/libsynapse.so target/release/libsynapse.so
	@RUSTC_WRAPPER= uv run maturin develop --release
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
TRIAL_JOBS := $(shell printf '%s\n' "$(MAKEFLAGS)" | sed -n 's/.*-j\([0-9][0-9]*\).*/\1/p')

.PHONY: test
test: ##H Run tests, e.g., on tests/storage/
	cargo +nightly test
	if [ -n "$$SYNAPSE_POSTGRES" ] && [ -z "$$SYNAPSE_POSTGRES_HOST" ]; then eval "$$(scripts-dev/start_test_postgres.sh)" || exit 1; fi; \
	uv run python scripts-dev/trial_ctrlc.py $(if $(TRIAL_JOBS),-j $(TRIAL_JOBS),) $(p)

# Match Complement's package and in-package parallelism to an explicit GNU
# Make -jN value for monolith runs. Worker-mode Complement deployments start
# many Synapse processes per homeserver, so keep their default at 2 even when
# make is invoked with -jN; callers can explicitly override this with
# COMPLEMENT_PARALLEL.
COMPLEMENT_MAKE_JOBS := $(shell printf '%s\n' "$(MAKEFLAGS)" | sed -n 's/.*-j\([0-9][0-9]*\).*/\1/p')
COMPLEMENT_DEFAULT_PARALLEL := $(if $(WORKERS),2,$(if $(COMPLEMENT_MAKE_JOBS),$(COMPLEMENT_MAKE_JOBS),2))

.PHONY: complement
complement: ##H Run Complement tests (use -jN to set Complement parallelism)
	COMPLEMENT_PARALLEL=$${COMPLEMENT_PARALLEL:-$(COMPLEMENT_DEFAULT_PARALLEL)} ./scripts-dev/complement.sh $(COMPLEMENT_ARGS)


.PHONY: build
build: ##H Build the package
	uv build

.PHONY: all
all:	##H Run the main targets
all: format lint sync test


.PHONY: publish
publish: build ##H Upload the package to PyPI using twine
	uv run --with twine twine upload dist/*


.PHONY: clean
clean: ##H Clean the virtual environment and caches
	#rm -rf $(VENV)
	find . -type f -name '*.pyc' -delete
	find . -type d -name '__pycache__' -exec rm -rf {} +
	rm -rf .mypy_cache


.PHONY: _help
_help: ##H Show this help, list available targets
	@grep -hE '^[a-zA-Z0-9_\/-]+:[[:space:]]*##H .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":[[:space:]]*##H "}; {printf "$(STYLE_CYAN)%-15s$(STYLE_RESET) %s\n", $$1, $$2}'
