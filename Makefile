# nzbd — developer Makefile.
#
# Fresh clone, get productive:
#   make setup      # install the toolchain, PP tools and git hooks
#   make run        # build + run the daemon (first-run setup UI on :6789)
#   make check      # everything CI enforces: fmt + clippy + tests + MSRV
#
# Run `make` (or `make help`) to list every target.

CARGO   ?= cargo
# The daemon binary package (cargo -p nzbd).
DAEMON  := nzbd
# Minimum supported Rust (keep in sync with Cargo.toml rust-version).
MSRV    := 1.85
UNAME_S := $(shell uname -s)

# Optional overrides for `make run`, e.g.
#   make run CONFIG=dev/config/nzbd.toml BIND=0.0.0.0:6789
CONFIG  ?=
BIND    ?=
RUN_ARGS := run
ifneq ($(strip $(CONFIG)),)
RUN_ARGS += --config $(CONFIG)
endif
ifneq ($(strip $(BIND)),)
RUN_ARGS += --bind $(BIND)
endif

.DEFAULT_GOAL := help

##@ Help

.PHONY: help
help: ## List all targets
	@awk 'BEGIN {FS = ":.*##"} \
		/^##@/ {printf "\n\033[1m%s\033[0m\n", substr($$0, 5); next} \
		/^[a-zA-Z0-9_.-]+:.*##/ {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}' \
		$(MAKEFILE_LIST)

##@ Toolchain & setup

.PHONY: setup
setup: toolchain tools hooks ## One-shot dev setup: toolchain + PP tools + git hooks
	@echo "OK - dev environment ready; try 'make run' or 'make check'"

.PHONY: toolchain
toolchain: ## Rust components (fmt, clippy, llvm-tools) + MSRV toolchain + cargo-llvm-cov
	rustup component add rustfmt clippy llvm-tools-preview
	rustup toolchain install $(MSRV) --profile minimal
	$(CARGO) install cargo-llvm-cov --locked || true

.PHONY: tools
tools: ## Install the post-processing tools the tests exercise (par2, 7z)
ifeq ($(UNAME_S),Darwin)
	brew install par2 p7zip
else ifeq ($(UNAME_S),Linux)
	@if command -v apt-get >/dev/null 2>&1; then \
		sudo apt-get update && sudo apt-get install -y par2 p7zip-full; \
	else \
		echo "Install 'par2' and '7z' with your package manager (non-apt distro)."; \
	fi
else
	@echo "Install 'par2' and '7z' manually on $(UNAME_S)."
endif

.PHONY: hooks
hooks: ## Point git at the committed pre-commit / pre-push hooks
	git config core.hooksPath .githooks
	@echo "OK - hooks enabled (pre-commit: fmt; pre-push: clippy + tests)"

##@ Build & run

.PHONY: build
build: ## Debug build of the daemon
	$(CARGO) build -p $(DAEMON)

.PHONY: release
release: ## Optimized release build of the daemon
	$(CARGO) build --release -p $(DAEMON)

.PHONY: run
run: ## Run the daemon (CONFIG=... BIND=... optional; no config -> first-run setup UI)
	$(CARGO) run -p $(DAEMON) -- $(RUN_ARGS)

.PHONY: docker
docker: ## Build the container from the working tree and run it (dev/ compose)
	cd dev && docker compose up --build

##@ Test & quality gates

.PHONY: test
test: ## Whole workspace test suite (unit + e2e + cluster + daemon + UI boot)
	$(CARGO) test --workspace

.PHONY: test-strict
test-strict: ## Like `test`, but a missing par2/7z is a failure, not a skip (as in CI)
	NZBD_REQUIRE_TOOLS=1 $(CARGO) test --workspace

.PHONY: ui-test
ui-test: ## Fast UI boot smoke test only (executes the embedded page script via node)
	node crates/nzbd/tests/ui_boot_harness.js crates/nzbd-api/ui/index.html

.PHONY: fmt
fmt: ## Format the whole workspace
	$(CARGO) fmt --all

.PHONY: fmt-check
fmt-check: ## Check formatting without writing (CI gate)
	$(CARGO) fmt --all --check

.PHONY: lint
lint: ## Clippy across all targets with warnings denied (CI gate)
	$(CARGO) clippy --workspace --all-targets -- -D warnings

.PHONY: msrv
msrv: ## Type-check on the minimum supported Rust (1.85)
	$(CARGO) +$(MSRV) check --workspace --all-targets

.PHONY: coverage
coverage: ## Line-coverage summary (needs cargo-llvm-cov; `make toolchain` installs it)
	$(CARGO) llvm-cov --workspace --summary-only

.PHONY: coverage-html
coverage-html: ## Full HTML coverage report under target/llvm-cov/html/
	$(CARGO) llvm-cov --workspace --html
	@echo "report: target/llvm-cov/html/index.html"

.PHONY: check
check: fmt-check lint test msrv ## Everything CI enforces, in one shot (run before pushing)
	@echo "OK - all local gates passed"

##@ Housekeeping

.PHONY: clean
clean: ## Remove build artifacts (cargo clean)
	$(CARGO) clean
