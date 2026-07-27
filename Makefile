.PHONY: build install check test coverage coverage-tools e2e

PREFIX ?= $(HOME)/.local
COVERAGE_BASE ?= github/main

build:
	cargo build --release

install: build
	@test "$$(id -u)" -ne 0 || { echo "make install is per-user; do not run it with sudo" >&2; exit 1; }
	target/release/flocal daemon preflight-service "$(PREFIX)/bin/flocal"
	@if [ "$$(uname -s)" = Linux ]; then state=$$(systemctl --user show --property=LoadState --value flocal-daemon.service) || exit $$?; if [ "$$state" != not-found ]; then systemctl --user stop flocal-daemon.service; fi; fi
	@if [ "$$(uname -s)" = Darwin ] && launchctl print gui/$$(id -u)/local.file-local.flocal-daemon >/dev/null 2>&1; then launchctl bootout gui/$$(id -u) "$$HOME/Library/LaunchAgents/local.file-local.flocal-daemon.plist"; fi
	install -d "$(PREFIX)/bin"
	install -m 755 target/release/flocal "$(PREFIX)/bin/flocal"
	"$(PREFIX)/bin/flocal" daemon install-service

test:
	cargo test --all-targets -- --test-threads=1

coverage-tools:
	@cargo llvm-cov --version 2>/dev/null | grep -q 'cargo-llvm-cov 0.8.7' || cargo install cargo-llvm-cov --version 0.8.7 --locked

coverage: coverage-tools
	cargo llvm-cov --all-targets --all-features --ignore-filename-regex 'tests/e2e/' --cobertura --output-path target/coverage.xml --fail-under-lines 90 -- --test-threads=1
	python3 tools/check_changed_coverage.py target/coverage.xml $(COVERAGE_BASE) 90

e2e:
	cargo test --test e2e -- --ignored --test-threads=1

check: coverage
	cargo fmt --all -- --check
	cargo clippy --all-targets --all-features -- -D warnings
