.PHONY: build install check test coverage coverage-tools

PREFIX ?= $(HOME)/.local
COVERAGE_BASE ?= github/main

build:
	cargo build --release

install: build
	install -d "$(PREFIX)/bin"
	install -m 755 target/release/flocal "$(PREFIX)/bin/flocal"

test:
	cargo test --all-targets

coverage-tools:
	@cargo llvm-cov --version 2>/dev/null | grep -q 'cargo-llvm-cov 0.8.7' || cargo install cargo-llvm-cov --version 0.8.7 --locked

coverage: coverage-tools
	cargo llvm-cov --all-targets --all-features --cobertura --output-path target/coverage.xml --fail-under-lines 90
	python3 tools/check_changed_coverage.py target/coverage.xml $(COVERAGE_BASE) 90

check: coverage
	cargo fmt --all -- --check
	cargo clippy --all-targets --all-features -- -D warnings
