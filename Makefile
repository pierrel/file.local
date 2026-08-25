.PHONY: build install check test elisp-test test-make-prefix test-make-prefix-value coverage coverage-tools e2e

PREFIX ?=
override PREFIX := $(value PREFIX)
export PREFIX
COVERAGE_BASE ?= github/main

build:
	cargo build --release

install: build
	@prefix="$${PREFIX:-$$HOME/.local}"; \
	target/release/flocal daemon install "$$prefix/bin/flocal"

test:
	cargo test --all-targets -- --test-threads=1

elisp-test:
	cd emacs && eldev test

test-make-prefix:
	sh tests/make_prefix.sh

test-make-prefix-value:
	@printf '%s\n' "$$PREFIX"

coverage-tools:
	@cargo llvm-cov --version 2>/dev/null | grep -q 'cargo-llvm-cov 0.8.7' || cargo install cargo-llvm-cov --version 0.8.7 --locked

coverage: coverage-tools
	cargo llvm-cov --all-targets --all-features --ignore-filename-regex 'tests/e2e/' --cobertura --output-path target/coverage.xml --fail-under-lines 90 -- --test-threads=1
	python3 tools/check_changed_coverage.py target/coverage.xml $(COVERAGE_BASE) 90

e2e:
	cargo test --test e2e -- --ignored --test-threads=1 --skip scenarios::upgrades::legacy_

check: coverage test-make-prefix elisp-test
	cargo fmt --all -- --check
	cargo clippy --all-targets --all-features -- -D warnings
