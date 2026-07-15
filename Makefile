.PHONY: build install check test

PREFIX ?= $(HOME)/.local

build:
	cargo build --release

install: build
	install -d "$(PREFIX)/bin"
	install -m 755 target/release/flocal "$(PREFIX)/bin/flocal"

test:
	cargo test --all-targets

check:
	cargo fmt --all -- --check
	cargo clippy --all-targets --all-features -- -D warnings
	cargo test --all-targets
