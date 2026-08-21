# One image serves both peers. Bases are pinned by manifest-list digest;
# the tags in comments name them for humans. Dependency layers are cached
# by building against a dummy source tree first, which works on both the
# legacy builder and BuildKit.

# rust:1-bookworm
FROM rust@sha256:77fac8b98f9f46062bb680b6d25d5bcaabfc400143952ebc572e924bcbedc3fa AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo 'fn main() {}' > src/main.rs \
    && touch src/lib.rs \
    && cargo build --release --locked --features e2e-test-hooks \
    && rm -rf src target/release/flocal target/release/deps/flocal*
COPY src ./src
# COPY preserves context mtimes, which predate the dummy build above; touch
# so cargo rebuilds the real sources instead of reusing the dummy library.
RUN touch src/main.rs src/lib.rs && cargo build --release --locked --features e2e-test-hooks

# debian:bookworm-slim
FROM debian@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818
RUN apt-get update \
    && apt-get install -y --no-install-recommends openssh-server openssh-client bash \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /run/sshd \
    && ssh-keygen -A \
    && useradd --create-home --shell /bin/bash peer
COPY --from=builder /build/target/release/flocal /usr/local/libexec/flocal-real
COPY tests/e2e/flocal-wrapper.sh /usr/local/bin/flocal
COPY tests/e2e/systemctl /usr/local/bin/systemctl
COPY tests/e2e/entrypoint.sh /usr/local/bin/e2e-entrypoint
RUN chmod 755 /usr/local/bin/flocal /usr/local/bin/systemctl /usr/local/bin/e2e-entrypoint /usr/local/libexec/flocal-real
ENTRYPOINT ["/usr/local/bin/e2e-entrypoint"]
