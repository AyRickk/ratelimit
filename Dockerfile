# The service's image (docs/design.md D33): a static binary at /bin/ratelimit on distroless,
# as upstream's, for linux/amd64 and linux/arm64. Cross-compiled with cargo-zigbuild on the
# build platform.
FROM --platform=$BUILDPLATFORM docker.io/library/rust:1.99.0-slim-bookworm@sha256:2c3a22f0a5533ea2dd5a16627bc841228151faa2d4de2644ac9987e4a2f1f2fa AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && printf '%s\n' \
        'ziglang==0.15.2 --hash=sha256:806316453d1ede7174ed3fefb8e5348154629089c3bb5fe812dfd0e172fac130 --hash=sha256:edc0aa60ec964a4cf462d40f68d7de242ddf37fd9a80f2afaee6397059463230' \
        > /tmp/zig.txt \
    && pip3 install --break-system-packages --no-cache-dir --require-hashes -r /tmp/zig.txt \
    && cargo install --locked cargo-zigbuild@0.24.0 \
    && rustup target add x86_64-unknown-linux-musl aarch64-unknown-linux-musl
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates crates
ARG TARGETARCH
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    case "$TARGETARCH" in \
      amd64) target=x86_64-unknown-linux-musl ;; \
      arm64) target=aarch64-unknown-linux-musl ;; \
      *) echo "unsupported architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac \
    && cargo zigbuild --release --locked -p ratelimit --bin ratelimit --target "$target" \
    && cp "target/$target/release/ratelimit" /ratelimit

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /ratelimit /bin/ratelimit
ENTRYPOINT ["/bin/ratelimit"]
