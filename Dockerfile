# syntax=docker/dockerfile:1.7
FROM rust:1.96.1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev cmake clang libssl-dev pkg-config && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY vendor ./vendor
RUN --mount=type=cache,id=walleye-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=walleye-cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=walleye-cargo-target,target=/src/target \
    cargo build --locked -j 12 -p walleye-node -p walleye-workload -p walleye-bitr-server && \
    mkdir /out && cp target/debug/walleye-node target/debug/walleye-workload target/debug/walleye-bitr-server /out/
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/ /usr/local/bin/
EXPOSE 8080 9090 30080
CMD ["walleye-node"]
