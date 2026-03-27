# build for Rust rise_indexer
FROM rust:slim-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Cache dependency compilation separately from application code.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo "fn main() {}" > src/main.rs
RUN cargo build --release --bin rise_indexer --locked


# Remove cache build
RUN rm -f target/release/rise_indexer target/release/deps/rise_indexer*

COPY src ./src
RUN cargo build --release --bin rise_indexer --locked


FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/rise_indexer /app/rise_indexer

# Mount database volume at /data.
VOLUME ["/data"]
EXPOSE 8545

CMD ["/app/rise_indexer"]
