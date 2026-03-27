# Risechain indexer

A POC high-performance Rise blockchain data streaming and indexing platform built for Rise chain.

## Features
-   Real-time + Backfill: WebSocket subscription + historical
    indexing
-   Caching Layer: Redis-based smart cache resolver
-   Lightweight Storage: SQLite for fast local querying and Web-UI support
-   RPC Proxy: JSON-RPC handler with caching support
-   Dockerized: Easy local setup with Docker Compose

## Architecture

### Sherds subcribe service

![Architecture](./images/rise_indexing.svg)

### RPC Request flow

![Request flow](./images/rise_request.svg)

# 📁 Project Structure

    rise-indexer/
    ├── database/              # SQLite service & storage
    ├── fetcher/               # Indexing logic
    │   ├── sherds_sub.rs      # Real-time WebSocket subscription
    │   └── backfill.rs        # Historical data indexing
    ├── redis/                 # Redis caching layer
    ├── rpc_service/           # JSON-RPC handler service
    │   |-- helper.rs          # Hepler handling transaction data
    |   |-- reqwest_builder.rs # RPC reqwest builder
    |── cache_resolver.rs      # Resolve redis cache logic
    │── config.rs              # Configuration 
    │── constants.rs           # Constant values
    │── main.rs                # Application entry point
    ├── docker-compose.yml     # Docker services definition
    └── .env.example           # Environment configuration example

## ⚙️ Prerequisites

-   Rust 1.80+
-   Docker & Docker Compose

## Quick start

### 1. Configure environment

```bash
cp .env.example .env
```

Setup env

```env
RISE_RPC_URL=https://testnet.riselabs.xyz
RISE_WS_URL=wss://testnet.riselabs.xyz/ws
SERVER_ADDR=0.0.0.0:8545
DB_PATH=/data/rise.db
REDIS_URL=redis://cache:3000
RUST_LOG=rise_indexer=info
```

### 2. Start indexer

```bash
docker compose up build -d
```

Services:

| Service   | Description                          | Port |
| --------- | ------------------------------------ | ---- |
| `indexer` | JSON-RPC proxy + background indexers | 8545 |
| `redis`   | Redis in-memory cache                | 6379 |
| `database`| SQLite Web browser UI                | 5432 |

### 3. Browse the Sqlite3-UI

Open [http://localhost:5432](http://localhost:5432) to inspect indexed transactions
and receipts via the SQLite Web UI.
