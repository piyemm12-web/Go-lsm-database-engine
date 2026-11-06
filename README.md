# My-KV-Store (LSM-Tree Key-Value Database)

A fast, concurrent, and crash-resilient **Log-Structured Merge-tree (LSM-Tree)** key-value storage engine engineered from scratch in **Go**.

The project features a full database storage engine (WAL, MemTable, SSTables, Compaction), an interactive CLI shell, a RESTful HTTP service with dynamic IP rate limiting (supporting multiple algorithms), and a modern embedded web control center dashboard.

---

## Table of Contents

- [Overview](#overview)
- [System Architecture](#system-architecture)
- [How I Built This Project](#how-i-built-this-project)
  - [1. Write Path & Write-Ahead Log (WAL)](#1-write-path--write-ahead-log-wal)
  - [2. In-Memory Layer (SkipList MemTable)](#2-in-memory-layer-skiplist-memtable)
  - [3. On-Disk SSTables (Sorted String Tables)](#3-on-disk-sstables-sorted-string-tables)
  - [4. Read Path & Tombstone Deletions](#4-read-path--tombstone-deletions)
  - [5. Background Compaction Engine](#5-background-compaction-engine)
  - [6. Multi-Strategy Dynamic Rate Limiting](#6-multi-strategy-dynamic-rate-limiting)
  - [7. Embedded Web UI Dashboard](#7-embedded-web-ui-dashboard)
- [What I Used (Tech Stack & Concepts)](#what-i-used-tech-stack--concepts)
- [Project Directory Structure](#project-directory-structure)
- [How to Run This](#how-to-run-this)
  - [Prerequisites](#prerequisites)
  - [Option A: Running the REST API & Web Dashboard](#option-a-running-the-rest-api--web-dashboard)
  - [Option B: Running the Interactive CLI REPL](#option-b-running-the-interactive-cli-repl)
  - [Building Standalone Executables](#building-standalone-executables)
  - [Running the Test Suite](#running-the-test-suite)
- [HTTP REST API Reference](#http-rest-api-reference)

---

## Overview

Traditional B-Tree databases suffer from random disk I/O when processing heavy write workloads. This project implements a **Log-Structured Merge-tree (LSM-Tree)** engine—the same fundamental storage architecture behind modern distributed stores like Google Bigtable, Apache Cassandra, RocksDB, and LevelDB.

All incoming writes are appended sequentially to a persistent log and inserted into an in-memory SkipList. Once full, in-memory buffers are flushed to immutable disk blocks (SSTables) while background compaction manages disk space and eliminates stale or deleted records.

---

## System Architecture

```
                      ┌────────────────────────────────────────┐
                      │          HTTP REST API & Web UI        │
                      │       (with Dynamic IP Rate Limiter)   │
                      └──────────────────┬─────────────────────┘
                                         │
                                         ▼
                               ┌──────────────────┐
                               │  Engine DB Layer │
                               └─────────┬────────┘
                                         │
                   ┌─────────────────────┴─────────────────────┐
                   ▼                                           ▼
      ┌─────────────────────────┐                 ┌─────────────────────────┐
      │  Write-Ahead Log (WAL)  │                 │    MemTable (SkipList)   │
      │   (CRC32 Append-Only)   │                 │     (In-Memory O(logN))  │
      └─────────────────────────┘                 └────────────┬────────────┘
                                                               │ (When Full)
                                                               ▼
                                                  ┌─────────────────────────┐
                                                  │    Immutable MemTable   │
                                                  └────────────┬────────────┘
                                                               │ (Flush to Disk)
                                                               ▼
                                                  ┌─────────────────────────┐
                                                  │     SSTables (.db)      │
                                                  │  [Data Blocks + Index]  │
                                                  └────────────┬────────────┘
                                                               │
                                                               ▼
                                                  ┌─────────────────────────┐
                                                  │  Background Compaction  │
                                                  │   (Merge & Tombstones)  │
                                                  └─────────────────────────┘
```

---

## How I Built This Project

The project is designed modularly under the `internal/` package with zero third-party dependencies.

### 1. Write Path & Write-Ahead Log (WAL)
- Located in `internal/wal/`
- Every `Put` and `Delete` mutation is first written to an append-only disk log before updating in-memory state.
- **Data Integrity**: Each entry is serialized with a 4-byte CRC32 IEEE checksum, 4-byte key length, 4-byte value length, and the binary key/value payloads.
- **Crash Recovery**: When opening the database via `db.Open()`, the engine scans `wal.log`, verifies record checksums, and replays active state back into memory. Corrupted or partially written records are safely rejected.

### 2. In-Memory Layer (SkipList MemTable)
- Located in `internal/memtable/`
- Implemented a probabilistic **SkipList** data structure providing average $O(\log n)$ insertion, search, and deletion complexity.
- Keeps all live keys in strictly sorted lexicographical order, making flushing to disk sequential and extremely fast.
- Once the active MemTable reaches `MaxMemtableSize` (default: 4MB), it is converted into a read-only `immutable` MemTable and a background flush task writes it to disk.

### 3. On-Disk SSTables (Sorted String Tables)
- Located in `internal/sstable/`
- SSTables are immutable files storing sorted key-value pairs divided into:
  - **Data Blocks**: Length-prefixed keys and values.
  - **Index Block**: Located at the end of the file, storing keys and their exact byte offsets.
  - **Footer Metadata**: Contains the index offset, total index entries, and a 4-byte Magic Number (`0x48564B53`) to validate format integrity.
- SSTables support point lookups via binary search over the in-memory or block-indexed keys.

### 4. Read Path & Tombstone Deletions
- Located in `internal/db/db.go`
- Queries search through memory and disk in chronological order:
  1. Active **MemTable** (latest writes)
  2. **Immutable MemTable** (flushing to disk)
  3. **SSTables** ordered newest to oldest
- **Deletions (Tombstones)**: Deleting a key inserts a tombstone marker (zero-length value). If a tombstone is encountered during a read traversal, the engine returns `ErrKeyNotFound` immediately without scanning older SSTables.

### 5. Background Compaction Engine
- Located in `internal/compaction/`
- Over time, SSTables accumulate multiple versions of updated keys and obsolete tombstones, causing read amplification.
- The `CompactionManager` runs as an asynchronous background worker. When multiple SSTable segments are detected, it executes a multi-way merge sort:
  - Consolidates duplicate keys, keeping only the latest version.
  - Purges deleted keys/tombstones.
  - Produces clean, merged SSTable files and atomically deletes old segments.

### 6. Multi-Strategy Dynamic Rate Limiting
- Located in `internal/middleware/ratelimit.go`
- A concurrency-safe HTTP middleware that protects database endpoints per client IP.
- Implemented **3 swappable rate limiting algorithms**:
  1. **Sliding Window Counter**: Smooths traffic boundaries by calculating a weighted sum of requests between the previous and current time window.
  2. **Leaky Bucket**: Enforces a constant output processing rate using a virtual drip queue to eliminate traffic spikes.
  3. **Token Bucket**: Allows controlled traffic bursts by refilling tokens at a steady rate up to capacity.
- **Runtime Reconfiguration**: Algorithms, request limits, and window intervals can be changed dynamically on the fly via the REST API or Web Dashboard without restarting the server.
- **Atomic Telemetry**: Tracks requests allowed, requests rejected, and per-IP client counts with atomic counters.

### 7. Embedded Web UI Dashboard
- Located in `cmd/kv-http-server/static/`
- A high-contrast, cyberpunk/glassmorphic single-page dashboard built with responsive CSS and vanilla JavaScript.
- Embedded directly into the Go server binary using `//go:embed static/*` (`io/fs`), producing a single self-contained executable.
- Allows operators to perform live CRUD operations, view database health, observe real-time rate limiter metrics, and dynamically swap limiter strategies.

---

## What I Used (Tech Stack & Concepts)

| Category | Technology / Technique | Purpose |
| :--- | :--- | :--- |
| **Language** | **Go (Golang 1.26+)** | High performance, memory safety, strong concurrency primitives |
| **Standard Library** | `net/http` | REST API server, custom handlers, and HTTP routing |
| **Asset Embedding** | `embed` (`embed.FS`) | Bundles HTML/CSS/JS frontend into the compiled binary |
| **Concurrency** | `sync`, `sync/atomic` | `RWMutex` for database locks; atomic operations for rate-limit metrics |
| **Integrity & Checksums** | `hash/crc32` (IEEE) | WAL corruption detection and data recovery validation |
| **Binary Serialization** | `encoding/binary`, `bytes` | Custom byte layouts for SSTable blocks, indices, and WAL frames |
| **Frontend UI** | HTML5, Modern CSS3, JavaScript | Interactive control center dashboard (zero npm/framework overhead) |
| **Core Architecture** | **LSM-Tree** | High write-throughput storage engine design |
| **Data Structures** | **SkipList** | Probabilistic sorted in-memory index ($O(\log n)$ operations) |
| **Algorithms** | **Merge Sort, Token/Leaky Bucket** | SSTable multi-way compaction & rate limiting algorithms |
| **Zero Dependencies** | **100% Native Go** | No external third-party Go modules or heavy frameworks |

---

## Project Directory Structure

```text
my-kvstore/
├── cmd/
│   ├── kv-server/                 # Interactive CLI REPL application
│   │   └── main.go
│   └── kv-http-server/            # REST API server & Web Dashboard entrypoint
│       ├── main.go
│       └── static/                # Embedded web frontend assets
│           └── index.html         # Glassmorphic control center UI
├── internal/
│   ├── db/                        # Database coordinator (Open, Put, Get, Delete, recovery)
│   │   ├── db.go
│   │   └── options.go
│   ├── memtable/                  # In-memory storage layer
│   │   ├── memtable.go            # MemTable wrapper & flush logic
│   │   └── skiplist.go            # Probabilistic SkipList implementation
│   ├── wal/                       # Write-Ahead Log
│   │   ├── wal.go                 # WAL manager & replay
│   │   └── entry.go               # CRC32 serialization & deserialization
│   ├── sstable/                   # Disk storage layer
│   │   ├── block.go               # SSTable block and index definition
│   │   ├── builder.go             # SSTable file serializer
│   │   └── reader.go              # SSTable file reader & lookup
│   ├── compaction/                # Background merge & compaction
│   │   └── compaction.go          # Multi-way SSTable merge manager
│   ├── middleware/                # Rate limiter & security middleware
│   │   ├── ratelimit.go           # Sliding Window, Leaky Bucket, Token Bucket
│   │   └── ratelimit_test.go      # Concurrency & algorithm unit tests
│   └── server/                    # HTTP REST server routes & handlers
│       ├── server.go              # CRUD endpoints, metrics, and health handlers
│       └── server_test.go         # REST API unit tests
├── data/                          # Default directory for WAL logs and SSTable files
├── go.mod                         # Go module definition
└── README.md                      # Documentation
```

---

## How to Run This

### Prerequisites
- **Go 1.22+** (tested with Go 1.26+) installed on your machine.
- Verify installation by running:
  ```bash
  go version
  ```

---

### Option A: Running the REST API & Web Dashboard

To launch the HTTP server with the embedded web dashboard and rate limiter:

```bash
go run ./cmd/kv-http-server
```

#### Custom Flags:
You can customize the port, storage directory, default rate limit, and window:

```bash
go run ./cmd/kv-http-server -port 8080 -data-dir ./data -strategy sliding_window -limit 20 -window 60
```

| Flag | Default | Description |
| :--- | :--- | :--- |
| `-port` | `8080` | Port for the HTTP server to listen on |
| `-data-dir` | `./data` | Directory where WAL and SSTable files are saved |
| `-strategy` | `sliding_window` | Rate limiter: `sliding_window`, `leaky_bucket`, or `token_bucket` |
| `-limit` | `10` | Max requests allowed within the window |
| `-window` | `60` | Rate limiter window duration in seconds |

#### Access the Web Dashboard:
Once started, open your browser and navigate to:
```
http://localhost:8080
```
From here you can read/write keys, inspect live rate limiter metrics, trigger requests, and switch rate-limiting algorithms.

---

### Option B: Running the Interactive CLI REPL

If you want a direct terminal interface without HTTP:

```bash
go run ./cmd/kv-server
```

#### Available CLI Commands:
```text
=== Starting Key-Value Store Engine ===
Database ready! Available commands:
  SET <key> <value>
  GET <key>
  DELETE <key>
  EXIT
-------------------------------------
kv> SET user:101 Piyush
OK
kv> GET user:101
Piyush
kv> DELETE user:101
OK
kv> GET user:101
(nil) - key not found
kv> EXIT
```

---

### Building Standalone Executables

Compile self-contained binary executables for distribution:

```bash
# Build the HTTP server with embedded frontend
go build -o kv-http-server.exe ./cmd/kv-http-server

# Build the CLI shell
go build -o kv-server.exe ./cmd/kv-server
```

---

### Running the Test Suite

Run unit and integration tests across all packages:

```bash
go test -v ./...
```

---

## HTTP REST API Reference

All `/api/` endpoints are protected by the rate limiter middleware.

### 1. Store a Key-Value Pair
- **Endpoint**: `POST /api/kv`
- **Request Body**:
  ```json
  {
    "key": "session_token",
    "value": "xyz987abc123"
  }
  ```
- **cURL Example**:
  ```bash
  curl -X POST http://localhost:8080/api/kv \
    -H "Content-Type: application/json" \
    -d "{\"key\": \"session_token\", \"value\": \"xyz987abc123\"}"
  ```

### 2. Retrieve a Key
- **Endpoint**: `GET /api/kv/{key}` or `GET /api/kv?key={key}`
- **cURL Example**:
  ```bash
  curl http://localhost:8080/api/kv/session_token
  ```
- **Response**:
  ```json
  {
    "key": "session_token",
    "value": "xyz987abc123"
  }
  ```

### 3. Update an Existing Key
- **Endpoint**: `PUT /api/kv/{key}`
- **Request Body**:
  ```json
  {
    "value": "new_updated_token"
  }
  ```
- **cURL Example**:
  ```bash
  curl -X PUT http://localhost:8080/api/kv/session_token \
    -H "Content-Type: application/json" \
    -d "{\"value\": \"new_updated_token\"}"
  ```

### 4. Delete a Key
- **Endpoint**: `DELETE /api/kv/{key}`
- **cURL Example**:
  ```bash
  curl -X DELETE http://localhost:8080/api/kv/session_token
  ```

### 5. Inspect Rate Limiter Metrics
- **Endpoint**: `GET /api/metrics`
- **Response**:
  ```json
  {
    "active_strategy": "sliding_window",
    "limit": 10,
    "window_seconds": 60,
    "total_requests": 142,
    "allowed_requests": 139,
    "rejected_requests": 3,
    "unique_ips": 1
  }
  ```

### 6. Dynamically Update Rate Limiter Configuration
- **Endpoint**: `POST /api/ratelimit/config`
- **Request Body**:
  ```json
  {
    "strategy": "token_bucket",
    "limit": 50,
    "window_seconds": 30
  }
  ```
- **cURL Example**:
  ```bash
  curl -X POST http://localhost:8080/api/ratelimit/config \
    -H "Content-Type: application/json" \
    -d "{\"strategy\": \"token_bucket\", \"limit\": 50, \"window_seconds\": 30}"
  ```

### 7. Health Check
- **Endpoint**: `GET /api/health`
- **Response**:
  ```json
  {
    "status": "healthy",
    "engine": "lsm-tree"
  }
  ```
