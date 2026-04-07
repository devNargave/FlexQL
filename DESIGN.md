# FlexQL — Design Document

## Repository

> **Add your GitHub repository link here before submission.**

---

## 1. System Architecture

FlexQL is a client-server relational database implemented entirely in C11.

```
┌──────────────────────────┐     TCP (default port 9000)    ┌──────────────────────────────────┐
│     flexql-client        │  ──[4-byte len][SQL string]──► │         flexql-server            │
│  (REPL + libflexql.a)   │  ◄──[4-byte len][result]──────  │  Parser → Executor → Storage    │
└──────────────────────────┘                                 └──────────────────────────────────┘
```

### Wire Protocol

Every message is a 4-byte unsigned integer length header followed immediately by
the payload. Two modes are supported:

**Normal mode** — one SQL string → one response.

**Batch mode** — used by the high-performance bulk-insert path:
1. Client sends `BATCH:<n>` marker
2. Client sends `n` SQL strings (all in one `writev()` syscall)
3. Server sends `n` responses (all in one `net_send_batch()` call)

This collapses 50 × 1 000-row INSERTs from **50 round-trips down to 1**.

---

## 2. Storage Design

**Choice: row-major, singly-linked list with arena allocation.**

Each `Table` keeps a singly-linked list (`head → Row → Row → …`). Every
`Row` owns a heap-allocated `FieldVal[]` array — one entry per column. This
is classic row-major (NSM) layout.

### Why row-major?

| Criterion | Row-major ✅ | Column-major |
|-----------|-------------|--------------|
| Single-row INSERT | O(1) prepend | Append to N column arrays |
| Point lookup (PK) | O(1) via hash index | Same |
| Bulk INSERT (1 000 rows) | One `rwlock_wlock` | N per-column locks |
| Full scan `SELECT *` | Sequential linked-list | Better for aggregates |

OLTP workloads (many inserts, point queries, occasional scans) strongly favour
row-major layout.

### Arena allocator

Instead of `malloc()` per `Row` struct, rows are slab-allocated from
`Arena` blocks of 4 096 rows each. A single `calloc(sizeof(Arena))` serves
4 096 inserts. This eliminates per-row allocator overhead and improves cache
locality.

### Schema storage

Each `Table` embeds a `Schema` struct (array of `ColDef`) validated at
`INSERT` time. Column count and type coercion are enforced.

### Expiration timestamps

Each `Row` carries `expires_at` (`time_t`, 0 = never). An optional extra
value appended to `VALUES(…)` sets the expiry. A background thread sweeps
expired rows every 5 seconds under a per-table write-lock.

---

## 3. Indexing

**Data structure: open-addressing hash table with linear probing.**

`PrimaryIndex` maps `int64_t` primary-key values → `Row*` pointers using a
64-bit finalised hash (Murmur-inspired). Load factor is capped at 50%;
the table doubles automatically.

| Operation | Complexity |
|-----------|------------|
| Insert    | O(1) amortised |
| Lookup    | O(1) average |
| Delete    | O(1) with tombstone flag |

The index is used automatically when `WHERE pk_col = <literal>` targets an
INT primary key column with the `=` operator.

Initial capacity is 65 536 entries, pre-sized to avoid early rehashing during
bulk-insert workloads.

---

## 4. Caching Strategy

**Strategy: LRU (Least-Recently-Used) query-result cache.**

Implemented as a doubly-linked list (MRU at head, LRU at tail) plus an
open-addressing hash table for O(1) key lookup. Capacity: 2 048 entries.

**What is cached:** `SELECT *` queries with no `WHERE` clause and no `JOIN`
are cached keyed on the table name. The complete serialised result blob is
stored — a cache hit skips storage access entirely.

**Invalidation:** After every successful `INSERT`, `lru_invalidate_table()`
walks the cache and removes all entries whose key contains the table name.
This is O(n) in cache size but acceptable since writes are less frequent
than reads in the target workload.

**Why LRU:** Simple, correct, and provides excellent hit rates for workloads
with temporal locality (the same table queried repeatedly). The 4.3–5× cache
speedup measured confirms this.

---

## 5. Multithreading Design

### Thread pool

The server accepts connections in the main thread and dispatches each new
client to a fixed pool of `THREAD_POOL_SZ = 32` worker threads via a
mutex-protected FIFO task queue with `pthread_cond` signalling.

### Concurrency control

| Shared resource | Lock | Rationale |
|----------------|------|-----------|
| `Catalog.tables[]` | `pthread_rwlock` | Many concurrent readers; CREATE TABLE is rare |
| `Table.head` + arena | `pthread_rwlock` | Concurrent SELECTs read; INSERT and expiry write |
| `PrimaryIndex` | Protected by Table write-lock | Index only modified under same lock |
| `LRUCache` | `pthread_rwlock` | Reads promote LRU order (need write-lock) |

Read-write locks allow unlimited concurrent SELECTs to run in parallel —
the dominant case for a read-heavy workload. The bulk-insert path takes a
single write-lock for an entire batch of 1 000 rows.

### Background expiry thread

A dedicated thread sweeps expired rows every 5 seconds. It snapshots the
table pointer array under a catalog read-lock, then acquires each table's
write-lock individually, minimising contention with query workers.

---

## 6. High-Performance INSERT Path

The bulk-insert design eliminates three sources of overhead that made the
naive implementation too slow for 10 million rows:

### 1 — Zero-malloc INSERT parser

The new `parse_insert_fast()` function directly scans the SQL string in two
forward passes without allocating a token array. A single `malloc` covers all
value strings (flat buffer) plus a pointer index table.

**Result:** Parser throughput went from **19 K rows/sec → 5.7 M rows/sec**
(295× speedup) on a single core.

### 2 — Multi-row INSERT syntax

```sql
INSERT INTO t VALUES (v1a,v1b,...),(v2a,v2b,...), ... ,(vNa,vNb,...);
```

One SQL string carries 1 000 tuples. The executor processes all 1 000 rows
under a **single `rwlock_wlock`** and with a **single `lru_invalidate_table`**
call. Per-batch lock overhead drops from 1 000 lock acquisitions to 1.

### 3 — Batch protocol (pipeline)

The client sends 50 multi-row INSERT statements in **one `writev()` syscall**
and receives 50 responses in one pass. This reduces network round-trips from
10 000 000 (naive) to **200** for a 10 million row load.

### Combined throughput

| Configuration | INSERT rate | 10 M rows |
|--------------|------------|-----------|
| Original (1 row / RTT) | ~2 K rows/sec | ~1.4 hours |
| After optimisation | **~180 K rows/sec** | **~55 seconds** |

---

## 7. Client API

| Function | Description |
|----------|-------------|
| `flexql_open(host, port, &db)` | Establish TCP connection, allocate opaque handle |
| `flexql_close(db)` | Close socket, free handle |
| `flexql_exec(db, sql, cb, arg, &errmsg)` | Execute SQL, invoke `cb` per result row |
| `flexql_free(ptr)` | Free memory allocated by the API |
| `flexql_exec_batch(db, sqls, n, &errmsg)` | Send N SQLs in one RTT (batch protocol) |

The `FlexQL*` handle is an opaque `typedef struct FlexQL FlexQL` — the
socket fd is not visible to users.

---

## 8. Supported SQL Subset

| Statement | Supported |
|-----------|-----------|
| `CREATE TABLE t (col TYPE [PRIMARY KEY] [NOT NULL], …)` | ✅ |
| Types: `INT`, `DECIMAL`, `VARCHAR`, `DATETIME`, `TEXT`, `FLOAT`, … | ✅ |
| `INSERT INTO t VALUES (v1,v2,…)` | ✅ |
| `INSERT INTO t VALUES (…),(…),(…)` — multi-row | ✅ |
| `SELECT * FROM t` | ✅ |
| `SELECT col1, col2 FROM t` | ✅ |
| `SELECT … WHERE col op val` (=, <, >, <=, >=) | ✅ single condition |
| `SELECT … INNER JOIN t2 ON t1.col = t2.col` | ✅ |
| `SELECT … INNER JOIN … WHERE …` | ✅ |
| `AND` / `OR` in WHERE | ✗ not required |
| `UPDATE` / `DELETE` | ✗ not required |

---

## 9. Build & Run

### Requirements

- GCC ≥ 9, C11 support
- `libpthread`
- `libreadline-dev` (optional, for REPL command history)

### Build

```bash
cd flexql
make all          # server, client lib, REPL, benchmark, unit tests
make test         # run 51 unit tests
```

### Run

```bash
# Start server (terminal 1)
./bin/flexql-server 9000

# Start server with persistence enabled (WAL)
mkdir -p ./persist
FLEXQL_PERSIST_DIR=./persist ./bin/flexql-server 9000

# Interactive REPL (terminal 2)
./bin/flexql-client 127.0.0.1 9000

# Benchmark — 10 million rows
./bin/flexql-bench 127.0.0.1 9000 10000000
```

### Official C++ benchmark integration (benchmark_flexql.cpp)

The upstream benchmark file `benchmark_flexql.cpp` can be compiled as C++ and linked
against the FlexQL C client API (`include/flexql.h` + `bin/libflexql.a`).

```bash
# Build FlexQL
make clean
make -j

# Terminal 1: start a fresh server (recommended: no persistence for benchmark)
kill -9 $(pidof flexql-server) 2>/dev/null || true
FLEXQL_NO_EXPIRY=1 ./bin/flexql-server 9000

# Terminal 2: compile benchmark
g++ -O3 -march=native -std=c++17 -Iinclude benchmark_flexql.cpp bin/libflexql.a -lpthread -o bin/benchmark_flexql_cpp

# Run unit tests
./bin/benchmark_flexql_cpp --unit-test

# Run 10M benchmark
./bin/benchmark_flexql_cpp 10000000
```

Note: run the benchmark on a fresh server. Re-running without restarting the server can
fail with `Table already exists` and may show duplicated rows in SELECT validations.

### REPL commands

```
flexql> CREATE TABLE STUDENT(ID INT PRIMARY KEY NOT NULL, NAME VARCHAR NOT NULL);
flexql> INSERT INTO STUDENT VALUES (1,'Alice');
flexql> SELECT * FROM STUDENT;
flexql> .help
flexql> .exit
```

---

## 10. Measured Performance Results

All measurements on localhost (loopback), single client connection.

| Metric | Result |
|--------|--------|
| INSERT rate (batch mode) | **~180 K rows/sec** |
| Time for 10 M rows | **~55 seconds** |
| SELECT * full scan (1 M rows) | **~1.1 sec (~900 K rows/sec)** |
| SELECT * cache hit (1 M rows) | **~220 ms (~4.5 M rows/sec, 5× speedup)** |
| Point lookup (WHERE PK = x) | **< 1 ms** (index bypass for small tables) |
| Parser throughput | **5.7 M rows/sec** (zero-malloc scanner) |

---

## 11. Folder Structure

```
flexql/
├── bin/                      compiled binaries
├── build/                    object files
├── include/
│   ├── flexql.h              public API (4 required functions + batch extension)
│   ├── common/types.h        ColDef, Row, Schema, FieldVal, constants
│   ├── network/net.h         TCP connect/listen, batch send/recv
│   ├── parser/parser.h       AST types, parse_sql(), stmt_free()
│   ├── storage/storage.h     catalog, table, bulk insert API
│   ├── storage/storage_internal.h  Table/Catalog struct definitions
│   ├── index/index.h         PrimaryIndex hash table
│   ├── cache/cache.h         LRU cache
│   ├── query/executor.h      ExecCtx, QueryResult, exec_stmt()
│   └── concurrency/threadpool.h   ThreadPool, RWLock wrappers
├── src/
│   ├── client/client.c       libflexql.a implementation + REPL binary
│   ├── server/server.c       TCP server, BATCH protocol, expiry thread
│   ├── network/net.c         writev-based send, batch framing
│   ├── parser/parser.c       zero-malloc INSERT scanner + general tokeniser
│   ├── storage/storage.c     arena allocator, row-major table engine
│   ├── index/index.c         open-addressing hash index
│   ├── cache/cache.c         LRU doubly-linked list + hash map
│   ├── query/executor.c      CREATE/INSERT/SELECT/JOIN execution engine
│   └── concurrency/threadpool.c   POSIX thread pool
├── tests/
│   ├── unit_test.c           51 unit tests (parser, index, cache, executor)
│   └── bench.c               10 M-row benchmark client
├── Makefile
└── DESIGN.md                 this file
```
