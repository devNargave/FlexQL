# FlexQL — Lightweight In-Memory Relational Database

**FlexQL** is a client-server relational database written in C11. It supports a subset of SQL, a LRU query cache, a WAL-based persistence layer, multi-threaded request handling via a thread pool, and a high-performance batch INSERT path capable of loading **10 million rows in ~55 seconds**.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Build](#build)
3. [Run the Server](#run-the-server)
4. [Connect with the REPL Client](#connect-with-the-repl-client)
5. [Basic SQL Commands](#basic-sql-commands)
6. [Testing Multithreading](#testing-multithreading)
7. [Testing Persistence & Fault Tolerance (WAL)](#testing-persistence--fault-tolerance-wal)
8. [Running the Benchmark](#running-the-benchmark)
9. [Running Unit Tests](#running-unit-tests)
10. [Folder Structure](#folder-structure)

---

## Quick Start

```bash
# 1. Build everything
make all

# 2. Start the server (Terminal 1)
./bin/flexql-server

# 3. Open the REPL client (Terminal 2)
./bin/flexql-client 127.0.0.1 9000

# 4. Run SQL
flexql> CREATE TABLE STUDENT(ID INT PRIMARY KEY NOT NULL, NAME VARCHAR NOT NULL);
flexql> INSERT INTO STUDENT VALUES (1,'Alice');
flexql> SELECT * FROM STUDENT;
```

---

## Running the official C++ benchmark (benchmark_flexql.cpp)

The upstream benchmark file `benchmark_flexql.cpp` can be compiled as C++ and linked against the FlexQL C client API (`include/flexql.h` + `bin/libflexql.a`).

### Step 1 — Build FlexQL

```bash
make clean
make -j
```

### Step 2 — Start server (Terminal 1)

Recommended for benchmark runs: disable expiry and do not enable persistence to keep a clean state.

```bash
kill -9 $(pidof flexql-server) 2>/dev/null || true
FLEXQL_NO_EXPIRY=1 ./bin/flexql-server
```

### Step 3 — Compile benchmark_flexql.cpp (Terminal 2)

```bash
g++ -O3 -march=native -std=c++17 -Iinclude benchmark_flexql.cpp bin/libflexql.a -lpthread -o bin/benchmark_flexql_cpp
```

### Step 4 — Run unit tests (Terminal 2)

```bash
./bin/benchmark_flexql_cpp --unit-test
```

### Step 5 — Run 10M insert benchmark (Terminal 2)

```bash
./bin/benchmark_flexql_cpp 10000000
```

Note: run the benchmark on a fresh server. Re-running without restarting the server can fail with `Table already exists` and may show duplicated rows in SELECT validations.

---

## Build

### Requirements

| Dependency | Version |
|---|---|
| GCC | ≥ 9 (C11) |
| libpthread | system |
| libreadline-dev | optional (REPL history) |

```bash
cd flexql_optimized_final
make all        # builds server, client, benchmark binary, unit tests
make test       # runs all 51 unit tests
make clean      # removes build artefacts
```

Binaries are placed in `bin/`:

| Binary | Purpose |
|---|---|
| `bin/flexql-server` | TCP database server |
| `bin/flexql-client` | Interactive REPL |
| `bin/flexql-bench` | 10 M-row benchmark |
| `bin/unit_test` | Unit test runner |

---

## Run the Server

```bash
./bin/flexql-server [port]

# Examples
./bin/flexql-server                         # in-memory only (default port 9000)
./bin/flexql-server 9001                    # in-memory only (custom port)
```

The server listens on TCP `0.0.0.0:<port>`. A 32-worker thread pool handles concurrent connections.

### Enable WAL persistence

Persistence is enabled via the `FLEXQL_PERSIST_DIR` environment variable:

```bash
mkdir -p ./persist
FLEXQL_PERSIST_DIR=./persist ./bin/flexql-server
```

This creates/uses `./persist/flexql.wal`.

---

## Connect with the REPL Client

```bash
./bin/flexql-client <host> <port>

# Example
./bin/flexql-client 127.0.0.1 9000
```

### REPL Meta-Commands

| Command | Description |
|---|---|
| `.help` | Show available meta-commands |
| `.clear` | Reset multi-line SQL buffer |
| `.exit` or `.quit` | Disconnect and exit |

---

## Basic SQL Commands

### Create a Table

```sql
CREATE TABLE employees (
    id      INT          PRIMARY KEY NOT NULL,
    name    VARCHAR      NOT NULL,
    salary  DECIMAL,
    joined  DATETIME
);
```

Supported types: `INT`, `DECIMAL`, `FLOAT`, `VARCHAR`, `TEXT`, `DATETIME`

### Insert a Single Row

```sql
INSERT INTO employees VALUES (1, 'Alice', 95000.00, '2022-01-15');
```

### Insert Multiple Rows (fast path)

```sql
INSERT INTO employees VALUES
  (2, 'Bob',   88000.00, '2021-06-01'),
  (3, 'Carol', 102000.00, '2023-03-20');
```

### Insert with Expiry (TTL)

Append expiry timestamp (Unix epoch) as the last value:

```sql
INSERT INTO sessions VALUES (42, 'token_xyz', 1800000000);
```

### Select All Rows

```sql
SELECT * FROM employees;
```

### Select Specific Columns

```sql
SELECT id, name FROM employees;
```

### Select with WHERE Filter

```sql
SELECT * FROM employees WHERE salary > 90000;
SELECT * FROM employees WHERE id = 1;
```

Supported operators: `=`, `<`, `>`, `<=`, `>=`

### Inner Join

```sql
SELECT employees.name, departments.dept_name
FROM employees
INNER JOIN departments ON employees.id = departments.emp_id;
```

### Inner Join with WHERE

```sql
SELECT e.name, d.dept_name
FROM employees e
INNER JOIN departments d ON e.id = d.emp_id
WHERE e.salary > 90000;
```

---

## Testing Multithreading

FlexQL uses a 32-worker thread pool (`THREAD_POOL_SZ = 32`). To verify concurrent correctness:

### Step 1 — Start the server

```bash
./bin/flexql-server 9000
```

### Step 2 — Open multiple REPL sessions simultaneously

Open 4 terminals and run the REPL in each:

```bash
# Each terminal:
./bin/flexql-client 127.0.0.1 9000
```

### Step 3 — Create the shared table (from any client)

```sql
CREATE TABLE counter (id INT PRIMARY KEY NOT NULL, val INT NOT NULL);
INSERT INTO counter VALUES (1, 0);
```

### Step 4 — Run concurrent inserts from all clients

Each terminal inserts its own rows concurrently:

```sql
-- Client 1
INSERT INTO counter VALUES (101, 1),(102, 2),(103, 3);

-- Client 2
INSERT INTO counter VALUES (201, 1),(202, 2),(203, 3);

-- Client 3
INSERT INTO counter VALUES (301, 1),(302, 2),(303, 3);

-- Client 4
INSERT INTO counter VALUES (401, 1),(402, 2),(403, 3);
```

### Step 5 — Verify no rows are lost

```sql
SELECT * FROM counter;
```

All rows from all clients should appear. The `pthread_rwlock` on each table guarantees safe concurrent access — multiple readers run in parallel; writers acquire exclusive locks.

### Automated Concurrent Benchmark

```bash
# Run the benchmark with concurrency built in
./bin/flexql-bench 127.0.0.1 9000 1000000
```

The benchmark spawns parallel workers to stress the thread pool. Watch server stdout for worker thread IDs handling requests concurrently.

---

## Testing Persistence & Fault Tolerance (WAL)

FlexQL uses an asynchronous Write-Ahead Log (WAL) stored in `persist/flexql.wal`. When enabled, `CREATE TABLE` and `INSERT` operations are appended to the WAL so that data survives server restarts.

### Step 1 — Start the server with persistence enabled

```bash
mkdir -p ./persist
FLEXQL_PERSIST_DIR=./persist ./bin/flexql-server
```

### Step 2 — Insert data

```bash
./bin/flexql-client 127.0.0.1 9000
```

```sql
CREATE TABLE orders (id INT PRIMARY KEY NOT NULL, item VARCHAR NOT NULL, qty INT);
INSERT INTO orders VALUES (1, 'Widget', 100);
INSERT INTO orders VALUES (2, 'Gadget', 50);
INSERT INTO orders VALUES (3, 'Doohickey', 200);
SELECT * FROM orders;
-- Should show 3 rows
```

### Step 3 — Simulate a crash (kill the server)

```bash
# In the server terminal, press Ctrl+C  — or —
pkill -9 flexql-server
```

### Step 4 — Restart the server with the same persist directory

```bash
FLEXQL_PERSIST_DIR=./persist ./bin/flexql-server
```

### Step 5 — Reconnect and verify data survived

```bash
./bin/flexql-client 127.0.0.1 9000
```

```sql
SELECT * FROM orders;
-- All 3 rows must be present — replayed from WAL
```

### Step 6 — Verify WAL file contents (optional)

```bash
ls -lh ./persist/flexql.wal
# Should be non-empty; contains binary records for CREATE and INSERT ops
```

### WAL Fault-Tolerance Properties

| Scenario | Expected behaviour |
|---|---|
| Server killed mid-insert | Partial batch not in WAL is silently dropped; committed rows restored |
| WAL directory missing | Server prints error and runs in-memory mode |
| WAL file corrupted | Replay stops at bad record; prior rows still recovered |
| Normal shutdown + restart | Full replay; all data restored |

---

## Running the Benchmark

```bash
# Benchmark 10 million rows
./bin/flexql-bench 127.0.0.1 9000 10000000

# Benchmark 1 million rows (faster)
./bin/flexql-bench 127.0.0.1 9000 1000000
```

Expected output on localhost:

```
Inserting 10000000 rows in batch mode...
Done in 55.2 s  →  181 234 rows/sec
SELECT * (1 M rows, cold):  1.08 s
SELECT * (1 M rows, cached): 0.22 s   (4.9× speedup)
Point lookup WHERE id=500000: 0.31 ms
```

---

## Running Unit Tests

```bash
make test
# or directly:
./bin/unit_test
```

51 tests cover: parser, zero-malloc INSERT scanner, index hash operations, LRU cache eviction, and executor correctness.

---

## Folder Structure

```
flexql_optimized_final/
├── bin/                        compiled binaries
├── build/                      object files (.o)
├── include/
│   ├── flexql.h                public C API (flexql_open/close/exec/batch)
│   ├── common/types.h          Row, Schema, ColDef, FieldVal types
│   ├── network/net.h           TCP send/recv, batch framing
│   ├── parser/parser.h         AST types, parse_sql(), stmt_free()
│   ├── storage/storage.h       catalog and table API
│   ├── storage/storage_internal.h  Table/Catalog struct internals
│   ├── index/index.h           PrimaryIndex hash table
│   ├── cache/cache.h           LRU cache interface
│   ├── query/executor.h        ExecCtx, QueryResult, exec_stmt()
│   └── concurrency/threadpool.h    ThreadPool, RWLock wrappers
├── src/
│   ├── client/client.c         libflexql.a + REPL binary
│   ├── server/server.c         TCP server + BATCH protocol + expiry thread
│   ├── network/net.c           writev-based send, batch framing
│   ├── parser/parser.c         zero-malloc INSERT scanner + tokeniser
│   ├── storage/storage.c       arena allocator, row-major table engine
│   ├── storage/persistence.c   WAL write thread + replay-on-startup
│   ├── index/index.c           open-addressing hash index
│   ├── cache/cache.c           LRU doubly-linked list + hash map
│   ├── query/executor.c        CREATE/INSERT/SELECT/JOIN execution
│   └── concurrency/threadpool.c    POSIX thread pool (16 workers)
├── tests/
│   ├── unit_test.c             51 unit tests
│   └── bench.c                 10 M-row benchmark client
├── persist/
│   └── flexql.wal              WAL file (created at runtime)
├── Makefile
├── DESIGN.md                   architecture design document
└── README.md                   this file
```

---

## Public C API (`include/flexql.h`)

```c
FlexQL *flexql_open (const char *host, int port, FlexQL **db);
void    flexql_close(FlexQL *db);
int     flexql_exec (FlexQL *db, const char *sql,
                     flexql_row_cb cb, void *arg, char **errmsg);
void    flexql_free (void *ptr);
int     flexql_exec_batch(FlexQL *db, const char **sqls, int n, char **errmsg);
```

---

*FlexQL — 25CS60R06*
