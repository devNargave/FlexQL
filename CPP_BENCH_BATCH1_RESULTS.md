# C++ Benchmark Results — `INSERT_BATCH_SIZE = 1`

This report captures runs of the provided C++ benchmark (`benchmark_flexql.cpp`) with:

- `static const int INSERT_BATCH_SIZE = 1;`
- Server started with `FLEXQL_NO_EXPIRY=1`
- Runs executed on localhost (`127.0.0.1:9000`)

## Commands used

### Start server (clean)

```bash
pkill -TERM flexql-server 2>/dev/null || true
sleep 0.5
pkill -KILL flexql-server 2>/dev/null || true

nohup env FLEXQL_NO_EXPIRY=1 ./bin/flexql-server 9000 > bench_server.log 2>&1 &
```

### Run benchmark (1,000,000 rows)

```bash
/usr/bin/time -v ./bin/benchmark_flexql_cpp 1000000 2>&1 | tee bench_cpp_1m.log
```

### Restart server (clean) + Run benchmark (10,000,000 rows)

```bash
pkill -TERM flexql-server 2>/dev/null || true
sleep 0.5
pkill -KILL flexql-server 2>/dev/null || true

nohup env FLEXQL_NO_EXPIRY=1 ./bin/flexql-server 9000 >> bench_server.log 2>&1 &

/usr/bin/time -v ./bin/benchmark_flexql_cpp 10000000 2>&1 | tee bench_cpp_10m.log
```

## Results

### 1,000,000 rows (batch size = 1)

From `bench_cpp_1m.log`:

- Rows inserted: `1000000`
- Elapsed: `2003 ms`
- Throughput: `499251 rows/sec`

`/usr/bin/time -v` summary (high level):

- Elapsed (wall): `0:02.00`
- Max RSS: `4216 kB`
- Exit status: `0`

### 10,000,000 rows (batch size = 1)

From `bench_cpp_10m.log`:

- Rows inserted: `10000000`
- Elapsed: `16525 ms`
- Throughput: `605143 rows/sec`

`/usr/bin/time -v` summary (high level):

- Elapsed (wall): `0:16.53`
- Max RSS: `4296 kB`
- Exit status: `0`

### Verification rerun (fresh rebuild + fresh server) — 10,000,000 rows

To confirm the timing is real and not affected by stale server state, the 10M benchmark was re-run after:

- `make clean && make -j`
- rebuilding `bin/benchmark_flexql_cpp`
- restarting the server from a clean state

From `bench_cpp_10m_rerun.log`:

- Rows inserted: `10000000`
- Elapsed: `16167 ms`
- Throughput: `618543 rows/sec`

`/usr/bin/time -v` summary (high level):

- Elapsed (wall): `0:21.10`
- Max RSS: `4188 kB`
- Exit status: `0`

## Important note about “batch size = 1” in this codebase

Even though the benchmark builds one-row INSERT statements when `INSERT_BATCH_SIZE = 1`, your FlexQL **client library** (`src/client/client.c`) implements **client-side multi-row INSERT coalescing** (pipeline) for INSERT statements.

That means the number above is **not a strict “1 row per network round-trip”** measurement.

- The pipeline coalesces up to `PIPE_DEPTH` rows (currently `5000`) into one multi-row INSERT before sending.
- This greatly reduces round trips and is a major reason the throughput remains high.

If you need a *true worst-case* test (1 row per network request), the client-side coalescing must be disabled (e.g., compile-time switch / env flag / set `PIPE_DEPTH=1`).

## Raw logs (embedded)

<details>
<summary><strong>bench_cpp_1m.log</strong> (click to expand)</summary>

```text
Connected to FlexQL
Running SQL subset checks plus insertion benchmark...
Target insert rows: 1000000

[PASS] CREATE TABLE BIG_USERS (0 ms)

Starting insertion benchmark for 1000000 rows...
Progress: 100000/1000000
Progress: 200000/1000000
Progress: 300000/1000000
Progress: 400000/1000000
Progress: 500000/1000000
Progress: 600000/1000000
Progress: 700000/1000000
Progress: 800000/1000000
Progress: 900000/1000000
Progress: 1000000/1000000
[PASS] INSERT benchmark complete
Rows inserted: 1000000
Elapsed: 2003 ms
Throughput: 499251 rows/sec

[[...Running Unit Tests...]]

[PASS] CREATE TABLE TEST_USERS (2 ms)
[PASS] INSERT TEST_USERS ID=1 (0 ms)
[PASS] INSERT TEST_USERS ID=2 (0 ms)
[PASS] INSERT TEST_USERS ID=3 (0 ms)
[PASS] INSERT TEST_USERS ID=4 (0 ms)
[PASS] Basic SELECT * validation
[PASS] Single-row value validation
[PASS] Filtered rows validation
[PASS] Empty result-set validation
[PASS] CREATE TABLE TEST_ORDERS (0 ms)
[PASS] INSERT TEST_ORDERS ORDER_ID=101 (0 ms)
[PASS] INSERT TEST_ORDERS ORDER_ID=102 (0 ms)
[PASS] INSERT TEST_ORDERS ORDER_ID=103 (0 ms)
[PASS] Join with no matches validation
[PASS] Invalid SQL should fail
[PASS] Missing table should fail

Unit Test Summary: 21/21 passed, 0 failed.

        Command being timed: "./bin/benchmark_flexql_cpp 1000000"
        User time (seconds): 1.35
        System time (seconds): 0.03
        Percent of CPU this job got: 68%
        Elapsed (wall clock) time (h:mm:ss or m:ss): 0:02.00
        Average shared text size (kbytes): 0
        Average unshared data size (kbytes): 0
        Average stack size (kbytes): 0
        Average total size (kbytes): 0
        Maximum resident set size (kbytes): 4216
        Average resident set size (kbytes): 0
        Major (requiring I/O) page faults: 2
        Minor (reclaiming a frame) page faults: 537
        Voluntary context switches: 221
        Involuntary context switches: 146
        Swaps: 0
        File system inputs: 16
        File system outputs: 0
        Socket messages sent: 0
        Socket messages received: 0
        Signals delivered: 0
        Page size (bytes): 4096
        Exit status: 0
```

</details>

<details>
<summary><strong>bench_cpp_10m.log</strong> (click to expand)</summary>

```text
nohup: ignoring input
Connected to FlexQL
Running SQL subset checks plus insertion benchmark...
Target insert rows: 10000000

[PASS] CREATE TABLE BIG_USERS (0 ms)

Starting insertion benchmark for 10000000 rows...
Progress: 1000000/10000000
Progress: 2000000/10000000
Progress: 3000000/10000000
Progress: 4000000/10000000
Progress: 5000000/10000000
Progress: 6000000/10000000
Progress: 7000000/10000000
Progress: 8000000/10000000
Progress: 9000000/10000000
Progress: 10000000/10000000
[PASS] INSERT benchmark complete
Rows inserted: 10000000
Elapsed: 16525 ms
Throughput: 605143 rows/sec

[[...Running Unit Tests...]]

[PASS] CREATE TABLE TEST_USERS (2 ms)
[PASS] INSERT TEST_USERS ID=1 (0 ms)
[PASS] INSERT TEST_USERS ID=2 (0 ms)
[PASS] INSERT TEST_USERS ID=3 (0 ms)
[PASS] INSERT TEST_USERS ID=4 (0 ms)
[PASS] Basic SELECT * validation
[PASS] Single-row value validation
[PASS] Filtered rows validation
[PASS] Empty result-set validation
[PASS] CREATE TABLE TEST_ORDERS (0 ms)
[PASS] INSERT TEST_ORDERS ORDER_ID=101 (0 ms)
[PASS] INSERT TEST_ORDERS ORDER_ID=102 (0 ms)
[PASS] INSERT TEST_ORDERS ORDER_ID=103 (0 ms)
[PASS] Join with no matches validation
[PASS] Invalid SQL should fail
[PASS] Missing table should fail

Unit Test Summary: 21/21 passed, 0 failed.

        Command being timed: "./bin/benchmark_flexql_cpp 10000000"
        User time (seconds): 11.05
        System time (seconds): 0.27
        Percent of CPU this job got: 68%
        Elapsed (wall clock) time (h:mm:ss or m:ss): 0:16.53
        Average shared text size (kbytes): 0
        Average unshared data size (kbytes): 0
        Average stack size (kbytes): 0
        Average total size (kbytes): 0
        Maximum resident set size (kbytes): 4296
        Average resident set size (kbytes): 0
        Major (requiring I/O) page faults: 4
        Minor (reclaiming a frame) page faults: 545
        Voluntary context switches: 2045
        Involuntary context switches: 479
        Swaps: 0
        File system inputs: 496
        File system outputs: 8
        Socket messages sent: 0
        Socket messages received: 0
        Signals delivered: 0
        Page size (bytes): 4096
        Exit status: 0
```

</details>

<details>
<summary><strong>bench_cpp_10m_rerun.log</strong> (fresh rebuild + fresh server)</summary>

```text
Connected to FlexQL
Running SQL subset checks plus insertion benchmark...
Target insert rows: 10000000

[PASS] CREATE TABLE BIG_USERS (0 ms)

Starting insertion benchmark for 10000000 rows...
Progress: 1000000/10000000
Progress: 2000000/10000000
Progress: 3000000/10000000
Progress: 4000000/10000000
Progress: 5000000/10000000
Progress: 6000000/10000000
Progress: 7000000/10000000
Progress: 8000000/10000000
Progress: 9000000/10000000
Progress: 10000000/10000000
[PASS] INSERT benchmark complete
Rows inserted: 10000000
Elapsed: 16167 ms
Throughput: 618543 rows/sec

[[...Running Unit Tests...]]

Unit Test Summary: 21/21 passed, 0 failed.

        Command being timed: "./bin/benchmark_flexql_cpp 10000000"
        User time (seconds): 10.80
        System time (seconds): 0.23
        Percent of CPU this job got: 52%
        Elapsed (wall clock) time (h:mm:ss or m:ss): 0:21.10
        Maximum resident set size (kbytes): 4188
        Exit status: 0
```

</details>

<details>
<summary><strong>bench_server.log</strong> (click to expand)</summary>

```text
nohup: ignoring input
[FlexQL] Server listening on port 9000
[FlexQL] Expiry sweeper disabled (FLEXQL_NO_EXPIRY=1)
nohup: ignoring input
[FlexQL] Server listening on port 9000
[FlexQL] Expiry sweeper disabled (FLEXQL_NO_EXPIRY=1)
Command 'ohup' not found, did you mean:
  command 'nohup' from deb coreutils (9.4-3ubuntu6.2)
Try: sudo apt install <deb name>
nohup: ignoring input
[FlexQL] Server listening on port 9000
[FlexQL] Expiry sweeper disabled (FLEXQL_NO_EXPIRY=1)
```

</details>
