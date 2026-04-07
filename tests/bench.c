#include "flexql.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static const long long DEFAULT_INSERT_ROWS = 10LL;

struct QueryStats {
    long long rows;
};

struct RowCollector {
    char **rows;
    size_t count;
    size_t capacity;
};

static long long elapsed_ms(const struct timespec *start, const struct timespec *end) {
    long long sec_ms = (long long)(end->tv_sec - start->tv_sec) * 1000LL;
    long long nsec_ms = (long long)(end->tv_nsec - start->tv_nsec) / 1000000LL;
    return sec_ms + nsec_ms;
}

static int count_rows_callback(void *data, int argc, char **argv, char **azColName) {
    (void)argc;
    (void)argv;
    (void)azColName;
    if (data != NULL) {
        struct QueryStats *stats = (struct QueryStats *)data;
        stats->rows++;
    }
    return 0;
}

static int append_collected_row(struct RowCollector *collector, const char *row) {
    char **new_rows;
    char *copy;
    size_t new_capacity;

    if (collector->count == collector->capacity) {
        new_capacity = collector->capacity == 0 ? 8 : collector->capacity * 2;
        new_rows = (char **)realloc(collector->rows, new_capacity * sizeof(char *));
        if (new_rows == NULL) {
            return 0;
        }
        collector->rows = new_rows;
        collector->capacity = new_capacity;
    }

    copy = strdup(row);
    if (copy == NULL) {
        return 0;
    }

    collector->rows[collector->count++] = copy;
    return 1;
}

static void free_collector_rows(struct RowCollector *collector) {
    size_t i;
    for (i = 0; i < collector->count; ++i) {
        free(collector->rows[i]);
    }
    free(collector->rows);
    collector->rows = NULL;
    collector->count = 0;
    collector->capacity = 0;
}

static int collect_rows_callback(void *data, int argc, char **argv, char **azColName) {
    struct RowCollector *collector = (struct RowCollector *)data;
    size_t i;
    size_t needed = 1;
    char *row;
    size_t pos = 0;

    (void)azColName;

    for (i = 0; i < (size_t)argc; ++i) {
        const char *value = argv[i] != NULL ? argv[i] : "NULL";
        needed += strlen(value);
        if (i + 1 < (size_t)argc) {
            needed += 1;
        }
    }

    row = (char *)malloc(needed);
    if (row == NULL) {
        return 1;
    }

    for (i = 0; i < (size_t)argc; ++i) {
        const char *value = argv[i] != NULL ? argv[i] : "NULL";
        size_t len = strlen(value);
        if (i > 0) {
            row[pos++] = '|';
        }
        memcpy(row + pos, value, len);
        pos += len;
    }
    row[pos] = '\0';

    if (!append_collected_row(collector, row)) {
        free(row);
        return 1;
    }

    free(row);
    return 0;
}

static int run_exec(FlexQL *db, const char *sql, const char *label) {
    char *err_msg = NULL;
    struct timespec start;
    struct timespec end;
    int rc;
    long long elapsed;

    clock_gettime(CLOCK_MONOTONIC, &start);
    rc = flexql_exec(db, sql, NULL, NULL, &err_msg);
    clock_gettime(CLOCK_MONOTONIC, &end);
    elapsed = elapsed_ms(&start, &end);

    if (rc != FLEXQL_OK) {
        printf("[FAIL] %s -> %s\n", label, err_msg != NULL ? err_msg : "unknown error");
        if (err_msg != NULL) {
            flexql_free(err_msg);
        }
        return 0;
    }

    printf("[PASS] %s (%lld ms)\n", label, elapsed);
    return 1;
}

static int run_query(FlexQL *db, const char *sql, const char *label) {
    struct QueryStats stats;
    char *err_msg = NULL;
    struct timespec start;
    struct timespec end;
    int rc;
    long long elapsed;

    stats.rows = 0;
    clock_gettime(CLOCK_MONOTONIC, &start);
    rc = flexql_exec(db, sql, count_rows_callback, &stats, &err_msg);
    clock_gettime(CLOCK_MONOTONIC, &end);
    elapsed = elapsed_ms(&start, &end);

    if (rc != FLEXQL_OK) {
        printf("[FAIL] %s -> %s\n", label, err_msg != NULL ? err_msg : "unknown error");
        if (err_msg != NULL) {
            flexql_free(err_msg);
        }
        return 0;
    }

    printf("[PASS] %s | rows=%lld | %lld ms\n", label, stats.rows, elapsed);
    return 1;
}

static int query_rows(FlexQL *db, const char *sql, struct RowCollector *collector) {
    char *err_msg = NULL;
    int rc;

    collector->rows = NULL;
    collector->count = 0;
    collector->capacity = 0;

    rc = flexql_exec(db, sql, collect_rows_callback, collector, &err_msg);
    if (rc != FLEXQL_OK) {
        printf("[FAIL] %s -> %s\n", sql, err_msg != NULL ? err_msg : "unknown error");
        if (err_msg != NULL) {
            flexql_free(err_msg);
        }
        free_collector_rows(collector);
        return 0;
    }

    return 1;
}

static int assert_rows_equal(const char *label,
                             const struct RowCollector *actual,
                             const char *const *expected,
                             size_t expected_count) {
    size_t i;

    if (actual->count == expected_count) {
        int same = 1;
        for (i = 0; i < expected_count; ++i) {
            if (strcmp(actual->rows[i], expected[i]) != 0) {
                same = 0;
                break;
            }
        }
        if (same) {
            printf("[PASS] %s\n", label);
            return 1;
        }
    }

    printf("[FAIL] %s\n", label);
    printf("Expected (%zu):\n", expected_count);
    for (i = 0; i < expected_count; ++i) {
        printf("  %s\n", expected[i]);
    }
    printf("Actual (%zu):\n", actual->count);
    for (i = 0; i < actual->count; ++i) {
        printf("  %s\n", actual->rows[i]);
    }
    return 0;
}

static int expect_query_failure(FlexQL *db, const char *sql, const char *label) {
    char *err_msg = NULL;
    int rc = flexql_exec(db, sql, NULL, NULL, &err_msg);

    if (rc == FLEXQL_OK) {
        printf("[FAIL] %s (expected failure, got success)\n", label);
        return 0;
    }

    if (err_msg != NULL) {
        flexql_free(err_msg);
    }
    printf("[PASS] %s\n", label);
    return 1;
}

static int assert_row_count(const char *label, const struct RowCollector *rows, size_t expected_count) {
    if (rows->count == expected_count) {
        printf("[PASS] %s\n", label);
        return 1;
    }

    printf("[FAIL] %s (expected %zu, got %zu)\n", label, expected_count, rows->count);
    return 0;
}

static int run_data_level_unit_tests(FlexQL *db) {
    int all_ok = 1;
    int total_tests = 0;
    int failed_tests = 0;
    struct RowCollector rows;

    printf("\n[[...Running Unit Tests...]]\n\n");

    #define RECORD(expr) \
        do { \
            int _result = (expr); \
            total_tests++; \
            if (!_result) { \
                all_ok = 0; \
                failed_tests++; \
            } \
        } while (0)

    RECORD(run_exec(
        db,
        "CREATE TABLE TEST_USERS(ID DECIMAL, NAME VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);",
        "CREATE TABLE TEST_USERS"));

    {
        char sql[256];

        snprintf(sql, sizeof(sql),
                 "INSERT INTO TEST_USERS VALUES (%d, '%s', %d, %d);",
                 1, "Alice", 1200, 1893456000);
        RECORD(run_exec(db, sql, "INSERT TEST_USERS ID=1"));

        snprintf(sql, sizeof(sql),
                 "INSERT INTO TEST_USERS VALUES (%d, '%s', %d, %d);",
                 2, "Bob", 450, 1893456000);
        RECORD(run_exec(db, sql, "INSERT TEST_USERS ID=2"));

        snprintf(sql, sizeof(sql),
                 "INSERT INTO TEST_USERS VALUES (%d, '%s', %d, %d);",
                 3, "Carol", 2200, 1893456000);
        RECORD(run_exec(db, sql, "INSERT TEST_USERS ID=3"));

        snprintf(sql, sizeof(sql),
                 "INSERT INTO TEST_USERS VALUES (%d, '%s', %d, %d);",
                 4, "Dave", 800, 1893456000);
        RECORD(run_exec(db, sql, "INSERT TEST_USERS ID=4"));
    }

    if (query_rows(db, "SELECT * FROM TEST_USERS;", &rows)) {
        static const char *expected[] = {
            "1|Alice|1200|1893456000",
            "2|Bob|450|1893456000",
            "3|Carol|2200|1893456000",
            "4|Dave|800|1893456000"
        };
        RECORD(1);
        RECORD(assert_rows_equal("Basic SELECT * validation", &rows, expected, 4));
        free_collector_rows(&rows);
    } else {
        RECORD(0);
    }

    if (query_rows(db, "SELECT NAME, BALANCE FROM TEST_USERS WHERE ID = 2;", &rows)) {
        static const char *expected[] = {"Bob|450"};
        RECORD(1);
        RECORD(assert_rows_equal("Single-row value validation", &rows, expected, 1));
        free_collector_rows(&rows);
    } else {
        RECORD(0);
    }

    if (query_rows(db, "SELECT NAME FROM TEST_USERS WHERE BALANCE > 1000;", &rows)) {
        static const char *expected[] = {"Alice", "Carol"};
        RECORD(1);
        RECORD(assert_rows_equal("Filtered rows validation", &rows, expected, 2));
        free_collector_rows(&rows);
    } else {
        RECORD(0);
    }

    if (query_rows(db, "SELECT ID FROM TEST_USERS WHERE BALANCE > 5000;", &rows)) {
        RECORD(1);
        RECORD(assert_row_count("Empty result-set validation", &rows, 0));
        free_collector_rows(&rows);
    } else {
        RECORD(0);
    }

    RECORD(run_exec(
        db,
        "CREATE TABLE TEST_ORDERS(ORDER_ID DECIMAL, USER_ID DECIMAL, AMOUNT DECIMAL, EXPIRES_AT DECIMAL);",
        "CREATE TABLE TEST_ORDERS"));

    RECORD(run_exec(
        db,
        "INSERT INTO TEST_ORDERS VALUES (101, 1, 50, 1893456000);",
        "INSERT TEST_ORDERS ORDER_ID=101"));

    RECORD(run_exec(
        db,
        "INSERT INTO TEST_ORDERS VALUES (102, 1, 150, 1893456000);",
        "INSERT TEST_ORDERS ORDER_ID=102"));

    RECORD(run_exec(
        db,
        "INSERT INTO TEST_ORDERS VALUES (103, 3, 500, 1893456000);",
        "INSERT TEST_ORDERS ORDER_ID=103"));

    if (query_rows(
            db,
            "SELECT TEST_USERS.NAME, TEST_ORDERS.AMOUNT "
            "FROM TEST_USERS INNER JOIN TEST_ORDERS ON TEST_USERS.ID = TEST_ORDERS.USER_ID "
            "WHERE TEST_ORDERS.AMOUNT > 900;",
            &rows)) {
        RECORD(1);
        RECORD(assert_row_count("Join with no matches validation", &rows, 0));
        free_collector_rows(&rows);
    } else {
        RECORD(0);
    }

    RECORD(expect_query_failure(db, "SELECT UNKNOWN_COLUMN FROM TEST_USERS;", "Invalid SQL should fail"));
    RECORD(expect_query_failure(db, "SELECT * FROM MISSING_TABLE;", "Missing table should fail"));

    printf("\nUnit Test Summary: %d/%d passed, %d failed.\n\n",
           total_tests - failed_tests, total_tests, failed_tests);

    #undef RECORD
    return all_ok;
}

static int run_insert_benchmark(FlexQL *db, long long target_rows) {
    long long inserted = 0;
    long long progress_step;
    long long next_progress;
    struct timespec bench_start;
    struct timespec bench_end;
    long long elapsed;
    long long throughput;

    if (!run_exec(
            db,
            "CREATE TABLE BIG_USERS(ID DECIMAL, NAME VARCHAR(64), EMAIL VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);",
            "CREATE TABLE BIG_USERS")) {
        return 0;
    }

    printf("\nStarting insertion benchmark for %lld rows...\n", target_rows);
    clock_gettime(CLOCK_MONOTONIC, &bench_start);

    progress_step = target_rows / 10;
    if (progress_step <= 0) {
        progress_step = 1;
    }
    next_progress = progress_step;

    while (inserted < target_rows) {
        char sql[512];
        char *err_msg = NULL;
        long long id = inserted + 1;
        double balance = 1000.0 + (double)(id % 10000);
        int rc;

        snprintf(sql, sizeof(sql),
                 "INSERT INTO BIG_USERS VALUES (%lld, 'user%lld', 'user%lld@mail.com', %.0f, 1893456000);",
                 id, id, id, balance);

        rc = flexql_exec(db, sql, NULL, NULL, &err_msg);
        if (rc != FLEXQL_OK) {
            printf("[FAIL] INSERT BIG_USERS batch -> %s\n", err_msg != NULL ? err_msg : "unknown error");
            if (err_msg != NULL) {
                flexql_free(err_msg);
            }
            return 0;
        }

        inserted++;

        if (inserted >= next_progress || inserted == target_rows) {
            printf("Progress: %lld/%lld\n", inserted, target_rows);
            next_progress += progress_step;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &bench_end);
    elapsed = elapsed_ms(&bench_start, &bench_end);
    throughput = elapsed > 0 ? (target_rows * 1000LL / elapsed) : target_rows;

    printf("[PASS] INSERT benchmark complete\n");
    printf("Rows inserted: %lld\n", target_rows);
    printf("Elapsed: %lld ms\n", elapsed);
    printf("Throughput: %lld rows/sec\n", throughput);

    return 1;
}

int main(int argc, char **argv) {
    FlexQL *db = NULL;
    long long insert_rows = DEFAULT_INSERT_ROWS;
    int run_unit_tests_only = 0;

    if (argc > 1) {
        if (strcmp(argv[1], "--unit-test") == 0) {
            run_unit_tests_only = 1;
        } else {
            insert_rows = atoll(argv[1]);
            if (insert_rows <= 0) {
                printf("Invalid row count. Use a positive integer or --unit-test.\n");
                return 1;
            }
        }
    }

    if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) {
        printf("Cannot open FlexQL\n");
        return 1;
    }

    printf("Connected to FlexQL\n");

    if (run_unit_tests_only) {
        int ok = run_data_level_unit_tests(db);
        flexql_close(db);
        return ok ? 0 : 1;
    }

    printf("Running SQL subset checks plus insertion benchmark...\n");
    printf("Target insert rows: %lld\n\n", insert_rows);

    if (!run_insert_benchmark(db, insert_rows)) {
        flexql_close(db);
        return 1;
    }

    if (!run_data_level_unit_tests(db)) {
        flexql_close(db);
        return 1;
    }

    flexql_close(db);
    return 0;
}
