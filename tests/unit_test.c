#include "flexql.h"
/*
 * Unit tests — run without a server, tests internal components directly.
 * Compile: gcc -O2 -Iinclude tests/unit_test.c src/parser/parser.c \
 *              src/storage/storage.c src/index/index.c src/cache/cache.c \
 *              src/query/executor.c src/concurrency/threadpool.c \
 *              -D_GNU_SOURCE -lpthread -o bin/unit_test
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "parser/parser.h"
#include "storage/storage_internal.h"
#include "query/executor.h"
#include "cache/cache.h"
#include "index/index.h"

static int tests_run = 0, tests_passed = 0;
#define TEST(name, cond) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  [PASS] %s\n", name); } \
    else       { printf("  [FAIL] %s  (line %d)\n", name, __LINE__); } \
} while(0)

/* ─── Parser tests ───────────────────────────────────────────────────── */
static void test_parser(void) {
    printf("\n=== Parser ===\n");
    Stmt s; char *e = NULL;

    /* CREATE TABLE */
    int rc = parse_sql(
        "CREATE TABLE STUDENT (ID INT PRIMARY KEY NOT NULL, NAME VARCHAR NOT NULL);",
        &s, &e);
    TEST("CREATE TABLE parse ok",       rc == FLEXQL_OK);
    TEST("CREATE TABLE kind",           s.kind == STMT_CREATE_TABLE);
    TEST("CREATE TABLE name",           strcmp(s.table, "STUDENT") == 0);
    TEST("CREATE TABLE ncols",          s.schema.ncols == 2);
    TEST("CREATE TABLE col0 name",      strcmp(s.schema.cols[0].name, "ID") == 0);
    TEST("CREATE TABLE col0 INT",       s.schema.cols[0].type == COL_INT);
    TEST("CREATE TABLE pk_col",         s.schema.pk_col == 0);
    TEST("CREATE TABLE col1 VARCHAR",   s.schema.cols[1].type == COL_VARCHAR);

    /* INSERT */
    rc = parse_sql("INSERT INTO STUDENT VALUES (1,'Alice');", &s, &e);
    TEST("INSERT parse ok",   rc == FLEXQL_OK);
    TEST("INSERT kind",       s.kind == STMT_INSERT);
    TEST("INSERT table",      strcmp(s.table, "STUDENT") == 0);
    TEST("INSERT nvals",      s.insert_vals.nrows==1 && s.insert_vals.ncols_per_row==2);
    TEST("INSERT val0",       s.insert_vals.idx && strcmp(iv_get(&s.insert_vals,0,0),"1")==0);
    TEST("INSERT val1",       s.insert_vals.idx && strcmp(iv_get(&s.insert_vals,0,1),"Alice")==0);

    insert_vals_free(&s.insert_vals);

    /* SELECT * */
    rc = parse_sql("SELECT * FROM STUDENT;", &s, &e);
    TEST("SELECT * parse ok", rc == FLEXQL_OK);
    TEST("SELECT * star",     s.sel_cols.star == 1);
    TEST("SELECT * no where", s.has_where == 0);

    /* SELECT with WHERE */
    rc = parse_sql("SELECT * FROM STUDENT WHERE ID = 1;", &s, &e);
    TEST("SELECT WHERE ok",   rc == FLEXQL_OK);
    TEST("WHERE col",         strcmp(s.where.col, "ID") == 0);
    TEST("WHERE op",          strcmp(s.where.op, "=") == 0);
    TEST("WHERE val",         strcmp(s.where.val, "1") == 0);

    /* SELECT specific cols */
    rc = parse_sql("SELECT ID, NAME FROM STUDENT;", &s, &e);
    TEST("SELECT cols ok",    rc == FLEXQL_OK);
    TEST("SELECT star=0",     s.sel_cols.star == 0);
    TEST("SELECT ncols=2",    s.sel_cols.ncols == 2);

    /* INNER JOIN */
    rc = parse_sql(
        "SELECT * FROM A INNER JOIN B ON A.ID = B.AID;",
        &s, &e);
    TEST("INNER JOIN ok",     rc == FLEXQL_OK);
    TEST("JOIN has_join",     s.join.has_join == 1);
    TEST("JOIN table2",       strcmp(s.join.table2, "B") == 0);
    TEST("JOIN col1",         strcmp(s.join.col1, "ID")  == 0);
    TEST("JOIN col2",         strcmp(s.join.col2, "AID") == 0);
}

/* ─── Index tests ────────────────────────────────────────────────────── */
static void test_index(void) {
    printf("\n=== Primary Index ===\n");
    PrimaryIndex *idx = pidx_create(16);

    /* dummy rows */
    Row r1 = {0}, r2 = {0}, r3 = {0};
    pidx_insert(idx, 1,   &r1);
    pidx_insert(idx, 100, &r2);
    pidx_insert(idx, -5,  &r3);

    TEST("Lookup existing 1",    pidx_lookup(idx, 1)   == &r1);
    TEST("Lookup existing 100",  pidx_lookup(idx, 100) == &r2);
    TEST("Lookup existing -5",   pidx_lookup(idx, -5)  == &r3);
    TEST("Lookup missing 42",    pidx_lookup(idx, 42)  == NULL);

    pidx_delete(idx, 1);
    TEST("Lookup after delete",  pidx_lookup(idx, 1)   == NULL);
    TEST("Others intact after delete", pidx_lookup(idx, 100) == &r2);

    /* stress: insert 100k entries */
    PrimaryIndex *big = pidx_create(64);
    for (int i = 0; i < 100000; i++) pidx_insert(big, i, &r1);
    int ok = 1;
    for (int i = 0; i < 100000; i++) if (pidx_lookup(big, i) != &r1) { ok=0; break; }
    TEST("Index 100k lookup all correct", ok);
    pidx_destroy(big);

    pidx_destroy(idx);
}

/* ─── Cache tests ────────────────────────────────────────────────────── */
static void test_cache(void) {
    printf("\n=== LRU Cache ===\n");
    LRUCache *c = lru_create(4);

    lru_put(c, "q1", "result1", 7);
    lru_put(c, "q2", "result2", 7);

    size_t len;
    const char *v = lru_get(c, "q1", &len);
    TEST("Cache hit q1",     v && strcmp(v,"result1")==0);
    TEST("Cache miss q9",    lru_get(c,"q9",&len) == NULL);

    /* fill to capacity + 1 → evict LRU */
    lru_put(c, "q3", "r3", 2);
    lru_put(c, "q4", "r4", 2);
    lru_put(c, "q5", "r5", 2);  /* q2 should be evicted (LRU) */

    /* invalidate */
    lru_put(c, "SELECT * FROM STUDENT", "rows...", 7);
    lru_invalidate_table(c, "STUDENT");
    TEST("Invalidate removes entry", lru_get(c, "SELECT * FROM STUDENT", &len) == NULL);

    lru_destroy(c);
}

/* ─── Storage + Executor integration ─────────────────────────────────── */
static void test_executor(void) {
    printf("\n=== Executor integration ===\n");

    Catalog  *cat = catalog_create();
    LRUCache *cac = lru_create(32);
    ExecCtx   ctx = { cat, cac };

    Stmt s; char *e = NULL;

    /* CREATE */
    parse_sql("CREATE TABLE T (ID INT PRIMARY KEY NOT NULL, VAL VARCHAR NOT NULL);", &s, &e);
    QueryResult qr = exec_stmt(&ctx, &s);
    TEST("CREATE exec ok",  qr.errmsg == NULL);
    qresult_free(&qr);

    /* INSERT */
    parse_sql("INSERT INTO T VALUES (1,'hello');", &s, &e);
    qr = exec_stmt(&ctx, &s);
    TEST("INSERT exec ok", qr.errmsg == NULL);
    qresult_free(&qr);

    parse_sql("INSERT INTO T VALUES (2,'world');", &s, &e);
    qr = exec_stmt(&ctx, &s);
    TEST("INSERT 2 exec ok", qr.errmsg == NULL);
    qresult_free(&qr);

    /* SELECT * */
    parse_sql("SELECT * FROM T;", &s, &e);
    qr = exec_stmt(&ctx, &s);
    TEST("SELECT * ok",      qr.errmsg == NULL);
    TEST("SELECT * has data", qr.data && strlen(qr.data) > 0);
    TEST("SELECT * contains hello", qr.data && strstr(qr.data, "hello") != NULL);
    TEST("SELECT * contains world", qr.data && strstr(qr.data, "world") != NULL);
    qresult_free(&qr);

    /* SELECT WHERE */
    parse_sql("SELECT * FROM T WHERE ID = 1;", &s, &e);
    qr = exec_stmt(&ctx, &s);
    TEST("SELECT WHERE ok",      qr.errmsg == NULL);
    TEST("SELECT WHERE hit",     qr.data && strstr(qr.data, "hello") != NULL);
    TEST("SELECT WHERE no miss",  qr.data && strstr(qr.data, "world") == NULL);
    qresult_free(&qr);

    /* CREATE second table for JOIN */
    parse_sql("CREATE TABLE U (ID INT PRIMARY KEY NOT NULL, TID INT NOT NULL, DESC VARCHAR NOT NULL);", &s, &e);
    qr = exec_stmt(&ctx, &s); qresult_free(&qr);
    parse_sql("INSERT INTO U VALUES (10, 1, 'desc_for_1');", &s, &e);
    qr = exec_stmt(&ctx, &s); qresult_free(&qr);

    parse_sql("SELECT * FROM T INNER JOIN U ON T.ID = U.TID;", &s, &e);
    qr = exec_stmt(&ctx, &s);
    TEST("INNER JOIN ok",      qr.errmsg == NULL);
    TEST("INNER JOIN has data", qr.data && strstr(qr.data, "desc_for_1") != NULL);
    qresult_free(&qr);

    lru_destroy(cac);
    catalog_destroy(cat);
}

int main(void) {
    printf("FlexQL Unit Tests\n");
    printf("=================\n");

    test_parser();
    test_index();
    test_cache();
    test_executor();

    printf("\n=================\n");
    printf("Results: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
