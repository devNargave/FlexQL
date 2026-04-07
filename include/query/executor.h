#pragma once
#include "parser/parser.h"
#include "storage/storage.h"
#include "index/index.h"
#include "cache/cache.h"

/*
 * QueryResult is a serialised text blob:
 *   <nrows>\n
 *   <ncols>\n
 *   col1\tcol2\t...\n       <- header row
 *   v1\tv2\t...\n           <- data rows
 *
 * Caller must free with free().
 */
typedef struct {
    char  *data;    /* serialised result  */
    size_t len;
    char  *errmsg;  /* NULL on success    */
} QueryResult;

/* Context threaded through the executor */
typedef struct {
    Catalog    *catalog;
    LRUCache   *cache;
} ExecCtx;

QueryResult exec_stmt(ExecCtx *ctx, const Stmt *stmt);
void        qresult_free(QueryResult *qr);
