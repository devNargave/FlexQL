#pragma once
#include <stdint.h>
#include <stddef.h>
#include <time.h>

typedef enum {
    COL_INT      = 0,
    COL_DECIMAL  = 1,
    COL_VARCHAR  = 2,
    COL_DATETIME = 3
} ColType;

#define MAX_COLS        64
#define MAX_NAME_LEN    64
#define MAX_TABLES      256
#define MAX_VARCHAR_LEN 4096
#define CACHE_SIZE      4096
#define THREAD_POOL_SZ  32
#define MAX_BATCH_ROWS  100000      /* max tuples in one multi-row INSERT */

typedef struct {
    char    name[MAX_NAME_LEN];
    ColType type;
    int     is_primary_key;
    int     not_null;
    int     varchar_len;
} ColDef;

typedef struct {
    char   name[MAX_NAME_LEN];
    int    ncols;
    ColDef cols[MAX_COLS];
    int    pk_col;
} Schema;

typedef struct {
    int64_t  ival;
    double   dval;
    const char *sval;
    time_t   tval;
} FieldVal;

typedef struct Row {
    FieldVal   *fields;
    time_t      expires_at;
    struct Row *next;
} Row;

#define OP_QUERY    0x01
#define OP_RESULT   0x02
#define OP_ERROR    0x03
#define OP_OK       0x04

#define MAX_MSG_SIZE (256 * 1024 * 1024)
