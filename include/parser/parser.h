#pragma once
#include "common/types.h"

typedef enum { STMT_CREATE_TABLE, STMT_INSERT, STMT_SELECT } StmtKind;

typedef struct {
    char col[MAX_NAME_LEN];
    char op[4];
    char val[MAX_VARCHAR_LEN];
} WhereCond;

typedef struct {
    int  star;
    int  ncols;
    char cols[MAX_COLS][MAX_NAME_LEN];
} SelectCols;

typedef struct {
    int  has_join;
    char table2[MAX_NAME_LEN];
    char col1[MAX_NAME_LEN];
    char col2[MAX_NAME_LEN];
} JoinDesc;

/*
 * Multi-row INSERT values.
 * vals_buf  – heap block holding all value strings, '\0' separated
 * idx[r][c] – pointer into vals_buf for row r, col c
 * Must call insert_vals_free() when done.
 */
typedef struct {
    int    nrows;
    int    ncols_per_row;
    char **idx;        /* [nrows * ncols_per_row] pointers into vals_buf */
    char  *vals_buf;   /* single heap alloc for all value strings        */
} InsertVals;

void insert_vals_free(InsertVals *iv);

/* Returns pointer to value string for row r, col c */
static inline const char *iv_get(const InsertVals *iv, int r, int c) {
    return iv->idx[r * iv->ncols_per_row + c];
}

typedef struct {
    StmtKind   kind;
    char       table[MAX_NAME_LEN];
    Schema     schema;
    InsertVals insert_vals;
    SelectCols sel_cols;
    JoinDesc   join;
    int        has_where;
    WhereCond  where;
} Stmt;

int parse_sql(const char *sql, Stmt *out, char **errmsg);

/* Free heap resources allocated by parse_sql */
void stmt_free(Stmt *stmt);
