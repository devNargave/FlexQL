#pragma once
#include "common/types.h"

typedef struct Table Table;
typedef struct Catalog Catalog;

Catalog *catalog_create(void);
void     catalog_destroy(Catalog *cat);
Table   *catalog_get_table(Catalog *cat, const char *name);
int      catalog_create_table(Catalog *cat, const Schema *schema, char **errmsg);

int  table_insert(Table *t, Row *row, char **errmsg);
int  table_insert_bulk(Table *t, FieldVal *flat_fields, time_t *expires,
                       int count, char **errmsg);
Row *table_rows(Table *t);
const Schema *table_schema(const Table *t);

Row  *row_alloc(int ncols);
void  row_free (Row *row, int ncols, const Schema *s);
void  table_expire(Table *t);
