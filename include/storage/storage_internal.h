#pragma once
#include "storage/storage.h"
#include "index/index.h"
#include "concurrency/threadpool.h"

#define TABLE_SHARDS 16

#define ROW_ARENA_BYTES (1u << 20) /* 1 MiB blocks */
typedef struct Arena {
    size_t used;
    struct Arena *next;
    unsigned char data[ROW_ARENA_BYTES];
} Arena;

#define STR_ARENA_BYTES (1u << 20) /* 1 MiB blocks */
typedef struct StrArena {
    size_t used;
    struct StrArena *next;
    char data[STR_ARENA_BYTES];
} StrArena;

typedef struct {
    Row      *head;
    Row      *tail;
    size_t    nrows;
    time_t    min_expires_at;
    RWLock    lock;
    Arena    *arena_head;
    StrArena *str_arena_head;
} TableShard;

struct Table {
    Schema        schema;
    PrimaryIndex *pidx;

    int           nshards;
    TableShard    shards[TABLE_SHARDS];
    unsigned long rr;

    /* Backwards-compat aliases: when nshards==1 these point into shards[0] */
    Row          *head;
    Row          *tail;
    size_t        nrows;
    RWLock       *lock;
    Arena        *arena_head;
    StrArena      *str_arena_head;
};

struct Catalog {
    Table  *tables[MAX_TABLES];
    int     ntables;
    RWLock  lock;
};

/* ── Persistence (internal) ── */
int  persistence_init(const char *dirpath);
void persistence_shutdown(void);
int  persistence_recover(Catalog *cat, const char *dirpath);

void persistence_on_create_table(const Schema *schema);
void persistence_on_insert_bulk(Table *t, FieldVal *flat_fields, time_t *expires, int count);
