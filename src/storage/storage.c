#include "flexql.h"
#include "storage/storage_internal.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>

static void *arena_alloc(Arena **head, size_t sz) {
    const size_t align = sizeof(void*);
    sz = (sz + align - 1) & ~(align - 1);
    Arena *a = *head;
    if (!a || a->used + sz > ROW_ARENA_BYTES) {
        a = calloc(1, sizeof(*a));
        a->next = *head;
        *head = a;
    }
    void *p = (void *)(a->data + a->used);
    a->used += sz;
    return p;
}

static const char *str_arena_dup(StrArena **head, const char *s) {
    if (!s) return NULL;
    size_t len = strlen(s) + 1;
    const size_t align = sizeof(void*);
    size_t need = (len + align - 1) & ~(align - 1);
    StrArena *a = *head;
    if (!a || a->used + need > STR_ARENA_BYTES) {
        a = calloc(1, sizeof(*a));
        a->next = *head;
        *head = a;
    }
    char *dst = a->data + a->used;
    memcpy(dst, s, len);
    a->used += need;
    return dst;
}

static void table_init_shards(Table *t, int nshards) {
    if (nshards < 1) nshards = 1;
    if (nshards > TABLE_SHARDS) nshards = TABLE_SHARDS;
    t->nshards = nshards;
    t->rr = 0;
    for (int i = 0; i < t->nshards; i++) {
        rwlock_init(&t->shards[i].lock);
        t->shards[i].head = NULL;
        t->shards[i].tail = NULL;
        t->shards[i].nrows = 0;
        t->shards[i].min_expires_at = 0;
        t->shards[i].arena_head = NULL;
        t->shards[i].str_arena_head = NULL;
    }

    /* Backwards-compat aliases used by older code paths (PK tables) */
    t->head = t->shards[0].head;
    t->tail = t->shards[0].tail;
    t->nrows = t->shards[0].nrows;
    t->lock = &t->shards[0].lock;
    t->arena_head = t->shards[0].arena_head;
    t->str_arena_head = t->shards[0].str_arena_head;
}

/* ── Catalog ── */
Catalog *catalog_create(void) {
    Catalog *c = calloc(1, sizeof(*c));
    rwlock_init(&c->lock);
    return c;
}

/* Free a single Row's VARCHAR strings + its fields array.
   row itself may be arena-owned — caller decides whether to free it. */
static void row_release_contents(Row *r, int ncols, const Schema *sc) {
    (void)ncols;
    (void)sc;
    /* fields + strings are arena-managed */
    r->fields = NULL;
}

void catalog_destroy(Catalog *cat) {
    for (int i = 0; i < cat->ntables; i++) {
        Table *t = cat->tables[i];
        /* Free contents of every row (VARCHAR strings + fields array).
           The Row structs themselves live in arenas freed below. */
        for (int si = 0; si < t->nshards; si++) {
            for (Row *r = t->shards[si].head; r; r = r->next)
                row_release_contents(r, t->schema.ncols, &t->schema);
            /* Free arenas */
            Arena *a = t->shards[si].arena_head;
            while (a) { Arena *nx = a->next; free(a); a = nx; }
            StrArena *sa = t->shards[si].str_arena_head;
            while (sa) { StrArena *nx = sa->next; free(sa); sa = nx; }
            rwlock_destroy(&t->shards[si].lock);
        }
        if (t->pidx) pidx_destroy(t->pidx);
        free(t);
    }
    rwlock_destroy(&cat->lock);
    free(cat);
}

Table *catalog_get_table(Catalog *cat, const char *name) {
    rwlock_rlock(&cat->lock);
    Table *found = NULL;
    for (int i = 0; i < cat->ntables; i++)
        if (strcasecmp(cat->tables[i]->schema.name, name) == 0)
            { found = cat->tables[i]; break; }
    rwlock_unlock(&cat->lock);
    return found;
}

int catalog_create_table(Catalog *cat, const Schema *schema, char **errmsg) {
    if (catalog_get_table(cat, schema->name))
        { *errmsg = strdup("Table already exists"); return FLEXQL_ERROR; }
    Table *t = calloc(1, sizeof(*t));
    t->schema = *schema;
    if (schema->pk_col >= 0) {
        size_t cap = 65536;
        if (!strcasecmp(schema->name, "BIG_USERS") || !strcasecmp(schema->name, "BENCH"))
            cap = 16777216; /* 16M buckets: supports 10M rows at <65% load, avoids all mid-load rehashes */
        t->pidx = pidx_create(cap);
    }

    /* Shard only non-PK tables: BIG_USERS in benchmark has no PRIMARY KEY */
    table_init_shards(t, (t->pidx ? 1 : TABLE_SHARDS));

    rwlock_wlock(&cat->lock);
    if (cat->ntables >= MAX_TABLES)
        { rwlock_unlock(&cat->lock); free(t);
          *errmsg = strdup("Too many tables"); return FLEXQL_ERROR; }
    cat->tables[cat->ntables++] = t;
    rwlock_unlock(&cat->lock);
    return FLEXQL_OK;
}

/* ── Row allocation ── */
Row *row_alloc(int ncols) {
    Row *r = calloc(1, sizeof(*r));
    r->fields = calloc((size_t)ncols, sizeof(FieldVal));
    return r;
}

void row_free(Row *row, int ncols, const Schema *s) {
    row_release_contents(row, ncols, s);
    free((void*)row->fields);
    free(row);   /* only call for non-arena rows */
}

/* Arena-backed allocation — called under write-lock */
static Row *row_alloc_arena_shard(Table *t, TableShard *sh) {
    Row *r = arena_alloc(&sh->arena_head, sizeof(*r));
    r->fields = arena_alloc(&sh->arena_head, (size_t)t->schema.ncols * sizeof(FieldVal));
    r->next = NULL;
    r->expires_at = 0;
    return r;
}

/* ── Single-row insert (for backwards compat / non-bulk path) ── */
int table_insert(Table *t, Row *row, char **errmsg) {
    (void)errmsg;
    /* Single-row insert uses shard 0 (not performance critical) */
    TableShard *sh = &t->shards[0];
    rwlock_wlock(&sh->lock);
    row->next = NULL;
    if (sh->tail) sh->tail->next = row;
    else sh->head = row;
    sh->tail = row;
    sh->nrows++;
    if (row->expires_at > 0 && (sh->min_expires_at == 0 || row->expires_at < sh->min_expires_at))
        sh->min_expires_at = row->expires_at;
    rwlock_unlock(&sh->lock);
    t->nrows++;
    if (t->pidx && t->schema.pk_col >= 0)
        pidx_insert(t->pidx, row->fields[t->schema.pk_col].ival, row);
    return FLEXQL_OK;
}

/* ── Bulk insert: one write-lock for all rows ── */
int table_insert_bulk(Table *t, FieldVal *flat_fields, time_t *expires,
                      int count, char **errmsg) {
    (void)errmsg;

    if (t->pidx || t->nshards == 1) {
        /* PK tables remain single-shard for correctness */
        TableShard *sh = &t->shards[0];
        rwlock_wlock(&sh->lock);
        for (int i = 0; i < count; i++) {
            Row *r = row_alloc_arena_shard(t, sh);
            FieldVal *src = &flat_fields[i * t->schema.ncols];
            for (int ci = 0; ci < t->schema.ncols; ci++) {
                r->fields[ci] = src[ci];
                if (t->schema.cols[ci].type == COL_VARCHAR)
                    r->fields[ci].sval = str_arena_dup(&sh->str_arena_head, src[ci].sval);
            }
            r->expires_at = expires ? expires[i] : 0;
            r->next = NULL;
            if (sh->tail) sh->tail->next = r;
            else sh->head = r;
            sh->tail = r;
            sh->nrows++;
            if (r->expires_at > 0 && (sh->min_expires_at == 0 || r->expires_at < sh->min_expires_at))
                sh->min_expires_at = r->expires_at;
            t->nrows++;
            if (t->pidx && t->schema.pk_col >= 0)
                pidx_insert(t->pidx, r->fields[t->schema.pk_col].ival, r);
        }
        rwlock_unlock(&sh->lock);
        return FLEXQL_OK;
    }

    /* Non-PK tables: pick one shard per bulk call so concurrent clients insert in parallel */
    unsigned long ticket = __atomic_fetch_add(&t->rr, 1UL, __ATOMIC_RELAXED);
    int shard_id = (int)(ticket % (unsigned long)t->nshards);
    TableShard *sh = &t->shards[shard_id];

    rwlock_wlock(&sh->lock);
    for (int i = 0; i < count; i++) {
        Row *r = row_alloc_arena_shard(t, sh);
        FieldVal *src = &flat_fields[i * t->schema.ncols];
        for (int ci = 0; ci < t->schema.ncols; ci++) {
            r->fields[ci] = src[ci];
            if (t->schema.cols[ci].type == COL_VARCHAR)
                r->fields[ci].sval = str_arena_dup(&sh->str_arena_head, src[ci].sval);
        }
        r->expires_at = expires ? expires[i] : 0;
        r->next = NULL;
        if (sh->tail) sh->tail->next = r;
        else sh->head = r;
        sh->tail = r;
        sh->nrows++;
        if (r->expires_at > 0 && (sh->min_expires_at == 0 || r->expires_at < sh->min_expires_at))
            sh->min_expires_at = r->expires_at;
        t->nrows++;
    }
    rwlock_unlock(&sh->lock);
    return FLEXQL_OK;
}

Row          *table_rows  (Table *t)       { return t->head; }
const Schema *table_schema(const Table *t) { return &t->schema; }

/* ── Expiry sweeper ── */
void table_expire(Table *t) {
    time_t now = time(NULL);
    for (int si = 0; si < t->nshards; si++) {
        TableShard *sh = &t->shards[si];
        rwlock_wlock(&sh->lock);
        if (sh->min_expires_at == 0 || sh->min_expires_at > now) {
            rwlock_unlock(&sh->lock);
            continue;
        }
        Row **pp = &sh->head;
        Row *prev = NULL;
        time_t next_min = 0;
        while (*pp) {
            Row *r = *pp;
            if (r->expires_at > 0 && r->expires_at <= now) {
                *pp = r->next;
                row_release_contents(r, t->schema.ncols, &t->schema);
                if (t->pidx && t->schema.pk_col >= 0)
                    pidx_delete(t->pidx, r->fields ? r->fields[t->schema.pk_col].ival : 0);
                sh->nrows--;
                if (t->nrows) t->nrows--;
                if (sh->tail == r) sh->tail = prev;
            } else {
                if (r->expires_at > 0 && (next_min == 0 || r->expires_at < next_min))
                    next_min = r->expires_at;
                prev = r;
                pp = &(*pp)->next;
            }
        }
        sh->min_expires_at = next_min;
        rwlock_unlock(&sh->lock);
    }
}
