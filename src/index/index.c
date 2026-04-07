#include "index/index.h"
#include <stdlib.h>
#include <string.h>

/* Open-addressing hash table with linear probing */
typedef struct {
    int64_t key;
    Row    *row;
    int     used;
    int     deleted;
} HEntry;

struct PrimaryIndex {
    HEntry *buckets;
    size_t  cap;
    size_t  used;
};

static size_t hash64(int64_t k) {
    uint64_t x = (uint64_t)k;
    x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
    return (size_t)(x ^ (x >> 31));
}

PrimaryIndex *pidx_create(size_t cap) {
    if (cap < 16) cap = 16;
    /* round up to power of 2 */
    size_t c = 1;
    while (c < cap) c <<= 1;
    PrimaryIndex *idx = calloc(1, sizeof(*idx));
    idx->buckets = calloc(c, sizeof(HEntry));
    idx->cap  = c;
    idx->used = 0;
    return idx;
}

void pidx_destroy(PrimaryIndex *idx) {
    free(idx->buckets);
    free(idx);
}

static void pidx_grow(PrimaryIndex *idx) {
    size_t new_cap = idx->cap * 2;
    HEntry *nb = calloc(new_cap, sizeof(HEntry));
    for (size_t i = 0; i < idx->cap; i++) {
        HEntry *e = &idx->buckets[i];
        if (!e->used || e->deleted) continue;
        size_t h = hash64(e->key) & (new_cap - 1);
        while (nb[h].used) h = (h + 1) & (new_cap - 1);
        nb[h] = *e;
    }
    free(idx->buckets);
    idx->buckets = nb;
    idx->cap     = new_cap;
}

void pidx_insert(PrimaryIndex *idx, int64_t key, Row *row) {
    /* grow at ~80% load to reduce rehash overhead during bulk inserts */
    if (idx->used * 10 >= idx->cap * 8) pidx_grow(idx);
    size_t h = hash64(key) & (idx->cap - 1);
    while (idx->buckets[h].used && !idx->buckets[h].deleted)
        h = (h + 1) & (idx->cap - 1);
    idx->buckets[h].key     = key;
    idx->buckets[h].row     = row;
    idx->buckets[h].used    = 1;
    idx->buckets[h].deleted = 0;
    idx->used++;
}

Row *pidx_lookup(PrimaryIndex *idx, int64_t key) {
    size_t h = hash64(key) & (idx->cap - 1);
    size_t probe = 0;
    while (probe < idx->cap) {
        HEntry *e = &idx->buckets[h];
        if (!e->used) return NULL;
        if (!e->deleted && e->key == key) return e->row;
        h = (h + 1) & (idx->cap - 1);
        probe++;
    }
    return NULL;
}

void pidx_delete(PrimaryIndex *idx, int64_t key) {
    size_t h = hash64(key) & (idx->cap - 1);
    size_t probe = 0;
    while (probe < idx->cap) {
        HEntry *e = &idx->buckets[h];
        if (!e->used) return;
        if (!e->deleted && e->key == key) {
            e->deleted = 1;
            idx->used--;
            return;
        }
        h = (h + 1) & (idx->cap - 1);
        probe++;
    }
}
