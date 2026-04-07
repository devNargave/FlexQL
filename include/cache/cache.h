#pragma once
#include <stddef.h>

/*
 * LRU cache keyed on the query string.
 * Values are heap-allocated serialised result blobs (the same wire
 * format the server would send to the client).  On a cache hit the
 * server can skip storage access entirely.
 */
typedef struct LRUCache LRUCache;

LRUCache *lru_create (size_t capacity);
void      lru_destroy(LRUCache *c);

/* Returns NULL on miss; returned pointer is valid until next put/evict */
const char *lru_get(LRUCache *c, const char *key, size_t *out_len);

/* Inserts/updates entry.  data is copied. */
void lru_put(LRUCache *c, const char *key, const char *data, size_t len);

/* Invalidate all entries whose key contains table_name (after writes) */
void lru_invalidate_table(LRUCache *c, const char *table_name);
