#include "cache/cache.h"
#include "concurrency/threadpool.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef struct CNode {
    char        *key;
    char        *data;
    size_t       len;
    struct CNode *prev, *next;   /* doubly-linked LRU list */
    struct CNode *hash_next;     /* hash bucket chain */
} CNode;

struct LRUCache {
    CNode  **buckets;
    size_t   nbuckets;
    CNode   *head, *tail;        /* MRU..LRU */
    size_t   size, capacity;
    RWLock   lock;
};

static size_t khash(const char *s, size_t nb) {
    size_t h = 14695981039346656037ULL;
    for (; *s; s++) h = (h ^ (unsigned char)*s) * 1099511628211ULL;
    return h % nb;
}

LRUCache *lru_create(size_t capacity) {
    LRUCache *c   = calloc(1, sizeof(*c));
    c->capacity   = capacity;
    c->nbuckets   = capacity * 2 + 1;
    c->buckets    = calloc(c->nbuckets, sizeof(CNode*));
    rwlock_init(&c->lock);
    return c;
}

static void cnode_free(CNode *n) {
    free(n->key);
    free(n->data);
    free(n);
}

void lru_destroy(LRUCache *c) {
    /* walk the linked list — every node is in the list exactly once */
    CNode *n = c->head;
    while (n) {
        CNode *nx = n->next;
        cnode_free(n);
        n = nx;
    }
    rwlock_destroy(&c->lock);
    free(c->buckets);
    free(c);
}

static CNode *bucket_find(LRUCache *c, const char *key) {
    size_t h = khash(key, c->nbuckets);
    for (CNode *e = c->buckets[h]; e; e = e->hash_next)
        if (strcmp(e->key, key) == 0) return e;
    return NULL;
}

static void list_remove(LRUCache *c, CNode *n) {
    if (n->prev) n->prev->next = n->next; else c->head = n->next;
    if (n->next) n->next->prev = n->prev; else c->tail = n->prev;
    n->prev = n->next = NULL;
}

static void list_prepend(LRUCache *c, CNode *n) {
    n->prev = NULL;
    n->next = c->head;
    if (c->head) c->head->prev = n;
    c->head = n;
    if (!c->tail) c->tail = n;
}

static void evict_lru(LRUCache *c) {
    CNode *lru = c->tail;
    if (!lru) return;
    list_remove(c, lru);
    /* remove from hash */
    size_t h = khash(lru->key, c->nbuckets);
    CNode **pp = &c->buckets[h];
    while (*pp && *pp != lru) pp = &(*pp)->hash_next;
    if (*pp) *pp = lru->hash_next;
    cnode_free(lru);
    c->size--;
}

const char *lru_get(LRUCache *c, const char *key, size_t *out_len) {
    rwlock_wlock(&c->lock);   /* promote to write to update LRU order */
    CNode *n = bucket_find(c, key);
    if (!n) { rwlock_unlock(&c->lock); return NULL; }
    list_remove(c, n);
    list_prepend(c, n);
    *out_len = n->len;
    const char *ret = n->data;
    rwlock_unlock(&c->lock);
    return ret;
}

void lru_put(LRUCache *c, const char *key, const char *data, size_t len) {
    rwlock_wlock(&c->lock);
    CNode *n = bucket_find(c, key);
    if (n) {
        free(n->data);
        n->data = malloc(len + 1);
        memcpy(n->data, data, len);
        n->data[len] = '\0';
        n->len = len;
        list_remove(c, n);
        list_prepend(c, n);
        rwlock_unlock(&c->lock);
        return;
    }
    while (c->size >= c->capacity) evict_lru(c);
    n             = calloc(1, sizeof(*n));
    n->key        = strdup(key);
    n->data       = malloc(len + 1);
    memcpy(n->data, data, len);
    n->data[len]  = '\0';
    n->len        = len;
    list_prepend(c, n);
    /* insert into hash */
    size_t h = khash(key, c->nbuckets);
    n->hash_next  = c->buckets[h];
    c->buckets[h] = n;
    c->size++;
    rwlock_unlock(&c->lock);
}

void lru_invalidate_table(LRUCache *c, const char *table_name) {
    if (c->size == 0) return;
    rwlock_wlock(&c->lock);
    CNode *n = c->head;
    while (n) {
        CNode *nx = n->next;
        if (strcasestr(n->key, table_name)) {
            list_remove(c, n);
            size_t h = khash(n->key, c->nbuckets);
            CNode **pp = &c->buckets[h];
            while (*pp && *pp != n) pp = &(*pp)->hash_next;
            if (*pp) *pp = n->hash_next;
            cnode_free(n);
            c->size--;
        }
        n = nx;
    }
    rwlock_unlock(&c->lock);
}
