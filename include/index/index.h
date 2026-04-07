#pragma once
#include "common/types.h"
#include "storage/storage.h"

/*
 * Primary index: hash table keyed on the INT primary-key value.
 * Supports O(1) average point lookup.
 */
typedef struct PrimaryIndex PrimaryIndex;

PrimaryIndex *pidx_create (size_t initial_capacity);
void          pidx_destroy(PrimaryIndex *idx);

void  pidx_insert(PrimaryIndex *idx, int64_t key, Row *row);
Row  *pidx_lookup(PrimaryIndex *idx, int64_t key);
void  pidx_delete(PrimaryIndex *idx, int64_t key);
