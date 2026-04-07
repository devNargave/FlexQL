#pragma once
#include <pthread.h>
#include <stddef.h>

/* ─── Simple read-write lock wrapper ────────────────────────────────── */
typedef pthread_rwlock_t RWLock;
#define rwlock_init(l)    pthread_rwlock_init(l, NULL)
#define rwlock_destroy(l) pthread_rwlock_destroy(l)
#define rwlock_rlock(l)   pthread_rwlock_rdlock(l)
#define rwlock_wlock(l)   pthread_rwlock_wrlock(l)
#define rwlock_unlock(l)  pthread_rwlock_unlock(l)

/* ─── Thread pool ────────────────────────────────────────────────────── */
typedef struct ThreadPool ThreadPool;
typedef void (*TaskFn)(void *arg);

ThreadPool *tp_create (size_t nthreads);
void        tp_destroy(ThreadPool *tp);           /* waits for all tasks */
int         tp_submit (ThreadPool *tp, TaskFn fn, void *arg);
