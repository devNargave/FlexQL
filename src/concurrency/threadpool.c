#include "concurrency/threadpool.h"
#include <stdlib.h>
#include <string.h>

typedef struct Task { TaskFn fn; void *arg; struct Task *next; } Task;

struct ThreadPool {
    pthread_t      *threads;
    size_t          nthreads;
    Task           *head, *tail;
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    int             shutdown;
    size_t          pending;
};

static void *worker(void *arg) {
    ThreadPool *tp = arg;
    for (;;) {
        pthread_mutex_lock(&tp->mu);
        while (!tp->head && !tp->shutdown)
            pthread_cond_wait(&tp->cv, &tp->mu);
        if (tp->shutdown && !tp->head) {
            pthread_mutex_unlock(&tp->mu);
            return NULL;
        }
        Task *t = tp->head;
        tp->head = t->next;
        if (!tp->head) tp->tail = NULL;
        pthread_mutex_unlock(&tp->mu);
        t->fn(t->arg);
        free(t);
        pthread_mutex_lock(&tp->mu);
        tp->pending--;
        pthread_cond_broadcast(&tp->cv);
        pthread_mutex_unlock(&tp->mu);
    }
}

ThreadPool *tp_create(size_t nthreads) {
    ThreadPool *tp = calloc(1, sizeof(*tp));
    tp->nthreads = nthreads;
    tp->threads  = calloc(nthreads, sizeof(pthread_t));
    pthread_mutex_init(&tp->mu, NULL);
    pthread_cond_init(&tp->cv, NULL);
    for (size_t i = 0; i < nthreads; i++)
        pthread_create(&tp->threads[i], NULL, worker, tp);
    return tp;
}

void tp_destroy(ThreadPool *tp) {
    pthread_mutex_lock(&tp->mu);
    while (tp->pending > 0)
        pthread_cond_wait(&tp->cv, &tp->mu);
    tp->shutdown = 1;
    pthread_cond_broadcast(&tp->cv);
    pthread_mutex_unlock(&tp->mu);
    for (size_t i = 0; i < tp->nthreads; i++)
        pthread_join(tp->threads[i], NULL);
    pthread_mutex_destroy(&tp->mu);
    pthread_cond_destroy(&tp->cv);
    free(tp->threads);
    free(tp);
}

int tp_submit(ThreadPool *tp, TaskFn fn, void *arg) {
    Task *t = malloc(sizeof(*t));
    t->fn = fn; t->arg = arg; t->next = NULL;
    pthread_mutex_lock(&tp->mu);
    if (tp->tail) tp->tail->next = t; else tp->head = t;
    tp->tail = t;
    tp->pending++;
    pthread_cond_signal(&tp->cv);
    pthread_mutex_unlock(&tp->mu);
    return 0;
}
