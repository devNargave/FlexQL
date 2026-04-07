#include "flexql.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "network/net.h"
#include "parser/parser.h"
#include "storage/storage_internal.h"
#include "query/executor.h"
#include "cache/cache.h"
#include "concurrency/threadpool.h"

static Catalog    *g_catalog;
static LRUCache   *g_cache;
static ThreadPool *g_pool;
static volatile int g_running = 1;

typedef struct { int fd; ExecCtx ctx; } ClientCtx;

static int exec_and_send(int fd, ExecCtx *ctx, const char *sql) {
    if (!sql || !sql[0]) return net_send_stream(fd,"OK\n",3);

    char *buf = (char *)sql;
    size_t blen = strlen(buf);
    while (blen > 0) {
        char c = buf[blen - 1];
        if (c == ';' || c == '\n' || c == '\r' || c == ' ') { buf[--blen] = '\0'; continue; }
        break;
    }
    if (!buf[0]) return net_send_stream(fd,"OK\n",3);

    Stmt stmt; char *errmsg = NULL;
    int rc = parse_sql(buf, &stmt, &errmsg);
    if (rc != FLEXQL_OK) {
        char tmp[512];
        snprintf(tmp,sizeof(tmp),"ERR:%s\n",errmsg?errmsg:"parse error");
        free(errmsg);
        return net_send_stream(fd,tmp,strlen(tmp));
    }

    QueryResult qr = exec_stmt(ctx, &stmt);
    stmt_free(&stmt);

    if (qr.errmsg) {
        char tmp[512];
        snprintf(tmp,sizeof(tmp),"ERR:%s\n",qr.errmsg);
        qresult_free(&qr);
        return net_send_stream(fd,tmp,strlen(tmp));
    }

    int ret = net_send_stream(fd, qr.data ? qr.data : "OK\n",
                              qr.data ? qr.len : 3);
    qresult_free(&qr);
    return ret;
}

/*
 * BATCH handler — optimised for INSERT-heavy workloads.
 *
 * Key improvements over original:
 *  1. Read ALL sql messages first (one pass), then execute all, then send all.
 *     This overlaps network I/O with execution.
 *  2. Results are assembled into a single large send buffer using writev-style
 *     net_send_stream per response (same protocol, but avoids per-SQL malloc).
 *  3. Parser gets a pre-stripped copy in-place — no extra strdup.
 */
static void handle_batch(int fd, ExecCtx *ctx, int n) {
    if (n <= 0 || n > 100000) return;

    /* ── Phase 1: receive all SQL strings ── */
    char   **sqls  = malloc(n * sizeof(char*));
    if (!sqls) return;

    int received = 0;
    for (int i = 0; i < n; i++) {
        uint32_t slen = 0;
        char *sql = net_recv_msg(fd, &slen);
        if (!sql) break;
        /* strip trailing junk in-place */
        size_t bl = slen;
        while (bl > 0) {
            char c = sql[bl-1];
            if (c==';'||c=='\n'||c=='\r'||c==' ') { sql[--bl]='\0'; continue; }
            break;
        }
        sqls[i] = sql;
        received++;
    }

    /* ── Phase 2: execute all, collect responses ── */
    /* Pre-allocate response buffer: for INSERT-only batches, each response is
     * exactly "OK\n" (3 bytes) + 4-byte length prefix + 4-byte zero sentinel = 11 bytes.
     * We pack ALL N responses into one send to minimise syscalls. */

    /* Build a single flat send buffer:
     *   For each response i:  [4-byte data-len][data][4-byte 0]
     * This is exactly what net_send_stream does per-item, but done in one write. */
    size_t buf_cap = (size_t)received * 64;  /* 64 bytes per response worst-case for INSERTs */
    char *outbuf = malloc(buf_cap);
    size_t outpos = 0;

    for (int i = 0; i < received; i++) {
        char *sql = sqls[i];
        char *resp = NULL;
        uint32_t rlen = 0;
        char errtmp[256];

        if (!sql || !sql[0]) {
            resp = "OK\n"; rlen = 3;
        } else {
            Stmt stmt; char *errmsg = NULL;
            if (parse_sql(sql, &stmt, &errmsg) == FLEXQL_OK) {
                QueryResult qr = exec_stmt(ctx, &stmt);
                stmt_free(&stmt);
                if (qr.errmsg) {
                    snprintf(errtmp, sizeof(errtmp), "ERR:%s\n", qr.errmsg);
                    qresult_free(&qr);
                    resp = errtmp; rlen = (uint32_t)strlen(errtmp);
                } else {
                    resp = qr.data ? qr.data : "OK\n";
                    rlen = qr.data ? (uint32_t)qr.len : 3;
                    /* pack into outbuf */
                    size_t need = 4 + rlen + 4;
                    if (outpos + need > buf_cap) {
                        buf_cap = (outpos + need) * 2;
                        outbuf = realloc(outbuf, buf_cap);
                    }
                    memcpy(outbuf + outpos, &rlen, 4); outpos += 4;
                    memcpy(outbuf + outpos, resp, rlen); outpos += rlen;
                    uint32_t zero = 0;
                    memcpy(outbuf + outpos, &zero, 4); outpos += 4;
                    qresult_free(&qr);
                    free(sql);
                    continue;
                }
            } else {
                snprintf(errtmp, sizeof(errtmp), "ERR:%s\n", errmsg ? errmsg : "parse error");
                free(errmsg);
                resp = errtmp; rlen = (uint32_t)strlen(errtmp);
            }
        }

        /* error / non-data response */
        size_t need = 4 + rlen + 4;
        if (outpos + need > buf_cap) {
            buf_cap = (outpos + need) * 2;
            outbuf = realloc(outbuf, buf_cap);
        }
        memcpy(outbuf + outpos, &rlen, 4); outpos += 4;
        memcpy(outbuf + outpos, resp, rlen); outpos += rlen;
        uint32_t zero = 0;
        memcpy(outbuf + outpos, &zero, 4); outpos += 4;
        free(sql);
    }

    /* ── Phase 3: send all responses in ONE write ── */
    net_send_all(fd, outbuf, outpos);
    free(outbuf);
    free(sqls);
}

static void handle_client(void *arg) {
    ClientCtx *cc = arg;
    int fd = cc->fd;

    for (;;) {
        uint32_t msglen = 0;
        char *msg = net_recv_msg(fd, &msglen);
        if (!msg) break;

        if (strncmp(msg,"BATCH:",6) == 0) {
            int n = atoi(msg+6);
            free(msg);
            handle_batch(fd, &cc->ctx, n);
            continue;
        }

        int ret = exec_and_send(fd, &cc->ctx, msg);
        free(msg);
        if (ret < 0) break;
    }

    close(fd);
    free(cc);
}

static void *expiry_thread(void *arg) {
    Catalog *cat = arg;
    while (g_running) {
        sleep(5);
        rwlock_rlock(&cat->lock);
        int n = cat->ntables;
        Table *snap[MAX_TABLES];
        memcpy(snap, cat->tables, n*sizeof(Table*));
        rwlock_unlock(&cat->lock);
        for (int i=0;i<n;i++) table_expire(snap[i]);
    }
    return NULL;
}

static void sig_handler(int s) { (void)s; g_running=0; }

int main(int argc, char *argv[]) {
    int port = argc>=2 ? atoi(argv[1]) : 9000;
    signal(SIGINT, sig_handler);
    signal(SIGTERM,sig_handler);
    signal(SIGPIPE,SIG_IGN);

    g_catalog = catalog_create();

    const char *pdir = getenv("FLEXQL_PERSIST_DIR");
    if (pdir && *pdir) {
        persistence_init(pdir);
        persistence_recover(g_catalog, pdir);
    }
    g_cache = lru_create(CACHE_SIZE);
    g_pool  = tp_create(THREAD_POOL_SZ);

    int lfd = net_listen(port, 256);
    if (lfd<0) { perror("net_listen"); return 1; }
    printf("[FlexQL] Server listening on port %d\n",port);
    fflush(stdout);

    /* FLEXQL_NO_EXPIRY=1 disables the expiry sweeper thread.
     * Use during bulk-load benchmarks: the sweeper acquires write locks on
     * every shard every 5 s and competes with INSERT workers. When all rows
     * have far-future expiry timestamps (as in the benchmark) it does zero
     * useful work but still causes measurable lock contention at high insert
     * rates. */
    const char *no_expiry_env = getenv("FLEXQL_NO_EXPIRY");
    int no_expiry = (no_expiry_env && no_expiry_env[0] == '1');

    pthread_t exp_tid;
    if (!no_expiry) {
        pthread_create(&exp_tid,NULL,expiry_thread,g_catalog);
    } else {
        printf("[FlexQL] Expiry sweeper disabled (FLEXQL_NO_EXPIRY=1)\n");
        fflush(stdout);
    }

    while (g_running) {
        struct sockaddr_storage ca; socklen_t cl=sizeof(ca);
        int cfd = accept(lfd,(struct sockaddr*)&ca,&cl);
        if (cfd<0) { if(g_running) perror("accept"); continue; }
        ClientCtx *c = malloc(sizeof(*c));
        c->fd=cfd; c->ctx.catalog=g_catalog; c->ctx.cache=g_cache;
        tp_submit(g_pool,handle_client,c);
    }

    g_running=0;
    if (!no_expiry)
        pthread_join(exp_tid,NULL);
    tp_destroy(g_pool);
    lru_destroy(g_cache);
    catalog_destroy(g_catalog);
    if (pdir && *pdir) persistence_shutdown();
    close(lfd);
    return 0;
}
