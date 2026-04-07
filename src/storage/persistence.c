#include "flexql.h"
#include "storage/storage_internal.h"
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>

typedef struct WalItem {
    struct WalItem *next;
    size_t len;
    unsigned char data[];
} WalItem;

static struct {
    int fd;
    pthread_t tid;
    pthread_mutex_t mu;
    pthread_cond_t cv;
    WalItem *head;
    WalItem *tail;
    int running;
    char *path;
} g_wal;

enum { REC_CREATE = 1, REC_INSERT = 2 };

static int write_all(int fd, const void *buf, size_t len) {
    const unsigned char *p = (const unsigned char*)buf;
    size_t off = 0;
    while (off < len) {
        ssize_t n = write(fd, p + off, len - off);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1;
        off += (size_t)n;
    }
    return 0;
}

static void *wal_thread(void *arg) {
    (void)arg;
    pthread_mutex_lock(&g_wal.mu);
    while (g_wal.running || g_wal.head) {
        while (!g_wal.head && g_wal.running)
            pthread_cond_wait(&g_wal.cv, &g_wal.mu);
        WalItem *it = g_wal.head;
        if (it) {
            g_wal.head = it->next;
            if (!g_wal.head) g_wal.tail = NULL;
        }
        pthread_mutex_unlock(&g_wal.mu);

        if (it) {
            (void)write_all(g_wal.fd, it->data, it->len);
            free(it);
        }

        pthread_mutex_lock(&g_wal.mu);
    }
    pthread_mutex_unlock(&g_wal.mu);
    return NULL;
}

static void wal_enqueue(const unsigned char *data, size_t len) {
    if (g_wal.fd < 0) return;
    WalItem *it = malloc(sizeof(*it) + len);
    if (!it) return;
    it->next = NULL;
    it->len = len;
    memcpy(it->data, data, len);

    pthread_mutex_lock(&g_wal.mu);
    if (g_wal.tail) g_wal.tail->next = it;
    else g_wal.head = it;
    g_wal.tail = it;
    pthread_cond_signal(&g_wal.cv);
    pthread_mutex_unlock(&g_wal.mu);
}

static void buf_put_u8(unsigned char **p, uint8_t v) { *(*p)++ = v; }
static void buf_put_u32(unsigned char **p, uint32_t v) { memcpy(*p, &v, 4); *p += 4; }
static void buf_put_u64(unsigned char **p, uint64_t v) { memcpy(*p, &v, 8); *p += 8; }
static void buf_put_i64(unsigned char **p, int64_t v) { memcpy(*p, &v, 8); *p += 8; }
static void buf_put_d64(unsigned char **p, double v) { memcpy(*p, &v, 8); *p += 8; }

static void buf_put_str(unsigned char **p, const char *s) {
    uint32_t n = s ? (uint32_t)strlen(s) : 0;
    buf_put_u32(p, n);
    if (n) { memcpy(*p, s, n); *p += n; }
}

int persistence_init(const char *dirpath) {
    memset(&g_wal, 0, sizeof(g_wal));
    g_wal.fd = -1;

    pthread_mutex_init(&g_wal.mu, NULL);
    pthread_cond_init(&g_wal.cv, NULL);

    if (!dirpath || !*dirpath) return FLEXQL_OK;

    (void)mkdir(dirpath, 0755);

    size_t plen = strlen(dirpath) + 32;
    g_wal.path = malloc(plen);
    if (!g_wal.path) return FLEXQL_ERROR;
    snprintf(g_wal.path, plen, "%s/flexql.wal", dirpath);

    g_wal.fd = open(g_wal.path, O_CREAT | O_APPEND | O_WRONLY, 0644);
    if (g_wal.fd < 0) {
        free(g_wal.path);
        g_wal.path = NULL;
        return FLEXQL_ERROR;
    }

    g_wal.running = 1;
    pthread_create(&g_wal.tid, NULL, wal_thread, NULL);
    return FLEXQL_OK;
}

void persistence_shutdown(void) {
    pthread_mutex_lock(&g_wal.mu);
    g_wal.running = 0;
    pthread_cond_broadcast(&g_wal.cv);
    pthread_mutex_unlock(&g_wal.mu);

    if (g_wal.tid) pthread_join(g_wal.tid, NULL);

    if (g_wal.fd >= 0) close(g_wal.fd);
    g_wal.fd = -1;

    WalItem *it = g_wal.head;
    while (it) {
        WalItem *nx = it->next;
        free(it);
        it = nx;
    }
    g_wal.head = g_wal.tail = NULL;

    free(g_wal.path);
    g_wal.path = NULL;

    pthread_mutex_destroy(&g_wal.mu);
    pthread_cond_destroy(&g_wal.cv);
}

void persistence_on_create_table(const Schema *schema) {
    if (g_wal.fd < 0 || !schema) return;

    size_t pay = 0;
    pay += 4 + strlen(schema->name);
    pay += 4; /* ncols */
    pay += 4; /* pk_col */
    for (int i = 0; i < schema->ncols; i++) {
        pay += 4 + strlen(schema->cols[i].name);
        pay += 4; /* type */
    }

    size_t cap = 1 + 4 + pay;
    unsigned char *buf = malloc(cap);
    if (!buf) return;

    unsigned char *p = buf;
    buf_put_u8(&p, REC_CREATE);

    unsigned char *lenp = p;
    buf_put_u32(&p, 0);

    buf_put_str(&p, schema->name);
    buf_put_u32(&p, (uint32_t)schema->ncols);
    buf_put_u32(&p, (uint32_t)schema->pk_col);
    for (int i = 0; i < schema->ncols; i++) {
        buf_put_str(&p, schema->cols[i].name);
        buf_put_u32(&p, (uint32_t)schema->cols[i].type);
    }

    uint32_t paylen = (uint32_t)(p - (lenp + 4));
    memcpy(lenp, &paylen, 4);

    wal_enqueue(buf, (size_t)(p - buf));
    free(buf);
}

void persistence_on_insert_bulk(Table *t, FieldVal *flat_fields, time_t *expires, int count) {
    if (g_wal.fd < 0 || !t || !flat_fields || count <= 0) return;

    const Schema *sc = &t->schema;

    size_t pay = 0;
    pay += 4 + strlen(sc->name);
    pay += 4; /* count */
    pay += 1; /* has_exp */
    for (int ri = 0; ri < count; ri++) {
        FieldVal *row = &flat_fields[ri * sc->ncols];
        for (int ci = 0; ci < sc->ncols; ci++) {
            switch (sc->cols[ci].type) {
            case COL_INT:      pay += 8; break;
            case COL_DECIMAL:  pay += 8; break;
            case COL_DATETIME: pay += 8; break;
            case COL_VARCHAR:  pay += 4 + (row[ci].sval ? strlen(row[ci].sval) : 0); break;
            }
        }
        if (expires) pay += 8;
    }

    size_t cap = 1 + 4 + pay;
    unsigned char *buf = malloc(cap);
    if (!buf) return;
    unsigned char *p = buf;
    buf_put_u8(&p, REC_INSERT);
    unsigned char *lenp = p;
    buf_put_u32(&p, 0);

    buf_put_str(&p, sc->name);
    buf_put_u32(&p, (uint32_t)count);
    uint8_t has_exp = expires ? 1 : 0;
    buf_put_u8(&p, has_exp);

    for (int ri = 0; ri < count; ri++) {
        FieldVal *row = &flat_fields[ri * sc->ncols];
        for (int ci = 0; ci < sc->ncols; ci++) {
            switch (sc->cols[ci].type) {
            case COL_INT:      buf_put_i64(&p, row[ci].ival); break;
            case COL_DECIMAL:  buf_put_d64(&p, row[ci].dval); break;
            case COL_VARCHAR:  buf_put_str(&p, row[ci].sval); break;
            case COL_DATETIME: buf_put_i64(&p, (int64_t)row[ci].tval); break;
            }
        }
        if (has_exp) buf_put_i64(&p, (int64_t)expires[ri]);
    }

    uint32_t paylen = (uint32_t)(p - (lenp + 4));
    memcpy(lenp, &paylen, 4);
    wal_enqueue(buf, (size_t)(p - buf));
    free(buf);
}

static int read_all(int fd, void *buf, size_t len) {
    unsigned char *p = (unsigned char*)buf;
    size_t off = 0;
    while (off < len) {
        ssize_t n = read(fd, p + off, len - off);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1;
        off += (size_t)n;
    }
    return 0;
}

static const unsigned char *get_u32(const unsigned char *p, const unsigned char *end, uint32_t *out) {
    if (p + 4 > end) return NULL;
    memcpy(out, p, 4);
    return p + 4;
}

static const unsigned char *get_i64(const unsigned char *p, const unsigned char *end, int64_t *out) {
    if (p + 8 > end) return NULL;
    memcpy(out, p, 8);
    return p + 8;
}

static const unsigned char *get_d64(const unsigned char *p, const unsigned char *end, double *out) {
    if (p + 8 > end) return NULL;
    memcpy(out, p, 8);
    return p + 8;
}

static const unsigned char *get_str(const unsigned char *p, const unsigned char *end, char *dst, size_t dstsz) {
    uint32_t n = 0;
    p = get_u32(p, end, &n);
    if (!p) return NULL;
    if (p + n > end) return NULL;
    if (dst && dstsz) {
        size_t c = n < dstsz - 1 ? n : dstsz - 1;
        memcpy(dst, p, c);
        dst[c] = '\0';
    }
    return p + n;
}

int persistence_recover(Catalog *cat, const char *dirpath) {
    if (!cat || !dirpath || !*dirpath) return FLEXQL_OK;

    size_t plen = strlen(dirpath) + 32;
    char *path = malloc(plen);
    if (!path) return FLEXQL_ERROR;
    snprintf(path, plen, "%s/flexql.wal", dirpath);

    int fd = open(path, O_RDONLY);
    free(path);
    if (fd < 0) return FLEXQL_OK;

    for (;;) {
        uint8_t type = 0;
        uint32_t len = 0;
        ssize_t n = read(fd, &type, 1);
        if (n == 0) break;
        if (n < 0) { if (errno == EINTR) continue; break; }
        if (read_all(fd, &len, 4) < 0) break;

        unsigned char *payload = malloc(len);
        if (!payload) break;
        if (read_all(fd, payload, len) < 0) { free(payload); break; }

        const unsigned char *p = payload;
        const unsigned char *end = payload + len;

        if (type == REC_CREATE) {
            Schema sc = {0};
            p = get_str(p, end, sc.name, sizeof(sc.name));
            if (!p) { free(payload); continue; }
            uint32_t nc = 0, pk = 0;
            p = get_u32(p, end, &nc);
            if (!p) { free(payload); continue; }
            p = get_u32(p, end, &pk);
            if (!p) { free(payload); continue; }
            sc.ncols = (int)nc;
            sc.pk_col = (int)pk;
            if (sc.ncols < 0 || sc.ncols > MAX_COLS) { free(payload); continue; }
            for (int i = 0; i < sc.ncols; i++) {
                p = get_str(p, end, sc.cols[i].name, sizeof(sc.cols[i].name));
                if (!p) break;
                uint32_t ct = 0;
                p = get_u32(p, end, &ct);
                if (!p) break;
                sc.cols[i].type = (ColType)ct;
            }
            char *errmsg = NULL;
            (void)catalog_create_table(cat, &sc, &errmsg);
            free(errmsg);
        } else if (type == REC_INSERT) {
            char tname[MAX_NAME_LEN] = {0};
            p = get_str(p, end, tname, sizeof(tname));
            if (!p) { free(payload); continue; }
            uint32_t cnt = 0;
            p = get_u32(p, end, &cnt);
            if (!p) { free(payload); continue; }
            uint8_t has_exp = 0;
            if (p + 1 > end) { free(payload); continue; }
            has_exp = *p++;

            Table *t = catalog_get_table(cat, tname);
            if (!t) { free(payload); continue; }
            const Schema *sc = table_schema(t);

            FieldVal *flat = calloc((size_t)cnt * (size_t)sc->ncols, sizeof(FieldVal));
            time_t *exps = has_exp ? calloc((size_t)cnt, sizeof(time_t)) : NULL;
            if (!flat || (has_exp && !exps)) { free(flat); free(exps); free(payload); continue; }

            for (uint32_t ri = 0; ri < cnt; ri++) {
                FieldVal *row = &flat[ri * (uint32_t)sc->ncols];
                for (int ci = 0; ci < sc->ncols; ci++) {
                    switch (sc->cols[ci].type) {
                    case COL_INT: {
                        int64_t v=0; p = get_i64(p,end,&v); row[ci].ival=v; break;
                    }
                    case COL_DECIMAL: {
                        double d=0; p = get_d64(p,end,&d); row[ci].dval=d; break;
                    }
                    case COL_VARCHAR: {
                        char tmp[MAX_VARCHAR_LEN] = {0};
                        p = get_str(p,end,tmp,sizeof(tmp));
                        row[ci].sval = strdup(tmp);
                        break;
                    }
                    case COL_DATETIME: {
                        int64_t tv=0; p = get_i64(p,end,&tv); row[ci].tval=(time_t)tv; break;
                    }
                    }
                    if (!p) break;
                }
                if (has_exp) {
                    int64_t ev=0; p = get_i64(p,end,&ev);
                    if (!p) break;
                    exps[ri] = (time_t)ev;
                }
                if (!p) break;
            }

            char *errmsg = NULL;
            (void)table_insert_bulk(t, flat, exps, (int)cnt, &errmsg);
            free(errmsg);

            for (uint32_t ri = 0; ri < cnt; ri++) {
                FieldVal *row = &flat[ri * (uint32_t)sc->ncols];
                for (int ci = 0; ci < sc->ncols; ci++)
                    if (sc->cols[ci].type == COL_VARCHAR)
                        free((void*)row[ci].sval);
            }
            free(flat);
            free(exps);
        }

        free(payload);
    }

    close(fd);
    return FLEXQL_OK;
}
