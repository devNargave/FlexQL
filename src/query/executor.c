#include "flexql.h"
#include "query/executor.h"
#include "storage/storage_internal.h"
#include "concurrency/threadpool.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <ctype.h>

/* ── String builder ── */
typedef struct { char *buf; size_t len; size_t cap; } SB;

/* Thread-local reusable buffers for high-throughput multi-row INSERT */
static _Thread_local FieldVal *tls_flat_fields;
static _Thread_local size_t    tls_flat_cap;
static _Thread_local time_t   *tls_exps;
static _Thread_local size_t    tls_exps_cap;

static void sb_init(SB *s) {
    s->cap=131072; s->buf=malloc(s->cap); s->buf[0]='\0'; s->len=0;
}
static void sb_grow(SB *s, size_t need) {
    while (s->len+need+1>=s->cap) { s->cap*=2; s->buf=realloc(s->buf,s->cap); }
}
static void sb_append(SB *s, const char *p) {
    size_t pl=strlen(p); sb_grow(s,pl);
    memcpy(s->buf+s->len,p,pl+1); s->len+=pl;
}

/* ── Field ↔ string ── */
static inline double pow10_i(int exp) {
    double base = 10.0;
    double res = 1.0;
    while (exp > 0) {
        if (exp & 1) res *= base;
        base *= base;
        exp >>= 1;
    }
    return res;
}

static inline int64_t fast_parse_i64(const char *s) {
    if (!s) return 0;
    while (*s == ' ' || *s == '\t' || *s == '\n' || *s == '\r') s++;
    int neg = 0;
    if (*s == '-') { neg = 1; s++; }
    else if (*s == '+') { s++; }
    int64_t v = 0;
    while ((unsigned)(*s - '0') <= 9u) {
        v = v * 10 + (int64_t)(*s - '0');
        s++;
    }
    return neg ? -v : v;
}

static inline double fast_parse_double(const char *s) {
    if (!s) return 0.0;
    while (*s == ' ' || *s == '\t' || *s == '\n' || *s == '\r') s++;
    int neg = 0;
    if (*s == '-') { neg = 1; s++; }
    else if (*s == '+') { s++; }

    double v = 0.0;
    while ((unsigned)(*s - '0') <= 9u) {
        v = v * 10.0 + (double)(*s - '0');
        s++;
    }

    if (*s == '.') {
        s++;
        double mul = 0.1;
        while ((unsigned)(*s - '0') <= 9u) {
            v += (double)(*s - '0') * mul;
            mul *= 0.1;
            s++;
        }
    }

    if (*s == 'e' || *s == 'E') {
        s++;
        int exp_neg = 0;
        if (*s == '-') { exp_neg = 1; s++; }
        else if (*s == '+') { s++; }
        int exp = 0;
        while ((unsigned)(*s - '0') <= 9u) {
            exp = exp * 10 + (int)(*s - '0');
            s++;
        }
        if (exp) {
            double p = pow10_i(exp);
            v = exp_neg ? (v / p) : (v * p);
        }
    }

    return neg ? -v : v;
}

static void field_to_str(const FieldVal *f, ColType t, char *out, size_t sz) {
    switch (t) {
    case COL_INT:      snprintf(out,sz,"%lld",(long long)f->ival); break;
    case COL_DECIMAL:  snprintf(out,sz,"%.14g",f->dval);            break;
    case COL_VARCHAR:  snprintf(out,sz,"%s",  f->sval?f->sval:""); break;
    case COL_DATETIME: {
        struct tm *tm=localtime(&f->tval);
        strftime(out,sz,"%Y-%m-%d %H:%M:%S",tm); break;
    }
    }
}

static int validate_select_cols(const Schema *sc, const SelectCols *sel, char **errmsg) {
    if (sel->star) return FLEXQL_OK;
    for (int si=0; si<sel->ncols; si++) {
        int found = 0;
        for (int ci=0; ci<sc->ncols; ci++)
            if (!strcasecmp(sc->cols[ci].name, sel->cols[si])) { found = 1; break; }
        if (!found) {
            *errmsg = strdup("Unknown column");
            return FLEXQL_ERROR;
        }
    }
    return FLEXQL_OK;
}

static int validate_where_col(const Schema *sc, const WhereCond *w, char **errmsg) {
    if (!w || !w->col[0]) return FLEXQL_OK;
    for (int ci=0; ci<sc->ncols; ci++)
        if (!strcasecmp(sc->cols[ci].name, w->col)) return FLEXQL_OK;
    *errmsg = strdup("Unknown column");
    return FLEXQL_ERROR;
}

static int eval_where(const Row *row, const Schema *sc, const WhereCond *w);

static int validate_select_cols_join(const Schema *sc1, const Schema *sc2,
                                    const SelectCols *sel, char **errmsg) {
    if (sel->star) return FLEXQL_OK;
    for (int si=0; si<sel->ncols; si++) {
        int found = 0;
        for (int ci=0; ci<sc1->ncols; ci++)
            if (!strcasecmp(sc1->cols[ci].name, sel->cols[si])) { found = 1; break; }
        if (!found) {
            for (int ci=0; ci<sc2->ncols; ci++)
                if (!strcasecmp(sc2->cols[ci].name, sel->cols[si])) { found = 1; break; }
        }
        if (!found) {
            *errmsg = strdup("Unknown column");
            return FLEXQL_ERROR;
        }
    }
    return FLEXQL_OK;
}

static int validate_where_col_join(const Schema *sc1, const Schema *sc2,
                                  const WhereCond *w, char **errmsg) {
    if (!w || !w->col[0]) return FLEXQL_OK;
    for (int ci=0; ci<sc1->ncols; ci++)
        if (!strcasecmp(sc1->cols[ci].name, w->col)) return FLEXQL_OK;
    for (int ci=0; ci<sc2->ncols; ci++)
        if (!strcasecmp(sc2->cols[ci].name, w->col)) return FLEXQL_OK;
    *errmsg = strdup("Unknown column");
    return FLEXQL_ERROR;
}

static int eval_where_join(const Row *r1, const Schema *sc1,
                           const Row *r2, const Schema *sc2,
                           const WhereCond *w)
{
    if (!w) return 1;
    for (int ci=0; ci<sc1->ncols; ci++)
        if (!strcasecmp(sc1->cols[ci].name, w->col))
            return eval_where(r1, sc1, w);
    for (int ci=0; ci<sc2->ncols; ci++)
        if (!strcasecmp(sc2->cols[ci].name, w->col))
            return eval_where(r2, sc2, w);
    return 0;
}

static void str_to_field(const char *s, ColType t, FieldVal *out) {
    switch (t) {
    case COL_INT:      out->ival=fast_parse_i64(s); break;
    case COL_DECIMAL:  out->dval=fast_parse_double(s); break;
    case COL_VARCHAR:
        out->sval = s ? s : "";
        break;
    case COL_DATETIME: {
        struct tm tm={0}; strptime(s,"%Y-%m-%d %H:%M:%S",&tm);
        out->tval=mktime(&tm); break;
    }
    }
}

/* ── WHERE evaluation ── */
static int eval_where(const Row *row, const Schema *sc, const WhereCond *w) {
    int ci=-1;
    for (int i=0;i<sc->ncols;i++)
        if (!strcasecmp(sc->cols[i].name,w->col)) { ci=i; break; }
    if (ci<0) return 0;

    char fstr[512];
    field_to_str(&row->fields[ci],sc->cols[ci].type,fstr,sizeof(fstr));

    int cmp;
    if (sc->cols[ci].type==COL_INT) {
        int64_t a=row->fields[ci].ival, b=fast_parse_i64(w->val);
        cmp=(a>b)-(a<b);
    } else if (sc->cols[ci].type==COL_DECIMAL) {
        double a=row->fields[ci].dval, b=fast_parse_double(w->val);
        cmp=(a>b)-(a<b);
    } else {
        cmp=strcmp(fstr,w->val);
    }

    if (!strcmp(w->op,"="))  return cmp==0;
    if (!strcmp(w->op,"<"))  return cmp<0;
    if (!strcmp(w->op,">"))  return cmp>0;
    if (!strcmp(w->op,"<=")) return cmp<=0;
    if (!strcmp(w->op,">=")) return cmp>=0;
    return 0;
}

/* ── Append one result row ── */
static void append_row(SB *sb, const Row *r, const Schema *sc,
                        const SelectCols *sel)
{
    char tmp[MAX_VARCHAR_LEN+64];
    if (sel->star) {
        for (int i=0;i<sc->ncols;i++) {
            field_to_str(&r->fields[i],sc->cols[i].type,tmp,sizeof(tmp));
            sb_append(sb,tmp);
            sb_append(sb, i<sc->ncols-1 ? "\t" : "\n");
        }
    } else {
        for (int si=0;si<sel->ncols;si++) {
            int ci=-1;
            for (int i=0;i<sc->ncols;i++)
                if (!strcasecmp(sc->cols[i].name,sel->cols[si])) { ci=i; break; }
            if (ci>=0) field_to_str(&r->fields[ci],sc->cols[ci].type,tmp,sizeof(tmp));
            else       snprintf(tmp,sizeof(tmp),"NULL");
            sb_append(sb,tmp);
            sb_append(sb, si<sel->ncols-1 ? "\t" : "\n");
        }
    }
}

static void append_header(SB *sb, const Schema *sc, const SelectCols *sel) {
    if (sel->star) {
        for (int i=0;i<sc->ncols;i++) {
            sb_append(sb,sc->cols[i].name);
            sb_append(sb, i<sc->ncols-1 ? "\t" : "\n");
        }
    } else {
        for (int i=0;i<sel->ncols;i++) {
            sb_append(sb,sel->cols[i]);
            sb_append(sb, i<sel->ncols-1 ? "\t" : "\n");
        }
    }
}

/* ── Main executor ── */
QueryResult exec_stmt(ExecCtx *ctx, const Stmt *stmt) {
    QueryResult qr={0};
    time_t now=time(NULL);

    /* ── CREATE TABLE ── */
    if (stmt->kind==STMT_CREATE_TABLE) {
        int rc=catalog_create_table(ctx->catalog,&stmt->schema,&qr.errmsg);
        if (rc==FLEXQL_OK) {
            persistence_on_create_table(&stmt->schema);
            qr.data=NULL; qr.len=0;
        }
        return qr;
    }

    /* ── INSERT (single or multi-row) ── */
    if (stmt->kind==STMT_INSERT) {
        Table *t=catalog_get_table(ctx->catalog,stmt->table);
        if (!t) { qr.errmsg=strdup("Table not found"); return qr; }
        const Schema *sc=table_schema(t);
        int nv=stmt->insert_vals.ncols_per_row;
        int nr=stmt->insert_vals.nrows;
        if (nr==0) { qr.errmsg=strdup("No values"); return qr; }

        int has_expiry=(nv==sc->ncols+1);
        if (!has_expiry && nv!=sc->ncols) {
            qr.errmsg=strdup("Column count mismatch"); return qr;
        }

        size_t need_fields = (size_t)nr * (size_t)sc->ncols;
        if (need_fields > tls_flat_cap) {
            FieldVal *nf = realloc(tls_flat_fields, need_fields * sizeof(FieldVal));
            if (!nf) { qr.errmsg=strdup("OOM"); return qr; }
            tls_flat_fields = nf;
            tls_flat_cap = need_fields;
        }
        if ((size_t)nr > tls_exps_cap) {
            time_t *ne = realloc(tls_exps, (size_t)nr * sizeof(time_t));
            if (!ne) { qr.errmsg=strdup("OOM"); return qr; }
            tls_exps = ne;
            tls_exps_cap = (size_t)nr;
        }

        time_t   *exps = tls_exps;
        FieldVal *flat_fields = tls_flat_fields;

        for (int ri=0;ri<nr;ri++) {
            FieldVal *row_fields = &flat_fields[ri * sc->ncols];
            for (int ci=0;ci<sc->ncols;ci++)
                str_to_field(iv_get(&stmt->insert_vals,ri,ci),
                             sc->cols[ci].type, &row_fields[ci]);
            if (has_expiry)
                exps[ri]=(time_t)fast_parse_i64(iv_get(&stmt->insert_vals,ri,sc->ncols));
            else 
                exps[ri]=0;
        }

        /* bulk insert under one write-lock */
        table_insert_bulk(t,flat_fields,exps,nr,&qr.errmsg);

        if (!qr.errmsg)
            persistence_on_insert_bulk(t, flat_fields, exps, nr);

        lru_invalidate_table(ctx->cache,stmt->table);
        qr.data=NULL; qr.len=0;
        return qr;
    }

    /* ── SELECT ── */
    if (stmt->kind==STMT_SELECT) {
        /* cache: simple SELECT * no WHERE no JOIN */
        size_t clen=0;
        const char *cached=lru_get(ctx->cache,stmt->table,&clen);
        if (cached && stmt->sel_cols.star && !stmt->has_where && !stmt->join.has_join) {
            qr.data=malloc(clen+1);
            memcpy(qr.data,cached,clen+1);
            qr.len=clen;
            return qr;
        }

        Table *t1=catalog_get_table(ctx->catalog,stmt->table);
        if (!t1) { qr.errmsg=strdup("Table not found"); return qr; }
        const Schema *sc1=table_schema(t1);

        SB sb; sb_init(&sb);

        if (!stmt->join.has_join) {
            /* simple SELECT */
            if (validate_select_cols(sc1, &stmt->sel_cols, &qr.errmsg) != FLEXQL_OK)
                { free(sb.buf); return qr; }
            if (stmt->has_where && validate_where_col(sc1, &stmt->where, &qr.errmsg) != FLEXQL_OK)
                { free(sb.buf); return qr; }
            append_header(&sb,sc1,&stmt->sel_cols);

            int used_index = 0;
            if (stmt->has_where && t1->pidx && sc1->pk_col >= 0 &&
                sc1->cols[sc1->pk_col].type == COL_INT &&
                !strcasecmp(sc1->cols[sc1->pk_col].name, stmt->where.col) &&
                !strcmp(stmt->where.op, "=")) {
                used_index = 1;
                int64_t key = fast_parse_i64(stmt->where.val);
                rwlock_rlock(t1->lock);
                Row *r = pidx_lookup(t1->pidx, key);
                if (r && !(r->expires_at>0 && r->expires_at<=now))
                    append_row(&sb,r,sc1,&stmt->sel_cols);
                rwlock_unlock(t1->lock);
            }

            if (!used_index) {
                for (int si = 0; si < t1->nshards; si++) {
                    TableShard *sh = &t1->shards[si];
                    rwlock_rlock(&sh->lock);
                    for (Row *r=sh->head;r;r=r->next) {
                        if (r->expires_at>0 && r->expires_at<=now) continue;
                        if (stmt->has_where && !eval_where(r,sc1,&stmt->where)) continue;
                        append_row(&sb,r,sc1,&stmt->sel_cols);
                    }
                    rwlock_unlock(&sh->lock);
                }
            }

            if (stmt->sel_cols.star && !stmt->has_where)
                lru_put(ctx->cache,stmt->table,sb.buf,sb.len);

        } else {
            /* INNER JOIN */
            Table *t2=catalog_get_table(ctx->catalog,stmt->join.table2);
            if (!t2) { free(sb.buf); qr.errmsg=strdup("Join table not found"); return qr; }
            const Schema *sc2=table_schema(t2);

            if (validate_select_cols_join(sc1, sc2, &stmt->sel_cols, &qr.errmsg) != FLEXQL_OK)
                { free(sb.buf); return qr; }
            if (stmt->has_where && validate_where_col_join(sc1, sc2, &stmt->where, &qr.errmsg) != FLEXQL_OK)
                { free(sb.buf); return qr; }

            /* header */
            if (stmt->sel_cols.star) {
                for (int i=0;i<sc1->ncols;i++) {
                    sb_append(&sb,stmt->table); sb_append(&sb,".");
                    sb_append(&sb,sc1->cols[i].name);
                    sb_append(&sb,"\t");
                }
                for (int i=0;i<sc2->ncols;i++) {
                    sb_append(&sb,stmt->join.table2); sb_append(&sb,".");
                    sb_append(&sb,sc2->cols[i].name);
                    sb_append(&sb, i<sc2->ncols-1?"\t":"\n");
                }
            } else {
                for (int si=0;si<stmt->sel_cols.ncols;si++) {
                    sb_append(&sb,stmt->sel_cols.cols[si]);
                    sb_append(&sb, si<stmt->sel_cols.ncols-1?"\t":"\n");
                }
            }

            int j1=-1,j2=-1;
            for (int i=0;i<sc1->ncols;i++)
                if (!strcasecmp(sc1->cols[i].name,stmt->join.col1)) { j1=i; break; }
            for (int i=0;i<sc2->ncols;i++)
                if (!strcasecmp(sc2->cols[i].name,stmt->join.col2)) { j2=i; break; }

            char tmp1[MAX_VARCHAR_LEN+64], tmp2[MAX_VARCHAR_LEN+64];
            for (int s1 = 0; s1 < t1->nshards; s1++) {
                TableShard *sh1 = &t1->shards[s1];
                rwlock_rlock(&sh1->lock);
                for (Row *r1=sh1->head;r1;r1=r1->next) {
                    if (r1->expires_at>0&&r1->expires_at<=now) continue;
                    if (j1<0) continue;
                    field_to_str(&r1->fields[j1],sc1->cols[j1].type,tmp1,sizeof(tmp1));
                    for (int s2 = 0; s2 < t2->nshards; s2++) {
                        TableShard *sh2 = &t2->shards[s2];
                        rwlock_rlock(&sh2->lock);
                        for (Row *r2=sh2->head;r2;r2=r2->next) {
                            if (r2->expires_at>0&&r2->expires_at<=now) continue;
                            if (j2<0) continue;
                            field_to_str(&r2->fields[j2],sc2->cols[j2].type,tmp2,sizeof(tmp2));
                            if (strcmp(tmp1,tmp2)!=0) continue;
                            if (stmt->has_where && !eval_where_join(r1,sc1,r2,sc2,&stmt->where)) continue;
                            if (stmt->sel_cols.star) {
                                for (int i=0;i<sc1->ncols;i++) {
                                    field_to_str(&r1->fields[i],sc1->cols[i].type,tmp1,sizeof(tmp1));
                                    sb_append(&sb,tmp1); sb_append(&sb,"\t");
                                }
                                for (int i=0;i<sc2->ncols;i++) {
                                    field_to_str(&r2->fields[i],sc2->cols[i].type,tmp2,sizeof(tmp2));
                                    sb_append(&sb,tmp2);
                                    sb_append(&sb, i<sc2->ncols-1?"\t":"\n");
                                }
                            } else {
                                char cell[MAX_VARCHAR_LEN+64];
                                for (int si=0;si<stmt->sel_cols.ncols;si++) {
                                    int found=0;
                                    for (int ci=0;ci<sc1->ncols;ci++)
                                        if (!strcasecmp(sc1->cols[ci].name,stmt->sel_cols.cols[si]))
                                            { field_to_str(&r1->fields[ci],sc1->cols[ci].type,cell,sizeof(cell)); found=1; break; }
                                    if (!found)
                                        for (int ci=0;ci<sc2->ncols;ci++)
                                            if (!strcasecmp(sc2->cols[ci].name,stmt->sel_cols.cols[si]))
                                                { field_to_str(&r2->fields[ci],sc2->cols[ci].type,cell,sizeof(cell)); found=1; break; }
                                    sb_append(&sb,found?cell:"NULL");
                                    sb_append(&sb, si<stmt->sel_cols.ncols-1?"\t":"\n");
                                }
                            }
                        }
                        rwlock_unlock(&sh2->lock);
                    }
                }
                rwlock_unlock(&sh1->lock);
            }
        }

        qr.data=sb.buf; qr.len=sb.len;
        return qr;
    }

    qr.errmsg=strdup("Unknown statement");
    return qr;
}

void qresult_free(QueryResult *qr) {
    if (qr->data) free(qr->data); 
    if (qr->errmsg) free(qr->errmsg);
    qr->data=qr->errmsg=NULL;
}
