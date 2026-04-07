#include "flexql.h"
#include "network/net.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>

/*
 * ── Multi-row INSERT coalescing ────────────────────────────────────────
 *
 * Strategy: extract the "(v1,v2,...)" tuple blob from each single-row INSERT
 * and accumulate them. Flush as one:
 *   INSERT INTO <tbl> VALUES (blob1),(blob2),...,(blobN);
 *
 * Server sees 1 SQL → 1 parse → 1 table_insert_bulk(N) → 1 response.
 * This is the maximum possible batching without changing the benchmark.
 *
 * PIPE_DEPTH controls rows per flush. 500 rows × ~80 bytes ≈ 40 KB per message.
 */

#define PIPE_DEPTH  5000         /* rows per coalesced INSERT (5000 reduces round-trips ~10x vs 500) */
#define BLOB_CAP    (64*1024*1024) /* 64 MB ring buffer for value blobs */

typedef struct {
    char   table[64];    /* current table being accumulated */
    char  *blobs;        /* flat buffer: "(v1,v2,...)\0(v1,v2,...)\0..." */
    size_t blobs_used;
    size_t blobs_cap;
    /* offsets into blobs for each row */
    size_t offsets[PIPE_DEPTH];
    int    nrows;
} Pipeline;

struct FlexQL {
    int      fd;
    Pipeline pipe;
};

/* ── Tiny helpers ── */

static inline int ci_starts(const char *s, const char *kw) {
    while (*kw) { if (tolower((unsigned char)*s) != *kw) return 0; s++; kw++; } return 1;
}

static int is_insert(const char *sql) {
    while (isspace((unsigned char)*sql)) sql++;
    return ci_starts(sql, "insert");
}

/*
 * Extract table name and the "(v1,v2,...)" blob from a single-row INSERT.
 * Points tbl_out into sql (null-terminated in place).
 * Points blob_out to the '(' of the values tuple (null-terminates at matching ')').
 * Returns 1 on success.
 */
static int extract_insert(char *sql, char **tbl_out, char **blob_out) {
    char *p = sql;
    /* skip INSERT */
    while (*p && !isspace((unsigned char)*p)) p++;
    while (isspace((unsigned char)*p)) p++;
    /* skip INTO */
    while (*p && !isspace((unsigned char)*p)) p++;
    while (isspace((unsigned char)*p)) p++;
    /* table name start */
    char *tbl = p;
    while (*p && !isspace((unsigned char)*p) && *p != '(') p++;
    if (p == tbl) return 0;
    char *tbl_end = p;
    /* skip whitespace */
    while (isspace((unsigned char)*p)) p++;
    /* skip optional column list before VALUES */
    if (*p == '(') {
        /* peek if followed by VALUES */
        char *look = p + 1;
        int depth = 1;
        while (*look && depth > 0) {
            if (*look == '(') depth++;
            else if (*look == ')') depth--;
            look++;
        }
        char *after = look;
        while (isspace((unsigned char)*after)) after++;
        if (ci_starts(after, "values")) {
            p = after; /* skip col list */
        }
    }
    /* skip VALUES keyword */
    if (!ci_starts(p, "values")) return 0;
    p += 6;
    while (isspace((unsigned char)*p)) p++;
    if (*p != '(') return 0;
    /* find the matching closing ')' */
    char *blob_start = p;
    int depth = 0;
    char *q = p;
    while (*q) {
        if (*q == '\'') {
            q++;
            while (*q && !(*q == '\'' && *(q+1) != '\'')) {
                if (*q == '\'' && *(q+1) == '\'') q += 2; else q++;
            }
            if (*q) q++; /* skip closing quote */
            continue;
        }
        if (*q == '(') depth++;
        else if (*q == ')') { depth--; if (depth == 0) { q++; break; } }
        q++;
    }
    /* null-terminate table name and blob */
    *tbl_end = '\0';
    char save = *q; *q = '\0'; /* null-terminate after ')' */
    (void)save;

    *tbl_out  = tbl;
    *blob_out = blob_start;
    return 1;
}

/* ── Streaming parser ── */
typedef struct {
    int    (*callback)(void*,int,char**,char**);
    void    *arg;
    char   **colnames;
    int      ncols;
    int      header_done;
    int      ret;
    char    *partial;
    size_t   partial_len;
    size_t   partial_cap;
} StreamParser;

static void sp_init(StreamParser *sp, int (*cb)(void*,int,char**,char**), void *arg) {
    memset(sp,0,sizeof(*sp));
    sp->callback=cb; sp->arg=arg;
    sp->partial_cap=4096;
    sp->partial=malloc(sp->partial_cap);
    sp->partial[0]='\0';
}
static void sp_free(StreamParser *sp) {
    for(int i=0;i<sp->ncols;i++) free(sp->colnames[i]);
    free(sp->colnames); free(sp->partial);
}
static void sp_partial_append(StreamParser *sp, const char *s, size_t n) {
    while (sp->partial_len+n+1>=sp->partial_cap) {
        sp->partial_cap*=2; sp->partial=realloc(sp->partial,sp->partial_cap);
    }
    memcpy(sp->partial+sp->partial_len,s,n);
    sp->partial_len+=n; sp->partial[sp->partial_len]='\0';
}
static void sp_process_line(StreamParser *sp, char *line) {
    if (!sp->header_done) {
        int nc=0; char *colnames[64]={0}; char *tok=line;
        while (tok&&nc<64) {
            char *tab=strchr(tok,'\t'); if(tab)*tab='\0';
            colnames[nc++]=strdup(tok); tok=tab?tab+1:NULL;
        }
        sp->ncols=nc; sp->colnames=malloc(nc*sizeof(char*));
        for(int i=0;i<nc;i++) sp->colnames[i]=colnames[i];
        sp->header_done=1; return;
    }
    if (!sp->callback||sp->ret!=0) return;
    char *vals[64]={0}; int vi=0; char *tok=line;
    while (tok&&vi<sp->ncols) {
        char *tab=strchr(tok,'\t'); if(tab)*tab='\0';
        vals[vi++]=tok; tok=tab?tab+1:NULL;
    }
    while (vi<sp->ncols) vals[vi++]=NULL;
    if (sp->callback(sp->arg,sp->ncols,vals,sp->colnames)!=0) sp->ret=FLEXQL_ERROR;
}
static void sp_feed(StreamParser *sp, const char *data, size_t len) {
    size_t i=0;
    while (i<len&&sp->ret==0) {
        const char *nl=(const char*)memchr(data+i,'\n',len-i);
        if (!nl) { sp_partial_append(sp,data+i,len-i); break; }
        size_t seg=(size_t)(nl-(data+i));
        sp_partial_append(sp,data+i,seg);
        if (sp->partial_len>0) sp_process_line(sp,sp->partial);
        sp->partial_len=0; sp->partial[0]='\0'; i+=seg+1;
    }
}
typedef struct { StreamParser *sp; char **errmsg; int is_error; int first_chunk; } ChunkCtx;
static int on_chunk(const char *chunk, uint32_t clen, void *arg) {
    ChunkCtx *cc=arg;
    if (cc->first_chunk) {
        cc->first_chunk=0;
        if (clen>=4&&strncmp(chunk,"ERR:",4)==0) {
            if (cc->errmsg) {
                size_t l=clen-4; char *e=malloc(l+1); memcpy(e,chunk+4,l);
                while(l>0&&(e[l-1]=='\n'||e[l-1]=='\r'))l--; e[l]='\0'; *cc->errmsg=e;
            }
            cc->is_error=1; return 0;
        }
        if (clen==3&&memcmp(chunk,"OK\n",3)==0) return 0;
    }
    if (!cc->is_error) sp_feed(cc->sp,chunk,(size_t)clen);
    return 0;
}

static int do_send_recv(int fd, const char *sql, size_t len,
                        int (*cb)(void*,int,char**,char**), void *arg, char **errmsg) {
    if (net_send_msg(fd, sql, (uint32_t)len) < 0) {
        if(errmsg)*errmsg=strdup("Send failed"); return FLEXQL_ERROR;
    }
    StreamParser sp; sp_init(&sp, cb, arg);
    ChunkCtx cc={&sp, errmsg, 0, 1};
    if (net_recv_stream(fd, on_chunk, &cc) < 0) {
        sp_free(&sp);
        if(errmsg&&!*errmsg)*errmsg=strdup("Recv failed"); return FLEXQL_ERROR;
    }
    int ret=cc.is_error?FLEXQL_ERROR:sp.ret;
    sp_free(&sp); return ret;
}

/* ── Flush: build one multi-row INSERT and send it ── */
static int pipe_flush(FlexQL *db, char **errmsg) {
    Pipeline *pp = &db->pipe;
    if (pp->nrows == 0) return FLEXQL_OK;

    /* Build: "INSERT INTO <tbl> VALUES <blob0>,<blob1>,...;" */
    size_t prefix_len = 14 + strlen(pp->table) + 8; /* "INSERT INTO X VALUES " */
    size_t total = prefix_len + pp->blobs_used + (size_t)pp->nrows + 2; /* commas + ';' */
    char *sql = malloc(total);
    if (!sql) { if(errmsg)*errmsg=strdup("OOM"); return FLEXQL_ERROR; }

    int pos = snprintf(sql, total, "INSERT INTO %s VALUES ", pp->table);

    for (int r = 0; r < pp->nrows; r++) {
        const char *blob = pp->blobs + pp->offsets[r];
        size_t blen = strlen(blob);
        memcpy(sql + pos, blob, blen);
        pos += (int)blen;
        if (r < pp->nrows - 1) sql[pos++] = ',';
    }
    sql[pos++] = ';';
    sql[pos]   = '\0';

    /* reset pipeline before sending */
    int n = pp->nrows;
    pp->nrows = 0;
    pp->blobs_used = 0;
    (void)n;

    int rc = do_send_recv(db->fd, sql, (size_t)pos, NULL, NULL, errmsg);
    free(sql);
    return rc;
}

/* ── Pipeline init/free ── */
static int pipe_init(Pipeline *pp) {
    memset(pp, 0, sizeof(*pp));
    pp->blobs_cap = BLOB_CAP;
    pp->blobs = malloc(pp->blobs_cap);
    return pp->blobs ? 0 : -1;
}
static void pipe_free(Pipeline *pp) { free(pp->blobs); }

/* ── Public API ── */

int flexql_open(const char *host, int port, FlexQL **db) {
    if (!host||!db) return FLEXQL_ERROR;
    int fd = net_connect(host, port);
    if (fd < 0) return FLEXQL_ERROR;
    FlexQL *h = calloc(1, sizeof(*h));
    if (!h) { close(fd); return FLEXQL_ERROR; }
    h->fd = fd;
    if (pipe_init(&h->pipe) < 0) { close(fd); free(h); return FLEXQL_ERROR; }
    *db = h;
    return FLEXQL_OK;
}

int flexql_close(FlexQL *db) {
    if (!db) return FLEXQL_ERROR;
    pipe_flush(db, NULL);
    pipe_free(&db->pipe);
    close(db->fd);
    free(db);
    return FLEXQL_OK;
}

int flexql_exec(FlexQL *db, const char *sql,
                int (*callback)(void*,int,char**,char**),
                void *arg, char **errmsg)
{
    if (!db||!sql) { if(errmsg)*errmsg=strdup("Invalid handle"); return FLEXQL_ERROR; }

    if (is_insert(sql)) {
        size_t len = strlen(sql);
        char *copy = malloc(len + 1);
        if (!copy) { if(errmsg)*errmsg=strdup("OOM"); return FLEXQL_ERROR; }
        memcpy(copy, sql, len + 1);

        char *tbl = NULL, *blob = NULL;
        int parsed = extract_insert(copy, &tbl, &blob);

        if (parsed) {
            Pipeline *pp = &db->pipe;
            /* flush if table changed */
            if (pp->nrows > 0 && strcasecmp(pp->table, tbl) != 0) {
                int rc = pipe_flush(db, errmsg);
                if (rc != FLEXQL_OK) { free(copy); return rc; }
            }
            /* flush if full */
            if (pp->nrows >= PIPE_DEPTH) {
                int rc = pipe_flush(db, errmsg);
                if (rc != FLEXQL_OK) { free(copy); return rc; }
            }
            /* set table name */
            if (pp->nrows == 0)
                strncpy(pp->table, tbl, sizeof(pp->table)-1);

            /* store blob */
            size_t blen = strlen(blob) + 1; /* include '\0' */
            if (pp->blobs_used + blen > pp->blobs_cap) {
                /* flush to make room */
                int rc = pipe_flush(db, errmsg);
                if (rc != FLEXQL_OK) { free(copy); return rc; }
                strncpy(pp->table, tbl, sizeof(pp->table)-1);
            }
            pp->offsets[pp->nrows] = pp->blobs_used;
            memcpy(pp->blobs + pp->blobs_used, blob, blen);
            pp->blobs_used += blen;
            pp->nrows++;
            free(copy);
            return FLEXQL_OK;
        }
        free(copy);
        /* fall through: unrecognised INSERT format → send as-is */
    }

    /* Non-INSERT or unrecognised: flush pipeline first */
    if (db->pipe.nrows > 0) {
        int rc = pipe_flush(db, errmsg);
        if (rc != FLEXQL_OK) return rc;
    }

    return do_send_recv(db->fd, sql, strlen(sql), callback, arg, errmsg);
}

int flexql_exec_batch(FlexQL *db, const char **sqls, int n, char **errmsg) {
    if (!db||!sqls||n<=0) return FLEXQL_ERROR;
    if (db->pipe.nrows > 0) { int rc=pipe_flush(db,errmsg); if(rc!=FLEXQL_OK) return rc; }
    char hdr[32]; snprintf(hdr,sizeof(hdr),"BATCH:%d",n);
    if (net_send_msg(db->fd,hdr,(uint32_t)strlen(hdr))<0) {
        if(errmsg)*errmsg=strdup("Batch header send failed"); return FLEXQL_ERROR;
    }
    uint32_t *lens=malloc(n*sizeof(uint32_t));
    for(int i=0;i<n;i++) lens[i]=(uint32_t)strlen(sqls[i]);
    int rc=net_send_batch(db->fd,sqls,lens,n);
    free(lens);
    if (rc<0) { if(errmsg)*errmsg=strdup("Batch send failed"); return FLEXQL_ERROR; }
    int errors=0;
    for(int i=0;i<n;i++) {
        char *local_err=NULL;
        StreamParser sp; sp_init(&sp,NULL,NULL);
        ChunkCtx cc={&sp,&local_err,0,1};
        net_recv_stream(db->fd,on_chunk,&cc);
        sp_free(&sp);
        if (cc.is_error) {
            if(!errors&&errmsg)*errmsg=local_err; else free(local_err); errors++;
        }
    }
    return errors?FLEXQL_ERROR:FLEXQL_OK;
}

void flexql_free(void *ptr) { free(ptr); }

#ifdef BUILD_REPL
static int repl_callback(void *d,int nc,char **v,char **c) {
    (void)d;
    for(int i=0;i<nc;i++) printf("%s = %s\n",c[i],v[i]?v[i]:"NULL");
    printf("\n"); return 0;
}
int main(int argc,char *argv[]) {
    const char *host="127.0.0.1"; int port=9000;
    if(argc>=3){host=argv[1];port=atoi(argv[2]);} else if(argc==2) port=atoi(argv[1]);
    FlexQL *db=NULL;
    if(flexql_open(host,port,&db)!=FLEXQL_OK){
        fprintf(stderr,"Cannot connect to %s:%d\n",host,port); return 1;
    }
    printf("Connected to FlexQL server\n");
    char sqlbuf[65536]=""; int in_stmt=0;
    for(;;){
        printf("%s",in_stmt?"    -> ":"flexql> "); fflush(stdout);
        char line[4096]=""; if(!fgets(line,sizeof(line),stdin)) break;
        line[strcspn(line,"\r\n")]='\0'; if(!*line) continue;
        if(!strcasecmp(line,".exit")||!strcasecmp(line,".quit")||
           !strcasecmp(line,"quit")||!strcasecmp(line,"exit")) break;
        if(!strcasecmp(line,".help")){printf("  .exit   Quit\n  .clear  Reset buffer\n\n");continue;}
        if(!strcasecmp(line,".clear")){sqlbuf[0]='\0';in_stmt=0;continue;}
        if(in_stmt) strncat(sqlbuf," ",sizeof(sqlbuf)-strlen(sqlbuf)-1);
        strncat(sqlbuf,line,sizeof(sqlbuf)-strlen(sqlbuf)-1);
        if(strchr(sqlbuf,';')){
            char *e=NULL;
            if(flexql_exec(db,sqlbuf,repl_callback,NULL,&e)!=FLEXQL_OK)
                fprintf(stderr,"Error: %s\n",e?e:"unknown");
            flexql_free(e); sqlbuf[0]='\0'; in_stmt=0;
        } else in_stmt=1;
    }
    flexql_close(db); printf("Connection closed\n"); return 0;
}
#endif
