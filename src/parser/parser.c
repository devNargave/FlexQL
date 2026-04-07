#include "flexql.h"
#include "parser/parser.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

/* ── InsertVals lifecycle ── */
void insert_vals_free(InsertVals *iv) {
    if (!iv) return;
    free(iv->idx);      iv->idx      = NULL;
    free(iv->vals_buf); iv->vals_buf = NULL;
    iv->nrows = iv->ncols_per_row = 0;
}
void stmt_free(Stmt *s) {
    if (s && s->kind == STMT_INSERT) insert_vals_free(&s->insert_vals);
}

/* ─────────────────────────────────────────────────────────────────────
 * Fast INSERT scanner — zero malloc during tokenisation.
 *
 * Directly scans the SQL for   INSERT INTO <tbl> VALUES (v,...),(v,...);
 * Copies value strings into one flat buffer and builds an index table.
 *
 * Returns FLEXQL_OK on success.
 * ───────────────────────────────────────────────────────────────────── */
static const char *skip_ws(const char *p) {
    while (*p && isspace((unsigned char)*p)) p++;
    return p;
}

static int kw(const char *p, const char *kw) {
    size_t n = strlen(kw);
    return strncasecmp(p, kw, n) == 0 &&
           !isalnum((unsigned char)p[n]) && p[n] != '_';
}

/* Scan one token (word, number, quoted string, punctuation).
 * Sets *start/*end to the token's span in the original string.
 * Returns pointer past token, or NULL on end/error.
 * For quoted strings, *start/*end span the interior (no quotes). */
static const char *next_tok(const char *p,
                             const char **start, const char **end) {
    p = skip_ws(p);
    if (!*p) return NULL;
    if (*p == '\'') {
        p++;
        *start = p;
        while (*p && !(*p == '\'' && *(p+1) != '\'')) {
            if (*p == '\'' && *(p+1) == '\'') p += 2;
            else p++;
        }
        *end = p;
        if (*p == '\'') p++;
        return p;
    }
    if (*p == '(' || *p == ')' || *p == ',' || *p == ';' || *p == '*') {
        *start = p; *end = p+1; return p+1;
    }
    if ((*p=='<'||*p=='>'||*p=='=')&&(*(p+1)=='='||*(p+1)=='<'||*(p+1)=='>')) {
        *start=p; *end=p+2; return p+2;
    }
    if (*p=='<'||*p=='>'||*p=='=') { *start=p; *end=p+1; return p+1; }
    *start = p;
    while (*p && !isspace((unsigned char)*p) &&
           *p!='('&&*p!=')'&&*p!=','&&*p!=';'&&*p!='\''&&
           *p!='<'&&*p!='>'&&*p!='='&&*p!='*') p++;
    *end = p;
    return p;
}

static int tok_eq(const char *s, const char *e, const char *kw2) {
    size_t tl = (size_t)(e-s), kl = strlen(kw2);
    if (tl!=kl) return 0;
    for (size_t i=0;i<tl;i++) if (tolower((unsigned char)s[i])!=tolower((unsigned char)kw2[i])) return 0;
    return 1;
}

static int parse_insert_fast(const char *sql, Stmt *out, char **errmsg) {
    const char *p = sql;
    const char *s, *e;

    /* INSERT */
    p = next_tok(p,&s,&e); if (!p||!tok_eq(s,e,"INSERT")) goto bad;
    /* INTO */
    p = next_tok(p,&s,&e); if (!p||!tok_eq(s,e,"INTO")) goto bad;
    /* table name */
    p = next_tok(p,&s,&e); if (!p) goto bad;
    size_t tlen=(size_t)(e-s);
    if (tlen>=MAX_NAME_LEN) tlen=MAX_NAME_LEN-1;
    memcpy(out->table,s,tlen); out->table[tlen]='\0';

    /* skip optional column list: if next is '(' followed by word then VALUES */
    p = skip_ws(p);
    if (*p=='(') {
        /* peek to see if it ends before VALUES */
        const char *look=p+1; int depth=1;
        while (*look&&depth>0) { if(*look=='(')depth++; else if(*look==')')depth--; look++; }
        look=skip_ws(look);
        if (kw(look,"VALUES")) { p=look; }  /* skip col list */
    }
    /* VALUES */
    p = next_tok(p,&s,&e); if (!p||!tok_eq(s,e,"VALUES")) goto bad;

    /* ── Pass 1: count rows/cols and measure total value bytes ── */
    const char *scan = p;
    int nrows=0, ncols=0;
    size_t val_bytes=0;
    {
        const char *sp=scan;
        int first_row=1;
        while (1) {
            sp=skip_ws(sp);
            if (!*sp||*sp==';') break;
            if (!first_row) { if(*sp==',') sp++; else break; }
            sp=skip_ws(sp);
            if (*sp!='(') break;
            sp++;
            int c=0;
            while (1) {
                sp=skip_ws(sp);
                if (!*sp||*sp==')') break;
                if (*sp==',') { sp++; continue; }
                const char *vs,*ve;
                sp=next_tok(sp,&vs,&ve);
                if (!sp) break;
                val_bytes+=(size_t)(ve-vs)+1;
                c++;
            }
            if (*sp==')') sp++;
            if (first_row) { ncols=c; first_row=0; }
            nrows++;
        }
    }
    if (nrows==0||ncols==0) goto bad;

    /* ── Allocate ── */
    char  *vbuf = malloc(val_bytes+1);
    char **idx  = malloc((size_t)nrows*ncols*sizeof(char*));
    if (!vbuf||!idx) { free(vbuf); free(idx); *errmsg=strdup("OOM"); return FLEXQL_ERROR; }

    /* ── Pass 2: fill ── */
    size_t vpos=0;
    int ri=0; int first_row2=1;
    while (ri<nrows) {
        p=skip_ws(p);
        if (!*p||*p==';') break;
        if (!first_row2) { if(*p==',') p++; else break; }
        p=skip_ws(p);
        if (*p!='(') break; p++;
        int ci=0;
        while (1) {
            p=skip_ws(p);
            if (!*p||*p==')') break;
            if (*p==',') { p++; continue; }
            const char *vs,*ve;
            p=next_tok(p,&vs,&ve);
            if (!p) break;
            if (ci<ncols) {
                size_t vl=(size_t)(ve-vs);
                memcpy(vbuf+vpos,vs,vl);
                vbuf[vpos+vl]='\0';
                idx[ri*ncols+ci]=vbuf+vpos;
                vpos+=vl+1;
            }
            ci++;
        }
        if (*p==')') p++;
        first_row2=0; ri++;
    }

    out->kind=STMT_INSERT;
    out->insert_vals.nrows=nrows;
    out->insert_vals.ncols_per_row=ncols;
    out->insert_vals.idx=idx;
    out->insert_vals.vals_buf=vbuf;
    return FLEXQL_OK;
bad:
    *errmsg=strdup("Malformed INSERT statement");
    return FLEXQL_ERROR;
}

/* ─────────────────────────────────────────────────────────────────────
 * General tokeniser (used for CREATE TABLE and SELECT only)
 * Much smaller input so a simple token array is fine.
 * ───────────────────────────────────────────────────────────────────── */
#define MAX_TOKENS 4096
#define MAX_TOK    256
typedef struct { char s[MAX_TOK]; } Token;

static int tokenise_general(const char *sql, Token *toks, int max) {
    int n=0; const char *p=sql;
    while (*p && n<max) {
        while (*p&&isspace((unsigned char)*p)) p++;
        if (!*p) break;
        if (*p=='\'') {
            p++; int i=0;
            while (*p) {
                if (*p=='\''&&*(p+1)=='\'') { if(i<MAX_TOK-1)toks[n].s[i++]='\''; p+=2; }
                else if (*p=='\'') { p++; break; }
                else { if(i<MAX_TOK-1)toks[n].s[i++]=*p; p++; }
            }
            toks[n].s[i]='\0'; n++; continue;
        }
        if (*p=='('||*p==')'||*p==','||*p==';'||*p=='*')
            { toks[n].s[0]=*p; toks[n].s[1]='\0'; n++; p++; continue; }
        if ((*p=='<'||*p=='>'||*p=='!')&&(*(p+1)=='='||*(p+1)=='<'||*(p+1)=='>'))
            { toks[n].s[0]=*p; toks[n].s[1]=*(p+1); toks[n].s[2]='\0'; n++; p+=2; continue; }
        if (*p=='<'||*p=='>'||*p=='=')
            { toks[n].s[0]=*p; toks[n].s[1]='\0'; n++; p++; continue; }
        int i=0;
        while (*p&&!isspace((unsigned char)*p)&&
               *p!='('&&*p!=')'&&*p!=','&&*p!=';'&&*p!='\''&&
               *p!='<'&&*p!='>'&&*p!='='&&*p!='*')
            { if(i<MAX_TOK-1)toks[n].s[i++]=*p; p++; }
        toks[n].s[i]='\0'; n++;
    }
    return n;
}

static int parse_type(const char *s, ColType *out, int *vlen) {
    if (!strcasecmp(s,"INT")||!strcasecmp(s,"INTEGER")||!strcasecmp(s,"BIGINT"))
        { *out=COL_INT; return 0; }
    if (!strcasecmp(s,"DECIMAL")||!strcasecmp(s,"FLOAT")||!strcasecmp(s,"DOUBLE")||
        !strcasecmp(s,"REAL")||!strcasecmp(s,"NUMERIC"))
        { *out=COL_DECIMAL; return 0; }
    if (!strcasecmp(s,"DATETIME")||!strcasecmp(s,"DATE")||!strcasecmp(s,"TIMESTAMP"))
        { *out=COL_DATETIME; return 0; }
    if (!strcasecmp(s,"TEXT")||!strcasecmp(s,"CHAR")||!strcasecmp(s,"STRING")||
        !strcasecmp(s,"BLOB"))
        { *out=COL_VARCHAR; *vlen=4096; return 0; }
    if (!strncasecmp(s,"VARCHAR",7))
        { *out=COL_VARCHAR; *vlen=(s[7]=='(')?atoi(s+8):255; return 0; }
    return -1;
}

int parse_sql(const char *sql, Stmt *out, char **errmsg) {
    memset(out,0,sizeof(*out));
    out->schema.pk_col=-1;

    /* Fast path for INSERT */
    const char *p=sql;
    while (*p&&isspace((unsigned char)*p)) p++;
    if (kw(p,"INSERT")) return parse_insert_fast(sql,out,errmsg);

    /* General tokeniser for CREATE / SELECT */
    Token *toks=malloc(MAX_TOKENS*sizeof(Token));
    if (!toks) { *errmsg=strdup("OOM"); return FLEXQL_ERROR; }
    int n=tokenise_general(sql,toks,MAX_TOKENS);
    if (n<=0) { free(toks); *errmsg=strdup("Empty SQL"); return FLEXQL_ERROR; }

    int i=0;
    out->kind=STMT_SELECT; /* default */

#define TOK(k)    toks[(k)].s
#define TOKEQ(k,v) (strcasecmp(TOK(k),(v))==0)
#define ERR(msg) do{*errmsg=strdup(msg);free(toks);return FLEXQL_ERROR;}while(0)
#define EXPECT(v) do{if(i>=n||!TOKEQ(i,v))ERR("Expected: "v);i++;}while(0)
#define NEED(k)   do{if((k)>=n)ERR("Unexpected end");}while(0)

    /* ── CREATE TABLE ── */
    if (TOKEQ(i,"CREATE")) {
        i++; EXPECT("TABLE"); NEED(i);
        strncpy(out->table,TOK(i),MAX_NAME_LEN-1); i++;
        out->kind=STMT_CREATE_TABLE;
        strncpy(out->schema.name,out->table,MAX_NAME_LEN-1);
        EXPECT("(");
        int ci=0;
        while (i<n&&!TOKEQ(i,")")) {
            if (TOKEQ(i,",")) { i++; continue; }
            if (ci>=MAX_COLS) ERR("Too many columns");
            ColDef *c=&out->schema.cols[ci];
            strncpy(c->name,TOK(i),MAX_NAME_LEN-1); i++;
            NEED(i);
            char tb[MAX_TOK*2]={0};
            strncpy(tb,TOK(i),sizeof(tb)-1); i++;
            if (!strcasecmp(tb,"VARCHAR")&&i<n&&TOKEQ(i,"(")) {
                strncat(tb,"(",2); i++;
                if (i<n) { strncat(tb,TOK(i),16); i++; }
                if (i<n&&TOKEQ(i,")")) { strncat(tb,")",2); i++; }
            }
            int vlen=255;
            if (parse_type(tb,&c->type,&vlen)<0) ERR("Unknown type");
            c->varchar_len=vlen;
            while (i<n&&!TOKEQ(i,",")&&!TOKEQ(i,")")) {
                if (TOKEQ(i,"PRIMARY")) { i++;
                    if (i<n&&TOKEQ(i,"KEY")) { i++; c->is_primary_key=1; out->schema.pk_col=ci; }
                } else if (TOKEQ(i,"NOT")) { i++;
                    if (i<n&&TOKEQ(i,"NULL")) { i++; c->not_null=1; }
                } else { i++; }
            }
            ci++;
        }
        if (i<n&&TOKEQ(i,")")) i++;
        out->schema.ncols=ci;
        free(toks); return FLEXQL_OK;
    }

    /* ── SELECT ── */
    if (TOKEQ(i,"SELECT")) {
        i++;
        out->kind=STMT_SELECT;
        if (i<n&&TOKEQ(i,"*")) { out->sel_cols.star=1; i++; }
        else {
            int sc=0;
            while (i<n&&!TOKEQ(i,"FROM")) {
                if (TOKEQ(i,",")) { i++; continue; }
                char *dot=strchr(TOK(i),'.');
                strncpy(out->sel_cols.cols[sc],dot?dot+1:TOK(i),MAX_NAME_LEN-1);
                sc++; i++;
            }
            out->sel_cols.ncols=sc;
        }
        EXPECT("FROM"); NEED(i);
        strncpy(out->table,TOK(i),MAX_NAME_LEN-1); i++;

        if (i<n&&TOKEQ(i,"INNER")) {
            i++; EXPECT("JOIN"); NEED(i);
            strncpy(out->join.table2,TOK(i),MAX_NAME_LEN-1); i++;
            EXPECT("ON");
            char left[MAX_TOK*2]={0},right[MAX_TOK*2]={0};
            while (i<n&&!TOKEQ(i,"=")&&!TOKEQ(i,"WHERE")&&!TOKEQ(i,";"))
                { if(strlen(left)+strlen(TOK(i))<sizeof(left)-1)strcat(left,TOK(i)); i++; }
            if (i<n&&TOKEQ(i,"=")) i++;
            while (i<n&&!TOKEQ(i,"WHERE")&&!TOKEQ(i,";"))
                { if(strlen(right)+strlen(TOK(i))<sizeof(right)-1)strcat(right,TOK(i)); i++; }
            char *d1=strchr(left,'.'),*d2=strchr(right,'.');
            strncpy(out->join.col1,d1?d1+1:left,MAX_NAME_LEN-1);
            strncpy(out->join.col2,d2?d2+1:right,MAX_NAME_LEN-1);
            out->join.has_join=1;
        }
        if (i<n&&TOKEQ(i,"WHERE")) {
            i++; NEED(i);
            char *dot=strchr(TOK(i),'.');
            strncpy(out->where.col,dot?dot+1:TOK(i),MAX_NAME_LEN-1); i++;
            NEED(i); strncpy(out->where.op,TOK(i),3); i++;
            NEED(i); strncpy(out->where.val,TOK(i),MAX_VARCHAR_LEN-1); i++;
            out->has_where=1;
        }
        free(toks); return FLEXQL_OK;
    }

    ERR("Unknown SQL statement");
}
