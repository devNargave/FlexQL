#ifndef FLEXQL_H
#define FLEXQL_H

#ifdef __cplusplus
extern "C" {
#endif

#define FLEXQL_OK    0
#define FLEXQL_ERROR 1

typedef struct FlexQL FlexQL;

/* Standard API (required by assignment) */
int  flexql_open (const char *host, int port, FlexQL **db);
int  flexql_close(FlexQL *db);
int  flexql_exec (FlexQL *db,
                  const char *sql,
                  int (*callback)(void*, int, char**, char**),
                  void *arg,
                  char **errmsg);
void flexql_free (void *ptr);

/* High-performance batch API — sends N SQLs in one RTT */
int flexql_exec_batch(FlexQL *db,
                      const char **sqls,
                      int n,
                      char **errmsg);

#ifdef __cplusplus
}
#endif
#endif
