#pragma once
#include <stdint.h>
#include <stddef.h>

int  net_send_all(int fd, const void *buf, size_t len);
int  net_recv_all(int fd, void       *buf, size_t len);

int   net_send_msg(int fd, const char *data, uint32_t len);
char *net_recv_msg(int fd, uint32_t *out_len);

int  net_connect(const char *host, int port);
int  net_listen (int port, int backlog);

/* Pipelined batch: send N msgs in one syscall, recv N responses */
int net_send_batch(int fd, const char **msgs, const uint32_t *lens, int n);
int net_recv_batch(int fd, int n,
                   int (*cb)(const char *resp, uint32_t len, void *arg),
                   void *arg);

/* ── Chunked streaming for large result sets ────────────────────────
 * Server sends: N × [4-byte len][data] then [4-byte 0] sentinel.
 * Client calls net_recv_stream() which invokes cb for each chunk.
 * Chunk size on server side: NET_CHUNK_SIZE bytes.
 */
#define NET_CHUNK_SIZE (4 * 1024 * 1024)   /* 4 MB per chunk */

/* Send data as a chunked stream; sends a zero-length terminator at end */
int net_send_stream(int fd, const char *data, size_t len);

/* Receive a chunked stream; cb called for each chunk (may be called many times).
 * cb receives each raw chunk. Returns 0 on success. */
int net_recv_stream(int fd,
                    int (*cb)(const char *chunk, uint32_t len, void *arg),
                    void *arg);
