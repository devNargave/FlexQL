#include "network/net.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>

#define SOCK_BUF (8*1024*1024)

static void tune_socket(int fd) {
    int flag = 1, sz = SOCK_BUF;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
    setsockopt(fd, SOL_SOCKET,  SO_SNDBUF,   &sz,   sizeof(sz));
    setsockopt(fd, SOL_SOCKET,  SO_RCVBUF,   &sz,   sizeof(sz));
}

int net_send_all(int fd, const void *buf, size_t len) {
    const char *p = buf; size_t sent = 0;
    while (sent < len) {
        ssize_t n = write(fd, p+sent, len-sent);
        if (n<=0) { if(errno==EINTR) continue; return -1; }
        sent += (size_t)n;
    }
    return 0;
}

int net_recv_all(int fd, void *buf, size_t len) {
    char *p = buf; size_t got = 0;
    while (got < len) {
        ssize_t n = read(fd, p+got, len-got);
        if (n<=0) { if(errno==EINTR) continue; return -1; }
        got += (size_t)n;
    }
    return 0;
}

int net_send_msg(int fd, const char *data, uint32_t len) {
    uint32_t hdr = len;
    struct iovec iov[2] = {
        { &hdr, 4 },
        { (void*)data, len }
    };
    int ni = len > 0 ? 2 : 1;
    size_t total = 4+len, sent = 0;
    while (sent < total) {
        ssize_t n = writev(fd, iov, ni);
        if (n<=0) { if(errno==EINTR) continue; return -1; }
        sent += (size_t)n;
        if (sent < total) {
            size_t skip = (size_t)n;
            for (int i=0; i<ni; i++) {
                if (skip >= iov[i].iov_len) { skip -= iov[i].iov_len; iov[i].iov_len=0; }
                else { iov[i].iov_base=(char*)iov[i].iov_base+skip; iov[i].iov_len-=skip; break; }
            }
        }
    }
    return 0;
}

char *net_recv_msg(int fd, uint32_t *out_len) {
    uint32_t hdr=0;
    if (net_recv_all(fd,&hdr,4)<0) return NULL;
    if (hdr==0) { *out_len=0; return calloc(1,1); }
    if (hdr>(uint32_t)(256*1024*1024)) return NULL;
    char *buf = malloc(hdr+1);
    if (!buf) return NULL;
    if (net_recv_all(fd,buf,hdr)<0) { free(buf); return NULL; }
    buf[hdr]='\0'; *out_len=hdr;
    return buf;
}

int net_send_batch(int fd, const char **msgs, const uint32_t *lens, int n) {
    size_t total=0;
    for (int i=0;i<n;i++) total+=4+lens[i];
    char *buf=malloc(total);
    if (!buf) return -1;
    char *p=buf;
    for (int i=0;i<n;i++) {
        uint32_t l=lens[i];
        memcpy(p,&l,4); p+=4;
        if (l>0) { memcpy(p,msgs[i],l); p+=l; }
    }
    int rc=net_send_all(fd,buf,total);
    free(buf);
    return rc;
}

int net_recv_batch(int fd, int n,
                   int (*cb)(const char*,uint32_t,void*), void *arg) {
    for (int i=0;i<n;i++) {
        uint32_t rlen=0;
        char *resp=net_recv_msg(fd,&rlen);
        if (!resp) return -1;
        int rc = cb ? cb(resp,rlen,arg) : 0;
        free(resp);
        if (rc) return rc;
    }
    return 0;
}

int net_connect(const char *host, int port) {
    struct addrinfo hints={0},*res=NULL;
    hints.ai_family=AF_UNSPEC; hints.ai_socktype=SOCK_STREAM;
    char ps[16]; snprintf(ps,16,"%d",port);
    if (getaddrinfo(host,ps,&hints,&res)!=0) return -1;
    int fd=-1;
    for (struct addrinfo *r=res;r;r=r->ai_next) {
        fd=socket(r->ai_family,r->ai_socktype,r->ai_protocol);
        if (fd<0) continue;
        tune_socket(fd);
        if (connect(fd,r->ai_addr,r->ai_addrlen)==0) break;
        close(fd); fd=-1;
    }
    freeaddrinfo(res);
    return fd;
}

int net_listen(int port, int backlog) {
    int opt=1;
    int fd=socket(AF_INET6,SOCK_STREAM,0);
    if (fd>=0) {
        setsockopt(fd,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));
        setsockopt(fd,SOL_SOCKET,SO_REUSEPORT,&opt,sizeof(opt));
        int v6=0; setsockopt(fd,IPPROTO_IPV6,IPV6_V6ONLY,&v6,sizeof(v6));
        struct sockaddr_in6 a={0};
        a.sin6_family=AF_INET6; a.sin6_port=htons((uint16_t)port);
        if (bind(fd,(struct sockaddr*)&a,sizeof(a))<0) { close(fd); fd=-1; }
    }
    if (fd<0) {
        fd=socket(AF_INET,SOCK_STREAM,0);
        if (fd<0) return -1;
        setsockopt(fd,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));
        setsockopt(fd,SOL_SOCKET,SO_REUSEPORT,&opt,sizeof(opt));
        struct sockaddr_in a={0};
        a.sin_family=AF_INET; a.sin_addr.s_addr=INADDR_ANY;
        a.sin_port=htons((uint16_t)port);
        if (bind(fd,(struct sockaddr*)&a,sizeof(a))<0) { close(fd); return -1; }
    }
    tune_socket(fd);
    if (listen(fd,backlog)<0) { close(fd); return -1; }
    return fd;
}

int net_send_stream(int fd, const char *data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        size_t chunk = len - sent;
        if (chunk > NET_CHUNK_SIZE) chunk = NET_CHUNK_SIZE;
        if (net_send_msg(fd, data + sent, (uint32_t)chunk) < 0) return -1;
        sent += chunk;
    }
    /* zero-length terminator */
    uint32_t zero = 0;
    return net_send_all(fd, &zero, 4);
}

int net_recv_stream(int fd,
                    int (*cb)(const char *chunk, uint32_t len, void *arg),
                    void *arg)
{
    for (;;) {
        uint32_t clen = 0;
        if (net_recv_all(fd, &clen, 4) < 0) return -1;
        if (clen == 0) return 0;   /* end-of-stream sentinel */
        if (clen > (uint32_t)(256 * 1024 * 1024)) return -1;
        char *buf = malloc(clen + 1);
        if (!buf) return -1;
        if (net_recv_all(fd, buf, clen) < 0) { free(buf); return -1; }
        buf[clen] = '\0';
        int rc = cb ? cb(buf, clen, arg) : 0;
        free(buf);
        if (rc) return rc;
    }
}
