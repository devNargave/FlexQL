/* Readline stub for environments without libreadline */
#ifndef READLINE_STUB_H
#define READLINE_STUB_H
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
static inline char *readline(const char *prompt) {
    char buf[65536];
    printf("%s", prompt);
    fflush(stdout);
    if (!fgets(buf, sizeof(buf), stdin)) return NULL;
    size_t l = strlen(buf);
    if (l > 0 && buf[l-1] == '\n') buf[l-1] = '\0';
    return strdup(buf);
}
static inline void add_history(const char *line) { (void)line; }
#define HISTORY_STUB 1
#endif
