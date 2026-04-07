CC      = gcc
CFLAGS  = -O3 -march=native -std=c11 -Wall -Wextra \
           -Wno-stringop-truncation -Wno-unused-parameter \
           -D_GNU_SOURCE -D_POSIX_C_SOURCE=200809L \
           -Iinclude
LDFLAGS = -lpthread

SRC_NET    = src/network/net.c
SRC_PARSE  = src/parser/parser.c
SRC_STORE  = src/storage/storage.c
SRC_PERSIST= src/storage/persistence.c
SRC_INDEX  = src/index/index.c
SRC_CACHE  = src/cache/cache.c
SRC_EXEC   = src/query/executor.c
SRC_POOL   = src/concurrency/threadpool.c

SERVER_BIN = bin/flexql-server
CLIENT_LIB = bin/libflexql.a
REPL_BIN   = bin/flexql-client
BENCH_BIN  = bin/flexql-bench
UNIT_BIN   = bin/unit_test

.PHONY: all clean dirs test

all: dirs $(SERVER_BIN) $(CLIENT_LIB) $(REPL_BIN) $(BENCH_BIN) $(UNIT_BIN)

dirs:
	@mkdir -p bin build

# ── Object files ───────────────────────────────────────────────────────
build/net.o:        $(SRC_NET)    ; $(CC) $(CFLAGS) -c $< -o $@
build/parser.o:     $(SRC_PARSE)  ; $(CC) $(CFLAGS) -c $< -o $@
build/storage.o:    $(SRC_STORE)  ; $(CC) $(CFLAGS) -c $< -o $@
build/persist.o:    $(SRC_PERSIST); $(CC) $(CFLAGS) -c $< -o $@
build/index.o:      $(SRC_INDEX)  ; $(CC) $(CFLAGS) -c $< -o $@
build/cache.o:      $(SRC_CACHE)  ; $(CC) $(CFLAGS) -c $< -o $@
build/executor.o:   $(SRC_EXEC)   ; $(CC) $(CFLAGS) -c $< -o $@
build/threadpool.o: $(SRC_POOL)   ; $(CC) $(CFLAGS) -c $< -o $@
build/server.o:     src/server/server.c ; $(CC) $(CFLAGS) -c $< -o $@
build/client.o:     src/client/client.c ; $(CC) $(CFLAGS) -c $< -o $@
build/client_repl.o: src/client/client.c
	$(CC) $(CFLAGS) -DBUILD_REPL -c $< -o $@

COMMON_OBJS = build/net.o build/parser.o build/storage.o build/persist.o build/index.o \
              build/cache.o build/executor.o build/threadpool.o

# ── Server ─────────────────────────────────────────────────────────────
$(SERVER_BIN): $(COMMON_OBJS) build/server.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)
	@echo "[OK] $@"

# ── Static client library ──────────────────────────────────────────────
$(CLIENT_LIB): build/client.o build/net.o
	ar rcs $@ $^
	@echo "[OK] $@"

# ── REPL (single TU: client_repl.o includes the main() via BUILD_REPL) ─
$(REPL_BIN): build/client_repl.o build/net.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)
	@echo "[OK] $@"

# ── Benchmark harness ──────────────────────────────────────────────────
$(BENCH_BIN): tests/bench.c build/client.o build/net.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)
	@echo "[OK] $@"

# ── Unit tests (no server needed) ─────────────────────────────────────
$(UNIT_BIN): tests/unit_test.c $(COMMON_OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)
	@echo "[OK] $@"

test: $(UNIT_BIN)
	./$(UNIT_BIN)

clean:
	rm -rf build bin
