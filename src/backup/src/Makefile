CC=g++
CFLAGS=-std=c++17 -w -I./
LDVERBS= -libverbs
LDPTHREAD= -lpthread
LDTBB=-ltbb
LDFLAGS= -libverbs -lpthread -ltbb
BACKUP=backup
EXTRA=extra
MASTER=master
all: $(BACKUP) $(EXTRA) $(MASTER)
$(BACKUP): $(wildcard main.cpp backup.cpp encode.cpp value_sgement.cpp rdma_connect.cpp libisal.a librocksdb.a)
	$(CC) $(CFLAGS) $^ $(LDVERBS) $(LDPTHREAD) $(LDTBB) -o $@
$(EXTRA): $(wildcard extra.cpp value_sgement.cpp backup.cpp encode.cpp db.cpp rdma_connect.cpp librocksdb.a libisal.a)
	$(CC) $(CFLAGS) $^ $(LDVERBS) $(LDPTHREAD) $(LDTBB) -o $@
$(MASTER): $(wildcard master.cpp master_rdma_connect.cpp)
	$(CC) $(CFLAGS) $^ $(LDVERBS) -o $@
clean:
	$(RM) $(BACKUP) $(EXTRA) $(MASTER)

.PHONY: $(BACKUP) $(EXTRA) $(MASTER)

b: $(BACKUP)
e: $(EXTRA)
m: $(MASTER)
