CC=g++
CFLAGS=-std=c++17 -w  -pthread  -I./
LDFLAGS= -libverbs -lpthread
OBJECTS=$(SUBSRCS:.cc=.o)
EXEC=primary

all: $(EXEC)
$(EXEC): $(wildcard main.cpp db.cpp value_sgement.cpp rdma_connect.cpp librocksdb.a) $(OBJECTS)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) -o $@

clean:
	$(RM) $(EXEC)

.PHONY: $(EXEC)

