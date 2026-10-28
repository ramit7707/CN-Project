CC = cc
CFLAGS = -Wall -Wextra -pthread -O2 -std=c99
LDFLAGS = -pthread

all: server client

server: server.o utils.o
	$(CC) $(LDFLAGS) -o server server.o utils.o

client: client.o utils.o
	$(CC) $(LDFLAGS) -o client client.o utils.o

server.o: server.c protocol.h utils.h
	$(CC) $(CFLAGS) -c server.c

client.o: client.c protocol.h utils.h
	$(CC) $(CFLAGS) -c client.c

utils.o: utils.c utils.h protocol.h
	$(CC) $(CFLAGS) -c utils.c

clean:
	rm -f server client *.o

.PHONY: all clean
