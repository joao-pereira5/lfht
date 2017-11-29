CC=gcc
CFLAGS=-std=gnu11 -g -Wall -O3 -flto
LFLAGS=-pthread -lpthread -static $(JEFLAGS)
JEFLAGS=-L`jemalloc-config --libdir` -Wl,-rpath,`jemalloc-config --libdir` -ljemalloc `jemalloc-config --libs`

all: bench

bench: bench.o ffp.o mr.o
	$(CC) $(CFLAGS) bench.o ffp.o mr.o $(LFLAGS) -I. -o bench

bench.o: bench.c
	$(CC) -c bench.c -I. $(CFLAGS) $(LFLAGS)

ffp.o: ffp.c
	$(CC) -c ffp.c -I. $(CFLAGS) $(LFLAGS)

mr.o: mr.c
	$(CC) -c mr.c -I. $(CFLAGS) $(LFLAGS)

clean:
	rm *.o bench
