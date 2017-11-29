CC=gcc
CFLAGS=-std=gnu11 -g -Wall -O3 -flto
LFLAGS=-pthread -lpthread
JEFLAGS=-L`jemalloc-config --libdir` -Wl,-rpath,`jemalloc-config --libdir` -ljemalloc `jemalloc-config --libs` -static
SEQFLAGS=-ldl

all: bench_glc bench_je bench_seq

bench_seq: bench.o ffp.o mr.o
	$(CC) $(CFLAGS) bench.o ffp.o mr.o deps/seqmalloc/seqmalloc.a $(LFLAGS) $(SEQFLAGS) -I. -o bench_seq

bench_je: bench.o ffp.o mr.o
	$(CC) $(CFLAGS) bench.o ffp.o mr.o $(LFLAGS) $(JEFLAGS) -I. -o bench_je

bench_glc: bench.o ffp.o mr.o
	$(CC) $(CFLAGS) bench.o ffp.o mr.o $(LFLAGS) -I. -o bench_glc

bench.o: bench.c
	$(CC) -c bench.c -I. $(CFLAGS) $(LFLAGS)

ffp.o: ffp.c
	$(CC) -c ffp.c -I. $(CFLAGS) $(LFLAGS)

mr.o: mr.c
	$(CC) -c mr.c -I. $(CFLAGS) $(LFLAGS)

clean:
	rm *.o bench_*
