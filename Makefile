CC=gcc
CFLAGS=-std=gnu11 -g -Wall -O2 -flto -I.
LFLAGS=-pthread -lpthread
JEFLAGS=-L`jemalloc-config --libdir` -Wl,-rpath,`jemalloc-config --libdir` -ljemalloc `jemalloc-config --libs` -static
SEQFLAGS=-ldl
LFFLAGS=-lstdc++ -ldl

all: bench_glc bench_je bench_seq bench_lf

bench_seq: bench.o ffp.o mr.o
	$(CC) $(CFLAGS) bench.o ffp.o mr.o deps/seqmalloc/seqmalloc.a $(LFLAGS) $(SEQFLAGS) -o bench_seq

bench_lf: bench.o ffp.o mr.o
	$(CC) $(CFLAGS) bench.o ffp.o mr.o deps/lfmalloc/lfmalloc.a $(LFLAGS) $(LFFLAGS) -o bench_lf

bench_je: bench.o ffp.o mr.o
	$(CC) $(CFLAGS) bench.o ffp.o mr.o $(LFLAGS) $(JEFLAGS) -o bench_je

bench_glc: bench.o ffp.o mr.o
	$(CC) $(CFLAGS) bench.o ffp.o mr.o $(LFLAGS) -o bench_glc

bench.o: bench.c ffp.h
	$(CC) -c bench.c $(CFLAGS) $(LFLAGS)

ffp.o: ffp.c
	$(CC) -c ffp.c $(CFLAGS) $(LFLAGS)

mr.o: mr.c
	$(CC) -c mr.c $(CFLAGS) $(LFLAGS)

clean:
	rm *.o bench_*
