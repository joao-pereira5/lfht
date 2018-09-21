CC=gcc
CFLAGS=-std=gnu11 -Wall -flto -I.
OPT= -O3
LFLAGS=-pthread -lpthread
DEBUG= -g -O0 -DFFP_DEBUG=1
JEFLAGS=-L`jemalloc-config --libdir` -Wl,-rpath,`jemalloc-config --libdir` -ljemalloc `jemalloc-config --libs` -static


default: bench bench_je
all: bench bench_je bench_debug bench_debug_je

bench_je: bench.o ffp.o mr.o
	$(CC) $(CFLAGS) $(OPT) bench.o ffp.o mr.o $(LFLAGS) $(JEFLAGS) -o bench_je

bench: bench.o ffp.o mr.o
	$(CC) $(CFLAGS) $(OPT) bench.o ffp.o mr.o $(LFLAGS) -o bench_glc

bench.o: bench.c ffp.h
	$(CC) -c bench.c $(CFLAGS) $(OPT) $(LFLAGS)

ffp.o: ffp.c
	$(CC) -c ffp.c $(CFLAGS) $(OPT) $(LFLAGS)

mr.o: mr.c
	$(CC) -c mr.c $(CFLAGS) $(OPT) $(LFLAGS) -static

bench_debug_je: bench_debug.o ffp_debug.o mr_debug.o
	$(CC) $(CFLAGS) $(DEBUG) bench_debug.o ffp_debug.o mr_debug.o $(LFLAGS) $(JEFLAGS) -o bench_debug_je

bench_debug: bench_debug.o ffp_debug.o mr_debug.o
	$(CC) $(CFLAGS) $(DEBUG) bench_debug.o ffp_debug.o mr_debug.o $(LFLAGS) -o bench_debug_glc

bench_debug.o: bench.c ffp.h
	$(CC) -c bench.c $(CFLAGS) $(DEBUG) $(LFLAGS) -o bench_debug.o

ffp_debug.o: ffp.c
	$(CC) -c ffp.c $(CFLAGS) $(DEBUG) $(LFLAGS) -o ffp_debug.o

mr_debug.o: mr.c
	$(CC) -c mr.c $(CFLAGS) $(DEBUG) $(LFLAGS) -static -o mr_debug.o
clean:
	rm -f *.o bench_*
