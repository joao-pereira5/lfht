CC=gcc
CFLAGS=-std=gnu11 -Wall -I. -fPIC
AR=ar
OPT=-O3
LFLAGS=-shared
DEBUG=-g -ggdb -Og -DFFP_DEBUG=1

default: libffp.a
debug: libffp_debug.a libffp_debug.so
all: libffp.a libffp.so

libffp.so: mr.o ffp.o
	$(CC) mr.o ffp.o $(CFLAGS) $(OPT) $(LFLAGS) -o libffp.so

libffp.a: ffp.o mr.o
	$(AR) rcu libffp.a ffp.o mr.o

ffp.o: ffp.c
	$(CC) -c ffp.c $(CFLAGS) $(OPT) $(LFLAGS)

mr.o: mr.c
	$(CC) -c mr.c $(CFLAGS) $(OPT) $(LFLAGS)

libffp_debug.so:
	$(CC) mr_debug.o ffp_debug.o mr.h $(CFLAGS) $(DEBUG) $(LFLAGS) -o libffp_debug.so

libffp_debug.a: ffp_debug.o mr_debug.o
	$(AR) rcu libffp_debug.a ffp_debug.o mr_debug.o

ffp_debug.o: ffp.c
	$(CC) -c ffp.c $(CFLAGS) $(DEBUG) $(LFLAGS) -o ffp_debug.o

mr_debug.o: mr.c
	$(CC) -c mr.c $(CFLAGS) $(DEBUG) $(LFLAGS) -o mr_debug.o
clean:
	rm -f *.o *.a *.so
