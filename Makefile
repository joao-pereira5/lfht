CC=gcc
CFLAGS=-std=gnu11 -Wall -I. -fPIC
AR=ar
OPT=-O3
LFLAGS=-shared
DEBUG=-g -ggdb -Og -DLFHT_DEBUG=1

default: liblfht.a
debug: liblfht_debug.a liblfht_debug.so
all: liblfht.a liblfht.so

liblfht.so: mr.o lfht.o
	$(CC) mr.o lfht.o $(CFLAGS) $(OPT) $(LFLAGS) -o liblfht.so

liblfht.a: lfht.o mr.o
	$(AR) rcu liblfht.a lfht.o mr.o

lfht.o: lfht.c
	$(CC) -c lfht.c $(CFLAGS) $(OPT) $(LFLAGS)

mr.o: mr.c
	$(CC) -c mr.c $(CFLAGS) $(OPT) $(LFLAGS)

liblfht_debug.so:
	$(CC) mr_debug.o lfht_debug.o mr.h $(CFLAGS) $(DEBUG) $(LFLAGS) -o liblfht_debug.so

liblfht_debug.a: lfht_debug.o mr_debug.o
	$(AR) rcu liblfht_debug.a lfht_debug.o mr_debug.o

lfht_debug.o: lfht.c
	$(CC) -c lfht.c $(CFLAGS) $(DEBUG) $(LFLAGS) -o lfht_debug.o

mr_debug.o: mr.c
	$(CC) -c mr.c $(CFLAGS) $(DEBUG) $(LFLAGS) -o mr_debug.o
clean:
	rm -f *.o *.a *.so
