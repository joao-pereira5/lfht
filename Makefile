CFLAGS=-std=gnu11 -Wall -Wextra -I. -fPIC
AR=ar
OPT=-O3 -DLFHT_STATS=1
LFLAGS=-shared
DEBUG=-g -ggdb -Og -DLFHT_DEBUG=1 -DLFHT_STATS=1

default: liblfht.a
debug: liblfht_debug.a liblfht_debug.so
all: liblfht.a liblfht.so

liblfht.so: lfht.o
	$(CC) lfht.o $(CFLAGS) $(OPT) $(LFLAGS) -o liblfht.so

liblfht.a: lfht.o
	$(AR) rc liblfht.a lfht.o

lfht.o: lfht.c
	$(CC) -c lfht.c $(CFLAGS) $(OPT) $(LFLAGS)

liblfht_debug.so:
	$(CC) lfht_debug.o $(CFLAGS) $(DEBUG) $(LFLAGS) -o liblfht_debug.so

liblfht_debug.a: lfht_debug.o
	$(AR) rc liblfht_debug.a lfht_debug.o

lfht_debug.o: lfht.c
	$(CC) -c lfht.c $(CFLAGS) $(DEBUG) $(LFLAGS) -o lfht_debug.o

clean:
	rm -f *.o *.a *.so
