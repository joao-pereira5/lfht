#include <stdlib.h>
#include <stdatomic.h>
#include <mr.h>

#define CACHE_LINE_SIZE 64

struct mr_entry {
	void *tls;
	atomic_flag claim;
} __attribute__((aligned(CACHE_LINE_SIZE)));

struct mr_entry *init_mr(int max_threads)
{
	struct mr_entry *array = calloc(max_threads, sizeof(struct mr_entry));
	for(int i=0; i<max_threads; i++){
		atomic_flag_clear(&(array[i].claim));
	}
	return array;
}

int mr_thread_acquire(
		struct mr_entry *array,
		int max_threads)
{
	int i = 0;
	while(1){
		if(!atomic_flag_test_and_set(&(array[i].claim)))
			return i;
		i = (i+1) % max_threads;
	}
}

void mr_thread_release(
		struct mr_entry *array,
		int thread_id)
{
	atomic_flag_clear(&(array[thread_id].claim));
}
