#include <stddef.h>

#ifndef LFHT_DEBUG
#define LFHT_DEBUG 0
#endif

struct lfht_head {
	struct lfht_node *entry_hash;
	struct mr_entry *thread_array;
	int max_threads;
};


struct lfht_head *init_lfht(
		int max_threads);

int lfht_init_thread(
		struct lfht_head *head);

void lfht_end_thread(
		struct lfht_head *head,
		int thread_id);

void *lfht_search(
		struct lfht_head *head,
		size_t hash,
		int thread_id);

void *lfht_insert(
		struct lfht_head *head,
		size_t hash,
		void *value,
		int thread_id);

void *lfht_remove(
		struct lfht_head *head,
		size_t hash,
		int thread_id);

//debug interface

#if LFHT_DEBUG

void *lfht_debug_search(
		struct lfht_head *head,
		size_t hash,
		int thread_id);

#endif
