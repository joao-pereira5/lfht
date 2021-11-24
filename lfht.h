#include <stddef.h>


#ifndef LFHT_DEBUG
#define LFHT_DEBUG 0
#endif

#define MAX_NODES 3
#define ROOT_HASH_SIZE 16
#define HASH_SIZE 4
#define CACHE_SIZE 64

#if LFHT_DEBUG
struct lfht_stats {
	int compression_counter;
	int compression_rollback_counter;
	int expansion_counter;
	int unfreeze_counter;
	int freeze_counter;
	int max_retry_counter;
	int operations;
	int api_calls;
	int max_depth;
};
#endif

struct lfht_head {
	struct lfht_node *entry_hash;
	int max_threads;
	int root_hash_size;
	int hash_size;
	int max_chain_nodes;
#if LFHT_DEBUG
	_Atomic(struct lfht_stats*) *stats;
#endif
};

struct lfht_head *init_lfht(
		int max_threads);

struct lfht_head *init_lfht_explicit(
		int max_threads,
		int root_hash_size,
		int hash_size,
		int max_chain_nodes);

// not thread safe
void free_lfht(struct lfht_head *lfht);

int lfht_init_thread(
		struct lfht_head *head);

void lfht_end_thread(
		struct lfht_head *head,
		int thread_id);

void *lfht_search(
		struct lfht_head *head,
		size_t hash,
		int thread_id);

struct lfht_node *lfht_insert(
		struct lfht_head *head,
		size_t hash,
		void *value,
		int thread_id);

void lfht_remove(
		struct lfht_head *head,
		size_t hash,
		int thread_id);

//debug interface

void *lfht_debug_search(
		struct lfht_head *head,
		size_t hash,
		int thread_id);
