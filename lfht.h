#include <stddef.h>

#ifndef LFHT_DEBUG
#define LFHT_DEBUG 0
#endif

#ifndef LFHT_STATS
#define LFHT_STATS 0
#endif

#define MAX_NODES 3
#define ROOT_HASH_SIZE 16
#define HASH_SIZE 4
#define CACHE_SIZE 64

#if LFHT_STATS
#include <time.h>

struct lfht_stats {
	int compression_counter;
	int compression_rollback_counter;
	int expansion_counter;
	int unfreeze_counter;
	int freeze_counter;
	unsigned long max_retry_counter;
	int operations;
	int inserts;
	int removes;
	int searches;
	int api_calls;
	int max_depth;
	unsigned long paths;
	unsigned long lookups;
	struct timespec term;
};
#endif

struct lfht_head {
	struct lfht_node *entry_hash;
	int max_threads;
	int root_hash_size;
	int hash_size;
	int max_chain_nodes;
#if LFHT_STATS
	_Atomic(struct lfht_stats*) *stats;
#endif
};

#if LFHT_STATS
void lfht_reset_stats(struct lfht_head *lfht, int tid) {
	struct lfht_stats *s = lfht->stats[tid];
	s->compression_counter = 0;
	s->compression_rollback_counter = 0;
	s->expansion_counter = 0;
	s->unfreeze_counter = 0;
	s->freeze_counter = 0;
	s->max_retry_counter = 0;
	s->operations = 0;
	s->inserts = 0;
	s->removes = 0;
	s->searches = 0;
	s->api_calls = 0;
	s->max_depth = 0;
	s->paths = 0;
}
#endif

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

