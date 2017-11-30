#define FFP_DEBUG 1


struct ffp_head {
	struct ffp_node *entry_hash;
	struct mr_entry *array;
	int max_threads;
};


struct ffp_head init_ffp(
		int max_threads);

int ffp_init_thread(
		struct ffp_head head);

void ffp_end_thread(
		struct ffp_head head,
		int thread_id);

void *ffp_search(
		struct ffp_head head,
		unsigned long long hash,
		int thread_id);

struct ffp_node *ffp_insert(
		struct ffp_head head,
		unsigned long long hash,
		void *value,
		int thread_id);

void ffp_remove(
		struct ffp_head head,
		unsigned long long hash,
		int thread_id);

//debug interface

#if FFP_DEBUG

void *ffp_debug_search(
		struct ffp_head head,
		unsigned long long hash,
		int thread_id);

#endif
