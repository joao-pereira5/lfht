#include <stdlib.h>
#include <stdint.h>
#include <stdatomic.h>
#include <lfht.h>

#if LFHT_DEBUG
#include <stdio.h>
#include <assert.h>
#define CYCLE_THRESHOLD 10000000
#endif

enum ntype {HASH, LEAF, FREEZE, UNFREEZE};

struct lfht_node;

// a node of the trie
// "size" = chunk size
// on level hash_pos/size of the tree
// hash_pos is incremented in chunks (see: get_bucket_index())
// 2^size = length of "array" of buckets
struct lfht_node_hash {
	int size;
	int hash_pos;
	_Atomic(int) role;
	struct lfht_node *prev;
	_Atomic(struct lfht_node *) array[0];
};

// key-value pair node
struct lfht_node_leaf {
	size_t hash;
	void *value;
	_Atomic(struct lfht_node *) next;
};

struct lfht_node {
	enum ntype type;
	union {
		struct lfht_node_hash hash;
		struct lfht_node_leaf leaf;
	};
};

// private functions

void search_remove(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *hnode,
		size_t hash);

struct lfht_node *search_insert(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *hnode,
		size_t hash,
		void *value);

void compress(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *target,
		size_t hash);

int unfreeze(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *target,
		size_t hash);

void abort_compress(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *target,
		struct lfht_node *freeze,
		_Atomic(struct lfht_node *) *atomic_bucket);

int expand(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node **new_hash,
		struct lfht_node *hnode,
		size_t hash,
		_Atomic(struct lfht_node *) *tail_nxt_ptr);

void adjust_chain_nodes(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *cnode,
		struct lfht_node *hnode);

void adjust_node(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *cnode,
		struct lfht_node *hnode);

void *search_node(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *hnode,
		size_t hash);

struct lfht_node *create_hash_node(
		int size,
		int hash_pos,
		struct lfht_node *prev);

struct lfht_node *get_next(
		struct lfht_node *node);

struct lfht_node *valid_ptr(struct lfht_node *next);

struct lfht_node *invalid_ptr(struct lfht_node *next);

unsigned is_invalid(struct lfht_node *ptr);

unsigned is_compression_node(struct lfht_node *node);

unsigned is_empty(struct lfht_node *hnode);

// debug functions

#if LFHT_DEBUG

void *lfht_debug_search(
		struct lfht_head *lfht,
		size_t hash,
		int thread_id);

void *debug_search_chain(
		struct lfht_node *cnode,
		struct lfht_node *hnode,
		size_t hash);

void *debug_search_hash(
		struct lfht_node *hnode,
		size_t hash);

#endif

// public functions
// defined by the header API

struct lfht_head *init_lfht(int max_threads) {
	return init_lfht_explicit(
			max_threads,
			ROOT_HASH_SIZE,
			HASH_SIZE,
			MAX_NODES);
}

struct lfht_head *init_lfht_explicit(
		int max_threads,
		int root_hash_size,
		int hash_size,
		int max_chain_nodes) {
	struct lfht_head *lfht = malloc(sizeof(struct lfht_head));

	lfht->entry_hash = create_hash_node(root_hash_size, 0, NULL);
	lfht->max_threads = max_threads;
	lfht->root_hash_size = root_hash_size;
	lfht->hash_size = hash_size;
	lfht->max_chain_nodes = max_chain_nodes;
#if LFHT_DEBUG
	lfht->stats = (_Atomic(struct lfht_stats*) *)
		malloc(max_threads*sizeof(_Atomic(struct lfht_stats*)));

	for(int i = 0; i < lfht->max_threads; i++) {
		atomic_init(&(lfht->stats[i]), NULL);
	}

	if(lfht->max_threads <= 1) {
		lfht_init_thread(lfht);
	}
#endif
	return lfht;
}

void free_lfht(struct lfht_head *lfht) {
#if LFHT_DEBUG
	for(int i = 0; i < lfht->max_threads; i++) {
		free(lfht->stats[i]);
	}
	free(lfht->stats);
#endif
}

int lfht_init_thread(struct lfht_head *lfht)
{
#if LFHT_DEBUG
	size_t stats_size = CACHE_SIZE * ((sizeof(struct lfht_stats) / CACHE_SIZE) + 1);
	struct lfht_stats *s = (struct lfht_stats *) aligned_alloc(CACHE_SIZE, stats_size);
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

	for(int i = 0; i < lfht->max_threads; i++) {
		struct lfht_stats *expect = NULL;
		if (atomic_compare_exchange_weak_explicit(
				&(lfht->stats[i]),
				&expect,
				s,
				memory_order_acq_rel,
				memory_order_consume)) {
			return i;
		}
	}

	free(s);
	return -1;
#else
	return 0;
#endif
}

void lfht_end_thread(struct lfht_head *lfht, int thread_id)
{
#if LFHT_DEBUG
	struct lfht_stats *s = lfht->stats[thread_id];
	clock_gettime(CLOCK_MONOTONIC_RAW, &(s->term));
#endif
}

void *lfht_search(
		struct lfht_head *lfht,
		size_t hash,
		int thread_id)
{
#if LFHT_DEBUG
	struct lfht_stats* stats = lfht->stats[thread_id];
	stats->api_calls++;
	stats->searches++;
#endif
	return search_node(
			lfht,
			thread_id,
			lfht->entry_hash,
			hash);
}

struct lfht_node *lfht_insert(
		struct lfht_head *lfht,
		size_t hash,
		void *value,
		int thread_id)
{
#if LFHT_DEBUG
	struct lfht_stats* stats = lfht->stats[thread_id];
	stats->api_calls++;
	stats->inserts++;
#endif
	return search_insert(
			lfht,
			thread_id,
			lfht->entry_hash,
			hash,
			value);
}

void lfht_remove(
		struct lfht_head *lfht,
		size_t hash,
		int thread_id)
{
#if LFHT_DEBUG
	struct lfht_stats* stats = lfht->stats[thread_id];
	stats->api_calls++;
	stats->removes++;
#endif
	return search_remove(
			lfht,
			thread_id,
			lfht->entry_hash,
			hash);
}

// auxiliary functions

struct lfht_node *create_freeze_node(
		struct lfht_node *next)
{
	struct lfht_node *node = malloc(sizeof(struct lfht_node));
	node->type = FREEZE;
	node->leaf.hash = 0;
	node->leaf.value = NULL;

	atomic_init(&(node->leaf.next), next);

#if LFHT_DEBUG
	assert(next);
	// a compression node should NEVER point
	// to another compression node
	assert(!is_compression_node(next));
#endif

	return node;
}

struct lfht_node *create_unfreeze_node(
		struct lfht_node *next)
{
	struct lfht_node *node = malloc(sizeof(struct lfht_node));
	node->type = UNFREEZE;
	node->leaf.hash = 0;
	node->leaf.value = NULL;

	atomic_init(&(node->leaf.next), next);

#if LFHT_DEBUG
	assert(next);
	assert(!is_compression_node(next));
#endif

	return node;
}

struct lfht_node *create_leaf_node(
		size_t hash,
		void *value,
		struct lfht_node *next)
{
	struct lfht_node *node = malloc(sizeof(struct lfht_node));
	node->type = LEAF;
	node->leaf.hash = hash;
	node->leaf.value = value;

	atomic_init(&(node->leaf.next), next);

	return node;
}

struct lfht_node *create_hash_node(
		int size,
		int hash_pos,
		struct lfht_node *prev)
{
	struct lfht_node *node = malloc(
			sizeof(struct lfht_node) + (1<<size)*sizeof(struct lfht_node *));
	node->type = HASH;
	node->hash.size = size;
	node->hash.hash_pos = hash_pos;
	atomic_init(&(node->hash.role), 0);
	node->hash.prev = prev;
	for(int i=0; i < 1<<size; i++) {
		atomic_init(&(node->hash.array[i]), node);
	}
	return node;
}

// for the current hash level, return the chunk of the
// hash which indexes the bucket array (of size 2^w)
int get_bucket_index(
		size_t hash,
		int hash_pos,
		int size)
{
	// hash_pos does not increment 1 by 1
	// it increments with the chunk size
	return (hash >> hash_pos) & ((1 << size) - 1);
}

_Atomic(struct lfht_node *) *get_atomic_bucket(
		size_t hash,
		struct lfht_node *hnode)
{
	int pos = get_bucket_index(
			hash,
			hnode->hash.hash_pos,
			hnode->hash.size);

	return &(hnode->hash.array[pos]);
}

// release-consume order get next
struct lfht_node *get_next(
		struct lfht_node *node)
{
#if LFHT_DEBUG
		assert(node);
		assert(&(node->leaf.next));
		assert(node);
#endif
	//return atomic_load_explicit(
	//		&(node->leaf.next),
	//		memory_order_consume);

	// avoiding hazards during compress operation WIP
	struct lfht_node* nxt = atomic_load_explicit(
			&(node->leaf.next),
			memory_order_seq_cst);

#if LFHT_DEBUG
	assert(nxt);
#endif
	return nxt;
}

// we do not use an "is_valid" field.
// to verify if a node is valid, the least significant bit
// of the *next field is used (the address, not the node it
// points to).
//
// validating a node using the mask ~1:
// 0xFFFF & ~1 === 0xFFFF & 0xFFFE = 0xFFFE
//
// invalidating a node:
// 0xFFFE | 1 === 0xFFFE | 0x0001 = 0xFFFF
//
// arg: the *next address of the node we want to invalidate
// return: pointer masked with "valid"
struct lfht_node *valid_ptr(struct lfht_node *next)
{
	return (struct lfht_node *) ((uintptr_t) next & ~1);
}

struct lfht_node *invalid_ptr(struct lfht_node *next)
{
	return (struct lfht_node *) ((uintptr_t) next | 1);
}

// mask address to get the valid/invalid bit flag
// 
// arg: the *next address of the node
// return: the least significant bit of *next
unsigned is_invalid(struct lfht_node *ptr)
{
	return (uintptr_t) ptr & 1;
}

unsigned is_compression_node(struct lfht_node *node)
{
	return node->type == FREEZE || node->type == UNFREEZE;
}

unsigned is_empty(struct lfht_node *hnode)
{
	for(int i = 0; i < (1<<hnode->hash.size); i++) {
		struct lfht_node *head;
		_Atomic(struct lfht_node *) *nxt_atomic_bucket =
			&(hnode->hash.array[i]);

		head = atomic_load_explicit(
				nxt_atomic_bucket,
				memory_order_consume);

		if(head != hnode) {
			// bucket not empty
			return 0;
		}
	}

	return 1;
}

int mark_invalid(struct lfht_node *cnode)
{
#if LFHT_DEBUG
	assert(cnode);
	assert(cnode->type == LEAF);
#endif
	struct lfht_node *expect = valid_ptr(get_next(cnode));

	// replace .next with the invalid address
	while(!atomic_compare_exchange_weak_explicit(
				&(cnode->leaf.next),
				&expect,
				invalid_ptr(expect),
				memory_order_acq_rel,
				memory_order_consume)) {
#if LFHT_DEBUG
		assert(expect);
#endif
		if(is_invalid(expect)) {
			// node was invalidated by another thread
			// aborting
			return 0;
		}
	}
	return 1;
}

// retries CAS until it succeeds
int force_cas(struct lfht_node *node, struct lfht_node *replace)
{
#if LFHT_DEBUG
	assert(node);
	assert(replace);
	assert(node->type == LEAF);
#endif
	struct lfht_node *expect = get_next(node);

	if(is_invalid(expect)) {
		return 0;
	}

	if(expect == replace) {
		// value already in place
		return 1;
	}

	while(!atomic_compare_exchange_weak_explicit(
				&(node->leaf.next),
				&expect,
				replace,
				memory_order_acq_rel,
				memory_order_consume)) {
		if(is_invalid(expect)) {
			return 0;
		}
	}
	return 1;
}

int give_role(struct lfht_node *hnode)
{
	for(int i = 0; i < (1<<hnode->hash.size); i++) {
		struct lfht_node *head;
		_Atomic(struct lfht_node *) *nxt_atomic_bucket =
			&(hnode->hash.array[i]);

		head = atomic_load_explicit(
				nxt_atomic_bucket,
				memory_order_consume);

		if(is_compression_node(head)) {
			return 1;
		}

		if(head == hnode) {
			continue;
		}

		atomic_store_explicit(
				&(hnode->hash.role),
				i,
				memory_order_release);

		// observe again
		head = atomic_load_explicit(
				nxt_atomic_bucket,
				memory_order_consume);

		return head != hnode;
	}

	return 0;
}

// this function changes the values of *hnode, *nodeptr, count and *last_valid_atomic
// *hnode -> will point to the hash node, containing the bucket of
//   the target node
// *nodeptr -> will point to the target node, if it exists
// count -> number of nodes of the last traversed chain up until the node was found,
//   or the length of the chain if the node isn't present
// *last_valid_atomic -> will point to an atomic node, 
// pointer to the last valid node of the chain
//
// returns: 0/1 success
int find_node(
		struct lfht_head *lfht,
		int thread_id,
		size_t hash,
		struct lfht_node **hnode,
		struct lfht_node **nodeptr,
		_Atomic(struct lfht_node *) **last_valid_atomic,
		unsigned int *count)
{
start: ;
#if LFHT_DEBUG
	assert(nodeptr);
	assert(hnode);
	assert(*hnode);
	assert((*hnode)->type == HASH);
	struct lfht_stats* stats = lfht->stats[thread_id];
#endif

	_Atomic(struct lfht_node *) *atomic_head =
		get_atomic_bucket(hash, *hnode);

	struct lfht_node *iter = atomic_load_explicit(
			atomic_head,
			memory_order_consume);

	struct lfht_node *head = iter;
	*nodeptr = head;

	if(is_compression_node(head)) {
		// skip compression node
		iter = valid_ptr(get_next(iter));
#if LFHT_DEBUG
		assert(iter);
		assert(iter->type == HASH);
#endif
	}

	if(last_valid_atomic) {
		*last_valid_atomic = atomic_head;
		*count = iter->type == LEAF ? 1 : 0;
	}

	// traverse chain (tail points back to hash node)
	while(iter != *hnode) {
#if LFHT_DEBUG
		stats->paths++;
#endif

		if(iter->type == HASH) {
			// travel down a level and search for the node there

			// important loop!
			// during adjust (after force_cas)
			// there is a window when nodes may skip a level
			while(iter->hash.prev != *hnode) {
				iter = iter->hash.prev;
			}

			*hnode = iter;
			goto start;
		}
#if LFHT_DEBUG
		assert(!is_compression_node(head));
		assert(iter->type == LEAF);
#endif

		struct lfht_node *nxt_iter = get_next(iter);
		if(!is_invalid(nxt_iter)) {
			// iter is a valid node

			if(iter->leaf.hash == hash) {
				// found node
				*nodeptr = iter;
				return 1;
			}
			*nodeptr = nxt_iter;

			if(last_valid_atomic) {
				*last_valid_atomic = &(iter->leaf.next);
				(*count)++;
			}
		}

		// advance chain
		iter = valid_ptr(nxt_iter);
	}

	return 0;
}

void make_unreachable(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *cnode,
		struct lfht_node *hnode)
{
#if LFHT_DEBUG
	struct lfht_stats* stats = lfht->stats[thread_id];
	stats->operations++;
#endif

start: ;
#if LFHT_DEBUG
	assert(cnode);
	assert(hnode);
	assert(cnode->type == LEAF);
	assert(hnode->type == HASH);

	stats->max_retry_counter++;
#endif
	struct lfht_node *iter;
	struct lfht_node *nxt = valid_ptr(get_next(cnode));

	while(nxt->type == LEAF) {
		iter = get_next(nxt);
		if(!is_invalid(iter)) {
			// found next valid node of the chain
			break;
		}
		nxt = valid_ptr(iter);
	}
	iter = nxt;

	// advance to the next hash
	while(iter->type != HASH) {
		iter = valid_ptr(get_next(iter));
	}

	if(iter != hnode && iter->hash.hash_pos < hnode->hash.hash_pos) {
		// TODO: current hash level is collapsing on the previous level?
		return;
	}

	if(iter != hnode) {
		hnode = iter;
		goto start;
	}

	int pos = get_bucket_index(
			cnode->leaf.hash,
			hnode->hash.hash_pos,
			hnode->hash.size);
	_Atomic(struct lfht_node *) *observed_bucket = &(hnode->hash.array[pos]);
	_Atomic(struct lfht_node *) *prev_atomic = observed_bucket;
	struct lfht_node *prev = atomic_load_explicit(
			prev_atomic,
			memory_order_consume);

	if(is_compression_node(prev)) {
		// skip compression node
		prev = valid_ptr(get_next(prev));
		return;
	}

	// let's find the last valid node before our target
	iter = prev;
	while(iter != cnode && iter->type == LEAF) {
		iter = get_next(iter);

		if(!is_invalid(iter)) {
			// node is valid, storing its .next atomic field
			prev_atomic = &(prev->leaf.next);
			prev = iter;
			continue;
		}
		iter = valid_ptr(iter);
	}

	if(iter == cnode) {
		// try to disconnect our target from chain
#if LFHT_DEBUG
		if (prev->type ==HASH && nxt->type == HASH) {
			assert(prev == nxt);
		}
#endif
		if(atomic_compare_exchange_strong_explicit(
					prev_atomic,
					&prev,
					nxt,
					memory_order_acq_rel,
					memory_order_consume)) {

			if(observed_bucket != prev_atomic) {
				// bucket not empty
				return;
			}
			// our removed node was the last of the chain

			int role = atomic_load_explicit(
					&(hnode->hash.role),
					memory_order_consume);

			if(role == pos) {
				compress(lfht, thread_id, hnode, cnode->leaf.hash);
			}
			return;
		}

		goto start;
	}
}

// remove functions

void search_remove(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *hnode,
		size_t hash)
{
#if LFHT_DEBUG
	assert(hnode);
	assert(hnode->type == HASH);
#endif

	struct lfht_node *cnode;
	if(!find_node(lfht, thread_id, hash, &hnode, &cnode, NULL, NULL)) {
		return;
	}

	if(!mark_invalid(cnode)) {
		return;
	}

	make_unreachable(lfht, thread_id, cnode, hnode);
}

// insertion functions

struct lfht_node *search_insert(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *hnode,
		size_t hash,
		void *value)
{
#if LFHT_DEBUG
	struct lfht_stats* stats = lfht->stats[thread_id];
	stats->operations++;
#endif

start: ;
#if LFHT_DEBUG
	stats->max_retry_counter++;
#endif

	struct lfht_node *cnode;
	 _Atomic(struct lfht_node*) *last_valid_atomic;
	unsigned int count;

	if(find_node(lfht, thread_id, hash, &hnode, &cnode, &last_valid_atomic, &count)) {
		// node already inserted
		return cnode;
	}

	if(cnode->type == FREEZE && !unfreeze(lfht, thread_id, hnode, hash)) {
#if LFHT_DEBUG
		assert(count == 0);
		assert(cnode->leaf.next == hnode);
#endif
		if(hnode->hash.prev != NULL) {
			// starting to insert a node in an already deleted
			// hash level will cause an infinite cycle
			hnode = hnode->hash.prev;
		}
		goto start;
	}
#if LFHT_DEBUG
	assert(cnode->type != UNFREEZE);
#endif

	// expand hash level
	if(count >= MAX_NODES) {
		struct lfht_node *new_hash;
		// add new level to tail of chain
		if(expand(lfht, thread_id, &new_hash, hnode, hash, last_valid_atomic)) {
			// level added
			hnode = new_hash;
		}

		goto start;
	}

	// insert new node in current bucket
	struct lfht_node *new_node = create_leaf_node(
			hash,
			value,
			hnode);
	if(atomic_compare_exchange_strong_explicit(
				last_valid_atomic,
				&cnode,
				new_node,
				memory_order_acq_rel,
				memory_order_consume)) {
		return new_node;
	}

	free(new_node);
	goto start;
}

// compression functions

void compress(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *target,
		size_t hash)
{
#if LFHT_DEBUG
	struct lfht_stats* stats = lfht->stats[thread_id];
	stats->operations++;
#endif

start: ;
#if LFHT_DEBUG
	assert(target);
	assert(target->type == HASH);

	stats->max_retry_counter++;
#endif

	if(target->hash.prev == NULL) {
		return;
	}

	if(!is_empty(target)) {
		// we cannot compress the root hash or a non empty hash
		if(!give_role(target)) {
			goto start;
		}

		return;
	}

	struct lfht_node *freeze = create_freeze_node(target);
	struct lfht_node *expect;
	struct lfht_node *prev_hash = target->hash.prev;
	_Atomic(struct lfht_node *) *atomic_bucket =
		get_atomic_bucket(hash, prev_hash);

	// try to place freeze node in front of bucket,
	// pointing to target hash
	expect = target;
	if(!atomic_compare_exchange_strong_explicit(
				atomic_bucket,
				&expect,
				freeze,
				memory_order_acq_rel,
				memory_order_consume)) {
		free(freeze);
		return;
	}
#if LFHT_DEBUG
	stats->freeze_counter++;
#endif

	// freeze empty buckets
	for(int i = 0; i < (1<<target->hash.size); i++) {
		_Atomic(struct lfht_node *) *nxt_atomic_bucket =
			&(target->hash.array[i]);

		expect = target;
		if(!atomic_compare_exchange_strong_explicit(
					nxt_atomic_bucket,
					&expect,
					freeze,
					memory_order_acq_rel,
					memory_order_consume)) {
			// bucket not empty

			// WARNING: be careful with the ABA problem
			// if we free() freeze node here, some other
			// thread might have it in local storage
			//
			//free(freeze);

			abort_compress(lfht, thread_id, target, freeze, atomic_bucket);
			goto start;
		}
	}

	// removing hash from the trie (commit)
	expect = freeze;
	if(!atomic_compare_exchange_strong_explicit(
				atomic_bucket,
				&expect,
				prev_hash,
				memory_order_acq_rel,
				memory_order_consume)) {

		// WARNING: be careful with the ABA problem
		// if we free() freeze node here, some other
		// thread might have it in local storage
		//
		//free(freeze);

		abort_compress(lfht, thread_id, target, freeze, atomic_bucket);
		goto start;
	}

	// compressed level
#if LFHT_DEBUG
	stats->compression_counter++;
#endif
	// try to compress previous level
	target = target->hash.prev;
	goto start;
}

// notify compressing thread to abort compression
int unfreeze(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *target,
		size_t hash)
{
#if LFHT_DEBUG
	assert(target);
	assert(target->type == HASH);
#endif
	_Atomic(struct lfht_node *) *atomic_bucket =
		get_atomic_bucket(hash, target->hash.prev);

	struct lfht_node *head = atomic_load_explicit(
			atomic_bucket,
			memory_order_consume);

	if(head == target) {
		// compression rolled back successfully
		return 1;
	}

	if(head->type == UNFREEZE) {
		return get_next(head) == target;
	}

	if(head->type != FREEZE || get_next(head) != target) {
		// level already removed
		return 0;
	}

	struct lfht_node *unfreeze = create_unfreeze_node(target);

	// try to place unfreeze node in front of bucket,
	// pointing to freeze node
	if(!atomic_compare_exchange_strong_explicit(
				atomic_bucket,
				&head,
				unfreeze,
				memory_order_acq_rel,
				memory_order_consume)) {
		// already compressed, unfrozen or removed
		free(unfreeze);

		if(head == target) {
			// compression rolled back successfully
			return 1;
		}

		if(head->type == UNFREEZE) {
			return get_next(head) == target;
		}

		return 0;
	}

#if LFHT_DEBUG
	struct lfht_stats* stats = atomic_load_explicit(&(lfht->stats[thread_id]), memory_order_relaxed);
	stats->unfreeze_counter++;
#endif
	return 1;
}

// target -> hash level we wanted to compress
// atomic_bucket -> atomic object of the previous level
//   bucket pointing to our target
void abort_compress(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *target,
		struct lfht_node *freeze,
		_Atomic(struct lfht_node *) *atomic_bucket)
{
	struct lfht_node *expect;
#if LFHT_DEBUG
	assert(target);
	assert(target->type == HASH);
	assert(freeze);
	assert(is_compression_node(freeze));
#endif

	// point all buckets to target
	for(int i = 0; i < (1<<target->hash.size); i++) {
		_Atomic(struct lfht_node *) *nxt_atomic_bucket = &(target->hash.array[i]);
		expect = freeze;

		// ignoring CAS failure
		// just making sure all buckets point to target
		// if it fails, they already do point to target or have a chain
		atomic_compare_exchange_strong_explicit(
				nxt_atomic_bucket,
				&expect,
				target,
				memory_order_acq_rel,
				memory_order_consume);
	}

	// commit level by removing compression bridge node
	atomic_store_explicit(
			atomic_bucket,
			target,
			memory_order_release);
#if LFHT_DEBUG
	struct lfht_stats* stats = atomic_load_explicit(&(lfht->stats[thread_id]), memory_order_relaxed);
	stats->compression_rollback_counter++;
#endif
}

// expansion functions

int expand(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node **new_hash,
		struct lfht_node *hnode,
		size_t hash,
		_Atomic(struct lfht_node *) *tail_nxt_ptr)
{
#if LFHT_DEBUG
	assert(hnode);
	assert(hnode->type == HASH);
#endif

	struct lfht_node *exp = hnode;

	*new_hash = create_hash_node(
			lfht->hash_size,
			hnode->hash.hash_pos + hnode->hash.size,
			hnode);

	// add new hash level to tail of chain
	if(atomic_compare_exchange_strong_explicit(
				tail_nxt_ptr,
				&exp,
				*new_hash,
				memory_order_acq_rel,
				memory_order_consume)) {
		// level added
		int pos = get_bucket_index(
				hash,
				hnode->hash.hash_pos,
				hnode->hash.size);

		// move all nodes of chain to new level
		_Atomic(struct lfht_node *) *atomic_bucket =
			&(hnode->hash.array[pos]);

		struct lfht_node *bucket = atomic_load_explicit(
				atomic_bucket,
				memory_order_consume);

		if(bucket->type != LEAF) {
			free(*new_hash);
			return 0;
		}

		adjust_chain_nodes(lfht, thread_id, bucket, *new_hash);

		// store() might interfere with freeze()
		// checking if bucket's head is still our
		// first observed node
		while(!atomic_compare_exchange_strong_explicit(
				atomic_bucket,
				&bucket,
				*new_hash,
				memory_order_acq_rel,
				memory_order_consume)) ;

#if LFHT_DEBUG
		struct lfht_stats* stats = atomic_load_explicit(&(lfht->stats[thread_id]), memory_order_relaxed);
		stats->expansion_counter++;
		int level = (*new_hash)->hash.hash_pos / (*new_hash)->hash.size;
		if(stats->max_depth < level) {
			stats->max_depth = level;
		}
#endif

		if(!give_role(*new_hash)) {
			compress(lfht, thread_id, *new_hash, hash);
		}

		return 1;
	}

	// failed
	free(*new_hash);
	return 0;
}

void adjust_chain_nodes(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *cnode,
		struct lfht_node *hnode)
{
#if LFHT_DEBUG
	assert(cnode);
	assert(hnode);
	assert(cnode->type == LEAF);
	assert(hnode->type == HASH);
#endif
	struct lfht_node *nxt_ptr = get_next(cnode);
	struct lfht_node *nxt = valid_ptr(nxt_ptr);

	if(nxt != hnode) {
		adjust_chain_nodes(lfht, thread_id, nxt, hnode);
	}
	// cnode is the chain's current tail

	if(is_invalid(nxt_ptr) || is_compression_node(cnode)) {
		return;
	}

	// move cnode to new level
	adjust_node(lfht, thread_id, cnode, hnode);
}

void adjust_node(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *cnode,
		struct lfht_node *hnode)
{
#if LFHT_DEBUG
	struct lfht_stats* stats = lfht->stats[thread_id];
	stats->operations++;
#endif

start: ;
#if LFHT_DEBUG
	assert(cnode);
	assert(hnode);
	assert(cnode->type == LEAF);
	assert(hnode->type == HASH);

	stats->max_retry_counter++;
#endif
	unsigned int count = 0;
	size_t hash = cnode->leaf.hash;
	_Atomic(struct lfht_node *) *current_valid =
		get_atomic_bucket(hash, hnode);
	struct lfht_node *expect = valid_ptr(atomic_load_explicit(
				current_valid,
				memory_order_consume));
	struct lfht_node *iter = expect;

	if(is_compression_node(iter)) {
		if(!unfreeze(lfht, thread_id, hnode, hash)) {
			// level already removed
			return;
		}

		// skip compression node
		iter = valid_ptr(get_next(iter));
#if LFHT_DEBUG
		assert(iter);
		assert(iter->type == HASH);
#endif
	}

	// find tail of target bucket on new hash level
	while(iter->type == LEAF) {
		struct lfht_node *nxt_ptr = get_next(iter);

		if(is_invalid(nxt_ptr)) {
			// skip invalid node
			iter = valid_ptr(nxt_ptr);
			continue;
		}

		current_valid = &(iter->leaf.next);
		expect = valid_ptr(nxt_ptr);
		iter = expect;
		count++;
	}

	if(iter != hnode) {
		// important loop!
		// during adjust (after force_cas)
		// there is a window when nodes may skip a level
		while(iter->hash.prev != hnode) {
			iter = iter->hash.prev;
		}

		hnode = iter;
		goto start;
	}

	// expansion required?
	if(count >= MAX_NODES) {
		struct lfht_node *new_hash;
		if(expand(lfht, thread_id, &new_hash, hnode, hash, current_valid)) {
			// adjust node at new level
			hnode = new_hash;
		}

		goto start;
	}

	// point node to newer level
	if(!force_cas(cnode, hnode)) {
		// node invalidated in the meantime
		return;
	}

	// inserting node in chain of the newer level
	if(atomic_compare_exchange_strong_explicit(
				current_valid,
				&expect,
				cnode,
				memory_order_acq_rel,
				memory_order_consume)) {
		if(is_invalid(get_next(cnode))) {
			// node invalidated while it was being adjusted
			make_unreachable(lfht, thread_id, cnode, hnode);
		}
		return;
	}
	// insertion failed
	goto start;
}

// searching functions

void *search_node(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node *hnode,
		size_t hash)
{
#if LFHT_DEBUG
	assert(hnode);
	assert(hnode->type == HASH);
#endif
	struct lfht_node *cnode;
	if(find_node(lfht, thread_id, hash, &hnode, &cnode, NULL, NULL)) {
		return cnode->leaf.value;
	}
	return NULL;
}

// debug functions

#if LFHT_DEBUG

void *lfht_debug_search(
		struct lfht_head *lfht,
		size_t hash,
		int thread_id)
{
	return debug_search_hash(lfht->entry_hash, hash);
}

void *debug_search_hash(
		struct lfht_node *hnode,
		size_t hash)
{
	int pos = get_bucket_index(
			hash,
			hnode->hash.hash_pos,
			hnode->hash.size);
	struct lfht_node *next_node = atomic_load_explicit(
			&(hnode->hash.array[pos]),
			memory_order_seq_cst);
	if(next_node == hnode)
		return NULL;
	else if(next_node->type == HASH)
		return debug_search_hash(next_node, hash);
	else
		return debug_search_chain(next_node, hnode, hash);
}

void *debug_search_chain(
		struct lfht_node *cnode,
		struct lfht_node *hnode,
		size_t hash)
{
	if(cnode->leaf.hash == hash) {
		if(is_invalid(atomic_load_explicit(
						&(cnode->leaf.next),
						memory_order_seq_cst)))
			fprintf(stderr, "Invalid node found: %p\n", cnode->leaf.value);
		else
			return cnode->leaf.value;
	}
	struct lfht_node *next_node = valid_ptr(atomic_load_explicit(
				&(cnode->leaf.next),
				memory_order_seq_cst));
	if(next_node == hnode)
		return NULL;
	else if(next_node->type == LEAF)
		return debug_search_chain(next_node, hnode, hash);
	while(next_node->hash.prev != hnode)
		next_node = next_node->hash.prev;
	return debug_search_hash(next_node, hash);
}

#endif
