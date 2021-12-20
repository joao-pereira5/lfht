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

int expand(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node **new_hash,
		struct lfht_node *hnode,
		size_t hash,
		struct lfht_node **tail_nxt_ptr);

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
		lfht->stats[i] = NULL;
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
	node->leaf.next = next;
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
	node->hash.prev = prev;
	for(int i=0; i < 1<<size; i++) {
		node->hash.array[i] = node;
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

struct lfht_node **get_atomic_bucket(
		size_t hash,
		struct lfht_node *hnode)
{
	int pos = get_bucket_index(
			hash,
			hnode->hash.hash_pos,
			hnode->hash.size);

	return (struct lfht_node **) &(hnode->hash.array[pos]);
}

// release-consume order get next
struct lfht_node *get_next(
		struct lfht_node *node)
{
#if LFHT_DEBUG
		assert(node);
#endif
	struct lfht_node* nxt = node->leaf.next;

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
		if(hnode->hash.array[i] != hnode) {
			// bucket not empty
			return 0;
		}
	}

	return 1;
}

int mark_invalid(struct lfht_node *cnode)
{
	// in a purely sequential implementation, there is
	// no need to mark a node as invalid
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
		struct lfht_node ***last_valid_atomic,
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

	struct lfht_node **head_ptr = get_atomic_bucket(hash, *hnode);
	struct lfht_node *iter = *head_ptr;
	struct lfht_node *head = iter;
	*nodeptr = head;

	if(last_valid_atomic) {
		*last_valid_atomic = head_ptr;
		*count = 0;
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
		assert(iter->type == LEAF);
#endif

		struct lfht_node *nxt_iter = get_next(iter);
		if(iter->leaf.hash == hash) {
			// found node
			*nodeptr = iter;
			return 1;
		}
		*nodeptr = nxt_iter;

		if(last_valid_atomic) {
			*last_valid_atomic = (struct lfht_node **) &(iter->leaf.next);
			(*count)++;
		}
		iter = nxt_iter;
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
	struct lfht_node *nxt = get_next(cnode);

	// advance to the next hash
	iter = nxt;
	while(iter->type != HASH) {
		iter = get_next(iter);
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

	struct lfht_node **target = (struct lfht_node **) &hnode->hash.array[pos];
	struct lfht_node *prev = *target;

	// let's find the last valid node before our target
	while(prev != cnode && prev->type == LEAF) {
		target = (struct lfht_node **) &(prev->leaf.next);
		prev = get_next(prev);
	}

	if(prev != cnode) {
		return;
	}

	// disconnect our target from chain
	*target = nxt;
	//if(((struct lfht_node **)&(hnode->hash.array[pos])) == target) {
	//	// empty bucket, try compress
	//	compress(lfht, thread_id, hnode, cnode->leaf.hash);
	//}
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

	// no need to mark invalid in a purely sequential implementation
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
	struct lfht_node **last_valid_atomic;
	unsigned int count;

	if(find_node(lfht, thread_id, hash, &hnode, &cnode, &last_valid_atomic, &count)) {
		// node already inserted
		return cnode;
	}

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
	*last_valid_atomic = new_node;
	return new_node;
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

	if(target->hash.prev == NULL || !is_empty(target)) {
		// we cannot compress the root hash or a non empty hash
		return;
	}

	struct lfht_node *prev_hash = target->hash.prev;
	struct lfht_node **bucket = get_atomic_bucket(hash, prev_hash);
	*bucket = prev_hash;
	target = target->hash.prev;
	goto start;
}

// expansion functions

int expand(
		struct lfht_head *lfht,
		int thread_id,
		struct lfht_node **new_hash,
		struct lfht_node *hnode,
		size_t hash,
		struct lfht_node **tail_nxt_ptr)
{
#if LFHT_DEBUG
	assert(hnode);
	assert(hnode->type == HASH);
#endif

	*new_hash = create_hash_node(
			HASH_SIZE,
			hnode->hash.hash_pos + hnode->hash.size,
			hnode);

	*tail_nxt_ptr = *new_hash;
	// level added
	int pos = get_bucket_index(
			hash,
			hnode->hash.hash_pos,
			hnode->hash.size);

	// move all nodes of chain to new level
	struct lfht_node **bucket_ptr = (struct lfht_node **) &(hnode->hash.array[pos]);
	struct lfht_node *bucket = *bucket_ptr;

	if(bucket->type != LEAF) {
		free(*new_hash);
		return 0;
	}

	adjust_chain_nodes(lfht, thread_id, bucket, *new_hash);
	*bucket_ptr = *new_hash;

#if LFHT_DEBUG
		struct lfht_stats* stats = lfht->stats[thread_id];
		stats->expansion_counter++;
		int level = (*new_hash)->hash.hash_pos / (*new_hash)->hash.size;
		if(stats->max_depth < level) {
			stats->max_depth = level;
		}
#endif
	return 1;
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
	struct lfht_node *nxt = get_next(cnode);

	if(nxt != hnode) {
		adjust_chain_nodes(lfht, thread_id, nxt, hnode);
	}
	// cnode is the chain's current tail

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
	struct lfht_node **current_valid = get_atomic_bucket(hash, hnode);

	// find tail of target bucket on new hash level
	struct lfht_node *iter = *current_valid;
	while(iter->type == LEAF) {
		current_valid = (struct lfht_node **) &(iter->leaf.next);
		iter = get_next(iter);
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

	cnode->leaf.next = hnode;
	*current_valid = cnode;
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
